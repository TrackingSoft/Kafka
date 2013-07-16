package Kafka::Cluster;

# Both Path::Class::Dir and Path::Class::File offer further useful behaviors.

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# PRECONDITIONS ----------------------------------------------------------------

our $VERSION = '0.8001';

=for If you want to set up a Zookeeper cluster on multiple servers

See Step 2-4 at
https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.8+Quick+Start

...

First start the zookeeper server.
> bin/zookeeper-server-start.sh config/zookeeper.properties

Start the Kafka brokers in separate shells:
> bin/kafka-server-start.sh config/server1.properties
...

Create a topic with a replication factor of N (3 for example).
> bin/kafka-create-topic.sh --topic mytopic --replica N --zookeeper localhost:2181

=cut

#-- load the modules -----------------------------------------------------------

use Capture::Tiny qw(
    capture
);
use Carp;
use Config::IniFiles;
use Const::Fast;
use Cwd;
use File::Copy;
use File::Find;
use File::Spec::Functions qw(
    catdir
    catfile
    splitpath
);
use File::Path qw(
    remove_tree
);
use FindBin qw(
    $Bin
);
use IO::File;
use Net::EmptyPort qw(
    check_port
    empty_port
);
use Params::Util qw(
    _NONNEGINT
    _POSINT
    _STRING
);
use Proc::Daemon;

use Kafka::IO;

#-- declarations ---------------------------------------------------------------

const our   $START_PORT                     => 9094;    # Port Number 9094-9099 Unassigned
const my    $MAX_ATTEMPT                    => 5;
const our   $DEFAULT_TOPIC                  => 'mytopic';
const my    $INI_SECTION                    => 'GENERAL';
const my    $RELATIVE_LOG4J_PROPERTY_FILE   => catfile( '..', '..', 'config', 'log4j.properties' );
const my    $DEFAULT_CLUSTER_FACTOR         => 3;       # The cluster contains 3 servers
const my    $ZOOKEEPER_PROPERTIES_FILE      => 'zookeeper.properties';
const my    $KAFKA_PROPERTIES_FILE          => 'server.properties';
const my    $KAFKA_LOGS_DIR_MASK            => 'kafka-logs-';
const my    $SERVER_START_CMD               => 'kafka-run-class.sh';
const my    $START_KAFKA_ARG                => 'kafka.Kafka';
const my    $START_ZOOKEEPER_ARG            => 'org.apache.zookeeper.server.quorum.QuorumPeerMain';

# File mask specific to version 0.8
const my    $KAFKA_0_8_REF_FILE_MASK        => catfile( 'bin', 'kafka-create-topic.sh' );

#-- constructor ----------------------------------------------------------------

# protection against re-create the cluster object
our $_used = 0;

# If the script is run from the directory to the installation of the kafka server,
# then you can work on the structure of the real files on the kafka server
sub new {
    my ( $class, %args ) = @_;

    # protection against re-create the cluster object
    !$_used
        or confess "The object of class '$class' already exists";

    # The argument is needed because multiple versions can be installed simultaneously
    _STRING( $args{kafka_dir} )                 # must match the settings of your system
        // confess( "The value of 'kafka_dir' should be a string" );

    my $kafka_cluster_factor = $args{cluster_factor} //= $DEFAULT_CLUSTER_FACTOR;
    _POSINT( $kafka_cluster_factor )
        // confess( "The value of 'cluster_factor' should be a positive integer" );

    # $self
    my $self = {
        kafka   => {},                          # {
                                                #   'does_not_start'        => ..., # boolean
                                                #   'base_dir'              => ...,
                                                #   'bin_dir'               => ...,
                                                #   'config_dir'            => ...,
                                                #   'data_dir'              => ...,
                                                #   'zookeeper_clientPort'  => ...,
                                                #   'is_first_run'          => ...,
                                                # }
        cluster => {},                          # {
                                                #   server (port)   => {
                                                #       'node_id'               => ...,
                                                #   },
                                                #   ...,
                                                # }
    };
    bless( $self, $class );

    # basic definitions (existence of most service directory is assumed)
    my ( $does_not_start, $kafka_base_dir, $kafka_bin_dir, $kafka_config_dir, $kafka_data_dir );
    $does_not_start         = $self->{kafka}->{does_not_start}  = $args{does_not_start};
    $kafka_base_dir         = $self->{kafka}->{base_dir}        = $args{kafka_dir};
    $kafka_bin_dir          = $self->{kafka}->{bin_dir}         = catdir( $Bin, 'bin' );
    $kafka_config_dir       = $self->{kafka}->{config_dir}      = catdir( $Bin, 'config' );
    my $run_in_base_dir     = $self->is_run_in_base_dir;
    if ( $run_in_base_dir ) {
        $kafka_data_dir     = $self->{kafka}->{data_dir}        = '/tmp';
    }
    else {
        $kafka_data_dir     = $self->{kafka}->{data_dir}        = catdir( $Bin, 'data' );
    }
    mkdir $kafka_data_dir or confess "Cannot create directory '$kafka_data_dir': $!"
        unless -d $kafka_data_dir;

    # verification environment
    confess( "File does not exist (kafka version is not 0.8 ?): $KAFKA_0_8_REF_FILE_MASK" )
        unless $self->_is_kafka_0_8;

    # zookeeper
    my ( $inifile, $cfg, $zookeeper_client_port, $cluster_factor );

    opendir( my $dh, $kafka_data_dir )
        or confess "can't opendir $kafka_data_dir: $!";
    while ( readdir( $dh ) ) {
        next unless /^$KAFKA_LOGS_DIR_MASK/;
        ++$cluster_factor;
        if ( !$cfg && -e ( $inifile = catfile( $kafka_data_dir, $_, $KAFKA_PROPERTIES_FILE ) ) ) {
            if ( !( $cfg = Config::IniFiles->new(
                    -file       => $inifile,
                    -fallback   => $INI_SECTION,
                ) ) ) {
                $self->_ini_error( $inifile );
            }
            else {
                ( undef, $zookeeper_client_port ) = split( /:/, $cfg->val( $INI_SECTION, 'zookeeper.connect' ) );
                if ( !check_port( $zookeeper_client_port ) ) {
                    if ( $does_not_start || $run_in_base_dir ) {
                        # We expect that the Zookeeper server must be running
                        confess( "Zookeeper server is not running on port $zookeeper_client_port" );
                    }
                    else {
                        undef $zookeeper_client_port;   # The zookeeper must be started
                    }
                }
                else {
                    # the port at which the clients will connect the Zookeeper server
                    $self->{kafka}->{zookeeper_clientPort} = $zookeeper_client_port;
                }
            }
        }
    }
    closedir $dh;

    confess 'Desired cluster factor does not correspond to the number of already existing data directory'
        if $cluster_factor && $cluster_factor != $kafka_cluster_factor;

    unless ( $zookeeper_client_port ) {
        # the port at which the clients will connect the Zookeeper server
        $self->{kafka}->{zookeeper_clientPort} = $zookeeper_client_port = $self->_start_zookeeper;
    }

    # Configuring
    my @ini_files;
    find(
        sub {
            push( @ini_files, $File::Find::name ) if $_ eq $KAFKA_PROPERTIES_FILE;
        },
        $kafka_data_dir
    );

    if ( $does_not_start ) {
        foreach my $inifile ( @ini_files ) {
            if ( !( my $cfg = Config::IniFiles->new(
                    -file       => $inifile,
                    -fallback   => $INI_SECTION,
                ) ) ) {
                $self->_ini_error( $inifile );
            }
            else {
                my $port            = $cfg->val( $INI_SECTION, 'port' );
                my $server          = $self->{cluster}->{ $port } = {};
                $server->{node_id}  = $cfg->val( $INI_SECTION, 'broker.id' );
            }
        }
    }
    else {
        my $port    = $START_PORT;
        my $node_id = 0;
        foreach my $server_num ( 1..$kafka_cluster_factor ) {
            $port = empty_port( $port - 1 );
            # server in the cluster identify by its port
            my $server = $self->{cluster}->{ $port } = {};

            my $log_dir         = $self->log_dir( $port );
            my $metrics_dir     = $self->_metrics_dir( $port );
            $server->{node_id}  = $node_id;
        }
        continue {
            ++$port;
            ++$node_id;
        }
    }

    $self->start if !$does_not_start;

    ++$_used;
    return $self;
}

#-- public attributes ----------------------------------------------------------

sub base_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{base_dir};
}

sub log_dir {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    return catdir( $self->_data_dir, "$KAFKA_LOGS_DIR_MASK$port" );
}

sub servers {
    my ( $self ) = @_;

    return( sort keys %{ $self->_cluster } );
}

sub node_id {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    return $self->_server( $port )->{node_id};
}

sub zookeeper_port {
    my ( $self ) = @_;

    return $self->{kafka}->{zookeeper_clientPort};
}

#-- public methods -------------------------------------------------------------

sub init {
    my ( $self, $port ) = @_;

    $self->_verify_run_dir;

    $self->stop;
    # WARNING: Deleting old Kafka server log directories
    say '# [', scalar( localtime ), '] Removing the kafka log tree: ', $self->_data_dir;
    $self->_remove_log_tree;

    return 1;
}

sub stop
{
    my ( $self, $port ) = @_;

    unless ( $port ) {
        my $cwd = getcwd();
        chdir $self->_data_dir;

        $self->stop( $_ ) for map { /(\d+)/ } glob 'kafka-*.pid';

        chdir $cwd;
        return;
    }

    $self->_verify_port( $port );

    my $pid_file = $self->_get_pid_file_name( $port );
    $self->_stop_server( $pid_file, 'kafka', $port );
}

sub start {
    my ( $self, $port ) = @_;

    unless ( $port ) {
        $self->stop;
        $self->_start_zookeeper unless $self->zookeeper_port;
        $self->start( $_ ) for $self->servers;

        if ( $self->{kafka}->{is_first_run} ) {
            # Create a topic with a properly replication factor
            $self->_create_topic;
            delete $self->{kafka}->{is_first_run};
        }

        return;
    }

    $self->_verify_port( $port );

    $self->stop( $port );

    $self->_create_kafka_log_dir( $port );

    my $pid_file = $self->_get_pid_file_name( $port );

    $self->_start_server(
        'kafka',
        $KAFKA_PROPERTIES_FILE,
        $START_KAFKA_ARG,
        $pid_file,
        $self->log_dir( $port ),
        $port
    );

    # Try sending request to make sure that Kafka server is really, really working now
    my $attempts = $MAX_ATTEMPT * 2;
    while( $attempts-- ) {
        eval {

            my $io = Kafka::IO->new(
                host       => 'localhost',
                port       => $port,
                RaiseError => 1,
            );

=for a brief description of the request

***** A MetadataRequest example:
Hex Stream: 000000300003000000000000000C746573742D726571756573740000000100146E6F745F7265706C696361626C655F746F706963

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:30:                    # MessageSize => int32 (a size 0x30 = 48 bytes)
*** Request header
RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
00:03:                          # ApiKey => int16
00:00:                          # ApiVersion => int16
00:00:00:00:                    # CorrelationId => int32
00:0C:                          # ClientId => string (a length 0xC = 12 bytes)
74:65:73:74:2D:72:65:71:75:65:  #   content = 'test-request'
73:74:
**** MetadataRequest
MetadataRequest => [TopicName]
*** Array data for 'topics':
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:14:                          # TopicName => string (a length 0x14 = 20 bytes)
    6E:6F:74:5F:72:65:70:6C:69:63:  #   content = 'not_replicable_topic'
    61:62:6C:65:5F:74:6F:70:69:63
    ] the end of the first element of 'topics' the array

=cut

            $io->send( pack( 'H*', '000000300003000000000000000C746573742D726571756573740000000100146E6F745F7265706C696361626C655F746F706963' ) );
            my $response = $io->receive( 4 );
            my $tail = $io->receive( unpack( "N", $$response ) );
            $$response .= $$tail;
            $io->close;
        };
        if( my $error = $@ ) {
            confess "Could not send control message: $error\n" unless $attempts;
        }
        else {
            last;
        }
        sleep 1;
    }
}

sub request {
    my ( $self, $port, $stream, $without_response ) = @_;

    my $io = Kafka::IO->new(
        host       => 'localhost',
        port       => $port,
        RaiseError => 1,
    );

    $io->send( pack( 'H*', $stream ) );
    my $response = q{};
    unless ( $without_response ) {
        $response = $io->receive( 4 );
        my $tail = $io->receive( unpack( 'l>', $$response ) );
        $$response .= $$tail;
    }
    $io->close;

    return $response;
};

sub is_run_in_base_dir {
    my ( $self ) = @_;

    return $self->base_dir eq $Bin;;
}

sub close {
    my ( $self ) = @_;

    $self->init;
    unless ( $self->is_run_in_base_dir ) {
        $self->_stop_zookeeper;
        # WARNING: Removing things is a much more dangerous proposition than creating things.
        say '# [', scalar( localtime ), '] Removing zookeeper log tree: ', $self->_data_dir;
        remove_tree( catdir( $self->_data_dir, 'zookeeper' ) );
    }
}

#-- private attributes ---------------------------------------------------------

sub _cluster {
    my ( $self ) = @_;

    return $self->{cluster};
}

sub _server {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    if ( !( my $server = $self->_cluster->{ $port } ) ) {
        confess "Server '$port' does not exists";
    }
    else {
        return $server;
    }
}

sub _metrics_dir {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    return catdir( $self->_data_dir, "metrics-logs-$port" );
}

sub _bin_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{bin_dir};
}

sub _config_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{config_dir};
}

sub _data_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{data_dir};
}

#-- private methods ------------------------------------------------------------

sub _kill_pid
{
    my ( $self, $pid, $what, $signal ) = @_;

    unless( $pid ) {
        carp( 'Invalid pid' );
        return;
    }

    $what //= 'process';
    $signal //= 'TERM';

    if( !kill( 0, $pid ) ) {
        carp( "$what $pid does not seem to be running" );
    }
    else {
        say '# [', scalar( localtime ), "] Stopping $what: pid = $pid, signal = $signal";
        kill $signal, $pid;
    }
}

sub _ini_error {
    my ( $self, $inifile ) = @_;

    my $error = "$inifile error:";
    map { $error .= "\n$_" } @Config::IniFiles::errors;
    confess $error;
}

sub _is_kafka_0_8 {
    my ( $self ) = @_;

    my $file_mask = catfile( $self->base_dir, $KAFKA_0_8_REF_FILE_MASK );
    my $cwd = getcwd();
    chdir $self->_config_dir;
    my $ref_file = ( glob $file_mask )[0];
    chdir $cwd;

    return -f $ref_file;
}

# It can be used not only for the registered servers
# (for example, for analysis outdated files)
sub _verify_port {
    my ( $self, $port ) = @_;

    return( _NONNEGINT( $port ) // confess 'The argument must be a positive integer' );
}

sub _verify_run_dir {
    my ( $self ) = @_;

    confess "Operation is not valid because running in the Kafka server directory - perform the operation manually"
        if $self->is_run_in_base_dir;
}

sub _get_pid_file_name {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );;

    return catfile( $self->_data_dir, "kafka-$port.pid" );
}

sub _get_zookeeper_pid_file_name {
    my ( $self ) = @_;

    return catfile( $self->_data_dir, "zookeeper.pid" );
}

sub _read_pid_file
{
    my ( $self, $pid_file ) = @_;

    if( !( my $PID = IO::File->new( $pid_file, 'r' ) ) ) {
        carp( "Cannot read pid file $pid_file: $!" );
    }
    else {
        my $pid = <$PID>;
        $PID->close;

        chomp $pid if $pid;
        return $pid if $pid && $pid =~ /^\d+$/;
        carp( "Invalid PID file: $pid_file" );
    }

    return; # no pid found
}

sub _remove_log_tree {
    my ( $self, $port ) = @_;

    $self->_verify_run_dir;

    unless ( $port ) {
        my $cwd = getcwd();
        chdir $self->_data_dir;

        $self->_remove_log_tree( $_ ) for map { /(\d+)/ } glob "$KAFKA_LOGS_DIR_MASK*";

        chdir $cwd;
        return;
    }

    $self->_verify_port( $port );

    # WARNING: Removing things is a much more dangerous proposition than creating things.
    remove_tree( $self->log_dir( $port ) );
    remove_tree( $self->_metrics_dir( $port ) );
}

sub _start_zookeeper {
    my ( $self ) = @_;

    return if $self->is_run_in_base_dir;

    # the port at which the clients will connect the Zookeeper server
    my $zookeeper_client_port = empty_port( $START_PORT - 1 );
    my $log_dir = $self->{kafka}->{zookeeper_dataDir} = catdir( $self->_data_dir, 'zookeeper' );

    unless( -d $log_dir ) {
        $self->{kafka}->{is_first_run} = 1;
        mkdir $log_dir
            or confess "Cannot create directory '$log_dir': $!";
    }

    my $property_file = catfile( $log_dir, $ZOOKEEPER_PROPERTIES_FILE );
    if ( !-e $property_file ) {
        my $src = catfile( $self->_config_dir, $ZOOKEEPER_PROPERTIES_FILE );
        copy( $src, $property_file )
            or confess "Copy failed '$src' -> '$property_file' : $!";
    }
    my $pid_file = $self->_get_zookeeper_pid_file_name;

    if ( !( my $cfg = Config::IniFiles->new(
            -file       => $property_file,
            -fallback   => $INI_SECTION,
        ) ) ) {
        $self->_ini_error( $property_file );
    }
    else {
        $cfg->setval( $INI_SECTION, 'clientPort'    => $zookeeper_client_port );
        $cfg->setval( $INI_SECTION, 'dataDir'       => $log_dir );
        $cfg->RewriteConfig( $property_file );
    }

    $self->_start_server(
        'zookeeper',
        $ZOOKEEPER_PROPERTIES_FILE,
        $START_ZOOKEEPER_ARG,
        $pid_file,
        $log_dir,
        $zookeeper_client_port
    );

    $self->{kafka}->{zookeeper_clientPort} = $zookeeper_client_port;

    return $zookeeper_client_port;
}

sub _create_kafka_log_dir {
    my ( $self, $port ) = @_;

    my $log_dir = $self->log_dir( $port );

    mkdir $log_dir
        or confess "Cannot create directory '$log_dir': $!";
    my $metrics_dir = $self->_metrics_dir( $port );
    mkdir $metrics_dir
        or confess "Cannot create directory '$metrics_dir': $!";

    my $inifile = catfile( $log_dir, $KAFKA_PROPERTIES_FILE );
    if ( !-e $inifile ) {
        my $src = catfile( $self->_config_dir, $KAFKA_PROPERTIES_FILE );
        copy( $src, $inifile )
            or confess "Copy failed '$src' -> '$inifile' : $!";
    }
    if ( !( my $cfg = Config::IniFiles->new(
            -file       => $inifile,
            -fallback   => $INI_SECTION,
        ) ) ) {
        $self->_ini_error( $inifile );
    }
    else {
        $cfg->setval( $INI_SECTION, 'port'                  => $port );
        $cfg->setval( $INI_SECTION, 'log.dir'               => $log_dir );
        $cfg->setval( $INI_SECTION, 'kafka.csv.metrics.dir' => $metrics_dir );
        $cfg->setval( $INI_SECTION, 'broker.id'             => $self->node_id( $port ) );
        $cfg->setval( $INI_SECTION, 'zookeeper.connect'     => 'localhost:'.$self->zookeeper_port );
        $cfg->RewriteConfig( $inifile );
    }
}

sub _start_server {
    my ( $self, $server_name, $property_file, $arg, $pid_file, $log_dir, $port ) = @_;

    my $cwd = getcwd();
    chdir $log_dir;

    $ENV{KAFKA_BASE_DIR} = $self->base_dir;
    $ENV{KAFKA_OPTS} = "-Dlog4j.configuration=file:$RELATIVE_LOG4J_PROPERTY_FILE";

    my $proc_daemon = Proc::Daemon->new;
    my $pid = $proc_daemon->Init( {
        work_dir     => $log_dir,
        child_STDOUT => '>>'.catfile( $log_dir, 'stdout.log' ),
        child_STDERR => '>>'.catfile( $log_dir, 'stderr.log' ),
    } );

    my $server_start_cmd = catfile(             # we are in the $log_dir, the full directory name may contain spaces
        '..',                                   # $data_dir
        '..',                                   # $Bin
        'bin',                                  # bin dir
        $SERVER_START_CMD
    );
    $property_file = catfile( $log_dir, $property_file );
    my $cmd_str = "$server_start_cmd $arg $property_file";
    if( $pid ) {
        say '# [', scalar( localtime ), "] Starting $server_name: port = ", $port, ", pid = $pid";

        my $attempts = $MAX_ATTEMPT * 2;
        while( $attempts-- ) {
            sleep 1;    # give it some time to warm up

            unless( kill 0, $pid ) {
                # not running?
                confess "Not running: $cmd_str";
            }

            # Find real pid using process table, because kafka uses sequence of bash
            # scripts calling each other which do not end with 'exec' and do not trap
            # signals. Proc::Daemon returns pid of the top-level script and killing it
            # won't work (no signals are trapped there) - actual java process keeps
            # running.
            if( !( my $real_pid = $proc_daemon->Status( qr/.*java.+$server_name.+\Q$property_file\E.*/ ) ) ) {
                confess 'Could not find server pid' unless $attempts;
            }
            else {
                my $fh = IO::File->new( $pid_file, 'w' )
                    or confess "Cannot write $pid_file: $!";
                say $fh $real_pid;
                $fh->close;
                last;
            }
        }

        # Expect to port is ready
        $attempts = $MAX_ATTEMPT * 2;
        while( $attempts-- ) {
            # The simplified test as readiness for operation will be evaluated on the Kafka server availability
            last if check_port( $port );
            sleep 1;
        }
    }
    else {
        exec( $server_start_cmd, $arg, $property_file )
            or confess "Cannot execute `$cmd_str`: $!";
    }

    delete $ENV{KAFKA_OPTS};
    delete $ENV{KAFKA_BASE_DIR};

    chdir $cwd;

    confess "Port $port is available after $server_name starting"
        unless check_port( $port );
}

sub _stop_zookeeper {
    my ( $self ) = @_;

    $self->_verify_run_dir;

    my $port = $self->zookeeper_port;
    my $pid_file = $self->_get_zookeeper_pid_file_name;

    confess 'Trying to stop the zookeeper server is not running'
        unless -e $pid_file;

    $self->_stop_server( $pid_file, 'zookeeper', $port );
    delete $self->{kafka}->{zookeeper_clientPort};
}

sub _stop_server {
    my ( $self, $pid_file, $server_name, $port ) = @_;

    my $attempt = 0;
    while( -e $pid_file ) {
        my $pid = $self->_read_pid_file( $pid_file );
        sleep 1 if $attempt;    # server is rather slow to respond to signals
        if ( $pid && kill( 0, $pid ) ) {
            confess "Cannot terminate running $server_name server\n" if ++$attempt > $MAX_ATTEMPT;
            $self->_kill_pid( $pid, $server_name, 'TERM' );
            next;
        }
        else {
            warn "Pid file $pid_file overwritten. Unclean shutdown?..\n"
                unless $attempt;
        }
        unlink $pid_file;
    }

    confess "Port $port is not available after $server_name stopping"
        if check_port( $port );
}

sub _create_topic {
    my ( $self ) = @_;

    $self->_verify_run_dir;

    my @servers = $self->servers;
    # choose the server port that was launched first
    # the log will be recorded in the appropriate directory
    my $port    = $servers[0];
    my $log_dir = $self->log_dir( $port );

    my @args = (
        'bin/kafka-create-topic.sh',
        "--topic $DEFAULT_TOPIC",
        '--replica '.scalar( @servers ),
        '--zookeeper localhost:'.$self->zookeeper_port,
    );

    my $cwd = getcwd();
    chdir $self->base_dir;

    say '# [', scalar( localtime ), "] Creating topic '$DEFAULT_TOPIC': replication factor = ", scalar( @servers );
    my ( $exit_status, $child_error );
    {
        my $out_fh = IO::File->new( catfile( $log_dir, 'kafka-create-topic-stdout.log' ), 'w+' );
        my $err_fh = IO::File->new( catfile( $log_dir, 'kafka-create-topic-stderr.log' ), 'w+' );

        capture {
            $exit_status = system( @args );
            $child_error = $?;
        } stdout => $out_fh, stderr => $err_fh;
    }

    chdir $cwd;

    confess "system( @args ) failed: $child_error"
        if $exit_status;
}

#-- Closes and cleans up -------------------------------------------------------

1;

__END__
