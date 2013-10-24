package Kafka::Cluster;

=head1 NAME

Kafka::Cluster - object interface to manage a test kafka cluster.

=head1 VERSION

This documentation refers to C<Kafka::Cluster> version 0.800_6 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.800_6';

use Exporter qw(
    import
);

our @EXPORT_OK = qw(
    $DEFAULT_TOPIC
    $START_PORT
);

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
use Try::Tiny;

use Kafka::IO;

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    # For examples see:
    # t/??_cluster.t, t/??_cluster_start.t, t/??_connection.t, t/??_cluster_stop.t

=head1 DESCRIPTION

This module is not a user module.

The main features of the C<Kafka::Cluster> module are:

=over 3

=item *

Automatic start and stop the zookeeper server.

=item *

Start-up, re-initialize, stop the kafka servers cluster.

=item *

The servers automatically uses free ports.

=item *

Create, delete the data structures used by servers.

=item *

Getting information about the used servers.

=item *

Connection to an earlier running cluster.

=item *

The ability to perform a query to necessary server cluster.

=back

=head2 EXPORT

The following constants are available for export

=cut

=head3 C<$START_PORT>

The port used to start the search of free ports - 9094.
Zookeeper server uses the first available port.

=cut
const our   $START_PORT                     => 9094;    # Port Number 9094-9099 Unassigned

=head3 C<$DEFAULT_TOPIC>

Used topic name.

=cut
const our   $DEFAULT_TOPIC                  => 'mytopic';

const my    $MAX_ATTEMPT                    => 5;
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

my $start_dir;

#-- constructor ----------------------------------------------------------------

# protection against re-create the cluster object
our $_used = 0;

=head2 CONSTRUCTOR

=head3 C<new>

Starts the server required for cluster or provides the ability to connect to a running cluster.
In the first call in start-up, also launching a zookeeper server.
Creates a C<Kafka::Cluster> object.

An error will cause the program to halt.

To identify a particular server in the cluster port is used.
The structures of these servers are created in the C<t/data>.

C<new()> takes arguments in key-value pairs.
The following arguments are currently recognized:

=over 3

=item C<kafka_dir =E<gt> $kafka_dir>

The root directory of the Kafka installation.

=item C<cluster_factor =E<gt> $cluster_factor>

Number kafka servers belonging to the generated cluster.

Optional, default = 3.

=item C<does_not_start =E<gt> $does_not_start>

Sign of the need to connect to the already created cluster.

Optional, default = false (create and run a new cluster).

=item C<t_dir =E<gt> $t_dir>

The required data structures are prepared to work in the directory C<t/>.
When connected to a cluster from another directory,
you must specify the path to the directory C<t/>.

Optional - not specified (operation carried out in the directory C<t/>).

=back

=cut
sub new {
    my ( $class, %args ) = @_;

    # protection against re-create the cluster object
    !$_used
        || confess "The object of class '$class' already exists";

    # The argument is needed because multiple versions can be installed simultaneously
    defined( _STRING( $args{kafka_dir} ) )      # must match the settings of your system
        // confess( "The value of 'kafka_dir' should be a string" );

    my $kafka_cluster_factor = $args{cluster_factor} //= $DEFAULT_CLUSTER_FACTOR;
    _POSINT( $kafka_cluster_factor )
        // confess( "The value of 'cluster_factor' should be a positive integer" );

    $start_dir = $args{t_dir} // $Bin;

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
    $kafka_bin_dir          = $self->{kafka}->{bin_dir}         = catdir( $start_dir, 'bin' );
    $kafka_config_dir       = $self->{kafka}->{config_dir}      = catdir( $start_dir, 'config' );
    my $run_in_base_dir     = $self->is_run_in_base_dir;
    if ( $run_in_base_dir ) {
        $kafka_data_dir     = $self->{kafka}->{data_dir}        = '/tmp';
    }
    else {
        $kafka_data_dir     = $self->{kafka}->{data_dir}        = catdir( $start_dir, 'data' );
    }

    # verification environment
    confess( "File does not exist (kafka version is not 0.8 ?): $KAFKA_0_8_REF_FILE_MASK" )
        unless $self->_is_kafka_0_8;

    # zookeeper
    my ( $inifile, $cfg, $zookeeper_client_port, $cluster_factor );

    opendir( my $dh, $kafka_data_dir )
        or confess "can't opendir $kafka_data_dir: $!";
    foreach my $file ( readdir( $dh ) ) {
        next if $file !~ /^$KAFKA_LOGS_DIR_MASK/;
        ++$cluster_factor;
        if ( !$cfg && -e ( $inifile = catfile( $kafka_data_dir, $file, $KAFKA_PROPERTIES_FILE ) ) ) {
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
        for ( 1..$kafka_cluster_factor ) {
            $port = empty_port( $port - 1 );
            my $server = $self->{cluster}->{ $port } = {};  # server in the cluster identify by its port
            $server->{node_id} = $node_id;

            ++$port;
            ++$node_id;
        }
    }

    $self->start if !$does_not_start;

    ++$_used;
    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are defined for the C<Kafka::Cluster> class:

=cut

=head3 C<base_dir>

Returns the root directory of the installation of Kafka.

=cut
sub base_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{base_dir};
}

=head3 C<log_dir( $port )>

Constructs and returns the path to the data directory kafka server with the specified port.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$port>

C<$port> denoting the port number of the kafka service.
The C<$port> should be a number.

=back

=cut
sub log_dir {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    return catdir( $self->_data_dir, "$KAFKA_LOGS_DIR_MASK$port" );
}

=head3 C<servers>

Returns a sorted list of ports kafka servers in the cluster.

=cut
sub servers {
    my ( $self ) = @_;

    return( sort keys %{ $self->_cluster } );
}

=head3 C<node_id( $port )>

Returns the node ID assigned by kafka server in the cluster.
Returns C <undef>, if the server does not have an ID
or a server with the specified port is not in the cluster.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$port>

C<$port> denoting the port number of the kafka service.
The C<$port> should be a number.

=back

=cut
sub node_id {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    return $self->_server( $port )->{node_id};
}

=head3 C<zookeeper_port>

Returns the port number used by zookeeper server.

=cut
sub zookeeper_port {
    my ( $self ) = @_;

    return $self->{kafka}->{zookeeper_clientPort};
}

#-- public methods -------------------------------------------------------------

=head3 C<init>

Initializes the data structures used by kafka servers.
At initialization, all the servers are stopped and deleted the data structure used by them.
Zookeeper server does not stop, his data structure does not remove.

=cut
sub init {
    my ( $self ) = @_;

    $self->_verify_run_dir;

    $self->stop;
    # WARNING: Deleting old Kafka server log directories
    say '# [', scalar( localtime ), '] Removing the kafka log tree: ', $self->_data_dir;
    $self->_remove_log_tree;

    return 1;
}

=head3 C<stop( $port )>

Stops kafka server with the specified port.
Stop all servers in the cluster, if the port is not specified.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$port>

C<$port> denoting the port number of the kafka service.
The C<$port> should be a number.

=back

=cut
sub stop {
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

=head3 C<start( $port )>

Starts (restarts) kafka server with the specified port.
Starts (restarts) all servers in the cluster, if the port is not specified.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$port>

C<$port> denoting the port number of the kafka service.
The C<$port> should be a number.

=back

=cut
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
        my $error;
        try {
            my $io = Kafka::IO->new(
                host       => 'localhost',
                port       => $port,
            );

# ***** A MetadataRequest example:
# Hex Stream: 000000300003000000000000000C746573742D726571756573740000000100146E6F745F7265706C696361626C655F746F706963
#
# **** Common Request and Response
# RequestOrResponse => Size (RequestMessage | ResponseMessage)
# 00:00:00:30:                    # MessageSize => int32 (a size 0x30 = 48 bytes)
# *** Request header
# RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
# 00:03:                          # ApiKey => int16
# 00:00:                          # ApiVersion => int16
# 00:00:00:00:                    # CorrelationId => int32
# 00:0C:                          # ClientId => string (a length 0xC = 12 bytes)
# 74:65:73:74:2D:72:65:71:75:65:  #   content = 'test-request'
# 73:74:
# **** MetadataRequest
# MetadataRequest => [TopicName]
# *** Array data for 'topics':
# 00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
#     [ the first element of the 'topics' array
#     00:14:                          # TopicName => string (a length 0x14 = 20 bytes)
#     6E:6F:74:5F:72:65:70:6C:69:63:  #   content = 'not_replicable_topic'
#     61:62:6C:65:5F:74:6F:70:69:63
#     ] the end of the first element of 'topics' the array
            $io->send( pack( 'H*', '000000300003000000000000000C746573742D726571756573740000000100146E6F745F7265706C696361626C655F746F706963' ) );

            my $response = $io->receive( 4 );
            my $tail = $io->receive( unpack( "N", $$response ) );
            $$response .= $$tail;
            $io->close;
        } catch {
#            confess "Could not send control message: $_\n" unless $attempts;
            ++$error;
        };

#        last unless $error;
        sleep 1;
    }
}

=head3 C<request( $port, $bin_stream, $without_response )>

Kafka server transmits a string of binary query retrieves and returns a binary response.
No response is expected, and returns an empty string if the argument C<$without_response> is true.

Kafka server is identified by the specified port.

This function take arguments. The following arguments is currently recognized:

=over 3

=item C<$port>

C<$port> denoting the port number of the kafka service.
The C<$port> should be a number.

=item C<$bin_stream>

C<$bin_stream> denoting an empty binary string of the request to kafka server.

=back

=cut
sub request {
    my ( $self, $port, $bin_stream, $without_response ) = @_;

    defined( _STRING( $bin_stream ) )
        // confess( "The value of '\$bin_stream' should be a string" );

    my $io = Kafka::IO->new(
        host       => 'localhost',
        port       => $port,
    );

    $io->send( pack( q{H*}, $bin_stream ) );
    my $response = q{};
    unless ( $without_response ) {
        $response = $io->receive( 4 );
        my $tail = $io->receive( unpack( q{l>}, $$response ) );
        $$response .= $$tail;
    }
    $io->close;

    return $response;
};

=head3 C<is_run_in_base_dir>

Returns true, if the work is performed in the root directory of the installation Kafka.

=cut
sub is_run_in_base_dir {
    my ( $self ) = @_;

    return $self->base_dir eq $start_dir;
}

=head3 C<close>

Stop all production servers (including the zookeeper server).
Deletes all data directories used by the servers.
After execution, the C<t/data> catalog does not contain service files.

=cut
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

# Returns a reference to a hash with descriptions kafka servers forming the cluster.
sub _cluster {
    my ( $self ) = @_;

    return $self->{cluster};
}

# Returns a hash with the description kafka server with the specified port.
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

# Constructs and returns the path to the metrics-directory of Kafka server with the specified port.
sub _metrics_dir {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    return catdir( $self->_data_dir, "metrics-logs-$port" );
}

# Returns the path to the directory with the external programs.
sub _bin_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{bin_dir};
}

# Returns the path to the configuration file templates.
sub _config_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{config_dir};
}

# Returns the path to the directory with the data used by the server.
sub _data_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{data_dir};
}

#-- private methods ------------------------------------------------------------

# Kills the process with the specified pid (argument $pid), sending a signal ($signal).
# Argument specifies the name of the process.
sub _kill_pid {
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

# Completes the program on configuration file error.
sub _ini_error {
    my ( $self, $inifile ) = @_;

    my $error = "$inifile error:";
    map { $error .= "\n$_" } @Config::IniFiles::errors;
    confess $error;
}

# Returns true, if set in the constructor Kafka installation directory does not contain installation version 0.8.
sub _is_kafka_0_8 {
    my ( $self ) = @_;

    my $file_mask = catfile( $self->base_dir, $KAFKA_0_8_REF_FILE_MASK );
    my $cwd = getcwd();
    chdir $self->_config_dir;
    my $ref_file = ( glob $file_mask )[0];
    chdir $cwd;

    return -f $ref_file;
}

# Returns true, if the argument contains a valid port number.
sub _verify_port {
    my ( $self, $port ) = @_;

    return( _NONNEGINT( $port ) // confess 'The argument must be a positive integer' );
}

# Terminates the program if you are working in an invalid directory.
sub _verify_run_dir {
    my ( $self ) = @_;

    confess 'Operation is not valid because running in the Kafka server directory - perform the operation manually'
        if $self->is_run_in_base_dir;
}

# Constructs and returns the path to the pid-file (the server with the specified port).
sub _get_pid_file_name {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );;

    return catfile( $self->_data_dir, "kafka-$port.pid" );
}

# Constructs and returns the path to the pid-file used by zookeeper server.
sub _get_zookeeper_pid_file_name {
    my ( $self ) = @_;

    return catfile( $self->_data_dir, "zookeeper.pid" );
}

# pid is obtained from a specified pid-file.
sub _read_pid_file {
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

# Deletes kafka server data directory trees.
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

# Starts zookeeper server.
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

# Creates data directories for the kafka server.
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

# Starts the server. The following arguments:
#   $server_name    - The name of the server.
#   $property_file  - The path to the configuration file.
#   $arg            - Additional arguments.
#   $pid_file       - Name of new pid-file.
#   $log_dir        - The path to the data directory.
#   $port           - The port that should be used to run the server.
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

# Shuts down the zookeeper server.
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

# Kills the server process. The following arguments:
#   $pid_file       - Path to pid-file.
#   $server_name    - The server name.
#   $port           - The port used by the server.
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

# Create a new topic with a replication factor.
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

=head1 DIAGNOSTICS

Error will causes to die automatically.
The error message will be displayed on the console.

=head1 SEE ALSO

The basic operation of the Kafka package modules:

L<Kafka|Kafka> - constants and messages used by the Kafka package modules.

L<Kafka::Connection|Kafka::Connection> - interface to connect to a Kafka cluster.

L<Kafka::Producer|Kafka::Producer> - interface for producing client.

L<Kafka::Consumer|Kafka::Consumer> - interface for consuming client.

L<Kafka::Message|Kafka::Message> - interface to access Kafka message
properties.

L<Kafka::Int64|Kafka::Int64> - functions to work with 64 bit elements of the
protocol on 32 bit systems.

L<Kafka::Protocol|Kafka::Protocol> - functions to process messages in the
Apache Kafka's Protocol.

L<Kafka::IO|Kafka::IO> - low-level interface for communication with Kafka server.

L<Kafka::Exceptions|Kafka::Exceptions> - module designated to handle Kafka exceptions.

L<Kafka::Internals|Kafka::Internals> - internal constants and functions used
by several package modules.

A wealth of detail about the Apache Kafka and the Kafka Protocol:

Main page at L<http://kafka.apache.org/>

Kafka Protocol at L<https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol>

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2013 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
