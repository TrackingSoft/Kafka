package Kafka::Cluster;

=head1 NAME

Kafka::Cluster - object interface to manage a test kafka cluster.

=head1 VERSION

This documentation refers to C<Kafka::Cluster> version 1.06 .

=cut

use 5.010;
use strict;
use warnings;

our $VERSION = '1.06';

use Exporter qw(
    import
);

our @EXPORT_OK = qw(
    $DEFAULT_TOPIC
    $START_PORT
);

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
    _HASH0
    _NONNEGINT
    _POSINT
    _STRING
);
use Proc::Daemon;
use Try::Tiny;

use Kafka::Internals qw(
    format_message
);
use Kafka::IO;


=head1 SYNOPSIS

    # For examples see:
    # t/*_cluster.t, t/*_cluster_start.t, t/*_connection.t, t/*_cluster_stop.t

=head1 DESCRIPTION

This module is not intended to be used by the end user.

The main features of the C<Kafka::Cluster> module are:

=over 3

=item *

Automatic start and stop of local zookeeper server for tests.

=item *

Start-up, re-initialize, stop cluster of kafka servers.

=item *

A free port is automatically selected for started servers.

=item *

Create, delete data structures used by servers.

=item *

Getting information about running servers.

=item *

Connection to earlier started cluster.

=item *

Perform query to a cluster.

=back

=head2 EXPORT

The following constants are available for export

=cut

=head3 C<$START_PORT>

Initial port number to start search for a free port - 9094.
Zookeeper server uses the first available port.

=cut
const our   $START_PORT                     => 9094;    # Port Number 9094-9099 Unassigned

=head3 C<$DEFAULT_TOPIC>

Default topic name.

=cut
const our   $DEFAULT_TOPIC                  => 'mytopic';
const our   $DEFAULT_REPLICATION_FACTOR     => 3;       # Cluster consists of 3 servers by default

const my    $MAX_START_ATTEMPT              => 10;
const my    $MAX_STOP_ATTEMPT               => 30;
const my    $INI_SECTION                    => 'GENERAL';
const my    $ZOOKEEPER_PROPERTIES_FILE      => 'zookeeper.properties';
const my    $KAFKA_PROPERTIES_FILE          => 'server.properties';
const my    $KAFKA_LOGS_DIR_MASK            => 'kafka-logs-';
const my    $START_KAFKA_CMD                => 'kafka-server-start.sh';
const my    $START_ZOOKEEPER_CMD            => 'zookeeper-server-start.sh';

const our   $KAFKA_HOST                     => 'localhost';
const our   $ZOOKEEPER_HOST                 => 'localhost';

const my    $RESTRICTED_PATH                => '/bin:/usr/bin:/usr/local/bin';

#-- constructor ----------------------------------------------------------------

# protection against re-creation of cluster object
our $_used = 0;

=head2 CONSTRUCTOR

=head3 C<new>

Starts server required for cluster or provides ability to connect to a running cluster.
A zookeeper server is launched during the first call to start.
Creates a C<Kafka::Cluster> object.

An error causes program to halt.

Port is used to identify a particular server in the cluster.
The structures of these servers are created in the C<t/data>.

C<new()> takes arguments in key-value pairs.
The following arguments are recognized:

=over 3

=item C<kafka_dir =E<gt> $kafka_dir>

The root directory of local Kafka installation.

=item C<replication_factor =E<gt> $replication_factor>

Number of kafka servers to be started in cluster.

Optional, default = 3.

=item C<partition =E<gt> $partitions>

The number of partitions per created topic.

Optional, default = 1.

=item C<reuse_existing =E<gt> $reuse_existing>

Connect to previously created cluster instead of creating a new one.

Optional, default = false (creates and runs a new cluster).

=item C<t_dir =E<gt> $t_dir>

Required data structures are prepared to work in provided directory C<t/>.
When connecting to a cluster from another directory,
you must specify path to C<t/> directory.

Optional - not specified (operation carried out in the directory C<t/>).

=back

=cut
sub new {
    my ( $class, %args ) = @_;

    # protection against re-creating of cluster object
    !$_used
        || confess "The object of class '$class' already exists";

    my $kafka_base_dir = $args{kafka_dir} // $ENV{KAFKA_BASE_DIR};
    defined( _STRING( $kafka_base_dir ) )      # must match local kafka insatll dir
        // confess( "The value of 'kafka_dir' should be a string" );

    $kafka_base_dir = Cwd::abs_path( $kafka_base_dir );
    unless( defined $kafka_base_dir && -d $kafka_base_dir ) {
        confess("Kafka directory not found: $kafka_base_dir");
    }

    my $kafka_replication_factor = $args{replication_factor} // $DEFAULT_REPLICATION_FACTOR;
    _POSINT( $kafka_replication_factor )
        // confess( "The value of 'replication_factor' should be a positive integer" );
    _POSINT( $args{partition} //= 1 )
        // confess( "The value of 'partition' should be a positive integer" );

    my $start_dir = $args{t_dir} // $Bin;

    my $self = {
        kafka   => { base_dir => $kafka_base_dir }, # {
                                                #   'base_dir'              => ...,
                                                #   'bin_dir'               => ...,
                                                #   'config_dir'            => ...,
                                                #   'data_dir'              => ...,
                                                #   'zookeeper_clientPort'  => ...,
                                                #   'is_first_run'          => ...,
                                                #   'partition'             => ...,
                                                # }
        cluster => {},                          # {
                                                #   server (port)   => {
                                                #       'node_id'               => ...,
                                                #   },
                                                #   ...,
                                                # }
    };
    bless( $self, $class );

    if( $args{properties} ) {
        confess("properties hash expected") unless _HASH0( $args{properties} );
        $self->{properties} = { %{ $args{properties} } };
    }
    else {
        $self->{properties} = {};
    }

    $self->{kafka}->{host} = $args{host} // $KAFKA_HOST;

    $self->{kafka}->{partition} = $args{partition};

    $self->{kafka}->{bin_dir} = catdir( $kafka_base_dir, 'bin' );
    confess("Kafka bin directory not found: $self->{kafka}->{bin_dir}") unless -d $self->{kafka}->{bin_dir};

    $self->{kafka}->{config_dir} = catdir( $kafka_base_dir, 'config' );
    confess("Kafka config directory not found: $self->{kafka}->{config_dir}") unless -d $self->{kafka}->{config_dir};

    my $kafka_data_dir = $self->{kafka}->{data_dir} = catdir( $start_dir, 'data' );

    my $reuse_existing = $args{reuse_existing} // 0;

    # zookeeper
    my ( $cfg, $zookeeper_client_port, $replication_factor );

    opendir( my $dh, $kafka_data_dir )
        or confess "cannot opendir $kafka_data_dir: $!";
    foreach my $file ( readdir( $dh ) ) {
        next unless $file =~ /^$KAFKA_LOGS_DIR_MASK/;
        ++$replication_factor;
        unless( $cfg ) {
            my $inifile = catfile( $kafka_data_dir, $file, $KAFKA_PROPERTIES_FILE );
            if ( -e $inifile ) {
                $cfg = Config::IniFiles->new(
                    -file       => $inifile,
                    -fallback   => $INI_SECTION,
                ) or $self->_ini_error( $inifile );

                ( undef, $zookeeper_client_port ) = split( /:/, $cfg->val( $INI_SECTION, 'zookeeper.connect' ) );
                if ( check_port( { host => $ZOOKEEPER_HOST, port => $zookeeper_client_port } ) ) {
                    # port at which clients will be able to connect to Zookeeper server
                    $self->{kafka}->{zookeeper_clientPort} = $zookeeper_client_port;
                } else {
                    if ( $reuse_existing ) {
                        # We expect that Zookeeper server must be running
                        confess( "Zookeeper server is not running on port $zookeeper_client_port" );
                    } else {
                        undef $zookeeper_client_port;   # zookeeper must be started
                    }
                }
            }
        }
    }
    closedir $dh;

    confess 'Desired cluster factor ($kafka_replication_factor) does not correspond to the number of existing data directories ($replication_factor)'
        if $replication_factor && $replication_factor != $kafka_replication_factor;

    unless ( $zookeeper_client_port ) {
        confess( "Zookeeper server is not running" )
            if $reuse_existing;
        # port at which clients will connect to Zookeeper server
        $self->{kafka}->{zookeeper_clientPort} = $zookeeper_client_port = $self->_start_zookeeper;
    }

    # Configuring
    my @ini_files;
    find(
        {
            wanted  => sub {
                push( @ini_files, $File::Find::name ) if $_ eq $KAFKA_PROPERTIES_FILE;
            },
            untaint => 1,
        },
        $kafka_data_dir
    );

    if ( $reuse_existing ) {
        foreach my $inifile ( @ini_files ) {
            my $cfg = Config::IniFiles->new(
                -file       => $inifile,
                -fallback   => $INI_SECTION,
            ) or $self->_ini_error( $inifile );

            my $port            = $cfg->val( $INI_SECTION, 'port' );
            my $server          = $self->{cluster}->{ $port } = {};
            $server->{node_id}  = $cfg->val( $INI_SECTION, 'broker.id' );
        }
    } else {
        my $port    = $START_PORT;
        my $node_id = 0;
        for ( 1 .. $kafka_replication_factor ) {
            $port = empty_port( $port - 1 );
            my $server = $self->{cluster}->{ $port } = {};  # server in the cluster identify by its port
            $server->{node_id} = $node_id;

            ++$port;
            ++$node_id;
        }

        $self->start_all;
    }

    ++$_used;
    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are defined for C<Kafka::Cluster> class:

=cut

=head3 C<base_dir>

Returns the root directory of local installation of Kafka.

=cut
sub base_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{base_dir};
}

=head3 C<log_dir( $port )>

Constructs and returns the path to kafka server data directory with specified port.

This function takes argument. The following arguments are supported:

=over 3

=item C<$port>

C<$port> denoting port number of the kafka service.
The C<$port> should be a number.

=back

=cut
sub log_dir {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    return catdir( $self->_data_dir, "$KAFKA_LOGS_DIR_MASK$port" );
}

=head3 C<servers>

Returns a sorted list of ports of all kafka servers in the cluster.

=cut
sub servers {
    my ( $self ) = @_;

    return( sort keys %{ $self->_cluster } );
}

=head3 C<node_id( $port )>

Returns node ID assigned to kafka server in the cluster.
Returns C <undef>, if  server does not have an ID
or no server with the specified port is present in the cluster.

This function takes argument the following argument:

=over 3

=item C<$port>

C<$port> denoting port number of the kafka service.
The C<$port> should be a number.

=back

=cut
sub node_id {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    return $self->_server( $port )->{node_id};
}

=head3 C<zookeeper_port>

Returns port number used by zookeeper server.

=cut
sub zookeeper_port {
    my ( $self ) = @_;

    return $self->{kafka}->{zookeeper_clientPort};
}

#-- public methods -------------------------------------------------------------

=head3 C<init>

Initializes data structures used by kafka servers.
At initialization all servers are stopped and data structures used by them are deleted.
Zookeeper server does not get stopped, its data structures is not removed.

=cut
sub init {
    my ( $self ) = @_;

    $self->stop_all;

    my $cwd = _clear_cwd();
    chdir _clear_tainted( $self->_data_dir );

    $self->_remove_log_tree( $_ ) for map { /(\d+)/ } glob "$KAFKA_LOGS_DIR_MASK*";

    chdir $cwd;

    return 1;
}

sub stop_all {
    my $self = shift;
    my $cwd = _clear_cwd();
    chdir _clear_tainted( $self->_data_dir );

    $self->stop( $_ ) for map { /(\d+)/ } glob 'kafka-*.pid';

    chdir $cwd;
}

=head3 C<stop( $port )>

Stops kafka server specified by port.
If port is omitted stops all servers in the cluster.

This function takes the following argument:

=over 3

=item C<$port>

C<$port> denoting port number of the kafka service.
The C<$port> should be a number.

=back

=cut
sub stop {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    my $pid_file = $self->_get_pid_file_name( $port );
    $self->_stop_server( $pid_file, 'kafka', $self->{kafka}->{host}, $port );

    return;
}

sub start_all {
    my $self = shift;
    $self->stop_all;
    $self->_start_zookeeper unless $self->zookeeper_port;
    $self->start( $_ ) for $self->servers;

    if ( $self->{kafka}->{is_first_run} ) {
        # Create a topic with proper replication factor
        $self->_create_topic;
        delete $self->{kafka}->{is_first_run};
    }
}

=head3 C<start( $port )>

Starts (restarts) kafka server with specified port.
If port is not specified starts (restarts) all servers in the cluster.

This function takes the following argument:

=over 3

=item C<$port>

C<$port> denoting port number of kafka service.
The C<$port> should be a number.

=back

=cut
sub start {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    $self->stop( $port );

    $self->_create_kafka_log_dir( $port );

    my $host = $self->{kafka}->{host};
    my $pid_file = $self->_get_pid_file_name( $port );

    $self->_start_server(
        'kafka',
        $KAFKA_PROPERTIES_FILE,
        $START_KAFKA_CMD,
        $pid_file,
        $self->log_dir( $port ),
        $host,
        $port,
    );

    # Try sending request to make sure that Kafka server is really working now
    my $attempts = $MAX_START_ATTEMPT;
    while ( $attempts-- ) {
        my $error;
        try {
            my $io = Kafka::IO->new(
                host => $host,
                port => $port,
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
            my $tail = $io->receive( unpack( 'N', $$response ) );
            $$response .= $$tail;
            $io->close;
        } catch {
#            confess "Could not send control message: $_\n" unless $attempts;
            ++$error;
        };

#        last unless $error;
        sleep 1;
    }

    return;
}

=head3 C<request( $port, $bin_stream, $without_response )>

Transmits a string of binary query to Kafka server and returns a binary response.
When no response is expected functions returns an empty string if argument C<$without_response> is set to true.

Kafka server is identified by specified port.

This function takes the following argument:

=over 3

=item C<$port>

C<$port> denoting port number of kafka service.
The C<$port> should be a number.

=item C<$bin_stream>

C<$bin_stream> denoting an empty binary string of request to kafka server.

=back

=cut
sub request {
    my ( $self, $port, $bin_stream, $without_response ) = @_;

    defined( _STRING( $bin_stream ) )
        // confess( "The value of '\$bin_stream' should be a string" );

    my $io = Kafka::IO->new(
        host       => $self->{kafka}->{host},
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

=head3 C<close>

Stops all production servers (including zookeeper server).
Deletes all data directories used by servers.

=cut
sub close {
    my ( $self ) = @_;

    $self->init;
    $self->_stop_zookeeper;
    my $dir = catdir( $self->_data_dir, 'zookeeper' );
    if( -d $dir ) {
        say format_message( '[%s] Removing zookeeper data: %s', scalar( localtime ), $dir );
        remove_tree( $dir );
    }

    $_used = 0;

    return;
}

#-- public functions -----------------------------------------------------------

=head3 C<data_cleanup>

This function stops all running servers processes, deletes all data directories and
service files in C<t/data> directory.

Returns number of deleted files.

C<data_cleanup()> takes arguments in key-value pairs.
The following arguments are recognized:

=over 3

=item C<kafka_dir =E<gt> $kafka_dir>

The root directory of Kafka installation.

=item C<t_dir =E<gt> $t_dir>

Required data structures are prepared to work in the directory C<t/>.
When connected to a cluster from another directory, you must specify path to the
C<t/> directory.

Optional - if not specified operation is carried out in the C<t/> directory.

=cut

my %_ignore_names = (
    '.'             => 1,
    '..'            => 1,
    '.gitignore'    => 1,
);

sub data_cleanup {
    my ( %args ) = @_;

    my $t_dir = $args{t_dir};
    !defined( $t_dir ) || defined( _STRING( $t_dir ) )
        // confess( "The value of 't_dir' should be a string" );

    my $start_dir = $args{t_dir} // $Bin;

    my $kafka_data_dir = catdir( $start_dir, 'data' );

    my $removed = 0;

    if ( -d $kafka_data_dir ) {
        my $cwd = getcwd();
        if ( chdir( $kafka_data_dir ) && getcwd() eq $kafka_data_dir ) {
            foreach my $pid_file ( glob '*.pid' ) {
                my $pid;
                if ( !( my $PID = IO::File->new( $pid_file, 'r' ) ) ) {
                    carp( "Cannot read pid file $pid_file: $!" );
                } else {
                    $pid = <$PID>;
                    $PID->close;

                    chomp $pid if $pid;
                }
                next unless $pid && $pid =~ /^\d+$/;

                kill 'KILL', $pid;

                my $cnt = $MAX_STOP_ATTEMPT;
                while ( kill( 0 => $pid ) && $cnt-- ) {
                    sleep 1;
                }
            }

            foreach my $file_or_dir ( glob '*' ) {
                next if exists $_ignore_names{ $file_or_dir };

                if ( -d $file_or_dir ) {
                    $removed += remove_tree( $file_or_dir );
                } else {
                    $removed += unlink $file_or_dir;
                }
            }

            chdir $cwd;
        } else {
            confess "Cannot change directory to $kafka_data_dir";
        }
    }

    return $removed;
}

#-- private functions ----------------------------------------------------------

sub _clear_tainted {

    $_[0] =~ /(.+)/;

    return $1;
}

sub _clear_cwd {
    return _clear_tainted( getcwd() );
}

#-- private attributes ---------------------------------------------------------

# Returns a reference to a hash describing kafka servers in the cluster.
sub _cluster {
    my ( $self ) = @_;

    return $self->{cluster};
}

# Returns hash with the details of kafka server by provided port.
sub _server {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    if ( !( my $server = $self->_cluster->{ $port } ) ) {
        confess "Server '$port' does not exists";
    } else {
        return $server;
    }
}

# Constructs and returns the path to the metrics-directory of Kafka server by specified port.
sub _metrics_dir {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );

    return catdir( $self->_data_dir, "metrics-logs-$port" );
}

# Returns path to Kafka bin directory.
sub _bin_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{bin_dir};
}

# Returns path to the configuration templates.
sub _config_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{config_dir};
}

# Returns path to the data directory.
sub _data_dir {
    my ( $self ) = @_;

    return $self->{kafka}->{data_dir};
}

#-- private methods ------------------------------------------------------------

# Kills process by specified pid (argument $pid), sending a signal ($signal).
# Argument specifies name of the process.
sub _kill_pid {
    my ( $self, $pid, $what, $signal ) = @_;

    unless ( $pid ) {
        carp( 'Invalid pid' );
        return;
    }

    $what //= 'process';
    $signal //= 'TERM';

    if ( !kill( 0, $pid ) ) {
        carp( "$what $pid does not seem to be running" );
    } else {
        say format_message( '[%s] Stopping %s: pid = %s, signal = %s', scalar( localtime ), $what, $pid, $signal );
        kill $signal, $pid;
    }

    return;
}

# Terminates program on configuration file error.
sub _ini_error {
    my ( $self, $inifile ) = @_;

    my $error = "$inifile error:";
    map { $error .= "\n$_" } @Config::IniFiles::errors;
    confess $error;
}

# Returns true, if argument is a valid port number.
sub _verify_port {
    my ( $self, $port ) = @_;

    return( _NONNEGINT( $port ) // confess 'The argument must be a positive integer' );
}

# Constructs and returns path to the pid-file (using specified port).
sub _get_pid_file_name {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );;

    return catfile( $self->_data_dir, "kafka-$port.pid" );
}

# Constructs and returns path to the pid-file used by zookeeper server.
sub _get_zookeeper_pid_file_name {
    my ( $self ) = @_;

    return catfile( $self->_data_dir, "zookeeper.pid" );
}

# reads pid from supplied pid-file.
sub _read_pid_file {
    my ( $self, $pid_file ) = @_;

    if ( !( my $PID = IO::File->new( $pid_file, 'r' ) ) ) {
        carp( "Cannot read pid file $pid_file: $!" );
    } else {
        my $pid = <$PID>;
        $PID->close;

        chomp $pid if $pid;
        return $pid if $pid && $pid =~ /^\d+$/;
        carp( "Invalid PID file: $pid_file" );
    }

    return; # no pid found
}

# Deletes kafka server data directory tree.
sub _remove_log_tree {
    my ( $self, $port ) = @_;

    $self->_verify_port( $port );
    my $dir = $self->log_dir( $port );
    if( -d $dir ) {
        say format_message( '[%s] Removing kafka data: %s', scalar( localtime ), $dir );
        remove_tree( $dir );
    }

    remove_tree( $self->_metrics_dir( $port ) );
    return;
}

# Starts zookeeper server.
sub _start_zookeeper {
    my ( $self ) = @_;

    # the port for connecting to Zookeeper server
    my $zookeeper_client_port = empty_port( $START_PORT - 1 );
    my $log_dir = $self->{kafka}->{zookeeper_dataDir} = catdir( $self->_data_dir, 'zookeeper' );

    unless ( -d $log_dir ) {
        $self->{kafka}->{is_first_run} = 1;
        mkdir $log_dir
            or confess "Cannot create directory '$log_dir': $!";
    }

    my $property_file = catfile( $log_dir, $ZOOKEEPER_PROPERTIES_FILE );
    unless( -e $property_file ) {
        my $src = catfile( $self->_config_dir, $ZOOKEEPER_PROPERTIES_FILE );
        copy( $src, $property_file )
            or confess "Copy failed '$src' -> '$property_file' : $!";
    }

    my $cfg = Config::IniFiles->new(
        -file       => $property_file,
        -fallback   => $INI_SECTION,
    ) or $self->_ini_error( $property_file );

    _set_cfg_val( $cfg, 'clientPort' => $zookeeper_client_port );
    _set_cfg_val( $cfg, 'dataDir'    => $log_dir );
    $cfg->RewriteConfig( $property_file );

    my $pid_file = $self->_get_zookeeper_pid_file_name;

    $self->_start_server(
        'zookeeper',
        $ZOOKEEPER_PROPERTIES_FILE,
        $START_ZOOKEEPER_CMD,
        $pid_file,
        $log_dir,
        $ZOOKEEPER_HOST,
        $zookeeper_client_port,
    );

    $self->{kafka}->{zookeeper_clientPort} = $zookeeper_client_port;

    return $zookeeper_client_port;
}

# Creates data directories for kafka server.
sub _create_kafka_log_dir {
    my ( $self, $port ) = @_;

    my $host = $self->{kafka}->{host};

    my $log_dir = $self->log_dir( $port );
    mkdir $log_dir
        or confess "Cannot create directory '$log_dir': $!";

    my $metrics_dir = $self->_metrics_dir( $port );
    mkdir $metrics_dir
        or confess "Cannot create directory '$metrics_dir': $!";

    my $inifile = catfile( $log_dir, $KAFKA_PROPERTIES_FILE );
    unless ( -e $inifile ) {
        my $src = catfile( $self->_config_dir, $KAFKA_PROPERTIES_FILE );
        copy( $src, $inifile )
            or confess "Copy failed '$src' -> '$inifile' : $!";
    }

    my $cfg = Config::IniFiles->new(
        -file       => $inifile,
        -fallback   => $INI_SECTION,
    ) or $self->_ini_error( $inifile );

    _set_cfg_val( $cfg, 'host.name'             => $host );
    _set_cfg_val( $cfg, 'port'                  => $port );
    _set_cfg_val( $cfg, 'listeners'             => undef ); # may conflict with host.name and port, so unset
    _set_cfg_val( $cfg, 'log.dir'               => $log_dir );
    _set_cfg_val( $cfg, 'log.dirs'              => undef ); # may conflict with log.dir, so unset
    _set_cfg_val( $cfg, 'kafka.csv.metrics.dir' => $metrics_dir );
    _set_cfg_val( $cfg, 'broker.id'             => $self->node_id( $port ) );
    _set_cfg_val( $cfg, 'zookeeper.connect'     => $ZOOKEEPER_HOST . ':' . $self->zookeeper_port );

    foreach my $p ( sort keys %{ $self->{properties} } ) {
        _set_cfg_val( $cfg, $p => $self->{properties}->{ $p } );
    }

    $cfg->RewriteConfig( $inifile );

    return;
}

# Starts server. Possible arguments:
#   $server_name    - Name of the server.
#   $property_file  - Path to the configuration file.
#   $arg            - Additional arguments.
#   $pid_file       - Name of new pid-file.
#   $log_dir        - Path to the data directory.
#   $port           - Port that should be used to run server.
sub _start_server {
    my ( $self, $server_name, $property_file, $cmd, $pid_file, $log_dir, $host, $port ) = @_;

    my $cwd = _clear_cwd();
    chdir _clear_tainted( $log_dir );

    my $proc_daemon = Proc::Daemon->new;
    my $pid = _clear_tainted( $proc_daemon->Init( {
        work_dir     => $log_dir,
        child_STDOUT => '>>'.catfile( $log_dir, 'stdout.log' ),
        child_STDERR => '>>'.catfile( $log_dir, 'stderr.log' ),
    } ) );

    my $server_start_cmd = catfile(
        $self->_bin_dir,
        $cmd,
    );
    $property_file = catfile( $log_dir, $property_file );
    my $cmd_str = "$server_start_cmd $property_file";
    if ( $pid ) {
        say format_message( '[%s] Starting %s: port = %s, pid = %s', scalar( localtime ), $server_name, $port, $pid );

        my $attempts = $MAX_START_ATTEMPT;
        while ( $attempts-- ) {
            sleep 1;    # give it some time to warm up

            unless ( kill 0, $pid ) {
                # not running?
                confess "Not running: $cmd_str";
            }

            # Find real pid using process table, because kafka uses sequence of bash
            # scripts calling each other which do not end with 'exec' and do not trap
            # signals. Proc::Daemon returns pid of the top-level script and killing it
            # won't work (no signals are trapped there) - actual java process keeps
            # running.
            my $script_pid = _clear_tainted( $proc_daemon->get_pid( qr/.*java.+$server_name.+\Q$property_file\E.*/ ) );
            if ( !( my $real_pid = $proc_daemon->Status( $script_pid ) ) ) {
                confess 'Could not find server pid' unless $attempts;
            } else {
                my $fh = IO::File->new( $pid_file, 'w' )
                    or confess "Cannot write $pid_file: $!";
                say $fh $real_pid;
                $fh->close;
                last;
            }
        }

        # Wait for port to be ready
        $attempts = $MAX_START_ATTEMPT;
        while ( $attempts-- ) {
            # The simplified test as readiness for operation will be evaluated on Kafka server availability
            last if check_port( { host => $host, port => $port } );
            sleep 1;
        }
    } else {
        local $ENV{PATH} = $RESTRICTED_PATH;
        exec( $server_start_cmd, $property_file )
            or confess "Cannot execute '$cmd_str': $!";
    }

    chdir $cwd;

    confess "Port $host:$port is available after starting $server_name"
        unless check_port( { host => $host, port => $port } );
}

# Shuts down zookeeper server.
sub _stop_zookeeper {
    my ( $self ) = @_;

    my $port = $self->zookeeper_port;
    my $pid_file = $self->_get_zookeeper_pid_file_name;

    confess 'Trying to stop zookeeper server while it is not running'
        unless -e $pid_file;

    $self->_stop_server( $pid_file, 'zookeeper', $ZOOKEEPER_HOST, $port );
    delete $self->{kafka}->{zookeeper_clientPort};

    return;
}

# Kills server process. Take the following arguments:
#   $pid_file       - Path to pid-file.
#   $server_name    - Server name.
#   $port           - Port used by the server.
sub _stop_server {
    my ( $self, $pid_file, $server_name, $host, $port ) = @_;

    my $attempt = 0;
    while ( -e $pid_file ) {
        my $pid = _clear_tainted( $self->_read_pid_file( $pid_file ) );
        sleep 1 if $attempt;    # server is rather slow to respond to signals
        if ( $pid && kill( 0, $pid ) ) {
            confess "Cannot terminate running $server_name server\n" if ++$attempt > $MAX_STOP_ATTEMPT;
            $self->_kill_pid( $pid, $server_name, 'TERM' );
            next;
        } else {
            warn "Pid file $pid_file overwritten. Unclean shutdown?..\n"
                unless $attempt;
        }
        unlink $pid_file;
    }

    confess "Port $host:$port is not available after stopping $server_name"
        if check_port( { host => $host, port => $port } );
}

# Creates a new topic with specified replication factor.
sub _create_topic {
    my ( $self ) = @_;

    my @servers = $self->servers;
    # choose server port that was launched first
    # log will be recorded in the appropriate directory
    my $port    = $servers[0];
    my $log_dir = $self->log_dir( $port );
    my $partitions = $self->{kafka}->{partition};

    my $cwd = _clear_cwd();
    chdir _clear_tainted( $self->base_dir );

    my @args = (
        catfile( 'bin', 'kafka-topics.sh' ),
        '--create',
        '--zookeeper'          => "$ZOOKEEPER_HOST:".$self->zookeeper_port,
        '--replication-factor' => scalar( @servers ),
        '--partitions'         => $partitions,
        '--topic'              => $DEFAULT_TOPIC,
    );

    say format_message( "[%s] Creating topic '%s': replication factor = %s, partition = %s", scalar( localtime ), $DEFAULT_TOPIC, scalar( @servers ), $partitions );
    my ( $exit_status, $child_error );
    {
        my $out_fh = IO::File->new( catfile( $log_dir, 'kafka-create-topic-stdout.log' ), 'w+' );
        my $err_fh = IO::File->new( catfile( $log_dir, 'kafka-create-topic-stderr.log' ), 'w+' );

        local $ENV{PATH} = $RESTRICTED_PATH;
        capture {
            $exit_status = system( @args );
            $child_error = $?;
        } stdout => $out_fh, stderr => $err_fh;
    }

    chdir $cwd;

    confess "system( @args ) failed: $child_error"
        if $exit_status;
}

sub _set_cfg_val {
    my( $cfg, $name, $value ) = @_;
    if( defined $value ) {
        unless( $cfg->setval( $INI_SECTION, $name, $value ) ) {
            $cfg->newval(  $INI_SECTION, $name, $value );
        }
    } else {
        $cfg->delval( $INI_SECTION, $name );
    }
}

1;

__END__

=head1 DIAGNOSTICS

An error causes script to die automatically.
Error message will be displayed on console.

=head1 SEE ALSO

The basic operation of the Kafka package modules:

L<Kafka|Kafka> - constants and messages used by Kafka package modules.

L<Kafka::Connection|Kafka::Connection> - interface to connect to a Kafka cluster.

L<Kafka::Producer|Kafka::Producer> - interface for producing client.

L<Kafka::Consumer|Kafka::Consumer> - interface for consuming client.

L<Kafka::Message|Kafka::Message> - interface to access Kafka message
properties.

L<Kafka::Int64|Kafka::Int64> - functions to work with 64 bit elements of the
protocol on 32 bit systems.

L<Kafka::Protocol|Kafka::Protocol> - functions to process messages in
Apache Kafka's Protocol.

L<Kafka::IO|Kafka::IO> - low-level interface for communication with Kafka server.

L<Kafka::Exceptions|Kafka::Exceptions> - module designated to handle Kafka exceptions.

L<Kafka::Internals|Kafka::Internals> - internal constants and functions used
by several package modules.

A wealth of detail about Apache Kafka and Kafka Protocol:

Main page at L<http://kafka.apache.org/>

Kafka Protocol at L<https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol>

=head1 SOURCE CODE

Kafka package is hosted on GitHub:
L<https://github.com/TrackingSoft/Kafka>

=head1 AUTHOR

Sergey Gladkov

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Sergiy Zuban

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2017 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
