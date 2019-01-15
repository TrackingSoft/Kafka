package Kafka::Connection;

=head1 NAME

Kafka::Connection - Object interface to connect to a kafka cluster.

=head1 VERSION

This documentation refers to C<Kafka::Connection> version 1.07 .

=cut

use 5.010;
use strict;
use warnings;

our $DEBUG = 0;

our $VERSION = '1.07';

use Exporter qw(
    import
);
our @EXPORT = qw(
    %RETRY_ON_ERRORS
);

use Data::Validate::Domain qw(
    is_hostname
);
use Data::Validate::IP qw(
    is_ipv4
    is_ipv6
);
use Const::Fast;
use List::Util qw(
    shuffle
);
use Params::Util qw(
    _ARRAY
    _ARRAY0
    _HASH
    _NONNEGINT
    _NUMBER
    _POSINT
    _STRING
);
use Scalar::Util qw(
    blessed
);
use Scalar::Util::Numeric qw(
    isint
);
use Storable qw(
    dclone
);
use Time::HiRes ();
use Try::Tiny;

use Kafka qw(
    %ERROR

    $ERROR_NO_ERROR
    $ERROR_UNKNOWN
    $ERROR_OFFSET_OUT_OF_RANGE
    $ERROR_INVALID_MESSAGE
    $ERROR_UNKNOWN_TOPIC_OR_PARTITION
    $ERROR_INVALID_FETCH_SIZE
    $ERROR_LEADER_NOT_AVAILABLE
    $ERROR_NOT_LEADER_FOR_PARTITION
    $ERROR_REQUEST_TIMED_OUT
    $ERROR_BROKER_NOT_AVAILABLE
    $ERROR_REPLICA_NOT_AVAILABLE
    $ERROR_MESSAGE_TOO_LARGE
    $ERROR_STALE_CONTROLLER_EPOCH
    $ERROR_NETWORK_EXCEPTION
    $ERROR_GROUP_LOAD_IN_PROGRESS
    $ERROR_OFFSET_METADATA_TOO_LARGE
    $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE
    $ERROR_NOT_COORDINATOR_FOR_GROUP
    $ERROR_NOT_ENOUGH_REPLICAS
    $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND
    $ERROR_REBALANCE_IN_PROGRESS
    $ERROR_UNSUPPORTED_VERSION

    $ERROR_CANNOT_BIND
    $ERROR_CANNOT_GET_METADATA
    $ERROR_CANNOT_RECV
    $ERROR_CANNOT_SEND
    $ERROR_LEADER_NOT_FOUND
    $ERROR_GROUP_COORDINATOR_NOT_FOUND
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_MISMATCH_CORRELATIONID
    $ERROR_NO_KNOWN_BROKERS
    $ERROR_RESPONSEMESSAGE_NOT_RECEIVED
    $ERROR_SEND_NO_ACK
    $ERROR_UNKNOWN_APIKEY
    $ERROR_INCOMPATIBLE_HOST_IP_VERSION
    $ERROR_NO_CONNECTION

    $IP_V4
    $IP_V6
    $KAFKA_SERVER_PORT
    $NOT_SEND_ANY_RESPONSE
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
    $SEND_MAX_ATTEMPTS
);

use Kafka::Exceptions;
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_METADATA
    $APIKEY_OFFSET
    $APIKEY_PRODUCE
    $APIKEY_FINDCOORDINATOR
    $APIKEY_APIVERSIONS
    $APIKEY_OFFSETCOMMIT
    $APIKEY_OFFSETFETCH
    $APIKEY_SASLHANDSHAKE
    $MAX_CORRELATIONID
    $MAX_INT32
    debug_level
    _get_CorrelationId
    format_message
);
use Kafka::IO;
use Kafka::IO::Async;
use Kafka::Protocol qw(
    $BAD_OFFSET
    $IMPLEMENTED_APIVERSIONS
    decode_fetch_response
    decode_metadata_response
    decode_offset_response
    decode_produce_response
    decode_api_versions_response
    decode_find_coordinator_response
    decode_offsetcommit_response
    decode_offsetfetch_response
    decode_saslhandshake_response
    encode_fetch_request
    encode_metadata_request
    encode_offset_request
    encode_produce_request
    encode_api_versions_request
    encode_find_coordinator_request
    encode_offsetcommit_request
    encode_offsetfetch_request
    encode_saslhandshake_request
);

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    # A simple example of Kafka::Connection usage:
    use Kafka::Connection;

    # connect to local cluster with the defaults
    my $connection;
    try {
        $connection = Kafka::Connection->new( host => 'localhost' );
    } catch {
        my $error = $_;
        if ( blessed( $error ) && $error->isa( 'Kafka::Exception' ) ) {
            warn $error->message, "\n", $error->trace->as_string, "\n";
            exit;
        } else {
            die $error;
        }
    };

    # Closes the connection and cleans up
    $connection->close;
    undef $connection;

=head1 DESCRIPTION

The main features of the C<Kafka::Connection> class are:

=over 3

=item *

Provides API for communication with Kafka 0.9+ cluster.

=item *

Performs requests encoding and responses decoding, provides automatic
selection or promotion of a leader server from Kafka cluster.

=item *

Provides information about Kafka cluster.

=back

=cut

my %protocol = (
    "$APIKEY_PRODUCE"   => {
        decode                  => \&decode_produce_response,
        encode                  => \&encode_produce_request,
    },
    "$APIKEY_FETCH"     => {
        decode                  => \&decode_fetch_response,
        encode                  => \&encode_fetch_request,
    },
    "$APIKEY_OFFSET"    => {
        decode                  => \&decode_offset_response,
        encode                  => \&encode_offset_request,
    },
    "$APIKEY_METADATA"  => {
        decode                  => \&decode_metadata_response,
        encode                  => \&encode_metadata_request,
    },
    "$APIKEY_APIVERSIONS"  => {
        decode                  => \&decode_api_versions_response,
        encode                  => \&encode_api_versions_request,
    },
    "$APIKEY_FINDCOORDINATOR"  => {
        decode                  => \&decode_find_coordinator_response,
        encode                  => \&encode_find_coordinator_request,
    },
    "$APIKEY_OFFSETCOMMIT"  => {
        decode                  => \&decode_offsetcommit_response,
        encode                  => \&encode_offsetcommit_request,
    },
    "$APIKEY_OFFSETFETCH"  => {
        decode                  => \&decode_offsetfetch_response,
        encode                  => \&encode_offsetfetch_request,
    },
    "$APIKEY_SASLHANDSHAKE"  => {
        decode                  => \&decode_saslhandshake_response,
        encode                  => \&encode_saslhandshake_request,
    },
);

=head2 EXPORT

The following constants are available for export

=cut

=head3 C<%RETRY_ON_ERRORS>

These are non-fatal errors, which when happen causes refreshing of meta-data from Kafka followed by
another attempt to fetch data.

=cut
# When any of the following error happens, a possible change in meta-data on server is expected.
const our %RETRY_ON_ERRORS => (
#    $ERROR_NO_ERROR                         => 1,   # 0 - No error
    $ERROR_UNKNOWN                          => 1,   # -1 - An unexpected server error
#    $ERROR_OFFSET_OUT_OF_RANGE              => 1,   # 1 - The requested offset is not within the range of offsets maintained by the server
    $ERROR_INVALID_MESSAGE                  => 1,   # 2 - Retriable - This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt
    $ERROR_UNKNOWN_TOPIC_OR_PARTITION       => 1,   # 3 - Retriable - This server does not host this topic-partition
#    $ERROR_INVALID_FETCH_SIZE               => 1,   # 4 - The requested fetch size is invalid
    $ERROR_LEADER_NOT_AVAILABLE             => 1,   # 5 - Retriable - Unable to write due to ongoing Kafka leader selection
    $ERROR_NOT_LEADER_FOR_PARTITION         => 1,   # 6 - Retriable - Server is not a leader for partition
    $ERROR_REQUEST_TIMED_OUT                => 1,   # 7 - Retriable - Request time-out
    $ERROR_BROKER_NOT_AVAILABLE             => 1,   # 8 - Broker is not available
    $ERROR_REPLICA_NOT_AVAILABLE            => 1,   # 9 - Replica not available
#    $ERROR_MESSAGE_TOO_LARGE                => 1,   # 10 - The request included a message larger than the max message size the server will accept
    $ERROR_STALE_CONTROLLER_EPOCH           => 1,   # 11 - The controller moved to another broker
#    $ERROR_OFFSET_METADATA_TOO_LARGE        => 1,   # 12 - The metadata field of the offset request was too large
    $ERROR_NETWORK_EXCEPTION                => 1,   # 13 Retriable - The server disconnected before a response was received
    $ERROR_GROUP_LOAD_IN_PROGRESS           => 1,   # 14 - Retriable - The coordinator is loading and hence can't process requests for this group
    $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE  => 1,   # 15 - Retriable - The group coordinator is not available
    $ERROR_NOT_COORDINATOR_FOR_GROUP        => 1,   # 16 - Retriable - This is not the correct coordinator for this group

#    $ERROR_INVALID_TOPIC_EXCEPTION          => 1,   # 17 - The request attempted to perform an operation on an invalid topic
#    $ERROR_RECORD_LIST_TOO_LARGE            => 1,   # 18 - The request included message batch larger than the configured segment size on the server
    $ERROR_NOT_ENOUGH_REPLICAS              => 1,   # 19 - Retriable - Messages are rejected since there are fewer in-sync replicas than required
    $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND => 1,   # 20 - Retriable - Messages are written to the log, but to fewer in-sync replicas than required
#    $ERROR_INVALID_REQUIRED_ACKS            => 1,   # 21 - Produce request specified an invalid value for required acks
#    $ERROR_ILLEGAL_GENERATION               => 1,   # 22 - Specified group generation id is not valid
#    $ERROR_INCONSISTENT_GROUP_PROTOCOL      => 1,   # 23 - The group member's supported protocols are incompatible with those of existing members
#    $ERROR_INVALID_GROUP_ID                 => 1,   # 24 - The configured groupId is invalid
#    $ERROR_UNKNOWN_MEMBER_ID                => 1,   # 25 - The coordinator is not aware of this member
#    $ERROR_INVALID_SESSION_TIMEOUT          => 1,   # 26 - The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)
    $ERROR_REBALANCE_IN_PROGRESS            => 1,   # 27 - The group is rebalancing, so a rejoin is needed
#    $ERROR_INVALID_COMMIT_OFFSET_SIZE       => 1,   # 28 - The committing offset data size is not valid
#    $ERROR_TOPIC_AUTHORIZATION_FAILED       => 1,   # 29 - Not authorized to access topics: Topic authorization failed
#    $ERROR_GROUP_AUTHORIZATION_FAILED       => 1,   # 30 - Not authorized to access group: Group authorization failed
#    $ERROR_CLUSTER_AUTHORIZATION_FAILED     => 1,   # 31 - Cluster authorization failed
#    $ERROR_INVALID_TIMESTAMP                => 1,   # 32 - The timestamp of the message is out of acceptable range
#    $ERROR_UNSUPPORTED_SASL_MECHANISM       => 1,   # 33 - The broker does not support the requested SASL mechanism
#    $ERROR_ILLEGAL_SASL_STATE               => 1,   # 34 - Request is not valid given the current SASL state
#    $ERROR_UNSUPPORTED_VERSION              => 1,   # 35 - The version of API is not supported
     $ERROR_NO_CONNECTION                    => 1,   # may be disconnected due to idle timeout etc.
);

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Creates C<Kafka::Connection> object for interaction with Kafka cluster.
Returns created C<Kafka::Connection> object.

C<new()> takes arguments in key-value pairs. The following arguments are currently recognized:

=over 3

=item C<host =E<gt> $host>

C<$host> is any Apache Kafka cluster host to connect to. It can be a hostname or the
IP-address in the "xx.xx.xx.xx" form.

Optional. Either C<host> or C<broker_list> must be supplied.

WARNING:

Make sure that you always connect to brokers using EXACTLY the same address or host name
as specified in broker configuration (host.name in server.properties).
Avoid using default value (when host.name is commented out) in server.properties - always use explicit value instead.

=item C<port =E<gt> $port>

Optional, default = C<$KAFKA_SERVER_PORT>.

C<$port> is the attribute denoting the port number of the service we want to
access (Apache Kafka service). C<$port> should be an integer number.

C<$KAFKA_SERVER_PORT> is the default Apache Kafka server port constant (C<9092>) that can
be imported from the L<Kafka|Kafka> module.

=item C<broker_list =E<gt> $broker_list>

Optional, C<$broker_list> is a reference to array of the host:port or [IPv6_host]:port strings, defining the list
of Kafka servers. This list will be used to locate the new leader if the server specified
via C<host =E<gt> $host> and C<port =E<gt> $port> arguments becomes unavailable. Either C<host>
or C<broker_list> must be supplied.

=item C<ip_version =E<gt> $ip_version>

Specify version of IP for interpreting of passed IP address and resolving of host name.

Optional, undefined by default, which works in the following way: version of IP address
is detected automatically, host name is resolved into IPv4 address.

See description of L<$IP_V4|Kafka::IO/$IP_V4>, L<$IP_V6|Kafka::IO/$IP_V6>
in C<Kafka> L<EXPORT|Kafka/EXPORT>.

=item C<timeout =E<gt> $timeout>

Optional, default = C<$Kafka::REQUEST_TIMEOUT>.

C<$timeout> specifies how long we wait for the remote server to respond.
C<$timeout> is in seconds, could be a positive integer or a floating-point number not bigger than int32 positive integer.

Special behavior when C<timeout> is set to C<undef>:

=back

=over 3

=item *

Alarms are not used internally (namely when performing C<gethostbyname>).

=item *

Default C<$REQUEST_TIMEOUT> is used for the rest of IO operations.

=back

=over 3

=item C<SEND_MAX_ATTEMPTS =E<gt> $attempts>

Optional, int32 signed integer, default = C<$Kafka::SEND_MAX_ATTEMPTS> .

In some circumstances (leader is temporarily unavailable, outdated metadata, etc) we may fail to send a message.
This property specifies the maximum number of attempts to send a message.
The C<$attempts> should be an integer number.

=item C<RETRY_BACKOFF =E<gt> $backoff>

Optional, default = C<$Kafka::RETRY_BACKOFF> .

Since leader election takes a bit of time, this property specifies the amount of time,
in milliseconds, that the producer waits before refreshing the metadata.
The C<$backoff> should be an integer number.

=item C<AutoCreateTopicsEnable =E<gt> $mode>

Optional, default value is 0 (false).

Kafka BUG "[KAFKA-1124]" (Fixed in Kafka 0.8.2):
I<AutoCreateTopicsEnable> controls how this module handles the first access to non-existent topic
when C<auto.create.topics.enable> in server configuration is C<true>.
If I<AutoCreateTopicsEnable> is false (default),
the first access to non-existent topic produces an exception;
however, the topic is created and next attempts to access it will succeed.

If I<AutoCreateTopicsEnable> is true, this module waits
(according to the C<SEND_MAX_ATTEMPTS> and C<RETRY_BACKOFF> properties)
until the topic is created,
to avoid errors on the first access to non-existent topic.

If C<auto.create.topics.enable> in server configuration is C<false>, this setting has no effect.

=item C<MaxLoggedErrors =E<gt> $number>

Optional, default value is 100.

Defines maximum number of last non-fatal errors that we keep in log. Use method L</nonfatal_errors> to
access those errors.

=item C<dont_load_supported_api_versions =E<gt> $boolean>

Optional, default value is 0 (false).

If set to false, when communicating with a broker, the client will
automatically try to find out the best version numbers to use for each of the
API endpoints.

If set to true, the client will always use
C<$Kafka::Protocol::DEFAULT_APIVERSION> as API version.

WARNING: API versions are supported starting from Kafka 0.10. Set this parameter to true
if you're connecting to 0.9.

=back

=cut
sub new {
    my ( $class, %p ) = @_;

    my $self = bless {
        host                    => q{},
        port                    => $KAFKA_SERVER_PORT,
        broker_list             => [],
        timeout                 => $REQUEST_TIMEOUT,
        async                   => 0,
        ip_version              => undef,
        SEND_MAX_ATTEMPTS       => $SEND_MAX_ATTEMPTS,
        RETRY_BACKOFF           => $RETRY_BACKOFF,
        AutoCreateTopicsEnable  => 0,
        MaxLoggedErrors         => 100,
        dont_load_supported_api_versions => 0,
    }, $class;

    exists $p{$_} and $self->{$_} = $p{$_} foreach keys %$self;

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'host' )
        unless defined( $self->{host} ) && ( $self->{host} eq q{} || defined( _STRING( $self->{host} ) ) ) && !utf8::is_utf8( $self->{host} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'port' )
        unless _POSINT( $self->{port} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'timeout (%s)', $self->{timeout} ) )
        unless !defined( $self->{timeout} ) || ( defined _NUMBER( $self->{timeout} ) && int( 1000 * $self->{timeout} ) >= 1 && int( $self->{timeout} * 1000 ) <= $MAX_INT32 );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'broker_list' )
        unless _ARRAY0( $self->{broker_list} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'SEND_MAX_ATTEMPTS' )
        unless _POSINT( $self->{SEND_MAX_ATTEMPTS} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'RETRY_BACKOFF' )
        unless _POSINT( $self->{RETRY_BACKOFF} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxLoggedErrors' )
        unless defined( _NONNEGINT( $self->{MaxLoggedErrors} ) );

    my $ip_version = $self->{ip_version};
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'ip_version (%s)', $ip_version ) )
        unless ( !defined( $ip_version ) || ( defined( _NONNEGINT( $ip_version ) ) && ( $ip_version == $IP_V4 || $ip_version == $IP_V6 ) ) );

    $self->{_metadata} = {};                # {
                                            #   TopicName => {
                                            #       Partition   => {
                                            #           'Leader'    => ...,
                                            #           'Replicas'  => [
                                            #               ...,
                                            #           ],
                                            #           'Isr'       => [
                                            #               ...,
                                            #           ],
                                            #       },
                                            #       ...,
                                            #   },
                                            #   ...,
                                            # }
    $self->{_leaders} = {};                 # {
                                            #   NodeId  => host:port or [IPv6_host]:port,
                                            #   ...,
                                            # }
    $self->{_group_coordinators} = {};      # {
                                            #   GroupId  => host:port or [IPv6_host]:port,
                                            #   ...,
                                            # }
    $self->{_nonfatal_errors} = [];
    my $IO_cache = $self->{_IO_cache} = {}; # host:port or [IPv6_host]:port => {
                                            #       'NodeId'    => ...,
                                            #       'IO'        => ...,
                                            #       'timeout'   => ...,
                                            #       'host'      => ...,
                                            #       'port'      => ...,
                                            #       'error'     => ...,
                                            #   },
                                            #   ...,

    # init IO cache
    foreach my $server ( ( $self->{host} ? $self->_build_server_name( $self->{host}, $self->{port} ) : (), @{ $self->{broker_list} } ) ) {
        $self->_error( $ERROR_MISMATCH_ARGUMENT, 'bad host:port or broker_list element' )
            unless $self->_is_like_server( $server );
        my ( $host, $port ) = _split_host_port( $server );
        my $correct_server = $self->_build_server_name( $host, $port );
        $IO_cache->{ $correct_server } = {
            NodeId  => undef,
            IO      => undef,
            host    => $host,
            port    => $port,
        };
    }

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'server is not specified' )
        unless keys( %$IO_cache );

    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are defined for the C<Kafka::Producer> class:

=cut

#-- public methods -------------------------------------------------------------

=head3 C<get_known_servers>

Returns the list of known Kafka servers (in host:port or [IPv6_host]:port format).

=cut
sub get_known_servers {
    my ( $self ) = @_;

    return keys %{ $self->{_IO_cache} };
}

sub _get_api_versions {
    my ( $self, $server ) = @_;

    my $server_metadata = $self->{_IO_cache}->{$server};
    defined $server_metadata
      or die "Fatal error: server '$server' is unknown in IO cache, which should not happen";

    # if we have cached data, just use it
    defined $server_metadata->{_api_versions}
      and return $server_metadata->{_api_versions};

    # no cached data. Initialize empty one
    my $server_api_versions = $server_metadata->{_api_versions} = {};

    # use empty data if client doesn't want to detect API versions
    $self->{dont_load_supported_api_versions}
      and return $server_api_versions;

    # call the server and try to get the supported API versions
    my $api_versions = [];
    my $error;
    try {
        # The ApiVersions API endpoint is only supported on Kafka versions >
        # 0.10.0.0 so this call may fail. We simply ignore this failure and
        # carry on.
        $api_versions = $self->_get_supported_api_versions( $server );
    }
    catch {
        $error = $_;
    };

    if( defined $error ) {
        if ( blessed( $error ) && $error->isa( 'Kafka::Exception' ) ) {
            if( $error->code == $ERROR_MISMATCH_ARGUMENT ) {
                # rethrow known fatal errors
                die $error;
            }
            $self->_remember_nonfatal_error( $error->code, $error, $server );
        } else {
            die $error;
        }
    }

    foreach my $element (@$api_versions) {
        # we want to choose which api version to use for each API call. We
        # try to use the max version that the server supports, with
        # fallback to the max version the protocol implements. If it's
        # lower than the min version the kafka server supports, we set it
        # to -1. If thie API endpoint is called, it'll die.
        my $kafka_min_version = $element->{MinVersion};
        my $kafka_max_version = $element->{MaxVersion};
        my $api_key = $element->{ApiKey};
        my $implemented_max_version = $IMPLEMENTED_APIVERSIONS->{$api_key} // -1;
        my $version = $kafka_max_version;
        $version > $implemented_max_version
          and $version = $implemented_max_version;
        $version < $kafka_min_version
          and $version = -1;
        $server_api_versions->{$api_key} = $version;
    }

    return $server_api_versions;
}

# Returns the list of supported API versions. This is not really. *Warning*,
# this call works only against Kafka 1.10.0.0

sub _get_supported_api_versions {
    my ( $self, $broker ) = @_;

    my $CorrelationId = _get_CorrelationId();
    my $decoded_request = {
        CorrelationId   => $CorrelationId,
        ClientId        => q{},
        ApiVersion => 0,
    };
    say STDERR format_message( '[%s] apiversions request: %s',
            scalar( localtime ),
            $decoded_request,
        ) if $self->debug_level;
    my $encoded_request = $protocol{ $APIKEY_APIVERSIONS }->{encode}->( $decoded_request );

    my $encoded_response_ref;

    # receive apiversions. We use a code block because it's actually a loop where
    # you can do last.
    {
        $self->_connectIO( $broker )
          or last;
        my $sent = $self->_sendIO( $broker, $encoded_request )
          or last;
        $encoded_response_ref = $self->_receiveIO( $broker );
    }

    unless ( $encoded_response_ref ) {
        # NOTE: it is possible to repeat the operation here
        $self->_error( $ERROR_CANNOT_RECV );
    }

    my $decoded_response = $protocol{ $APIKEY_APIVERSIONS }->{decode}->( $encoded_response_ref );
    say STDERR format_message( '[%s] apiversions response: %s',
            scalar( localtime ),
            $decoded_response,
        ) if $self->debug_level;
    ( defined( $decoded_response->{CorrelationId} ) && $decoded_response->{CorrelationId} == $CorrelationId )
        # FATAL error
        or $self->_error( $ERROR_MISMATCH_CORRELATIONID );
    my $ErrorCode = $decoded_response->{ErrorCode};

    # we asked a Kafka < 0.10 ( in this case the call is not
    # implemented and it dies
    $ErrorCode == $ERROR_NO_ERROR
      or $self->_error($ErrorCode);

    my $api_versions = $decoded_response->{ApiVersions};
    return $api_versions;
}

=head3 C<sasl_auth_plain( Username =E<gt> $username, Password =E<gt> $password )>

C<$username> and C<$password> are the username and password for
SASL PLAINTEXT authentication respectively.

=cut
sub sasl_auth_plain {
    my ($self, %p) = @_;
    my ($username, $password) = ($p{Username}, $p{Password});

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Username' )
        unless defined( $username ) && defined( _STRING( $username ) )  && !utf8::is_utf8( $password );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Password' )
        unless defined( $password ) && defined( _STRING( $password ) )  && !utf8::is_utf8( $password );

    my $decoded_request = {
        CorrelationId   => Kafka::Internals->_get_CorrelationId(),
        ClientId        => q{},
        ApiVersion => 0,
        Mechanism  => 'PLAIN',
    };

    my $encoded_request = $protocol{ $APIKEY_SASLHANDSHAKE }->{encode}->( $decoded_request );

    my @brokers = $self->_get_interviewed_servers;
    my $broker = $brokers[0];

    return unless $self->_connectIO( $broker ) &&
        $self->_sendIO( $broker, $encoded_request ) &&
        ( my $encoded_response_ref = $self->_receiveIO( $broker ) );

    my $decoded_response = $protocol{ $APIKEY_SASLHANDSHAKE }->{decode}->( $encoded_response_ref );

    my $msg = $username . "\0" . $username . "\0" . $password;

    my $encoded_sasl_req = pack( 'l>a'.length($msg), length($msg), $msg );

    $self->_sendIO( $broker, $encoded_sasl_req );

    my ($server_data, $io) = $self->_server_data_IO($broker);
    my $encoded_sasl_resp_len_ref = $io->receive( 4 ); # receive resp msg size (actually is 0)

    my $sasl_resp_len = unpack 'l>', $$encoded_sasl_resp_len_ref;
    my $encoded_sasl_resp_ref = $io->receive( $sasl_resp_len ) if $sasl_resp_len;

    return 1;
}

=head3 C<get_metadata( $topic )>

If C<$topic> is present, it must be a non-false string of non-zero length.

If  C<$topic> is absent, this method returns metadata for all topics.

Updates kafka cluster's metadata description and returns the hash reference to metadata,
which can be schematically described as:

    {
        TopicName => {
            Partition   => {
                'Leader'    => ...,
                'Replicas'  => [
                    ...,
                ],
                'Isr'       => [
                    ...,
                ],
            },
            ...,
        },
        ...,
    }

Consult Kafka "Wire protocol" documentation for more details about metadata structure.

=cut
sub get_metadata {
    my ( $self, $topic ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'topic' )
        unless !defined( $topic ) || ( ( $topic eq q{} || defined( _STRING( $topic ) ) ) && !utf8::is_utf8( $topic ) );

    $self->_update_metadata( $topic )
        # FATAL error
        or $self->_error( $ERROR_CANNOT_GET_METADATA, format_message( "topic='%s'", $topic ) );

    my $clone;
    if ( defined $topic ) {
        $clone = {
            $topic => dclone( $self->{_metadata}->{ $topic } )
        };
    } else {
        $clone = dclone( $self->{_metadata} );
    }

    return $clone;
}

=head3 C<is_server_known( $server )>

Returns true, if C<$server> (host:port  or [IPv6_host]:port) is known in cluster.

=cut
sub is_server_known {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT )
        unless $self->_is_like_server( $server );

    return exists $self->{_IO_cache}->{ $server };
}

# Returns true, if known C<$server> (host:port or [IPv6_host]:port) is accessible.
# Checks the accessibility of the server.
# This is evil: opens and closes a NEW connection immediately so do not use unless there is a strong reason for it.
sub _is_server_alive {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT )
        unless $self->_is_like_server( $server );

    $self->_error( $ERROR_NO_KNOWN_BROKERS, 'has not yet received the metadata?' )
        unless $self->get_known_servers;

    my $io_cache = $self->{_IO_cache};
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( "Unknown server '%s' (is not found in the metadata)", $server ) )
        unless exists( $io_cache->{ $server } );

    if ( my $io = $self->_connectIO( $server ) ) {
        return $io->_is_alive;
    } else {
        return;
    }
}

# this is evil, do not use unless there is a very strong reason for it
sub _is_server_connected {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT )
        unless $self->_is_like_server( $server );

    my $io_cache = $self->{_IO_cache};
    my $io;
    unless ( exists( $io_cache->{ $server } ) && ( $io = $io_cache->{ $server }->{IO} ) ) {
        return;
    }

    return $io->_is_alive;
}

=head3 C<receive_response_to_request( $request, $compression_codec )>

=over 3

=item C<$request>

C<$request> is a reference to the hash representing
the structure of the request.

This method encodes C<$request>, passes it to the leader of cluster, receives reply, decodes and returns
it in a form of hash reference.

=back

WARNING:

=over 3

=item *

This method should be considered private and should not be called by an end user.

=item *

In order to achieve better performance, this method does not perform arguments validation.

=back

=over 3

=item C<$compression_codec>

Optional.

C<$compression_codec> sets the required type of C<$messages> compression,
if the compression is desirable.

Supported codecs:
L<$COMPRESSION_NONE|Kafka/$COMPRESSION_NONE>,
L<$COMPRESSION_GZIP|Kafka/$COMPRESSION_GZIP>,
L<$COMPRESSION_SNAPPY|Kafka/$COMPRESSION_SNAPPY>,
L<$COMPRESSION_LZ4|Kafka/$COMPRESSION_LZ4>.

NOTE: $COMPRESSION_LZ4 requires Kafka 0.10 or higher, as initial implementation of LZ4 in Kafka did not follow the standard LZ4 framing specification.


=back

=cut
sub receive_response_to_request {
    my ( $self, $request, $compression_codec, $response_timeout ) = @_;

    my $api_key = $request->{ApiKey};

    my $host_to_send_to = $request->{__send_to__} // 'leader';

    # WARNING: The current version of the module limited to the following:
    # supports queries with only one combination of topic + partition (first and only).

    my $topic_data  = $request->{topics}->[0];
    my $topic_name  = $topic_data->{TopicName};
    my $partition   = $topic_data->{partitions}->[0]->{Partition};

    if (
           !%{ $self->{_metadata} }         # the first request
        || ( !$self->{AutoCreateTopicsEnable} && defined( $topic_name ) && !exists( $self->{_metadata}->{ $topic_name } ) )
    ) {
        $self->_update_metadata( $topic_name )  # hash metadata could be updated
            # FATAL error
            or $self->_error( $ERROR_CANNOT_GET_METADATA, format_message( "topic='%s'", $topic_name ), request => $request )
        ;
    }

    $request->{CorrelationId} = _get_CorrelationId() unless exists $request->{CorrelationId};

    say STDERR format_message( '[%s] compression_codec=%s, request=%s',
        scalar( localtime ),
        $compression_codec,
        $request,
    ) if $self->debug_level;

    my( $ErrorCode, $partition_data, $io_error );

    my $attempt = 0;
    # we save the original api version of the request, because in the attempt
    # loop we might be trying different brokers which may support different api
    # versions.
    my $original_request_api_version = $request->{ApiVersion};
    ATTEMPT: while ( ++$attempt <= ( $self->{SEND_MAX_ATTEMPTS} // 1 ) ) {
        $ErrorCode = $ERROR_NO_ERROR;
        undef $io_error;

        my $server;
        if ($host_to_send_to eq 'leader') {
            # hash metadata could be updated
            my $leader = $self->{_metadata}->{ $topic_name }->{ $partition }->{Leader};
            next ATTEMPT unless defined $leader;

            $server = $self->{_leaders}->{ $leader };
            unless ( $server ) {
                $ErrorCode = $ERROR_LEADER_NOT_FOUND;
                $self->_remember_nonfatal_error( $ErrorCode, $ERROR{ $ErrorCode }, $server, $topic_name, $partition );
                next ATTEMPT;
            }
        } elsif ( $host_to_send_to eq 'group_coordinator') {
            my $group_id = $request->{GroupId};
            if ( !%{ $self->{_group_coordinators} } && defined $group_id) {
                # first request
                $self->_update_group_coordinators($group_id);
            }
            $server = $self->{_group_coordinators}->{$group_id};
            unless ( $server ) {
                $ErrorCode = $ERROR_GROUP_COORDINATOR_NOT_FOUND;
                $self->_remember_nonfatal_error( $ErrorCode, $ERROR{ $ErrorCode }, $server, $topic_name, $partition );
                next ATTEMPT;
            }
        } else {
            die "__send_to__ must be either 'leader', 'group_coordinator', or void (will default to 'leader')";
        }

        # Send a request to the server
        if ( $self->_connectIO( $server ) ) {
            # we can connect to this server, so let's detect the api versions
            # it and use whatever it supports, except if the request forces us
            # to use an api version. Warning, the version might end up being
            # undef if detection against the Kafka server failed, or if
            # dont_load_supported_api_versions is true. However the Encoder
            # code knows how to handle it.
            $request->{ApiVersion} = $original_request_api_version;
            unless( defined $request->{ApiVersion} ) {
                $request->{ApiVersion} = $self->_get_api_versions( $server )->{ $api_key };
                # API versions request may fail and the server may be disconnected
                unless( $self->_is_IO_connected( $server ) ) {
                    # this attempt does not count, assuming that _get_api_versions will not try to get them from failing broker again
                    redo ATTEMPT;
                }
            }

            my $encoded_request = $protocol{ $api_key }->{encode}->( $request, $compression_codec );

            unless ( $self->_sendIO( $server, $encoded_request ) ) {
                $io_error = $self->_io_error( $server );
                $ErrorCode = $io_error ? $io_error->code : $ERROR_CANNOT_SEND;
                $self->_closeIO( $server, 1 );
            }
        }
        else {
            $io_error = $self->_io_error( $server );
            $ErrorCode = $io_error ? $io_error->code : $ERROR_CANNOT_BIND;
        }

        if ( $ErrorCode != $ERROR_NO_ERROR ) {
            # could not send request due to non-fatal IO error (fatal errors should be thrown by connectIO/sendIO already)
            $self->_remember_nonfatal_error( $ErrorCode, $self->_io_error( $server ), $server, $topic_name, $partition );
            if( $api_key == $APIKEY_PRODUCE && !( $ErrorCode == $ERROR_CANNOT_BIND || $ErrorCode == $ERROR_NO_CONNECTION ) ) {
                # do not retry failed produce requests which may have sent some data already
                $ErrorCode = $ERROR_CANNOT_SEND;
                last ATTEMPT;
            }
            next ATTEMPT;
        }

        my $response;
        if ( $api_key == $APIKEY_PRODUCE && $request->{RequiredAcks} == $NOT_SEND_ANY_RESPONSE ) {
            # Do not receive a response, self-forming own response
            $response = {
                CorrelationId                           => $request->{CorrelationId},
                topics                                  => [
                    {
                        TopicName                       => $topic_name,
                        partitions                      => [
                            {
                                Partition               => $partition,
                                ErrorCode               => 0,
                                Offset                  => $BAD_OFFSET,
                            },
                        ],
                    },
                ],
            };
        } else {
            my $encoded_response_ref = $self->_receiveIO( $server, $response_timeout );
            unless ( $encoded_response_ref ) {
                if ( $api_key == $APIKEY_PRODUCE ) {
                    # WARNING: Unfortunately, the sent package (one or more messages) does not have a unique identifier
                    # and there is no way to verify the delivery of data
                    $ErrorCode = $ERROR_SEND_NO_ACK;

                    # Should not be allowed to re-send data on the next attempt
                    # FATAL error
                    $self->_error( $ErrorCode, "no ack for request", io_error => $self->_io_error( $server ), request => $request );
                    last ATTEMPT;
                } else {
                    $ErrorCode = $ERROR_CANNOT_RECV;
                    $self->_remember_nonfatal_error( $ErrorCode, $self->_io_error( $server ), $server, $topic_name, $partition );
                    next ATTEMPT;
                }
            }
            if ( length( $$encoded_response_ref ) > 4 ) {   # MessageSize => int32
                # we also pass the api version that was used for the request,
                # so that we know how to decode the response
                $response = $protocol{ $api_key }->{decode}->( $encoded_response_ref, $request->{ApiVersion} );
                say STDERR format_message( '[%s] response: %s',
                        scalar( localtime ),
                        $response,
                    ) if $self->debug_level;
            } else {
                $self->_error( $ERROR_RESPONSEMESSAGE_NOT_RECEIVED, format_message("response length=%s", length( $$encoded_response_ref ) ), io_error => $self->_io_error( $server ), request => $request );
            }
        }

        # FATAL error if correllation does not match
        $self->_error( $ERROR_MISMATCH_CORRELATIONID, "$response->{CorrelationId} != $request->{CorrelationId}", request => $request, response => $response )
            unless $response->{CorrelationId} == $request->{CorrelationId}
        ;
        $topic_data     = $response->{topics}->[0];
        $partition_data = $topic_data->{ $api_key == $APIKEY_OFFSET ? 'PartitionOffsets' : 'partitions' }->[0];

        $ErrorCode = $partition_data->{ErrorCode};

        return $response if $ErrorCode == $ERROR_NO_ERROR; # success

        if( $api_key == $APIKEY_PRODUCE && $ErrorCode == $ERROR_REQUEST_TIMED_OUT ) {
            # special case: produce request timed out so we did not get expected ACK and should not retry sending request again
            # Should not be allowed to re-send data on the next attempt
            # FATAL error
            $self->_error( $ERROR_SEND_NO_ACK, format_message( "topic='%s', partition=%s response error: %s", $topic_name, $partition, $ErrorCode ), request => $request, response => $response );
            last ATTEMPT;
        }

        if ( exists $RETRY_ON_ERRORS{ $ErrorCode } ) {
            $self->_remember_nonfatal_error( $ErrorCode, $ERROR{ $ErrorCode }, $server, $topic_name, $partition );
            next ATTEMPT;
        }

        # FATAL error
        $self->_error( $ErrorCode, format_message( "topic='%s', partition=%s", $topic_name, $partition ), request => $request );
    } continue {
        # Expect to possible changes in the situation, such as restoration of connection
        say STDERR format_message( '[%s] sleeping for %d ms before making request attempt #%d (%s)',
                scalar( localtime ),
                $self->{RETRY_BACKOFF},
                $attempt + 1,
                $ErrorCode == $ERROR_NO_ERROR ? 'refreshing metadata' : "ErrorCode ${ErrorCode}",
            ) if $self->debug_level;

        Time::HiRes::sleep( $self->{RETRY_BACKOFF} / 1000 );

        $self->_update_metadata( $topic_name )
            # FATAL error
            or $self->_error( $ErrorCode || $ERROR_CANNOT_GET_METADATA, format_message( "topic='%s', partition=%s", $topic_name, $partition ), request => $request )
        ;
        if ( $host_to_send_to eq 'group_coordinator') {
            $self->_update_group_coordinators($request->{GroupId})
        }
    }

    # FATAL error
    if ( $ErrorCode ) {
        $self->_error( $ErrorCode, format_message( "topic='%s'%s", $topic_data->{TopicName}, $partition_data ? ", partition = ".$partition_data->{Partition} : '' ), request => $request, io_error => $io_error );
    } else {
        $self->_error( $ERROR_UNKNOWN_TOPIC_OR_PARTITION, format_message( "topic='%s', partition=%s", $topic_name, $partition ), request => $request, io_error => $io_error );
    }

    return;
}

=head3 C<exists_topic_partition( $topic, $partition )>

Returns true if the metadata contains information about specified combination of topic and partition.
Otherwise returns false.

C<exists_topic_partition()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

=back

=cut
sub exists_topic_partition {
    my ( $self, $topic, $partition ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'topic' )
        unless defined( $topic ) && ( $topic eq q{} || defined( _STRING( $topic ) ) ) && !utf8::is_utf8( $topic );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'partition' )
        unless defined( $partition ) && isint( $partition ) && $partition >= 0;

    unless ( %{ $self->{_metadata} } ) {    # the first request
        $self->_update_metadata( $topic )   # hash metadata could be updated
            # FATAL error
            or $self->_error( $ERROR_CANNOT_GET_METADATA, format_message( "topic='%s'", $topic ) );
    }

    return exists $self->{_metadata}->{ $topic }->{ $partition };
}

=head3 C<close_connection( $server )>

Closes connection with C<$server> (defined as host:port or [IPv6_host]:port).

=cut
sub close_connection {
    my ( $self, $server ) = @_;

    unless ( $self->is_server_known( $server ) ) {
        return;
    }

    $self->_closeIO( $server );
    return 1;
}

=head3 C<close>

Closes connection with all known Kafka servers.

=cut
sub close {
    my ( $self ) = @_;

    foreach my $server ( $self->get_known_servers ) {
        $self->_closeIO( $server );
    }

    return;
}

=head3 C<cluster_errors>

Returns a reference to a hash.

Each hash key is the identifier of the server (host:port or [IPv6_host]:port), and the value is the last communication error
with that server.

An empty hash is returned if there were no communication errors.

=cut
sub cluster_errors {
    my ( $self ) = @_;

    my %errors;
    foreach my $server ( $self->get_known_servers ) {
        if ( my $error = $self->_io_error( $server ) ) {
            $errors{ $server } = $error;
        }
    }

    return \%errors;
}

=head3 C<nonfatal_errors>

Returns a reference to an array of the last non-fatal errors.

Maximum number of entries is set using C<MaxLoggedErrors> parameter of L<constructor|/new>.

A reference to the empty array is returned if there were no non-fatal errors or parameter C<MaxLoggedErrors>
is set to 0.

=cut
sub nonfatal_errors {
    my ( $self ) = @_;

    return $self->{_nonfatal_errors};
}

=head3 C<clear_nonfatals>

Clears an array of the last non-fatal errors.

A reference to the empty array is returned because there are no non-fatal errors now.

=cut
sub clear_nonfatals {
    my ( $self ) = @_;

    @{ $self->{_nonfatal_errors} } = ();

    return $self->{_nonfatal_errors};
}

#-- private attributes ---------------------------------------------------------

#-- private functions ----------------------------------------------------------

sub _split_host_port {
    my ( $server ) = @_;

    my ( $host, $port ) = $server=~ /^(.+):(\d+)$/;
    $host = $1 if $host && $host =~ /^\[(.+)\]$/;

    return( $host, $port );
}

#-- private methods ------------------------------------------------------------

# Remember non-fatal error
sub _remember_nonfatal_error {
    my ( $self, $error_code, $error, $server, $topic, $partition ) = @_;

    my $max_logged_errors = $self->{MaxLoggedErrors}
        or return;

    shift( @{ $self->{_nonfatal_errors} } )
        if scalar( @{ $self->{_nonfatal_errors} } ) == $max_logged_errors;
    my $msg = format_message( "[%s] Non-fatal error: %s (ErrorCode %s, server '%s', topic '%s', partition %s)",
        scalar( localtime ),
        $error      // ( defined( $error_code ) && exists( $ERROR{ $error_code } ) ? $ERROR{ $error_code } : '<undef>' ),
        $error_code // 'IO error',
        $server,
        $topic,
        $partition,
    );

    say STDERR $msg
        if $self->debug_level;

    push @{ $self->{_nonfatal_errors} }, $msg;

    return $msg;
}

# Returns identifier of the cluster leader (host:port or [IPv6_host]:port)
sub _find_leader_server {
    my ( $self, $node_id ) = @_;

    my $leader_server;
    my $IO_cache = $self->{_IO_cache};
    my $NodeId;
    foreach my $server ( keys %$IO_cache ) {
        $NodeId = $IO_cache->{ $server }->{NodeId};
        if ( defined( $NodeId ) && $NodeId == $node_id ) {
            $leader_server = $server;
            last;
        }
    }

    return $leader_server;
}

# Form a list of servers to attempt querying of the metadata
sub _get_interviewed_servers {
    my ( $self ) = @_;

    my ( @priority, @secondary, @rest );
    my $IO_cache = $self->{_IO_cache};
    my $server_data;
    foreach my $server ( $self->get_known_servers ) {
        $server_data = $IO_cache->{ $server };
        if ( defined $server_data->{NodeId} ) {
            if ( $server_data->{IO} ) {
                push @priority, $server;
            } else {
                push @secondary, $server;
            }
        } else {
            push @rest, $server;
        }
    }

    return( shuffle( @priority ), shuffle( @secondary ), shuffle( @rest ) );
}

# Refresh group_coordinators for given topic
sub _update_group_coordinators {
    my ($self, $group_id) = @_;

    my $CorrelationId = _get_CorrelationId();
    my $decoded_request = {
        CorrelationId   => $CorrelationId,
        ClientId        => q{},
        CoordinatorKey  => $group_id,
        CoordinatorType => 0, # type is group
    };
    say STDERR format_message( '[%s] group coordinators request: %s',
            scalar( localtime ),
            $decoded_request,
        ) if $self->debug_level;
    my $encoded_request = $protocol{ $APIKEY_FINDCOORDINATOR }->{encode}->( $decoded_request );

    my $encoded_response_ref;
    my @brokers = $self->_get_interviewed_servers;

    # receive coordinator data
    foreach my $broker ( @brokers ) {
        last if  $self->_connectIO( $broker )
            &&   $self->_sendIO( $broker, $encoded_request )
            && ( $encoded_response_ref = $self->_receiveIO( $broker ) );
    }

    unless ( $encoded_response_ref ) {
        # NOTE: it is possible to repeat the operation here
        return;
    }

    my $decoded_response = $protocol{ $APIKEY_FINDCOORDINATOR }->{decode}->( $encoded_response_ref );
    say STDERR format_message( '[%s] group coordinators: %s',
            scalar( localtime ),
            $decoded_response,
        ) if $self->debug_level;
    ( defined( $decoded_response->{CorrelationId} ) && $decoded_response->{CorrelationId} == $CorrelationId )
        # FATAL error
        or $self->_error( $ERROR_MISMATCH_CORRELATIONID );
    $decoded_response->{ErrorCode}
        and $self->_error( $decoded_response->{ErrorCode} );

    my $IO_cache = $self->{_IO_cache};
    my $server = $self->_build_server_name( @{ $decoded_response }{ 'Host', 'Port' } );
        $IO_cache->{ $server } = {                      # can add new servers
            IO      => $IO_cache->{ $server }->{IO},    # IO or undef
            NodeId  => $decoded_response->{NodeId},
            host    => $decoded_response->{Host},
            port    => $decoded_response->{Port},
        };
    $self->{_group_coordinators}->{ $group_id } = $server;

    return 1;
}

# Refresh metadata for given topic
sub _update_metadata {
    my ( $self, $topic, $is_recursive_call ) = @_;

    my $CorrelationId = _get_CorrelationId();
    my $decoded_request = {
        CorrelationId   => $CorrelationId,
        ClientId        => q{},
        topics          => [
            $topic // (),
        ],
    };
    say STDERR format_message( '[%s] metadata request: %s',
            scalar( localtime ),
            $decoded_request,
        ) if $self->debug_level;
    my $encoded_request = $protocol{ $APIKEY_METADATA }->{encode}->( $decoded_request );

    my $encoded_response_ref;
    my @brokers = $self->_get_interviewed_servers;

    # receive metadata
    foreach my $broker ( @brokers ) {
        last if  $self->_connectIO( $broker )
            &&   $self->_sendIO( $broker, $encoded_request )
            && ( $encoded_response_ref = $self->_receiveIO( $broker ) );
    }

    unless ( $encoded_response_ref ) {
        # NOTE: it is possible to repeat the operation here
        return;
    }

    my $decoded_response = $protocol{ $APIKEY_METADATA }->{decode}->( $encoded_response_ref );
    say STDERR format_message( '[%s] metadata response: %s',
            scalar( localtime ),
            $decoded_response,
        ) if $self->debug_level;
    ( defined( $decoded_response->{CorrelationId} ) && $decoded_response->{CorrelationId} == $CorrelationId )
        # FATAL error
        or $self->_error( $ERROR_MISMATCH_CORRELATIONID );

    unless ( _ARRAY( $decoded_response->{Broker} ) ) {
        if ( $self->{AutoCreateTopicsEnable} ) {
            return $self->_attempt_update_metadata( $is_recursive_call, $topic, undef, $ERROR_NO_KNOWN_BROKERS );
        } else {
            # FATAL error
            $self->_error( $ERROR_NO_KNOWN_BROKERS, format_message( "topic='%s'", $topic ) );
        }
    }

    my $IO_cache = $self->{_IO_cache};

    # Clear the previous information about the NodeId in the IO cache
    $IO_cache->{ $_ }->{NodeId} = undef for @brokers;

    #  In the IO cache update/add obtained server information
    foreach my $received_broker ( @{ $decoded_response->{Broker} } ) {
        my $server = $self->_build_server_name( @{ $received_broker }{ 'Host', 'Port' } );
        $IO_cache->{ $server } = {                      # can add new servers
            IO      => $IO_cache->{ $server }->{IO},    # IO or undef
            NodeId  => $received_broker->{NodeId},
            host    => $received_broker->{Host},
            port    => $received_broker->{Port},
        };
    }

    #NOTE: IO cache does not remove server that's missing in metadata

    # Collect the received metadata
    my $received_metadata   = {};
    my $leaders             = {};

    my $ErrorCode = $ERROR_NO_ERROR;
    my( $TopicName, $partition );
    METADATA_CREATION:
    foreach my $topic_metadata ( @{ $decoded_response->{TopicMetadata} } ) {
        $TopicName = $topic_metadata->{TopicName};
        undef $partition;
        last METADATA_CREATION
            if ( $ErrorCode = $topic_metadata->{ErrorCode} ) != $ERROR_NO_ERROR;

        foreach my $partition_metadata ( @{ $topic_metadata->{PartitionMetadata} } ) {
            $partition = $partition_metadata->{Partition};
            last METADATA_CREATION
                if ( $ErrorCode = $partition_metadata->{ErrorCode} ) != $ERROR_NO_ERROR
                    && $ErrorCode != $ERROR_REPLICA_NOT_AVAILABLE;
            $ErrorCode = $ERROR_NO_ERROR;

            my $received_partition_data = $received_metadata->{ $TopicName }->{ $partition } = {};
            my $leader = $received_partition_data->{Leader} = $partition_metadata->{Leader};
            $received_partition_data->{Replicas}            = [ @{ $partition_metadata->{Replicas} } ];
            $received_partition_data->{Isr}                 = [ @{ $partition_metadata->{Isr} } ];

            $leaders->{ $leader } = $self->_find_leader_server( $leader );
        }
    }
    if ( $ErrorCode != $ERROR_NO_ERROR ) {
        if ( exists $RETRY_ON_ERRORS{ $ErrorCode } ) {
            return $self->_attempt_update_metadata( $is_recursive_call, $TopicName, $partition, $ErrorCode );
        } else {
            # FATAL error
            $self->_error( $ErrorCode, format_message( "topic='%s'%s", $TopicName, defined( $partition ) ? ", partition=$partition" : '' ) );
        }
    }

    # Update metadata for received topics
    $self->{_metadata}->{ $_ }  = $received_metadata->{ $_ } foreach keys %{ $received_metadata };
    $self->{_leaders}->{ $_ }   = $leaders->{ $_ } foreach keys %{ $leaders };

    return 1;
}

# trying to get the metadata without error
sub _attempt_update_metadata {
    my ( $self, $is_recursive_call, $topic, $partition, $error_code ) = @_;

    return if $is_recursive_call;
    $self->_remember_nonfatal_error( $error_code, $ERROR{ $error_code }, undef, $topic, $partition );

    my $attempts = $self->{SEND_MAX_ATTEMPTS};
    ATTEMPTS:
    while ( $attempts-- ) {
        say STDERR format_message( '[%s] sleeping for %d ms before making update metadata attempt #%d',
                scalar( localtime ),
                $self->{RETRY_BACKOFF},
                $self->{SEND_MAX_ATTEMPTS} - $attempts + 1,
            ) if $self->debug_level;
        Time::HiRes::sleep( $self->{RETRY_BACKOFF} / 1000 );
        return( 1 ) if $self->_update_metadata( $topic, 1 );
    }
    # FATAL error
    $self->_error( $error_code, format_message( "topic='%s'%s", $topic, defined( $partition ) ? ", partition=$partition" : '' ) );

    return;
}

# forms server identifier using supplied $host, $port
sub _build_server_name {
    my ( $self, $host, $port ) = @_;

    $host = "[$host]" if is_ipv6( $host );

    return "$host:$port";
}

# remembers error communicating with the server
sub _on_io_error {
    my ( $self, $server_data, $error ) = @_;
    $server_data->{error} = $error;
    if( $server_data->{IO} ) {
        $server_data->{IO}->close;
        $server_data->{IO} = undef;
    }

    if( blessed( $error ) && $error->isa('Kafka::Exception') ) {
        if( $error->code == $ERROR_MISMATCH_ARGUMENT || $error->code == $ERROR_INCOMPATIBLE_HOST_IP_VERSION ) {
            # rethrow known fatal errors
            die $error;
        }
    } else {
        # rethrow all unknown errors
        die $error;
    }

    return;
}

sub _io_error {
    my( $self, $server ) = @_;
    my $error;
    if( my $server_data = $self->{_IO_cache}->{ $server } ) {
        $error = $server_data->{error};
    }
    return $error;
}

sub _is_IO_connected {
    my ( $self, $server ) = @_;
    my $server_data = $self->{_IO_cache}->{ $server } or return;
    return $server_data->{IO};
}

# connects to a server (host:port or [IPv6_host]:port)
sub _connectIO {
    my ( $self, $server ) = @_;

    my $server_data = $self->{_IO_cache}->{ $server }
        or $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( "Unknown server '%s' (is not found in the metadata)", $server ) )
    ;
    unless( $server_data->{IO} ) {
        my $error;
        my $io_class = $self->{async} ? 'Kafka::IO::Async' : 'Kafka::IO';
        try {
            $server_data->{IO} = $io_class->new(
                host        => $server_data->{host},
                port        => $server_data->{port},
                timeout     => $self->{timeout},
                ip_version  => $self->{ip_version},
            );
            $server_data->{error} = undef;
        } catch {
            $error = $_;
        };

        if( defined $error ) {
            $self->_on_io_error( $server_data, $error );
            return;
        }
    }

    return $server_data->{IO};
}

sub _server_data_IO {
    my ( $self, $server ) = @_;
    my $server_data = $self->{_IO_cache}->{ $server }
        or $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( "Unknown server '%s' (is not found in the metadata)", $server ) )
    ;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( "Server '%s' is not connected", $server ) )
        unless $server_data->{IO}
    ;
    return ( $server_data, $server_data->{IO} );
}

# Send encoded request ($encoded_request) to server ($server)
sub _sendIO {
    my ( $self, $server, $encoded_request ) = @_;
    my( $server_data, $io ) = $self->_server_data_IO( $server );
    my $sent;
    my $error;
    try {
        $sent = $io->send( $encoded_request );
    } catch {
        $error = $_;
    };

    if( defined $error ) {
        $self->_on_io_error( $server_data, $error );
    }

    return $sent;
}

# Receive response from a given server
sub _receiveIO {
    my ( $self, $server, $response_timeout ) = @_;
    my( $server_data, $io ) = $self->_server_data_IO( $server );
    my $response_ref;
    my $error;
    try {
        $response_ref = $io->receive( 4, $response_timeout ); # response header must arrive within request-specific timeout if provided
        if ( $response_ref && length( $$response_ref ) == 4 ) {
            # received 4-byte response header with response size; try receiving the rest
            my $message_body_ref = $io->receive( unpack( 'l>', $$response_ref ) );
            $$response_ref .= $$message_body_ref;
        }
    } catch {
        $error = $_;
    };

    if( defined $error ) {
        $self->_on_io_error( $server_data, $error );
    }

    return $response_ref;
}

# Close connectino to $server
sub _closeIO {
    my ( $self, $server, $keep_error ) = @_;

    if ( my $server_data = $self->{_IO_cache}->{ $server } ) {
        if ( my $io = $server_data->{IO} ) {
            $io->close;
            $server_data->{error} = undef unless $keep_error;
            $server_data->{IO}    = undef;
        }
    }

    return;
}

# check validity of an argument to match host:port format
sub _is_like_server {
    my ( $self, $server ) = @_;

    unless (
               defined( $server )
            && defined( _STRING( $server ) )
            && !utf8::is_utf8( $server )
        ) {
        return;
    }

    my ( $host, $port ) = _split_host_port( $server );
    unless ( ( is_hostname( $host ) || is_ipv4( $host ) || is_ipv6( $host ) ) && $port ) {
        return;
    }

    return $server;
}

# Handler for errors
sub _error {
    my $self = shift;
    Kafka::Exception::Connection->throw( throw_args( @_ ) );
}

1;

__END__

=head1 DIAGNOSTICS

When error is detected, an exception, represented by object of L<Kafka::Exception::Connection|Kafka::Exception::Connection> class,
is thrown (see L<Kafka::Exceptions|Kafka::Exceptions>).

L<code|Kafka::Exceptions/code> and a more descriptive L<message|Kafka::Exceptions/message> provide
information about exception. Consult documentation of the L<Kafka::Exceptions|Kafka::Exceptions>
for the list of all available methods.

Here is the list of possible error messages that C<Kafka::Connection> may produce:

=over 3

=item C<Invalid argument>

Invalid argument was provided to C<new> L<constructor|/CONSTRUCTOR> or to other L<method|/METHODS>.

=item C<Cannot send>

Request cannot be sent to Kafka.

=item C<Cannot receive>

Response cannot be received from Kafka.

=item C<Cannot bind>

A successful TCP connection cannot be established on given host and port.

=item C<Cannot get metadata>

Error detected during parsing of response from Kafka.

=item C<Leader not found>

Failed to locate leader of Kafka cluster.

=item C<Mismatch CorrelationId>

Mismatch of C<CorrelationId> of request and response.

=item C<There are no known brokers>

Failed to locate cluster broker.

=item C<Cannot get metadata>

Received meta data is incorrect or missing.

=back

=head2 Debug mode

Debug output can be enabled by passing desired level via environment variable
using one of the following ways:

C<PERL_KAFKA_DEBUG=1>             - debug is enabled for the whole L<Kafka|Kafka> package.

C<PERL_KAFKA_DEBUG=Connection:1>  - enable debug for C<Kafka::Connection> only.

C<Kafka::Connection> prints to C<STDERR> information about non-fatal errors,
re-connection attempts and such when debug level is set to 1 or higher.

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

=head1 SOURCE CODE

Kafka package is hosted on GitHub:
L<https://github.com/TrackingSoft/Kafka>

=head1 AUTHOR

Sergey Gladkov

Please use GitHub project link above to report problems or contact authors.

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Sergiy Zuban

Vlad Marchenko

Damien Krotkine

Greg Franklin

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2017 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut

