package Kafka::Connection;

=head1 NAME

Kafka::Connection - Object interface to connect to a kafka cluster.

=head1 VERSION

This documentation refers to C<Kafka::Connection> version 1.001012 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $DEBUG = 0;

our $VERSION = '1.001012';

use Exporter qw(
    import
);
our @EXPORT = qw(
    %RETRY_ON_ERRORS
);

#-- load the modules -----------------------------------------------------------

use Carp;
use Data::Dumper ();
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
use Time::HiRes qw(
    sleep
);
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

    $ERROR_CANNOT_BIND
    $ERROR_CANNOT_GET_METADATA
    $ERROR_CANNOT_RECV
    $ERROR_CANNOT_SEND
    $ERROR_LEADER_NOT_FOUND
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_MISMATCH_CORRELATIONID
    $ERROR_NO_KNOWN_BROKERS
    $ERROR_RESPONSEMESSAGE_NOT_RECEIVED
    $ERROR_SEND_NO_ACK
    $ERROR_UNKNOWN_APIKEY

    $IP_V4
    $IP_V6
    $KAFKA_SERVER_PORT
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_MAX_ATTEMPTS
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
    $MAX_CORRELATIONID
    $MAX_INT32
    debug_level
    _get_CorrelationId
    format_message
);
use Kafka::IO;
use Kafka::Protocol qw(
    $BAD_OFFSET
    decode_fetch_response
    decode_metadata_response
    decode_offset_response
    decode_produce_response
    encode_fetch_request
    encode_metadata_request
    encode_offset_request
    encode_produce_request
);

#-- declarations ---------------------------------------------------------------

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

=item C<CorrelationId =E<gt> $correlation_id>

Optional, default = C<undef> .

C<Correlation> is a user-supplied integer. It will be passed back with the response by
the server, unmodified. The C<$correlation_id> should be an integer number.

An exception is thrown if C<CorrelationId> in response does not match the one supplied
in request.

If C<CorrelationId> is not provided, it is set to a random negative integer.

=item C<SEND_MAX_ATTEMPTS =E<gt> $attempts>

Optional, int32 signed integer, default = C<$Kafka::SEND_MAX_ATTEMPTS> .

In some circumstances (leader is temporarily unavailable, outdated metadata, etc) we may fail to send a message.
This property specifies the maximum number of attempts to send a message.
The C<$attempts> should be an integer number.

Do not use C<$Kafka::SEND_MAX_ATTEMPTS> in C<Kafka::Producer-<gt>send> request to prevent duplicates.

=item C<RECEIVE_MAX_ATTEMPTS =E<gt> $attempts>

Optional, int32 signed integer, default = C<$Kafka::RECEIVE_MAX_ATTEMPTS> .

In some circumstances (temporarily network issues, server high load, socket error, etc) we may fail to
receive a response.
This property specifies the maximum number of attempts to receive a message.
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

=back

=cut
sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        host                    => q{},
        port                    => $KAFKA_SERVER_PORT,
        broker_list             => [],
        timeout                 => $REQUEST_TIMEOUT,
        ip_version              => undef,
        CorrelationId           => undef,
        SEND_MAX_ATTEMPTS       => $SEND_MAX_ATTEMPTS,
        RECEIVE_MAX_ATTEMPTS    => $RECEIVE_MAX_ATTEMPTS,
        RETRY_BACKOFF           => $RETRY_BACKOFF,
        AutoCreateTopicsEnable  => 0,
        MaxLoggedErrors         => 100,
    }, $class;

    while ( @args ) {
        my $k = shift @args;

        # legacy, will be removed in future releases
        if ( $k eq 'SEND_MAX_RETRIES' ) {
            carp "Parameter 'SEND_MAX_RETRIES' is deprecated, use 'SEND_MAX_ATTEMPTS' instead";
            $k = 'SEND_MAX_ATTEMPTS';
        } elsif ( $k eq 'RECEIVE_MAX_RETRIES' ) {
            $k = 'RECEIVE_MAX_ATTEMPTS';
            carp "Parameter 'RECEIVE_MAX_RETRIES' is deprecated, use 'RECEIVE_MAX_ATTEMPTS' instead";
        }

        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    $self->{CorrelationId} //= _get_CorrelationId;

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'host' )
        unless defined( $self->{host} ) && ( $self->{host} eq q{} || defined( _STRING( $self->{host} ) ) ) && !utf8::is_utf8( $self->{host} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'port' )
        unless _POSINT( $self->{port} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'timeout (%s)', $self->{timeout} ) )
        unless ( _NUMBER( $self->{timeout} ) && $self->{timeout} > 0 && $self->{timeout} <= $MAX_INT32 ) || !defined( $self->{timeout} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'broker_list' )
        unless _ARRAY0( $self->{broker_list} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'CorrelationId (%s)', $self->{CorrelationId} ) )
        unless isint( $self->{CorrelationId} ) && $self->{CorrelationId} <= $MAX_CORRELATIONID;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'SEND_MAX_ATTEMPTS' )
        unless _POSINT( $self->{SEND_MAX_ATTEMPTS} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'RECEIVE_MAX_ATTEMPTS' )
        unless _POSINT( $self->{RECEIVE_MAX_ATTEMPTS} );
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
        or $self->_error( $ERROR_CANNOT_GET_METADATA, format_message( "topic = '%s'", $topic ) );

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

=head3 C<is_server_alive( $server )>

Returns true, if known C<$server> (host:port or [IPv6_host]:port) is accessible.
Checks the accessibility of the server.

=cut
sub is_server_alive {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT )
        unless $self->_is_like_server( $server );

    $self->_error( $ERROR_NO_KNOWN_BROKERS, 'has not yet received the metadata?' )
        unless $self->get_known_servers;

    my $io_cache = $self->{_IO_cache};
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( "Unknown server '%s' (is not found in the metadata)", $server ) )
        unless exists( $io_cache->{ $server } );

    if ( my $io = $self->_connectIO( $server ) ) {
        return $io->is_alive;
    } else {
        return;
    }
}

=head3 C<is_server_connected( $server )>

Returns true, if successful connection is established with C<$server> (host:port or [IPv6_host]:port).

=cut
sub is_server_connected {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT )
        unless $self->_is_like_server( $server );

    my $io_cache = $self->{_IO_cache};
    my $io;
    unless ( exists( $io_cache->{ $server } ) && ( $io = $io_cache->{ $server }->{IO} ) ) {
        return;
    }

    return $io->is_alive;
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
L<$COMPRESSION_SNAPPY|Kafka/$COMPRESSION_SNAPPY>.

=back

=cut
sub receive_response_to_request {
    my ( $self, $request, $compression_codec ) = @_;

    local $Data::Dumper::Sortkeys = 1 if $self->debug_level;

    my $api_key = $request->{ApiKey};

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
            or $self->_error( $ERROR_CANNOT_GET_METADATA, format_message( "topic = '%s'", $topic_name ) );
    }
    my $encoded_request = $protocol{ $api_key }->{encode}->( $request, $compression_codec );

    my $CorrelationId = $request->{CorrelationId} // _get_CorrelationId;

    say STDERR sprintf( '[%s] compression_codec = %d, request: %s',
            scalar( localtime ),
            $compression_codec // '<undef>',
            Data::Dumper->Dump( [ $request ], [ 'request' ] )
        ) if $self->debug_level;

    my $attempts = $api_key == $APIKEY_PRODUCE ? 1 : $self->{SEND_MAX_ATTEMPTS};
    my ( $ErrorCode, $partition_data, $server );
    ATTEMPTS:
    while ( $attempts-- ) {
        REQUEST:
        {
            $ErrorCode = $ERROR_NO_ERROR;
            if ( defined( my $leader = $self->{_metadata}->{ $topic_name }->{ $partition }->{Leader} ) ) {   # hash metadata could be updated
                unless ( $server = $self->{_leaders}->{ $leader } ) {
                    $ErrorCode = $ERROR_LEADER_NOT_FOUND;
                    $self->_remember_nonfatal_error( $ErrorCode, $ERROR{ $ErrorCode }, $server, $topic_name, $partition );
                    last REQUEST;       # go to the next attempt
                }

                # Send a request to the leader
                if ( !$self->_connectIO( $server ) ) {
                    $ErrorCode = $ERROR_CANNOT_BIND;
                } elsif ( !$self->_sendIO( $server, $encoded_request ) ) {
                    $ErrorCode = $ERROR_CANNOT_SEND;
                }
                if ( $ErrorCode != $ERROR_NO_ERROR ) {
                    $self->_remember_nonfatal_error( $ErrorCode, $self->{_IO_cache}->{ $server }->{error}, $server, $topic_name, $partition );
                    last REQUEST;    # go to the next attempt
                }

                my $response;
                if ( $api_key == $APIKEY_PRODUCE && $request->{RequiredAcks} == $NOT_SEND_ANY_RESPONSE ) {

                    # Do not receive a response, self-forming own response
                    $response = {
                        CorrelationId                           => $CorrelationId,
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
                    my $encoded_response_ref;
                    unless ( $encoded_response_ref = $self->_receiveIO( $server ) ) {
                        if ( $api_key == $APIKEY_PRODUCE ) {
# WARNING: Unfortunately, the sent package (one or more messages) does not have a unique identifier
# and there is no way to verify the delivery of data
                            $ErrorCode = $ERROR_SEND_NO_ACK;

                            # Should not be allowed to re-send data on the next attempt
                            # FATAL error
                            $self->_error( $ErrorCode, $self->{_IO_cache}->{ $server }->{error} );
                        } else {
                            $ErrorCode = $ERROR_CANNOT_RECV;
                            $self->_remember_nonfatal_error( $ErrorCode, $self->{_IO_cache}->{ $server }->{error}, $server, $topic_name, $partition );
                            last REQUEST;   # go to the next attempt
                        }
                    }
                    if ( length( $$encoded_response_ref ) > 4 ) {   # MessageSize => int32
                        $response = $protocol{ $api_key }->{decode}->( $encoded_response_ref );
                        say STDERR sprintf( '[%s] response: %s',
                                scalar( localtime ),
                                Data::Dumper->Dump( [ $response ], [ 'response' ] )
                            ) if $self->debug_level;
                    } else {
                        $self->_error( $ERROR_RESPONSEMESSAGE_NOT_RECEIVED );
                    }
                }

                $response->{CorrelationId} == $CorrelationId
                    # FATAL error
                    or $self->_error( $ERROR_MISMATCH_CORRELATIONID );
                $topic_data     = $response->{topics}->[0];
                $partition_data = $topic_data->{ $api_key == $APIKEY_OFFSET ? 'PartitionOffsets' : 'partitions' }->[0];

                if ( ( $ErrorCode = $partition_data->{ErrorCode} ) == $ERROR_NO_ERROR ) {
                    return $response;
                } elsif ( exists $RETRY_ON_ERRORS{ $ErrorCode } ) {
                    $self->_remember_nonfatal_error( $ErrorCode, $ERROR{ $ErrorCode }, $server, $topic_name, $partition );
                    last REQUEST;   # go to the next attempt
                } else {
                    # FATAL error
                    $self->_error( $ErrorCode, format_message( "topic = '%s', partition = %s", $topic_name, $partition ) );
                }
            }
        }

        # Expect to possible changes in the situation, such as restoration of connection
        say STDERR sprintf( '[%s] sleeping for %d ms before making request attempt #%d (%s)',
                scalar( localtime ),
                $self->{RETRY_BACKOFF},
                $self->{SEND_MAX_ATTEMPTS} - $attempts + 1,
                $ErrorCode == $ERROR_NO_ERROR ? 'refreshing metadata' : "ErrorCode ${ErrorCode}",
            ) if $self->debug_level;
        sleep $self->{RETRY_BACKOFF} / 1000;

        $self->_update_metadata( $topic_name )
            # FATAL error
            or $self->_error( $ErrorCode || $ERROR_CANNOT_GET_METADATA, format_message( "topic = '%s', partition = %s", $topic_name, $partition ) );
    }

    # FATAL error
    if ( $ErrorCode ) {
        $self->_error( $ErrorCode, format_message( "topic = '%s'%s", $topic_data->{TopicName}, $partition_data ? ", partition = ".$partition_data->{Partition} : q{} ) );
    } else {
        $self->_error( $ERROR_UNKNOWN_TOPIC_OR_PARTITION, format_message( "topic = '%s', partition = %s", $topic_name, $partition ) );
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
            or $self->_error( $ERROR_CANNOT_GET_METADATA, format_message( "topic = '%s'", $topic ) );
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
    my $io_cache = $self->{_IO_cache};
    foreach my $server ( keys %$io_cache ) {
        if ( my $error = $io_cache->{ $server }->{error} ) {
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

# Refresh metadata for given topic
sub _update_metadata {
    my ( $self, $topic, $is_recursive_call ) = @_;

    my $CorrelationId = $self->{CorrelationId};
    my $decoded_request = {
        CorrelationId   => $CorrelationId,
        ClientId        => q{},
        topics          => [
            $topic // (),
        ],
    };
    say STDERR sprintf( '[%s] metadata request: %s',
            scalar( localtime ),
            Data::Dumper->Dump( [ $decoded_request ], [ 'request' ] )
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
    say STDERR sprintf( '[%s] metadata response: %s',
            scalar( localtime ),
            Data::Dumper->Dump( [ $decoded_response ], [ 'response' ] )
        ) if $self->debug_level;
    ( defined( $decoded_response->{CorrelationId} ) && $decoded_response->{CorrelationId} == $CorrelationId )
        # FATAL error
        or $self->_error( $ERROR_MISMATCH_CORRELATIONID );

    unless ( _ARRAY( $decoded_response->{Broker} ) ) {
        if ( $self->{AutoCreateTopicsEnable} ) {
            return $self->_attempt_update_metadata( $is_recursive_call, $topic, undef, $ERROR_NO_KNOWN_BROKERS );
        } else {
            # FATAL error
            $self->_error( $ERROR_NO_KNOWN_BROKERS, format_message( "topic = '%s'", $topic ) );
        }
    }

    my $IO_cache = $self->{_IO_cache};

    # Clear the previous information about the NodeId in the IO cache
    $IO_cache->{ $_ }->{NodeId} = undef for @brokers;

    #  In the IO cache update/add obtained server information
    my $server;
    foreach my $received_broker ( @{ $decoded_response->{Broker} } ) {
        $server = $self->_build_server_name( @{ $received_broker }{ 'Host', 'Port' } );
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

    my ( $TopicName, $partition );
    my $ErrorCode = $ERROR_NO_ERROR;
    METADATA_CREATION:
    foreach my $topic_metadata ( @{ $decoded_response->{TopicMetadata} } ) {
        $partition = undef;

        $TopicName = $topic_metadata->{TopicName};
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
            $self->_error( $ErrorCode, format_message( "topic = '%s'%s", $TopicName, defined( $partition ) ? ", partition = $partition" : '' ) );
        }
    }

    %$received_metadata
        # FATAL error
        or $self->_error( $ERROR_CANNOT_GET_METADATA, format_message( "topic = '%s'", $topic ) );

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
        say STDERR sprintf( '[%s] sleeping for %d ms before making update metadata attempt #%d',
                scalar( localtime ),
                $self->{RETRY_BACKOFF},
                $self->{RECEIVE_MAX_ATTEMPTS} - $attempts + 1,
            ) if $self->debug_level;
        sleep $self->{RETRY_BACKOFF} / 1000;
        return( 1 ) if $self->_update_metadata( $topic, 1 );
    }
    # FATAL error
    $self->_error( $error_code, format_message( "topic = '%s'%s", $topic, defined( $partition ) ? ", partition = $partition" : '' ) );

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

    my $message;
    if ( !blessed( $error ) || !$error->isa( 'Kafka::Exception' ) ) {
        $message = $error;
    } else {
        $message = $error->message;
    }

    $server_data->{error}   = $message;
    $server_data->{IO}      = undef;

    return;
}

# connects to a server (host:port or [IPv6_host]:port)
sub _connectIO {
    my ( $self, $server ) = @_;

    my $server_data = $self->{_IO_cache}->{ $server };
    my $io;
    unless ( $server_data && ( $io = $server_data->{IO} ) ) {
        try {
            $server_data->{IO} = Kafka::IO->new(
                host        => $server_data->{host},
                port        => $server_data->{port},
                timeout     => $self->{timeout},
                ip_version  => $self->{ip_version},
            );
            $server_data->{error} = undef;
        } catch {
            # NOTE: it is possible to repeat the operation here
            my $error = $_;
            $self->_on_io_error( $server_data, $error );
            return;
        };
    }

    return $server_data->{IO};
}

# Send encoded request ($encoded_request) to server ($server)
sub _sendIO {
    my ( $self, $server, $encoded_request ) = @_;

    my $server_data = $self->{_IO_cache}->{ $server };
    my $sent;
    try {
        $sent = $server_data->{IO}->send( $encoded_request );
    } catch {
        # NOTE: it is possible to repeat the operation here
        my $error = $_;
        $self->_on_io_error( $server_data, $error );
    };

    return $sent;
}

# Receive response from a given server
sub _receiveIO {
    my ( $self, $server ) = @_;

    my $server_data = $self->{_IO_cache}->{ $server };
    my $response_ref;

    my $error;
    my $attempts = $self->{RECEIVE_MAX_ATTEMPTS};
    ATTEMPTS:
    while ( $attempts-- ) {
        $error = undef;
        try {
            my $io = $server_data->{IO};
            $response_ref   = $io->receive( 4 ) unless $response_ref;
            $$response_ref .= ${ $io->receive( unpack( 'l>', $$response_ref ) ) };
        } catch {
            $error = $_;
        };
        last unless $error;

        say STDERR sprintf( "[%s] sleeping for %d ms before making receive attempt #%d (error '%s')",
                scalar( localtime ),
                $self->{RETRY_BACKOFF},
                $self->{RECEIVE_MAX_ATTEMPTS} - $attempts + 1,
                $error,
            ) if $self->debug_level;
        sleep $self->{RETRY_BACKOFF} / 1000;
    }
    $self->_on_io_error( $server_data, $error )
        if $error;

    return $response_ref;
}

# Close connectino to $server
sub _closeIO {
    my ( $self, $server ) = @_;

    if ( my $server_data = $self->{_IO_cache}->{ $server } ) {
        if ( my $io = $server_data->{IO} ) {
            $io->close;
            $server_data->{error}   = undef;
            $server_data->{IO}      = undef;
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

    return;
}

#-- Closes and cleans up -------------------------------------------------------

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

=item C<Cannot recv>

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

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Sergiy Zuban

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2016 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
