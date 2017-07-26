package Kafka;

# Kafka allows you to produce and consume messages using
# the Apache Kafka distributed publish/subscribe messaging service.

=head1 NAME

Kafka - Apache Kafka interface for Perl.

=head1 VERSION

This documentation refers to C<Kafka> package version 1.05 .

=cut



use 5.010;
use strict;
use warnings;



our $VERSION = '1.05';

use Exporter qw(
    import
);

our @EXPORT_OK = qw(
    $BITS64
    $BLOCK_UNTIL_IS_COMMITTED
    $COMPRESSION_GZIP
    $COMPRESSION_NONE
    $COMPRESSION_SNAPPY
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    %ERROR
    $ERROR_BROKER_NOT_AVAILABLE
    $ERROR_CANNOT_BIND
    $ERROR_CANNOT_GET_METADATA
    $ERROR_CANNOT_RECV
    $ERROR_CANNOT_SEND
    $ERROR_COMPRESSION
    $ERROR_CONSUMER_COORDINATOR_NOT_AVAILABLE_CODE
    $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE
    $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE_CODE
    $ERROR_INVALID_MESSAGE
    $ERROR_CORRUPT_MESSAGE
    $ERROR_INVALID_FETCH_SIZE
    $ERROR_INVALID_MESSAGE_SIZE
    $ERROR_LEADER_NOT_AVAILABLE
    $ERROR_LEADER_NOT_FOUND
    $ERROR_GROUP_COORDINATOR_NOT_FOUND
    $ERROR_LOAD_IN_PROGRESS_CODE
    $ERROR_GROUP_LOAD_IN_PROGRESS
    $ERROR_GROUP_LOAD_IN_PROGRESS_CODE
    $ERROR_MESSAGE_SIZE_TOO_LARGE
    $ERROR_MESSAGE_TOO_LARGE
    $ERROR_NETWORK_EXCEPTION
    $ERROR_METADATA_ATTRIBUTES
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_MISMATCH_CORRELATIONID
    $ERROR_NO_CONNECTION
    $ERROR_NO_ERROR
    $ERROR_NO_KNOWN_BROKERS
    $ERROR_NOT_BINARY_STRING
    $ERROR_NOT_LEADER_FOR_PARTITION
    $ERROR_NOT_COORDINATOR_FOR_CONSUMER_CODE
    $ERROR_NOT_COORDINATOR_FOR_GROUP
    $ERROR_NOT_COORDINATOR_FOR_GROUP_CODE
    $ERROR_OFFSET_METADATA_TOO_LARGE
    $ERROR_OFFSET_METADATA_TOO_LARGE_CODE
    $ERROR_OFFSET_OUT_OF_RANGE
    $ERROR_PARTITION_DOES_NOT_MATCH
    $ERROR_REPLICA_NOT_AVAILABLE
    $ERROR_REQUEST_OR_RESPONSE
    $ERROR_REQUEST_TIMED_OUT
    $ERROR_RESPONSEMESSAGE_NOT_RECEIVED
    $ERROR_INCOMPATIBLE_HOST_IP_VERSION
    $ERROR_SEND_NO_ACK
    $ERROR_STALE_CONTROLLER_EPOCH
    $ERROR_STALE_CONTROLLER_EPOCH_CODE
    $ERROR_TOPIC_DOES_NOT_MATCH
    $ERROR_UNKNOWN
    $ERROR_UNKNOWN_APIKEY
    $ERROR_UNKNOWN_TOPIC_OR_PARTITION
    $ERROR_INVALID_TOPIC_CODE
    $ERROR_INVALID_TOPIC_EXCEPTION
    $ERROR_RECORD_LIST_TOO_LARGE
    $ERROR_RECORD_LIST_TOO_LARGE_CODE
    $ERROR_NOT_ENOUGH_REPLICAS
    $ERROR_NOT_ENOUGH_REPLICAS_CODE
    $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND
    $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND_CODE
    $ERROR_INVALID_REQUIRED_ACKS
    $ERROR_INVALID_REQUIRED_ACKS_CODE
    $ERROR_ILLEGAL_GENERATION
    $ERROR_ILLEGAL_GENERATION_CODE
    $ERROR_INCONSISTENT_GROUP_PROTOCOL
    $ERROR_INCONSISTENT_GROUP_PROTOCOL_CODE
    $ERROR_INVALID_GROUP_ID
    $ERROR_INVALID_GROUP_ID_CODE
    $ERROR_UNKNOWN_MEMBER_ID
    $ERROR_UNKNOWN_MEMBER_ID_CODE
    $ERROR_INVALID_SESSION_TIMEOUT
    $ERROR_INVALID_SESSION_TIMEOUT_CODE
    $ERROR_REBALANCE_IN_PROGRESS
    $ERROR_REBALANCE_IN_PROGRESS_CODE
    $ERROR_INVALID_COMMIT_OFFSET_SIZE
    $ERROR_INVALID_COMMIT_OFFSET_SIZE_CODE
    $ERROR_TOPIC_AUTHORIZATION_FAILED
    $ERROR_TOPIC_AUTHORIZATION_FAILED_CODE
    $ERROR_GROUP_AUTHORIZATION_FAILED
    $ERROR_GROUP_AUTHORIZATION_FAILED_CODE
    $ERROR_CLUSTER_AUTHORIZATION_FAILED
    $ERROR_CLUSTER_AUTHORIZATION_FAILED_CODE
    $ERROR_INVALID_TIMESTAMP
    $ERROR_UNSUPPORTED_SASL_MECHANISM
    $ERROR_ILLEGAL_SASL_STATE
    $ERROR_UNSUPPORTED_VERSION
    $IP_V4
    $IP_V6
    $KAFKA_SERVER_PORT
    $MESSAGE_SIZE_OVERHEAD
    $MIN_BYTES_RESPOND_HAS_DATA
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_EARLIEST_OFFSET
    $RECEIVE_EARLIEST_OFFSETS
    $RECEIVE_LATEST_OFFSET
    $RECEIVE_LATEST_OFFSETS
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
    $SEND_MAX_ATTEMPTS
    $WAIT_WRITTEN_TO_LOCAL_LOG
);



use Config;
use Const::Fast;



=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka qw(
        $BITS64
    );
    use Kafka::Connection;
    use Kafka::Producer;
    use Kafka::Consumer;

    # A simple example of Kafka usage

    # common information
    say 'This is Kafka package ', $Kafka::VERSION;
    say 'You have a ', $BITS64 ? '64' : '32', ' bit system';

    my ( $connection, $producer, $consumer );
    try {

        #-- Connect to local cluster
        $connection = Kafka::Connection->new( host => 'localhost' );
        #-- Producer
        $producer = Kafka::Producer->new( Connection => $connection );
        #-- Consumer
        $consumer = Kafka::Consumer->new( Connection  => $connection );

    } catch {
        my $error = $_;
        if ( blessed( $error ) && $error->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $error->code, ') ',  $error->message, "\n";
            exit;
        } else {
            die $error;
        }
    };

    # cleaning up
    undef $consumer;
    undef $producer;
    $connection->close;
    undef $connection;

    # another brief code example of the Kafka package
    # is provided in the "An Example" section.

=head1 ABSTRACT

The Kafka package is a set of Perl modules which provides a simple and
consistent application programming interface (API) to Apache Kafka 0.9+,
a high-throughput distributed messaging system.

=head1 DESCRIPTION

The user modules in this package provide an object oriented API.
The IO agents, requests sent, and responses received from the Apache Kafka
or mock servers are all represented by objects.
This makes a simple and powerful interface to these services.

The main features of the package are:

=over 3

=item *

Contains various reusable components (modules) that can be used separately
or together.

=item *

Provides an object oriented model of communication.

=item *

Supports parsing the Apache Kafka protocol.

=item *

Supports the Apache Kafka Requests and Responses. Within this package the
following implements of Kafka's protocol are implemented: PRODUCE, FETCH,
OFFSETS, and METADATA.

=item *

Simple producer and consumer clients.

=item *

A simple interface to control the test Kafka server cluster
(in the test directory).

=item *

Simple mock server instance (located in the test directory) for testing without
Apache Kafka server.

=item *

Support for working with 64 bit elements of the Kafka protocol on 32 bit systems.

=item *

Taint mode support.
The input data is not checked for tainted.
Returns untainted data.

=back

=head1 APACHE KAFKA'S STYLE COMMUNICATION

The Kafka package is based on Kafka's 0.9+ Protocol specification document at
L<https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol>

=over 3

=item

The Kafka's protocol is based on a request/response paradigm.
A client establishes a connection with a server and sends a request to the
server in the form of a request method, followed by a messages containing
request modifiers. The server responds with a success or error code,
followed by a messages containing entity meta-information and content.

Messages are the fundamental unit of communication. They are published to
a topic by a producer, which means they are physically sent to a server acting
as a broker. Some number of consumers subscribe to a topic, and each published
message is delivered to all the consumers.
The messages stream is partitioned on the brokers as a set of distinct
partitions. The semantic meaning of these partitions is left up to the producer
and the producer specifies which partition a message belongs to. Within
a partition the messages are stored in the order in which they arrive at the
broker, and will be given out to consumers in that same order.
In Apache Kafka, the consumers are responsible for maintaining state information
(offset) on what has been consumed.
A consumer can deliberately rewind back to an old offset and re-consume data.
Each message is uniquely identified by a 64-bit integer offset giving the
position of the start of this message in the stream of all messages ever sent
to that topic on that partition.
Reads are done by giving the 64-bit logical offset of a message and a max
chunk size.

The request is then passed through the client to a server and we get the
response in return to a consumer request that we can examine.
A request is always independent of any previous requests, i.e. the service
is stateless.
This API is completely stateless, with the topic and partition being passed in
on every request.

=back

=head2 The Connection Object

Clients use the Connection object to communicate with the Apache Kafka cluster.
The Connection object is an interface layer between your application code and
the Apache Kafka cluster.

Connection object is required to create instances of classes
L<Kafka::Producer|Kafka::Producer> or L<Kafka::Consumer|Kafka::Consumer>.

Kafka Connection API is implemented by L<Kafka::Connection|Kafka::Connection> class.

    use Kafka::Connection;

    # connect to local cluster with the defaults
    my $connection = Kafka::Connection->new( host => 'localhost' );

The main attributes of the Connection object are:

=over 3

=item *

B<host> and B<port> are the IO object attributes denoting any server from the Kafka
cluster a client wants to connect.

=item *

B<timeout> specifies how much time remote servers is given to respond before
disconnection occurs and internal exception is thrown.

=back

=head2 The IO Object

The L<Kafka::Connection|Kafka::Connection> object use internal class L<Kafka::IO|Kafka::IO>
to maintain communication with the particular server of Kafka cluster
The IO object is an interface layer between L<Kafka::Connection|Kafka::Connection> object and
the network.

Kafka IO API is implemented by L<Kafka::IO|Kafka::IO> class. Note that end user
normally should have no need to use L<Kafka::IO|Kafka::IO> but work with
L<Kafka::Connection|Kafka::Connection> instead.

    use Kafka::IO;

    # connect to local server with the defaults
    my $io = Kafka::IO->new( host => 'localhost' );

The main attributes of the IO object are:

=over 3

=item *

B<host> and B<port> are the IO object attributes denoting the server and
the port of Apache Kafka server.

=item *

B<timeout> specifies how much time is given remote servers to respond before
the IO object disconnects and generates an internal exception.

=back

=head2 The Producer Object

Kafka producer API is implemented by L<Kafka::Producer|Kafka::Producer> class.

    use Kafka::Producer;

    #-- Producer
    my $producer = Kafka::Producer->new( Connection => $connection );

    # Sending a single message
    $producer->send(
        'mytopic',          # topic
        0,                  # partition
        'Single message'    # message
    );

    # Sending a series of messages
    $producer->send(
        'mytopic',          # topic
        0,                  # partition
        [                   # messages
            'The first message',
            'The second message',
            'The third message',
        ]
    );

The main methods and attributes of the producer request are:

=over 3

=item *

The request method of the producer object is C<send()>.

=item *

B<topic> and B<partition> define respective parameters of the B<messages> we
want to send.

=item *

B<messages> is an arbitrary amount of data (a simple data string or reference to
an array of the data strings).

=back

=head2 The Consumer Object

Kafka consumer API is implemented by L<Kafka::Consumer|Kafka::Consumer> class.

    use Kafka::Consumer;

    $consumer = Kafka::Consumer->new( Connection => $connection );

The request methods of the consumer object are C<offsets()> and C<fetch()>.

C<offsets> method returns a reference to the list of offsets of received messages.

C<fetch> method returns a reference to the list of received
L<Kafka::Message|Kafka::Message> objects.

    use Kafka qw(
        $DEFAULT_MAX_BYTES
        $DEFAULT_MAX_NUMBER_OF_OFFSETS
        $RECEIVE_EARLIEST_OFFSET
    );

    # Get a list of valid offsets up to max_number before the given time
    my $offsets = $consumer->offsets(
        'mytopic',                      # topic
        0,                              # partition
        $RECEIVE_EARLIEST_OFFSET,      # time
        $DEFAULT_MAX_NUMBER_OF_OFFSETS  # max_number
    );
    say "Received offset: $_" foreach @$offsets;

    # Consuming messages
    my $messages = $consumer->fetch(
        'mytopic',                      # topic
        0,                              # partition
        0,                              # offset
        $DEFAULT_MAX_BYTES              # Maximum size of MESSAGE(s) to receive
    );
    foreach my $message ( @$messages ) {
        if ( $message->valid ) {
            say 'payload    : ', $message->payload;
            say 'key        : ', $message->key;
            say 'offset     : ', $message->offset;
            say 'next_offset: ', $message->next_offset;
        } else {
            say 'error      : ', $message->error;
        }
    }

See L<Kafka::Consumer|Kafka::Consumer> for additional information and documentation about
class methods and arguments.

=head2 The Message Object

Kafka message API is implemented by L<Kafka::Message|Kafka::Message> class.

    if ( $message->valid ) {
        say 'payload    : ', $message->payload;
        say 'key        : ', $message->key;
        say 'offset     : ', $message->offset;
        say 'next_offset: ', $message->next_offset;
    } else {
        say 'error      : ', $message->error;
    }

Methods available for L<Kafka::Message|Kafka::Message> object :

=over 3

=item *

C<payload> A simple message received from the Apache Kafka server.

=item *

C<key> An optional message key that was used for partition assignment.

=item *

C<valid> A message entry is valid.

=item *

C<error> A description of the message inconsistence.

=item *

C<offset> The offset beginning of the message in the Apache Kafka server.

=item *

C<next_offset> The offset beginning of the next message in the Apache Kafka
server.

=back

=head2 The Exception Object

A designated class C<Kafka::Exception> is used to provide a more detailed and
structured information when error is detected.

The following attributes are declared within C<Kafka::Exception>:
L<code|Kafka::Exceptions/code>, L<message|Kafka::Exceptions/message>.

Additional subclasses of C<Kafka::Exception> designed to report errors in respective
Kafka classes:
C<Kafka::Exception::Connection>,
C<Kafka::Exception::Consumer>,
C<Kafka::Exception::IO>,
C<Kafka::Exception::Int64>,
C<Kafka::Exception::Producer>.

Authors suggest using of L<Try::Tiny|Try::Tiny>'s C<try> and C<catch> to handle exceptions while
working with Kafka module.

=cut

=head1 EXPORT

None by default.

=head2 Additional constants

Additional constants are available for import, which can be used to define some
type of parameters, and to identify various error cases.

=over

=item C<$KAFKA_SERVER_PORT>

default Apache Kafka server port - 9092.

=cut
const our $KAFKA_SERVER_PORT                    => 9092;

=item C<$REQUEST_TIMEOUT>

1.5 sec - timeout in secs, for C<gethostbyname>, C<connect>, blocking C<receive> and
C<send> calls (could be any integer or floating-point type).

=cut
const our $REQUEST_TIMEOUT                      => 1.5;

# Important configuration properties

=item C<$DEFAULT_MAX_BYTES>

1MB - maximum size of message(s) to receive.

=cut
const our $DEFAULT_MAX_BYTES                    => 1_000_000;

=item C<$SEND_MAX_ATTEMPTS>

4 - The leader may be unavailable transiently, which can fail the sending of a message.
This property specifies the number of attempts to send of a message.

Do not use C<$Kafka::SEND_MAX_ATTEMPTS> in C<Kafka::Producer-<gt>send> request to prevent duplicates.

=cut
const our $SEND_MAX_ATTEMPTS                    => 4;

=item C<$RETRY_BACKOFF>

200 - (ms)

According to Apache Kafka documentation:

Producer Configs -
Before each retry, the producer refreshes the metadata of relevant topics.
Since leader election takes a bit of time, this property specifies the amount of time
that the producer waits before refreshing the metadata.

Consumer Configs -
Backoff time to wait before trying to determine the leader of a partition that has just lost its leader.

=cut
const our $RETRY_BACKOFF                        => 200;

# Used to ask for all messages before a certain time (ms). There are two special values.

=item C<$RECEIVE_LATEST_OFFSET>

B<DEPRECATED>: please use C<$RECEIVE_LATEST_OFFSETS>, as when using this
constant to retrieve offsets, you can get more than one. It's kept for backward
compatibility.

-1 : special value that denotes latest available offset.

=cut
const our $RECEIVE_LATEST_OFFSET                => -1;  # deprecated, this may return multiple offsets, so the naming is wrong).

=item C<$RECEIVE_LATEST_OFFSETS>

-1 : special value that denotes latest available offsets.

=cut
const our $RECEIVE_LATEST_OFFSETS                => -1;  # to receive the latest offsets.

=item C<$RECEIVE_EARLIEST_OFFSET>

-2 : special value that denotes earliest available offset.

=cut
const our $RECEIVE_EARLIEST_OFFSET             => -2;

=item C<$RECEIVE_EARLIEST_OFFSETS>

B<DEPRECATED>: please use C<$RECEIVE_EARLIEST_OFFSET>, as when using this
constant to retrieve offset, you can get only one. It's kept for backward
compatibility.

-2 : special value that denotes earliest available offset.

=cut
const our $RECEIVE_EARLIEST_OFFSETS             => -2;

=item C<$DEFAULT_MAX_NUMBER_OF_OFFSETS>

100 - maximum number of offsets to retrieve.

=cut
const our $DEFAULT_MAX_NUMBER_OF_OFFSETS        => 100;

=item C<$MIN_BYTES_RESPOND_IMMEDIATELY>

The minimum number of bytes of messages that must be available to give a response.

0 - the server will always respond immediately.

=cut
const our $MIN_BYTES_RESPOND_IMMEDIATELY        => 0;

=item C<$MIN_BYTES_RESPOND_HAS_DATA>

The minimum number of bytes of messages that must be available to give a response.

10 - the server will respond as soon as at least one partition has at least 10 bytes of data
(Offset => int64 + MessageSize => int32)
or the specified timeout occurs.

=cut
const our $MIN_BYTES_RESPOND_HAS_DATA           => 10;

=item C<$NOT_SEND_ANY_RESPONSE>

Indicates how many acknowledgements the servers should receive before responding to the request.

0 - the server does not send any response.

=cut
const our $NOT_SEND_ANY_RESPONSE                => 0;

=item C<$WAIT_WRITTEN_TO_LOCAL_LOG>

Indicates how long the servers should wait for the data to be written to the local long before responding to the request.

1 - the server will wait the data is written to the local log before sending a response.

=cut
const our $WAIT_WRITTEN_TO_LOCAL_LOG            => 1;

=item C<$BLOCK_UNTIL_IS_COMMITTED>

Wait for message to be committed by all sync replicas.

-1 - the server will block until the message is committed by all in sync replicas before sending a response.

=cut
const our $BLOCK_UNTIL_IS_COMMITTED              => -1;

=item C<$DEFAULT_MAX_WAIT_TIME>

The maximum amount of time (seconds, may be fractional) to wait when no sufficient amount of data is available
at the time the request is dispatched.

0.1 - allow the server to wait up to 0.1s to try to accumulate data before responding.

=cut
const our $DEFAULT_MAX_WAIT_TIME                => 0.1;

=item C<$MESSAGE_SIZE_OVERHEAD>

34 - size of protocol overhead (data added by protocol) for each message.

=back

=cut
# Look at the structure of 'Message sets'
# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
# for example the case with an empty key:
# Message format:
# v0
# Message => Crc MagicByte Attributes Key Value
#   Crc => int32
#   MagicByte => int8
#   Attributes => int8
#   Key => bytes
#   Value => bytes
#
# MessageSet => [Offset MessageSize Message]
#   00:00:00:00:00:00:00:00:        # Offset => int64
#   00:00:00:14:                    # MessageSize => int32 (a size 0x14 = 20 bytes)
# Message => Crc MagicByte Attributes Key Value
#   8d:c7:95:a2:                    # Crc => int32
#   00:                             # MagicByte => int8
#   00:                             # Attributes => int8 (the lowest 2 bits - Compression None)
#   ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
#   00:00:00:06:                    # Value => bytes (a length 0x6 = 6 bytes)
#
# v1 (supported since 0.10.0)
# Message => Crc MagicByte Attributes Key Value
#   Crc => int32
#   MagicByte => int8
#   Attributes => int8
#   Timestamp => int64              # new since 0.10.0
#   Key => bytes
#   Value => bytes
const our $MESSAGE_SIZE_OVERHEAD                => 34;

=pod

=head2 IP version

Specify IP protocol version for resolving of IP address and host names.

=over

=item C<$IP_V4>

Interpret address as IPv4 and force resolving of host name in IPv4.

=cut
const our $IP_V4                                => 4;

=item C<$IP_V6>

Interpret address as IPv6 and force resolving of host name in IPv6.

=back

=cut
const our $IP_V6                                => 6;

#-- Codec numbers:

=pod

=head2 Compression

According to Apache Kafka documentation:

Kafka currently supports two compression codecs with the following codec numbers:

=over

=item C<$COMPRESSION_NONE>

None = 0

=cut
const our $COMPRESSION_NONE             => 0;

=item C<$COMPRESSION_GZIP>

GZIP = 1

=cut
const our $COMPRESSION_GZIP             => 1;

=item C<$COMPRESSION_SNAPPY>

Snappy = 2

=cut
const our $COMPRESSION_SNAPPY           => 2;

=back

=head2 Error codes

Possible error codes (corresponds to descriptions in C<%ERROR>):

=over

=item C<$ERROR_MISMATCH_ARGUMENT>

-1000 - Invalid argument

=cut
const our $ERROR_MISMATCH_ARGUMENT              => -1000;

=item C<$ERROR_CANNOT_SEND>

-1001 - Cannot send

=cut
const our $ERROR_CANNOT_SEND                    => -1001;

=item C<$ERROR_SEND_NO_ACK>

-1002 - No acknowledgement for sent request

=cut
const our $ERROR_SEND_NO_ACK                    => -1002;

=item C<ERROR_CANNOT_RECV>

-1.05 - Cannot receive

=cut
const our $ERROR_CANNOT_RECV                    => -1.05;

=item C<ERROR_CANNOT_BIND>

-1004 - Cannot connect to broker

=cut
const our $ERROR_CANNOT_BIND                    => -1004;

=item C<$ERROR_METADATA_ATTRIBUTES>

-1005 - Unknown metadata attributes

=cut
const our $ERROR_METADATA_ATTRIBUTES            => -1005;

=item C<$ERROR_UNKNOWN_APIKEY>

-1006 - Unknown ApiKey

=cut
const our $ERROR_UNKNOWN_APIKEY                 => -1006;

=item C<$ERROR_CANNOT_GET_METADATA>

-1007 - Cannot get Metadata

=cut
const our $ERROR_CANNOT_GET_METADATA            => -1007;

=item C<$ERROR_LEADER_NOT_FOUND>

-1008 - Leader not found

=cut
const our $ERROR_LEADER_NOT_FOUND               => -1008;

=item C<$ERROR_MISMATCH_CORRELATIONID>

-1009 - Mismatch CorrelationId

=cut
const our $ERROR_MISMATCH_CORRELATIONID         => -1009;

=item C<$ERROR_NO_KNOWN_BROKERS>

-1010 - There are no known brokers

=cut
const our $ERROR_NO_KNOWN_BROKERS               => -1010;

=item C<$ERROR_REQUEST_OR_RESPONSE>

-1011 - Bad request or response element

=cut
const our $ERROR_REQUEST_OR_RESPONSE            => -1011;

=item C<$ERROR_TOPIC_DOES_NOT_MATCH>

-1012 - Topic does not match the requested

=cut
const our $ERROR_TOPIC_DOES_NOT_MATCH           => -1012;

=item C<$ERROR_PARTITION_DOES_NOT_MATCH>

-1013 - Partition does not match the requested

=cut
const our $ERROR_PARTITION_DOES_NOT_MATCH       => -1013;

=item C<$ERROR_NOT_BINARY_STRING>

-1014 - Not binary string

=cut
const our $ERROR_NOT_BINARY_STRING              => -1014;

=item C<$ERROR_COMPRESSION>

-1015 - Compression error

=cut
const our $ERROR_COMPRESSION                    => -1015;

=item C<$ERROR_RESPONSEMESSAGE_NOT_RECEIVED>

-1016 - 'ResponseMessage' not received

=cut
const our $ERROR_RESPONSEMESSAGE_NOT_RECEIVED   => -1016;

=item C<$ERROR_INCOMPATIBLE_HOST_IP_VERSION>

-1017 - Incompatible host name and IP version

=cut
const our $ERROR_INCOMPATIBLE_HOST_IP_VERSION   => -1017;


=item C<$ERROR_NO_CONNECTION>

-1018 - No IO connection

=cut
const our $ERROR_NO_CONNECTION   => -1018;

=item C<$ERROR_GROUP_COORDINATOR_NOT_FOUND>

-1008 - Group Coordinator not found

=cut
const our $ERROR_GROUP_COORDINATOR_NOT_FOUND               => -1019;

=back

Contains the descriptions of possible error codes obtained via ERROR_CODE box
of Apache Kafka Wire Format protocol response.

=over

=cut

#-- The Protocol Error Codes

# According
# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes

=item C<$ERROR_NO_ERROR>

0 - C<q{}>

No error - it worked!

=cut
const our $ERROR_NO_ERROR                       => 0;

=item C<$ERROR_UNKNOWN>

-1 - An unexpected server error.

=cut
const our $ERROR_UNKNOWN                        => -1;

=item C<$ERROR_OFFSET_OUT_OF_RANGE>

1 - The requested offset is not within the range of offsets maintained by the server.

=cut
const our $ERROR_OFFSET_OUT_OF_RANGE            => 1;

=item C<$ERROR_INVALID_MESSAGE>

2 - This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.

Synonym name $ERROR_CORRUPT_MESSAGE .

=cut
const our $ERROR_INVALID_MESSAGE                => 2;
const our $ERROR_CORRUPT_MESSAGE                => 2;

=item C<$ERROR_UNKNOWN_TOPIC_OR_PARTITION>

3 - This server does not host this topic-partition.

=cut
const our $ERROR_UNKNOWN_TOPIC_OR_PARTITION     => 3;

=item C<$ERROR_INVALID_FETCH_SIZE>

4 - The requested fetch size is invalid.

Synonym name $ERROR_INVALID_MESSAGE_SIZE .

=cut
const our $ERROR_INVALID_MESSAGE_SIZE           => 4;
const our $ERROR_INVALID_FETCH_SIZE             => 4;

=item C<$ERROR_LEADER_NOT_AVAILABLE>

5 - Unable to write due to ongoing Kafka leader selection.

This error is thrown if we are in the middle of a leadership election and there is
no current leader for this partition, hence it is unavailable for writes.

=cut
const our $ERROR_LEADER_NOT_AVAILABLE           => 5;

=item C<$ERROR_NOT_LEADER_FOR_PARTITION>

6 - Server is not a leader for partition.

This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition.
It indicates that the clients metadata is out of date.

=cut
const our $ERROR_NOT_LEADER_FOR_PARTITION       => 6;

=item C<$ERROR_REQUEST_TIMED_OUT>

7 - Request time-out.

This error is thrown if the request exceeds the user-specified time limit in the request.

=cut
const our $ERROR_REQUEST_TIMED_OUT              => 7;

=item C<$ERROR_BROKER_NOT_AVAILABLE>

8 - Broker is not available.

This is not a client facing error and is used mostly by tools when a broker is not alive.

=cut
const our $ERROR_BROKER_NOT_AVAILABLE           => 8;

=item C<$ERROR_REPLICA_NOT_AVAILABLE>

9 - The replica is not available for the requested topic-partition.

If replica is expected on a broker, but is not (this can be safely ignored).

=cut
const our $ERROR_REPLICA_NOT_AVAILABLE          => 9;

=item C<$ERROR_MESSAGE_TOO_LARGE>

10 - The request included a message larger than the max message size the server will accept.

The server has a configurable maximum message size to avoid unbounded memory allocation.
This error is thrown if the client attempt to produce a message larger than this maximum.

Synonym name $ERROR_MESSAGE_SIZE_TOO_LARGE .

=cut
const our $ERROR_MESSAGE_SIZE_TOO_LARGE         => 10;
const our $ERROR_MESSAGE_TOO_LARGE              => 10;

=item C<$ERROR_STALE_CONTROLLER_EPOCH>

11 - The controller moved to another broker.

According to Apache Kafka documentation:
Internal error code for broker-to-broker communication.

Synonym name $ERROR_STALE_CONTROLLER_EPOCH_CODE .

=cut
const our $ERROR_STALE_CONTROLLER_EPOCH_CODE    => 11;
const our $ERROR_STALE_CONTROLLER_EPOCH         => 11;

=item C<$ERROR_OFFSET_METADATA_TOO_LARGE>

12 - Specified metadata offset is too big

If you specify a value larger than configured maximum for offset metadata.

Synonym name $ERROR_OFFSET_METADATA_TOO_LARGE_CODE .

=cut
const our $ERROR_OFFSET_METADATA_TOO_LARGE      => 12;
const our $ERROR_OFFSET_METADATA_TOO_LARGE_CODE => 12;

=item C<$ERROR_NETWORK_EXCEPTION>

13 - The server disconnected before a response was received.

=cut
const our $ERROR_NETWORK_EXCEPTION              => 13;

=item C<$ERROR_GROUP_LOAD_IN_PROGRESS>

14 - The coordinator is loading and hence can't process requests for this group.

Synonym name $ERROR_GROUP_LOAD_IN_PROGRESS_CODE, $ERROR_LOAD_IN_PROGRESS_CODE .

=cut
const our $ERROR_LOAD_IN_PROGRESS_CODE          => 14;
const our $ERROR_GROUP_LOAD_IN_PROGRESS         => 14;
const our $ERROR_GROUP_LOAD_IN_PROGRESS_CODE    => 14;

=item C<$ERROR_GROUP_COORDINATOR_NOT_AVAILABLE>

15 - The group coordinator is not available.

Synonym name $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE_CODE, $ERROR_CONSUMER_COORDINATOR_NOT_AVAILABLE_CODE .

=cut
const our $ERROR_CONSUMER_COORDINATOR_NOT_AVAILABLE_CODE    => 15;
const our $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE            => 15;
const our $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE_CODE       => 15;

=item C<$ERROR_NOT_COORDINATOR_FOR_GROUP>

16 - This is not the correct coordinator for this group.

Synonym name $ERROR_NOT_COORDINATOR_FOR_GROUP_CODE, $ERROR_NOT_COORDINATOR_FOR_CONSUMER_CODE .

=cut
const our $ERROR_NOT_COORDINATOR_FOR_CONSUMER_CODE  => 16;
const our $ERROR_NOT_COORDINATOR_FOR_GROUP          => 16;
const our $ERROR_NOT_COORDINATOR_FOR_GROUP_CODE     => 16;

=item C<$ERROR_INVALID_TOPIC_EXCEPTION>

17 - The request attempted to perform an operation on an invalid topic.

Synonym name $ERROR_INVALID_TOPIC_CODE .

=cut
const our $ERROR_INVALID_TOPIC_CODE                 => 17;
const our $ERROR_INVALID_TOPIC_EXCEPTION            => 17;

=item C<$ERROR_RECORD_LIST_TOO_LARGE>

18 - The request included message batch larger than the configured segment size on the server.

Synonym name $ERROR_RECORD_LIST_TOO_LARGE_CODE .

=cut
const our $ERROR_RECORD_LIST_TOO_LARGE              => 18;
const our $ERROR_RECORD_LIST_TOO_LARGE_CODE         => 18;

=item C<$ERROR_NOT_ENOUGH_REPLICAS>

19 - Messages are rejected since there are fewer in-sync replicas than required.

Synonym name $ERROR_NOT_ENOUGH_REPLICAS_CODE .

=cut
const our $ERROR_NOT_ENOUGH_REPLICAS                => 19;
const our $ERROR_NOT_ENOUGH_REPLICAS_CODE           => 19;

=item C<$ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND>

20 - Messages are written to the log, but to fewer in-sync replicas than required.

Synonym name $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND_CODE .

=cut
const our $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND       => 20;
const our $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND_CODE  => 20;

=item C<$ERROR_INVALID_REQUIRED_ACKS>

21 - Produce request specified an invalid value for required acks.

Synonym name $ERROR_INVALID_REQUIRED_ACKS_CODE .

=cut
const our $ERROR_INVALID_REQUIRED_ACKS              => 21;
const our $ERROR_INVALID_REQUIRED_ACKS_CODE         => 21;

=item C<$ERROR_ILLEGAL_GENERATION>

22 - Specified group generation id is not valid.

Synonym name $ERROR_ILLEGAL_GENERATION_CODE .

=cut
const our $ERROR_ILLEGAL_GENERATION                 => 22;
const our $ERROR_ILLEGAL_GENERATION_CODE            => 22;

=item C<$ERROR_INCONSISTENT_GROUP_PROTOCOL>

23 - The group member's supported protocols are incompatible with those of existing members.

Synonym name $ERROR_INCONSISTENT_GROUP_PROTOCOL_CODE .

=cut
const our $ERROR_INCONSISTENT_GROUP_PROTOCOL        => 23;
const our $ERROR_INCONSISTENT_GROUP_PROTOCOL_CODE   => 23;

=item C<$ERROR_INVALID_GROUP_ID>

24 - The configured groupId is invalid.

Synonym name $ERROR_INVALID_GROUP_ID_CODE .

=cut
const our $ERROR_INVALID_GROUP_ID                   => 24;
const our $ERROR_INVALID_GROUP_ID_CODE              => 24;

=item C<$ERROR_UNKNOWN_MEMBER_ID>

25 - The coordinator is not aware of this member.

Synonym name $ERROR_UNKNOWN_MEMBER_ID_CODE .

=cut
const our $ERROR_UNKNOWN_MEMBER_ID                  => 25;
const our $ERROR_UNKNOWN_MEMBER_ID_CODE             => 25;

=item C<$ERROR_INVALID_SESSION_TIMEOUT>

26 - The session timeout is not within the range allowed by the broker
(as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).

Synonym name $ERROR_INVALID_SESSION_TIMEOUT_CODE .

=cut
const our $ERROR_INVALID_SESSION_TIMEOUT            => 26;
const our $ERROR_INVALID_SESSION_TIMEOUT_CODE       => 26;

=item C<$ERROR_REBALANCE_IN_PROGRESS>

27 - The group is rebalancing, so a rejoin is needed.

Synonym name $ERROR_REBALANCE_IN_PROGRESS_CODE .

=cut
const our $ERROR_REBALANCE_IN_PROGRESS              => 27;
const our $ERROR_REBALANCE_IN_PROGRESS_CODE         => 27;

=item C<$ERROR_INVALID_COMMIT_OFFSET_SIZE>

28 - The committing offset data size is not valid.

Synonym name $ERROR_INVALID_COMMIT_OFFSET_SIZE_CODE .

=cut
const our $ERROR_INVALID_COMMIT_OFFSET_SIZE         => 28;
const our $ERROR_INVALID_COMMIT_OFFSET_SIZE_CODE    => 28;

=item C<$ERROR_TOPIC_AUTHORIZATION_FAILED>

29 - Not authorized to access topics: [Topic authorization failed.].

Synonym name $ERROR_TOPIC_AUTHORIZATION_FAILED_CODE .

=cut
const our $ERROR_TOPIC_AUTHORIZATION_FAILED         => 29;
const our $ERROR_TOPIC_AUTHORIZATION_FAILED_CODE    => 29;

=item C<$ERROR_GROUP_AUTHORIZATION_FAILED>

30 - Not authorized to access group: Group authorization failed.

Synonym name $ERROR_GROUP_AUTHORIZATION_FAILED_CODE .

=cut
const our $ERROR_GROUP_AUTHORIZATION_FAILED         => 30;
const our $ERROR_GROUP_AUTHORIZATION_FAILED_CODE    => 30;

=item C<$ERROR_CLUSTER_AUTHORIZATION_FAILED>

31 - Cluster authorization failed.

Synonym name $ERROR_CLUSTER_AUTHORIZATION_FAILED_CODE .

=cut
const our $ERROR_CLUSTER_AUTHORIZATION_FAILED       => 31;
const our $ERROR_CLUSTER_AUTHORIZATION_FAILED_CODE  => 31;

=item C<$ERROR_INVALID_TIMESTAMP>

32 - The timestamp of the message is out of acceptable range.

=cut
const our $ERROR_INVALID_TIMESTAMP                  => 32;

=item C<$ERROR_UNSUPPORTED_SASL_MECHANISM>

33 - The broker does not support the requested SASL mechanism.

=cut
const our $ERROR_UNSUPPORTED_SASL_MECHANISM         => 33;

=item C<$ERROR_ILLEGAL_SASL_STATE>

34 - Request is not valid given the current SASL state.

=cut
const our $ERROR_ILLEGAL_SASL_STATE                 => 34;

=item C<$ERROR_UNSUPPORTED_VERSION>

35 - The version of API is not supported.

=cut
const our $ERROR_UNSUPPORTED_VERSION                => 35;

=item C<%ERROR>

Contains the descriptions for possible error codes.

=back

=cut
our %ERROR = (
    # Errors fixed by Kafka package
    $ERROR_MISMATCH_ARGUMENT                        => q{Invalid argument},
    $ERROR_CANNOT_SEND                              => q{Cannot send},
    $ERROR_SEND_NO_ACK                              => q{No acknowledgement for sent request},
    $ERROR_CANNOT_RECV                              => q{Cannot receive},
    $ERROR_CANNOT_BIND                              => q{Cannot connect to broker},
    $ERROR_METADATA_ATTRIBUTES                      => q{Unknown metadata attributes},
    $ERROR_UNKNOWN_APIKEY                           => q{Unknown ApiKey},
    $ERROR_CANNOT_GET_METADATA                      => q{Cannot get metadata},
    $ERROR_LEADER_NOT_FOUND                         => q{Leader not found},
    $ERROR_MISMATCH_CORRELATIONID                   => q{Mismatch CorrelationId},
    $ERROR_NO_KNOWN_BROKERS                         => q{There are no known brokers},
    $ERROR_REQUEST_OR_RESPONSE                      => q{Bad request or response element},
    $ERROR_TOPIC_DOES_NOT_MATCH                     => q{Topic does not match the requested},
    $ERROR_PARTITION_DOES_NOT_MATCH                 => q{Partition does not match the requested},
    $ERROR_NOT_BINARY_STRING                        => q{Not binary string},
    $ERROR_COMPRESSION                              => q{Compression error},
    $ERROR_RESPONSEMESSAGE_NOT_RECEIVED             => q{'ResponseMessage' not received},
    $ERROR_INCOMPATIBLE_HOST_IP_VERSION             => q{'Incompatible host name and IP version'},
    $ERROR_NO_CONNECTION                            => q{'No connection'},
    $ERROR_GROUP_COORDINATOR_NOT_FOUND              => q{'Group Coordinator Not Found'},

    #-- The Protocol Error Messages
    # https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
    $ERROR_NO_ERROR                                 => q{}, # 'No error--it worked!',
    $ERROR_UNKNOWN                                  => q{An unexpected server error},
    $ERROR_OFFSET_OUT_OF_RANGE                      => q{The requested offset is outside the range of offsets maintained by the server for the given topic/partition},
    $ERROR_INVALID_MESSAGE                          => q{Message contents does not match its CRC},
    $ERROR_UNKNOWN_TOPIC_OR_PARTITION               => q{Unknown topic or partition},
    $ERROR_INVALID_FETCH_SIZE                       => q{The requested fetch size is invalid},
    $ERROR_LEADER_NOT_AVAILABLE                     => q{Unable to write due to ongoing Kafka leader selection},
    $ERROR_NOT_LEADER_FOR_PARTITION                 => q{Server is not a leader for partition},
    $ERROR_REQUEST_TIMED_OUT                        => q{Request time-out},
    $ERROR_BROKER_NOT_AVAILABLE                     => q{Broker is not available},
    $ERROR_REPLICA_NOT_AVAILABLE                    => q{Replica not available},
    $ERROR_MESSAGE_TOO_LARGE                        => q{Message is too big},
    $ERROR_STALE_CONTROLLER_EPOCH                   => q{Stale Controller Epoch Code},
    $ERROR_OFFSET_METADATA_TOO_LARGE                => q{The metadata field of the offset request was too large},
    $ERROR_NETWORK_EXCEPTION                        => q{The server disconnected before a response was received},
    $ERROR_GROUP_LOAD_IN_PROGRESS                   => q{The coordinator is loading and hence can't process requests for this group},
    $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE          => q{The group coordinator is not available.},
    $ERROR_NOT_COORDINATOR_FOR_GROUP                => q{Request for a group that it is not a coordinator for},

    $ERROR_INVALID_TOPIC_EXCEPTION                  => q{A request which attempts to access an invalid topic},
    $ERROR_RECORD_LIST_TOO_LARGE                    => q{A message batch in a produce request exceeds the maximum configured segment size},
    $ERROR_NOT_ENOUGH_REPLICAS                      => q{Messages are rejected since there are fewer in-sync replicas than required},
    $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND         => q{Messages are written to the log, but to fewer in-sync replicas than required},
    $ERROR_INVALID_REQUIRED_ACKS                    => q{Produce request specified an invalid value for required acks},
    $ERROR_ILLEGAL_GENERATION                       => q{Specified group generation id is not valid},
    $ERROR_INCONSISTENT_GROUP_PROTOCOL              => q{The group member's supported protocols are incompatible with those of existing members},
    $ERROR_INVALID_GROUP_ID                         => q{The configured groupId is invalid},
    $ERROR_UNKNOWN_MEMBER_ID                        => q{The coordinator is not aware of this member},
    $ERROR_INVALID_SESSION_TIMEOUT                  => q{The session timeout is not within the range allowed by the broker},
    $ERROR_REBALANCE_IN_PROGRESS                    => q{The group is rebalancing, so a rejoin is needed},
    $ERROR_INVALID_COMMIT_OFFSET_SIZE               => q{The committing offset data size is not valid},
    $ERROR_TOPIC_AUTHORIZATION_FAILED               => q{Not authorized to access topics},
    $ERROR_GROUP_AUTHORIZATION_FAILED               => q{Not authorized to access group: Group authorization failed},
    $ERROR_CLUSTER_AUTHORIZATION_FAILED             => q{Cluster authorization failed},
    $ERROR_INVALID_TIMESTAMP                        => q{The timestamp of the message is out of acceptable range},
    $ERROR_UNSUPPORTED_SASL_MECHANISM               => q{The broker does not support the requested SASL mechanism},
    $ERROR_ILLEGAL_SASL_STATE                       => q{Request is not valid given the current SASL state},
    $ERROR_UNSUPPORTED_VERSION                      => q{The version of API is not supported},
);

=over

=item C<BITS64>

Know you are working on 64 or 32 bit system

=back

=cut
const our $BITS64   => ( defined( $Config{use64bitint} ) and $Config{use64bitint} eq 'define' ) || $Config{longsize} >= 8;

#-- public functions -----------------------------------------------------------

#-- private functions ----------------------------------------------------------

1;

__END__

=head1 An Example

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka qw(
        $KAFKA_SERVER_PORT
        $REQUEST_TIMEOUT
        $RECEIVE_EARLIEST_OFFSET
        $DEFAULT_MAX_NUMBER_OF_OFFSETS
        $DEFAULT_MAX_BYTES
    );
    use Kafka::Connection;
    use Kafka::Producer;
    use Kafka::Consumer;

    my ( $connection, $producer, $consumer );
    try {

        #-- Connection
        $connection = Kafka::Connection->new( host => 'localhost' );

        #-- Producer
        $producer = Kafka::Producer->new( Connection => $connection );

        # Sending a single message
        $producer->send(
            'mytopic',                      # topic
            0,                              # partition
            'Single message'                # message
        );

        # Sending a series of messages
        $producer->send(
            'mytopic',                      # topic
            0,                              # partition
            [                               # messages
                'The first message',
                'The second message',
                'The third message',
            ]
        );

        #-- Consumer
        $consumer = Kafka::Consumer->new( Connection => $connection );

        # Get a list of valid offsets up max_number before the given time
        my $offsets = $consumer->offsets(
            'mytopic',                      # topic
            0,                              # partition
            $RECEIVE_EARLIEST_OFFSET,      # time
            $DEFAULT_MAX_NUMBER_OF_OFFSETS  # max_number
        );

        if ( @$offsets ) {
            say "Received offset: $_" foreach @$offsets;
        } else {
            warn "Error: Offsets are not received\n";
        }

        # Consuming messages
        my $messages = $consumer->fetch(
            'mytopic',                      # topic
            0,                              # partition
            0,                              # offset
            $DEFAULT_MAX_BYTES              # Maximum size of MESSAGE(s) to receive
        );

        if ( $messages ) {
            foreach my $message ( @$messages ) {
                if ( $message->valid ) {
                    say 'payload    : ', $message->payload;
                    say 'key        : ', $message->key;
                    say 'offset     : ', $message->offset;
                    say 'next_offset: ', $message->next_offset;
                } else {
                    say 'error      : ', $message->error;
                }
            }
        }

    } catch {
        my $error = $_;
        if ( blessed( $error ) && $error->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $error->code, ') ',  $error->message, "\n";
            exit;
        } else {
            die $error;
        }
    };

    # Closes and cleans up
    undef $consumer;
    undef $producer;
    $connection->close;
    undef $connection;

=head1 DEPENDENCIES

In order to install and use this package you will need Perl version
5.10 or later. Some modules within this package depend on other
packages that are distributed separately from Perl. We recommend that
you have the following packages installed before you install
Kafka:

    Compress::Snappy
    Const::Fast
    Data::Compare
    Data::HexDump::Range
    Data::Validate::Domain
    Data::Validate::IP
    Exception::Class
    List::Utils
    Params::Util
    Scalar::Util::Numeric
    String::CRC32
    Sys::SigAction
    Try::Tiny

Kafka package has the following optional dependencies:

    Capture::Tiny
    Clone
    Config::IniFiles
    File::HomeDir
    Proc::Daemon
    Proc::ProcessTable
    Sub::Install
    Test::Deep
    Test::Exception
    Test::NoWarnings
    Test::TCP

If the optional modules are missing, some "prereq" tests are skipped.

=head1 DIAGNOSTICS

Debug output can be enabled by setting level via one of the following
environment variables:

C<PERL_KAFKA_DEBUG=1>               - debug is enabled for the whole C<Kafka> package.

C<PERL_KAFKA_DEBUG=IO:1>            - enable debug only for L<Kafka::IO|Kafka::IO> only.

C<PERL_KAFKA_DEBUG=Connection:1>    - enable debug only for particular L<Kafka::Connection|Kafka::Connection>.

It's possible to set different debug levels, like in the following example:

C<PERL_KAFKA_DEBUG=Connection:1,IO:2>

See documentation for a particular module for explanation of various debug levels.

=cut

=head1 BUGS AND LIMITATIONS

L<Connection|Kafka::Connection> constructor:

Make sure that you always connect to brokers using EXACTLY the same address or host name
as specified in broker configuration (host.name in server.properties).
Avoid using default value (when host.name is commented) in server.properties - always use explicit value instead.

L<Producer|Kafka::Producer> and L<Consumer|Kafka::Consumer> methods
only work with one topic and one partition at a time.
Also module does not implement the Offset Commit/Fetch API.

L<Producer|Kafka::Producer>'s, L<Consumer|Kafka::Consumer>'s, L<Connection|Kafka::Connection>'s
string arguments must be binary strings.
Using Unicode strings may cause an error or data corruption.

This module does not support Kafka protocol versions earlier than 0.8.

L<Kafka::IO-E<gt>new|Kafka::IO/new>' uses L<Sys::SigAction|Sys::SigAction> and C<alarm()>
to limit some internal operations. This means that if an external C<alarm()> was set, signal
delivery may be delayed.

With non-empty timeout, we use C<alarm()> internally in L<Kafka::IO|Kafka::IO> and try preserving existing C<alarm()> if possible.
However, if L<Time::HiRes::ualarm()|Time::HiRes/ualarm> is set before calling Kafka modules,
its behaviour is unspecified (i.e. it could be reset or preserved etc.).

For C<gethostbyname> operations the non-empty timeout is rounded to the nearest greater positive integer;
any timeouts less than 1 second are rounded to 1 second.

You can disable the use of C<alarm()> by setting C<timeout =E<gt> undef> in the constructor.

The Kafka package was written, tested, and found working on recent Linux distributions.

There are no known bugs in this package.

Please report problems to the L</"AUTHOR">.

Patches are welcome.

=head1 MORE DOCUMENTATION

All modules contain detailed information on the interfaces they provide.

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
