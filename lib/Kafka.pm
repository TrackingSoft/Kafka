package Kafka;

# Kafka allows you to produce and consume messages using
# the Apache Kafka distributed publish/subscribe messaging service.

=head1 NAME

Kafka - Apache Kafka interface for Perl.

=head1 VERSION

This documentation refers to C<Kafka> package version 0.800_5 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.800_5';

use Exporter qw(
    import
);

our @EXPORT_OK = qw(
    $BITS64
    $BLOCK_UNTIL_IS_COMMITTED
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    %ERROR
    $ERROR_BROKER_NOT_AVAILABLE
    $ERROR_CANNOT_BIND
    $ERROR_CANNOT_GET_METADATA
    $ERROR_CANNOT_RECV
    $ERROR_CANNOT_SEND
    $ERROR_COMPRESSED_PAYLOAD
    $ERROR_LEADER_NOT_FOUND
    $ERROR_INVALID_MESSAGE
    $ERROR_INVALID_MESSAGE_SIZE
    $ERROR_LEADER_NOT_AVAILABLE
    $ERROR_MESSAGE_SIZE_TOO_LARGE
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_MISMATCH_CORRELATIONID
    $ERROR_NO_ERROR
    $ERROR_NO_KNOWN_BROKERS
    $ERROR_NOT_BINARY_STRING
    $ERROR_NOT_LEADER_FOR_PARTITION
    $ERROR_OFFSET_METADATA_TOO_LARGE_CODE
    $ERROR_OFFSET_OUT_OF_RANGE
    $ERROR_PARTITION_DOES_NOT_MATCH
    $ERROR_REPLICA_NOT_AVAILABLE
    $ERROR_REQUEST_OR_RESPONSE
    $ERROR_REQUEST_TIMED_OUT
    $ERROR_STALE_CONTROLLER_EPOCH_CODE
    $ERROR_TOPIC_DOES_NOT_MATCH
    $ERROR_UNKNOWN
    $ERROR_UNKNOWN_APIKEY
    $ERROR_UNKNOWN_TOPIC_OR_PARTITION
    $KAFKA_SERVER_PORT
    $MESSAGE_SIZE_OVERHEAD
    $MIN_BYTES_RESPOND_HAS_DATA
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_EARLIEST_OFFSETS
    $RECEIVE_LATEST_OFFSET
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
    $SEND_MAX_RETRIES
    $WAIT_WRITTEN_TO_LOCAL_LOG
);

#-- load the modules -----------------------------------------------------------

use Config;
use Const::Fast;

#-- declarations ---------------------------------------------------------------

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
        if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $_->code, ') ',  $_->message, "\n";
            exit;
        } else {
            die $_;
        }
    };

    # cleaning up
    undef $consumer;
    undef $producer;
    undef $connection;

    # another brief code example of the Kafka package
    # is provided in the "An Example" section.

=head1 ABSTRACT

The Kafka package is a set of Perl modules which provides a simple and
consistent application programming interface (API) to Apache Kafka 0.8,
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
OFFSETS, and METADATA. Note that PRODUCE and FETCH do not support compression
at this point.

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

=back

=head1 APACHE KAFKA'S STYLE COMMUNICATION

The Kafka package is based on Kafka's 0.8 Protocol specification document at
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
        $RECEIVE_EARLIEST_OFFSETS
    );

    # Get a list of valid offsets up to max_number before the given time
    my $offsets = $consumer->offsets(
        'mytopic',                      # topic
        0,                              # partition
        $RECEIVE_EARLIEST_OFFSETS,      # time
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
        if( $message->valid ) {
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

    if( $message->valid ) {
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

C<error> A description of the message inconsistence (currently only for
compressed message).

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

Additional constants are available for import, which can be used to define some
type of parameters, and to identify various error cases.

=cut

=pod

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

=item C<$SEND_MAX_RETRIES>

3 - The leader may be unavailable transiently, which can fail the sending of a message.
This property specifies the number of retries when such failures occur.

=cut
const our $SEND_MAX_RETRIES                     => 3;

=item C<$RETRY_BACKOFF>

100 - (ms) Before each retry, the producer refreshes the metadata of relevant topics.
Since leader election takes a bit of time, this property specifies the amount of time
that the producer waits before refreshing the metadata.

=cut
const our $RETRY_BACKOFF                        => 100;

# Used to ask for all messages before a certain time (ms). There are two special values.

=item C<$RECEIVE_LATEST_OFFSET>

-1 : special value that denotes latest available offset.

=cut
const our $RECEIVE_LATEST_OFFSET                => -1;  # to receive the latest offset (this will only ever return one offset).

=item C<$RECEIVE_EARLIEST_OFFSETS>

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

1 - the server will respond as soon as at least one partition has at least 1 byte of data
or the specified timeout occurs.

=cut
const our $MIN_BYTES_RESPOND_HAS_DATA           => 1;

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

The maximum amount of time (ms) to wait when no sufficient amount of data is available at the time the request is dispatched.

100 - the server will block until the message is committed by all in sync replicas before sending a response.

=cut
const our $DEFAULT_MAX_WAIT_TIME                => 100;

=item C<$MESSAGE_SIZE_OVERHEAD>

26 - size of protocol overhead (data added by protocol) for each message.

=cut
# Look at the structure of 'Message sets'
# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
# for example the case with an empty key:
# MessageSet => [Offset MessageSize Message]
#   00:00:00:00:00:00:00:00:        # Offset => int64
#   00:00:00:14:                    # MessageSize => int32 (a size 0x14 = 20 bytes)
# Message => Crc MagicByte Attributes Key Value
#   8d:c7:95:a2:                    # Crc => int32
#   00:                             # MagicByte => int8
#   00:                             # Attributes => int8 (the last 3 bits - Compression None)
#   ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
#   00:00:00:06:                    # Value => bytes (a length 0x6 = 6 bytes)
const our $MESSAGE_SIZE_OVERHEAD                => 26;

=back

Possible error codes (complies with a hash of descriptions C<$ERROR>):

=over

=cut

#-- Errors fixed by Kafka package

=item C<$ERROR_MISMATCH_ARGUMENT>

-1000 - Invalid argument

=cut
const our $ERROR_MISMATCH_ARGUMENT              => -1000;

=item C<$ERROR_CANNOT_SEND>

-1001 - Can't send

=cut
const our $ERROR_CANNOT_SEND                    => -1001;

=item C<ERROR_CANNOT_RECV>

-1002 - Can't receive

=cut
const our $ERROR_CANNOT_RECV                    => -1002;

=item C<ERROR_CANNOT_BIND>

-1003 - Can't bind

=cut
const our $ERROR_CANNOT_BIND                    => -1003;

=item C<$ERROR_COMPRESSED_PAYLOAD>

-1004 - Compressed payload

=cut
const our $ERROR_COMPRESSED_PAYLOAD             => -1004;

=item C<$ERROR_UNKNOWN_APIKEY>

-1005 - Unknown ApiKey

=cut
const our $ERROR_UNKNOWN_APIKEY                 => -1005;

=item C<$ERROR_CANNOT_GET_METADATA>

-1006 - Can't get Metadata

=cut
const our $ERROR_CANNOT_GET_METADATA            => -1006;

=item C<$ERROR_LEADER_NOT_FOUND>

-1007 - Leader not found

=cut
const our $ERROR_LEADER_NOT_FOUND               => -1007;

=item C<$ERROR_MISMATCH_CORRELATIONID>

-1008 - Mismatch CorrelationId

=cut
const our $ERROR_MISMATCH_CORRELATIONID         => -1008;

=item C<$ERROR_NO_KNOWN_BROKERS>

-1009 - There are no known brokers

=cut
const our $ERROR_NO_KNOWN_BROKERS               => -1009;

=item C<$ERROR_REQUEST_OR_RESPONSE>

-1010 - Bad request or response element

=cut
const our $ERROR_REQUEST_OR_RESPONSE            => -1010;

=item C<$ERROR_TOPIC_DOES_NOT_MATCH>

-1011 - Topic does not match the requested

=cut
const our $ERROR_TOPIC_DOES_NOT_MATCH           => -1011;

=item C<$ERROR_PARTITION_DOES_NOT_MATCH>

-1012 - Partition does not match the requested

=cut
const our $ERROR_PARTITION_DOES_NOT_MATCH       => -1012;

=item C<$ERROR_NOT_BINARY_STRING>

-1013 - Not binary string

=cut
const our $ERROR_NOT_BINARY_STRING              => -1013;

=back

Contains the descriptions of possible error codes obtained via ERROR_CODE box
of Apache Kafka Wire Format protocol response.

=over

=cut

#-- The Protocol Error Codes

=item C<$ERROR_NO_ERROR>

0 - C<q{}>

No error

=cut
const our $ERROR_NO_ERROR                       => 0;

=item C<$ERROR_UNKNOWN>

-1 - An unexpected server error

=cut
const our $ERROR_UNKNOWN                        => -1;

=item C<$ERROR_OFFSET_OUT_OF_RANGE>

1 - The requested offset is outside the range of offsets available at the server
for the given topic/partition

=cut
const our $ERROR_OFFSET_OUT_OF_RANGE            => 1;

=item C<$ERROR_INVALID_MESSAGE>

2 - Message contents does not match its control sum

=cut
const our $ERROR_INVALID_MESSAGE                => 2;

=item C<$ERROR_UNKNOWN_TOPIC_OR_PARTITION>

3 - Unknown topic or partition

=cut
const our $ERROR_UNKNOWN_TOPIC_OR_PARTITION     => 3;

=item C<$ERROR_INVALID_MESSAGE_SIZE>

4 - Message has invalid size

=cut
const our $ERROR_INVALID_MESSAGE_SIZE           => 4;

=item C<$ERROR_LEADER_NOT_AVAILABLE>

5 - Unable to write due to ongoing Kafka leader selection

This error is thrown if we are in the middle of a leadership election and there is
no current leader for this partition, hence it is unavailable for writes.

=cut
const our $ERROR_LEADER_NOT_AVAILABLE           => 5;

=item C<$ERROR_NOT_LEADER_FOR_PARTITION>

6 - Server is not a leader for partition

Client attempts to send messages to a replica that is not the leader for given partition.
It usually indicates that client's metadata is out of date.

=cut
const our $ERROR_NOT_LEADER_FOR_PARTITION       => 6;

=item C<$ERROR_REQUEST_TIMED_OUT>

7 - Request time-out

Request exceeds the user-specified time limit for the request.

=cut
const our $ERROR_REQUEST_TIMED_OUT              => 7;

=item C<$ERROR_BROKER_NOT_AVAILABLE>

8 - Broker is not available

This is not a client facing error and is used only internally by intra-cluster broker communication.

=cut
const our $ERROR_BROKER_NOT_AVAILABLE           => 8;

=item C<$ERROR_REPLICA_NOT_AVAILABLE>

9 - Replica not available

According to Apache Kafka documentation: 'What is the difference between this and LeaderNotAvailable?'

=cut
const our $ERROR_REPLICA_NOT_AVAILABLE          => 9;

=item C<$ERROR_MESSAGE_SIZE_TOO_LARGE>

10 - Message is too big

The server has a configurable maximum message size to avoid unbounded memory allocation.
This error is thrown when client attempts to produce a message larger than possible maximum size.

=cut
const our $ERROR_MESSAGE_SIZE_TOO_LARGE         => 10;

=item C<$ERROR_STALE_CONTROLLER_EPOCH_CODE>

11 - Stale Controller Epoch Code

According to Apache Kafka documentation: '???'

=cut
const our $ERROR_STALE_CONTROLLER_EPOCH_CODE    => 11;

=item C<$ERROR_OFFSET_METADATA_TOO_LARGE_CODE>

12 - Specified metadata offset is too big

If you specify a value larger than configured maximum for offset metadata.

=cut
const our $ERROR_OFFSET_METADATA_TOO_LARGE_CODE => 12;

=item C<%ERROR>

Contains the descriptions for possible error codes.

=back

=cut
our %ERROR = (
    # Errors fixed by Kafka package
    $ERROR_MISMATCH_ARGUMENT                => q{Invalid argument},
    $ERROR_CANNOT_SEND                      => q{Can't send},
    $ERROR_CANNOT_RECV                      => q{Can't recv},
    $ERROR_CANNOT_BIND                      => q{Can't bind},
    $ERROR_COMPRESSED_PAYLOAD               => q{Compressed payload},
    $ERROR_UNKNOWN_APIKEY                   => q{Unknown ApiKey},
    $ERROR_CANNOT_GET_METADATA              => q{Can't get metadata},
    $ERROR_LEADER_NOT_FOUND                 => q{Leader not found},
    $ERROR_MISMATCH_CORRELATIONID           => q{Mismatch CorrelationId},
    $ERROR_NO_KNOWN_BROKERS                 => q{There are no known brokers},
    $ERROR_REQUEST_OR_RESPONSE              => q{Bad request or response element},
    $ERROR_TOPIC_DOES_NOT_MATCH             => q{Topic does not match the requested},
    $ERROR_PARTITION_DOES_NOT_MATCH         => q{Partition does not match the requested},
    $ERROR_NOT_BINARY_STRING                => q{Not binary string},

    #-- The Protocol Error Messages
    # https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
    $ERROR_NO_ERROR                         => q{}, # 'No error--it worked!',
    $ERROR_UNKNOWN                          => q{An unexpected server error},
    $ERROR_OFFSET_OUT_OF_RANGE              => q{The requested offset is outside the range of offsets maintained by the server for the given topic/partition},
    $ERROR_INVALID_MESSAGE                  => q{Message contents does not match its CRC},
    $ERROR_UNKNOWN_TOPIC_OR_PARTITION       => q{Unknown topic or partition},
    $ERROR_INVALID_MESSAGE_SIZE             => q{Message has invalid size},
    $ERROR_LEADER_NOT_AVAILABLE             => q{Unable to write due to ongoing Kafka leader selection},
    $ERROR_NOT_LEADER_FOR_PARTITION         => q{Server is not a leader for partition},
    $ERROR_REQUEST_TIMED_OUT                => q{Request time-out},
    $ERROR_BROKER_NOT_AVAILABLE             => q{Broker is not available},
    $ERROR_REPLICA_NOT_AVAILABLE            => q{Replica not available},
    $ERROR_MESSAGE_SIZE_TOO_LARGE           => q{Message is too big},
    $ERROR_STALE_CONTROLLER_EPOCH_CODE      => q{Stale Controller Epoch Code},
    $ERROR_OFFSET_METADATA_TOO_LARGE_CODE   => q{Specified metadata offset is too big},
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
        $RECEIVE_EARLIEST_OFFSETS
        $DEFAULT_MAX_NUMBER_OF_OFFSETS
        $DEFAULT_MAX_BYTES
    );
    use Kafka::Connection;
    use Kafka::Producer;
    use Kafka::Consumer;

    my ( $connection, $producer, $consumer );
    try {

        #-- Connection
        $connection = Kafka::IO->new( host => 'localhost' );

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
            $RECEIVE_EARLIEST_OFFSETS,      # time
            $DEFAULT_MAX_NUMBER_OF_OFFSETS  # max_number
        );

        if( @$offsets ) {
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
                if( $message->valid ) {
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
        if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $_->code, ') ',  $_->message, "\n";
            exit;
        } else {
            die $_;
        }
    };

    # Closes and cleans up
    undef $consumer;
    undef $producer;
    undef $connection;

=head1 DEPENDENCIES

In order to install and use this package you will need Perl version
5.10 or later. Some modules within this package depend on other
packages that are distributed separately from Perl. We recommend that
you have the following packages installed before you install
Kafka:

    Const::Fast
    Exception::Class
    List::MoreUtils
    Params::Util
    Scalar::Util::Numeric
    String::CRC32
    Sys::SigAction
    Try::Tiny

Kafka package has the following optional dependencies:

    Capture::Tiny
    Config::IniFiles
    Data::Compare
    Proc::Daemon
    Proc::ProcessTable
    Sub::Install
    Test::Deep
    Test::Exception
    Test::TCP

If the optional modules are missing, some "prereq" tests are skipped.

=head1 BUGS AND LIMITATIONS

Currently, the package does not implement send and response of compressed
messages. L<Producer|Kafka::Producer> and L<Consumer|Kafka::Consumer> methods
only work with one topic and one partition at a time.
Also module does not implement the Offset Commit/Fetch API.

L<Producer|Kafka::Producer>'s, L<Consumer|Kafka::Consumer>'s, L<Connection|Kafka::Connection>'s
string arguments must be binary strings.
Using Unicode strings may cause an error or data corruption.

This module does not support Kafka protocol versions earlier than 0.8.

The Kafka package was written, tested, and found working on recent Linux
distributions.

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
