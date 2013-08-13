package Kafka;

# Kafka allows you to produce and consume messages using the Apache Kafka distributed publish/subscribe messaging service.

#TODO: ? Kafka::Cluster - test environment should be cleaned including t/data or it should do the test

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

use Exporter qw(
    import
);

our @EXPORT_OK = qw(
    $BITS64
    $BLOCK_UNTIL_IS_COMMITED
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
    $ERROR_DESCRIPTION_LEADER_NOT_FOUND
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

our $VERSION = '0.8001';

#-- load the modules -----------------------------------------------------------

use Config;
use Const::Fast;

#-- declarations ---------------------------------------------------------------

const our $KAFKA_SERVER_PORT                    => 9092;

const our $REQUEST_TIMEOUT                      => 1.5;                 # The timeout in secs, for gethostbyname, connect, blocking receive and send calls (could be any integer or floating-point type)
                                                                        # The ack timeout of the producer requests.
                                                                        # Value must be non-negative and non-zero

# Important configuration properties
const our $DEFAULT_MAX_BYTES                    => 1_000_000;           # The maximum bytes to include in the message set for the partition
const our $SEND_MAX_RETRIES                     => 3;                   # The leader may be unavailable transiently,
                                                                        # which can fail the sending of a message.
                                                                        # This property specifies the number of retries when such failures occur.
const our $RETRY_BACKOFF                        => 100;                 # (ms) Before each retry, the producer refreshes the metadata of relevant topics.
                                                                        # Since leader election takes a bit of time,
                                                                        # this property specifies the amount of time
                                                                        # that the producer waits before refreshing the metadata.

# Used to ask for all messages before a certain time (ms). There are two special values.
const our $RECEIVE_LATEST_OFFSET                => -1;  # to receive the latest offset (this will only ever return one offset).
const our $RECEIVE_EARLIEST_OFFSETS             => -2;  # to receive the earliest available offsets.

# The minimum number of bytes of messages that must be available to give a response.
const our $MIN_BYTES_RESPOND_IMMEDIATELY        => 0;   # the server will always respond immediately.
const our $MIN_BYTES_RESPOND_HAS_DATA           => 1;   # the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs.

# Indicates how many acknowledgements the servers should receive before responding to the request.
const our $NOT_SEND_ANY_RESPONSE                => 0;   # the server does not send any response.
const our $WAIT_WRITTEN_TO_LOCAL_LOG            => 1;   # the server will wait the data is written to the local log before sending a response.
const our $BLOCK_UNTIL_IS_COMMITED              => -1;  # the server will block until the message is committed by all in sync replicas before sending a response.

# The maximum amount of time (ms) to block waiting if insufficient data is available at the time the request is issued
const our $DEFAULT_MAX_WAIT_TIME                => 100;

const our $DEFAULT_MAX_NUMBER_OF_OFFSETS        => 100; # Kafka is return up to 'MaxNumberOfOffsets' of offsets

#-- Errors fixed by Kafka package
const our $ERROR_MISMATCH_ARGUMENT              => -1000;
const our $ERROR_CANNOT_SEND                    => -1001;
const our $ERROR_CANNOT_RECV                    => -1002;
const our $ERROR_CANNOT_BIND                    => -1003;
const our $ERROR_COMPRESSED_PAYLOAD             => -1004;
const our $ERROR_UNKNOWN_APIKEY                 => -1005;
const our $ERROR_CANNOT_GET_METADATA            => -1006;
const our $ERROR_DESCRIPTION_LEADER_NOT_FOUND   => -1007;
const our $ERROR_MISMATCH_CORRELATIONID         => -1008;
const our $ERROR_NO_KNOWN_BROKERS               => -1009;
const our $ERROR_REQUEST_OR_RESPONSE            => -1010;
const our $ERROR_TOPIC_DOES_NOT_MATCH           => -1011;
const our $ERROR_PARTITION_DOES_NOT_MATCH       => -1012;
const our $ERROR_NOT_BINARY_STRING              => -1013;

#-- The Protocol Error Codes
const our $ERROR_NO_ERROR                       => 0;
const our $ERROR_UNKNOWN                        => -1;
const our $ERROR_OFFSET_OUT_OF_RANGE            => 1;
const our $ERROR_INVALID_MESSAGE                => 2;
const our $ERROR_UNKNOWN_TOPIC_OR_PARTITION     => 3;
const our $ERROR_INVALID_MESSAGE_SIZE           => 4;
const our $ERROR_LEADER_NOT_AVAILABLE           => 5;
const our $ERROR_NOT_LEADER_FOR_PARTITION       => 6;
const our $ERROR_REQUEST_TIMED_OUT              => 7;
const our $ERROR_BROKER_NOT_AVAILABLE           => 8;
const our $ERROR_REPLICA_NOT_AVAILABLE          => 9;
const our $ERROR_MESSAGE_SIZE_TOO_LARGE         => 10;
const our $ERROR_STALE_CONTROLLER_EPOCH_CODE    => 11;
const our $ERROR_OFFSET_METADATA_TOO_LARGE_CODE => 12;

our %ERROR = (
    # Errors fixed by Kafka package
    $ERROR_MISMATCH_ARGUMENT                => 'Invalid argument',
    $ERROR_CANNOT_SEND                      => "Can't send",
    $ERROR_CANNOT_RECV                      => "Can't recv",
    $ERROR_CANNOT_BIND                      => "Can't bind",
    $ERROR_COMPRESSED_PAYLOAD               => 'Compressed payload',
    $ERROR_UNKNOWN_APIKEY                   => 'Unknown ApiKey',
    $ERROR_CANNOT_GET_METADATA              => "Can't get metadata",
    $ERROR_DESCRIPTION_LEADER_NOT_FOUND     => 'Description leader not found',
    $ERROR_MISMATCH_CORRELATIONID           => 'Mismatch CorrelationId',
    $ERROR_NO_KNOWN_BROKERS                 => 'There are no known brokers',
    $ERROR_REQUEST_OR_RESPONSE              => 'Bad request or response element',
    $ERROR_TOPIC_DOES_NOT_MATCH             => 'Topic does not match the requested',
    $ERROR_PARTITION_DOES_NOT_MATCH         => 'Partition does not match the requested',
    $ERROR_NOT_BINARY_STRING                => 'Not binary string',

    #-- The Protocol Error Messages
    # https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
    $ERROR_NO_ERROR                         => q{}, # 'No error--it worked!',
    $ERROR_UNKNOWN                          => 'An unexpected server error',
    $ERROR_OFFSET_OUT_OF_RANGE              => 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition',
    $ERROR_INVALID_MESSAGE                  => 'Message contents does not match its CRC',
    $ERROR_UNKNOWN_TOPIC_OR_PARTITION       => 'Unknown topic or partition',
    $ERROR_INVALID_MESSAGE_SIZE             => 'Message has invalid size',
    $ERROR_LEADER_NOT_AVAILABLE             => 'Unable to write due to ongoing Kafka leader selection',
    $ERROR_NOT_LEADER_FOR_PARTITION         => 'Server is not a leader for partition',
    $ERROR_REQUEST_TIMED_OUT                => 'Request time-out',
    $ERROR_BROKER_NOT_AVAILABLE             => 'Broker is not available',
    $ERROR_REPLICA_NOT_AVAILABLE            => 'Replica not available',
    $ERROR_MESSAGE_SIZE_TOO_LARGE           => 'Message is too big',
    $ERROR_STALE_CONTROLLER_EPOCH_CODE      => 'Stale Controller Epoch Code',
    $ERROR_OFFSET_METADATA_TOO_LARGE_CODE   => 'Specified metadata offset is too big',
);

const our $BITS64                           => ( defined( $Config{use64bitint} ) and $Config{use64bitint} eq "define" ) || $Config{longsize} >= 8;

1;

__END__

=head1 NAME

Kafka - constants and messages used by the Kafka package modules

=head1 VERSION

This documentation refers to C<Kafka> package version 0.12

=head1 SYNOPSIS

An example of C<Kafka> usage:

    use Kafka qw(
        BITS64
        KAFKA_SERVER_PORT
        DEFAULT_TIMEOUT
        TIMESTAMP_EARLIEST
        DEFAULT_MAX_OFFSETS
        DEFAULT_MAX_SIZE
        );

    # common information
    print "This is Kafka package $Kafka::VERSION\n";
    print "You have a ", BITS64 ? "64" : "32", " bit system\n";

    use Kafka::IO;

    # connect to local server with the defaults
    my $io = Kafka::IO->new( host => "localhost" );

    # decoding of the error code
    unless ( $io )
    {
        print STDERR "last error: ",
            $Kafka::ERROR[Kafka::IO::last_errorcode], "\n";
    }

To see a brief but working code example of the C<Kafka> package usage
look at the L</"An Example"> section.

=head1 ABSTRACT

The Kafka package is a set of Perl modules which provides a simple and
consistent application programming interface (API) to Apache Kafka 0.7,
a high-throughput distributed messaging system.
This is a low-level API implementation which DOES NOT interract with
an Apache ZooKeeper for consumer coordination and/or load balancing.

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

Supports parsing the Apache Kafka Wire Format protocol.

=item *

Supports the Apache Kafka Requests and Responses (PRODUCE and FETCH with
no compression codec attribute now). Within this package we currently support
access to the PRODUCE Request, FETCH Request, OFFSETS Request, FETCH Response,
OFFSETS Response.

=item *

Simple producer and consumer clients.

=item *

Simple mock server instance for testing without Apache Kafka server.

=item *

Support for working with 64 bit elements of the Kafka Wire Format protocol
on 32 bit systems.

=back

=head1 APACHE KAFKA'S STYLE COMMUNICATION

The Kafka package is based on Kafka's 0.7 Wire Format specification document at
L<http://cwiki.apache.org/confluence/display/KAFKA/Wire+Format/>

=over 3

=item

The Kafka's Wire Format protocol is based on a request/response paradigm.
A client establishes a connection with a server and sends a request to the
server in the form of a request method, followed by a messages containing
request modifiers. The server responds with a success or error code,
followed by a messages containing entity meta-information and content.

=back

Communication with Kafka always assumes to follow these steps: First the IO
and client objects are created and configured.

The Kafka client has the class name L<Kafka::Producer|Kafka::Producer> or
L<Kafka::Consumer|Kafka::Consumer>.

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
Each message is uniquely identified by a 64-bit integer offset giving the byte
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

=head2 The IO Object

The clients uses the IO object to communicate with the Apache Kafka server.
The IO object is an interface layer between your application code and
the network.

IO object is required to create objects of classes
L<Kafka::Producer|Kafka::Producer> and L<Kafka::Consumer|Kafka::Consumer>.

Kafka IO API is implemented by L<Kafka::IO|Kafka::IO> class.

    use Kafka::IO;

    # connect to local server with the defaults
    my $io = Kafka::IO->new( host => "localhost" );

The main attributes of the IO object are:

=over 3

=item *

B<host>, and B<port> are the IO object attributes denoting the server and
the port name of the "documents" (messages) service we want to access
(Apache Kafka server).

=item *

B<timeout> specifies how much time we give remote servers to respond before
the IO object disconnects and creates an internal exception.

=back

=head2 The Producer Object

Kafka producer API is implemented by L<Kafka::Producer|Kafka::Producer> class.

    use Kafka::Producer;

    #-- Producer
    my $producer = Kafka::Producer->new( IO => $io );

    # Sending a single message
    $producer->send(
        "test",             # topic
        0,                  # partition
        "Single message"    # message
        );

    # Sending a series of messages
    $producer->send(
        "test",             # topic
        0,                  # partition
        [                   # messages
            "The first message",
            "The second message",
            "The third message",
        ]
        );

The main attributes of the producer request are:

=over 3

=item *

The request method of the producer object is C<send()>.

=item *

B<topic> and B<partition> encode parameters of the B<messages> we
want to send.

=item *

B<messages> is an arbitrary amount of data (a simple data string or
an array of the data strings).

=back

=head2 The Consumer Object

Kafka consumer API is implemented by L<Kafka::Consumer|Kafka::Consumer> class.

    use Kafka::Consumer;

    $consumer = Kafka::Consumer->new( IO => $io );

The request methods of the consumer object are C<offsets()> and C<fetch()>.

C<offsets> method returns a reference to the list of offsets of received messages.

C<fetch> method returns a reference to the list of received
L<Kafka::Message|Kafka::Message> objects.

    # Get a list of valid offsets up to max_number before the given time
    if ( my $offsets = $consumer->offsets(
        "test",             # topic
        0,                  # partition
        TIMESTAMP_EARLIEST, # time
        DEFAULT_MAX_OFFSETS # max_number
        ) )
    {
        foreach my $offset ( @$offsets )
        {
            print "Received offset: $offset\n";
        }
    }

    # Consuming messages
    if ( my $messages = $consumer->fetch(
        "test",             # topic
        0,                  # partition
        0,                  # offset
        DEFAULT_MAX_SIZE    # max_size
        ) )
    {
        foreach my $message ( @$messages )
        {
            if( $message->valid )
            {
                print "payload    : ", $message->payload,       "\n";
                print "offset     : ", $message->offset,        "\n";
                print "next_offset: ", $message->next_offset,   "\n";
            }
        }
    }

The arguments:

=over 3

=item *

B<topic> and B<partition> specify the location of the B<messages> we want to retrieve.

=item *

B<offset>, B<max_size> or B<time>, B<max_number> arguments are additional
information that specify attributes of the messages we want to access.

=item *

B<time> is the timestamp of the offsets before this time (ms). B<max_number>
is the maximum number of offsets to retrieve. This additional information
about the request must be used to describe the range of the B<messages>.

=back

=head2 The Message Object

Kafka message API is implemented by L<Kafka::Message|Kafka::Message> class.

    if( $message->valid )
    {
        print "payload    : ", $message->payload,       "\n";
        print "offset     : ", $message->offset,        "\n";
        print "next_offset: ", $message->next_offset,   "\n";
    }
    else
    {
        print "error      : ", $message->error,         "\n";
    }

Available methods of L<Kafka::Message|Kafka::Message> object are:

=over 3

=item *

C<payload> A simple message received from the Apache Kafka server.

=item *

C<valid> A message entry is valid if the CRC32 of the message payload matches
to the CRC stored with the message.

=item *

C<error> A description of the message inconsistence (currently only for message
is not valid or compressed).

=item *

C<offset> The offset beginning of the message in the Apache Kafka server.

=item *

C<next_offset> The offset beginning of the next message in the Apache Kafka
server.

=back

=head2 Common

Both Kafka::Producer and Kafka::Consumer objects described above also have
the following common methods:

=over 3

=item *

C<RaiseError> is a method which causes Kafka to die if an error is detected.

=item *

C<last_errorcode> and C<last_error> diagnostic methods. Use them to get detailed
error message if server or the resource might not be available, access to the
resource might be denied, or other things might have failed for some reason.

=item *

C<close> method: terminates connection with Kafka and clean up.

    my $producer = Kafka::Producer->new(
        IO          => $io,
        RaiseError  => 1
        );

    unless ( $producer->send( "test", 0, "Single message" )
    {
        print
            "error code       : ", $producer->last_errorcode,   "\n",
            "error description: ", $producer->last_error,       "\n";
    }

    $producer->close;

=back

=head2 EXPORT

None by default.

Additional constants are available for import, which can be used
to define some type of parameters, and to identify various error cases.

These are the defaults:

=over

=item C<KAFKA_SERVER_PORT>

default Apache Kafka server port - 9092.

=item C<DEFAULT_TIMEOUT>

timeout in secs, for C<gethostbyname>, C<connect>, blocking C<receive> and
C<send> calls (could be any integer or floating-point type) - 0.5 sec.

=item C<TIMESTAMP_LATEST>

timestamp of the offsets before this time (ms) special value -1 : latest

=item C<TIMESTAMP_EARLIEST>

timestamp of the offsets before this time (ms) special value -2 : earliest

=item C<DEFAULT_MAX_SIZE>

maximum size of message(s) to receive - 1MB

=item C<DEFAULT_MAX_OFFSETS>

maximum number of offsets to retrieve - 100

=item C<MAX_SOCKET_REQUEST_BYTES>

The maximum size of a request that the socket server will accept
(protection against OOM). Default limit (as configured in server.properties)
is 104857600

=back

Possible error codes returned by C<last_errorcode> method
(complies with an array of descriptions C<@Kafka::ERROR>):

=over

=item C<ERROR_INVALID_MESSAGE_CODE>

0 - Invalid message

=item C<ERROR_MISMATCH_ARGUMENT>

1 - Invalid argument

=item C<ERROR_WRONG_CONNECT>

2 - You must configure a host to connect to!

=item C<ERROR_CANNOT_SEND>

3 - Can't send

=item C<ERROR_CANNOT_RECV>

4 - Can't receive

=item C<ERROR_CANNOT_BIND>

5 - Can't bind

=item C<ERROR_CHECKSUM_ERROR>

6 - Checksum error

=item C<ERROR_COMPRESSED_PAYLOAD>

7 - Compressed payload

=item C<ERROR_NUMBER_OF_OFFSETS>

7 - Amount received offsets does not match 'NUMBER of OFFSETS'

=item C<ERROR_NOTHING_RECEIVE>

8 - Nothing to receive

=item C<ERROR_IN_ERRORCODE>

9 - Response contains an error in 'ERROR_CODE'

=back

Support for working with 64 bit elements of the Kafka Wire Format protocol
on 32 bit systems:

=over

=item C<BITS64>

Know you are working on 64 or 32 bit system

=back

=head2 GLOBAL VARIABLES

=over

=item C<@Kafka::ERROR>

Contain the descriptions for possible error codes returned by
C<last_errorcode> methods and functions of the package modules.

=item C<%Kafka::ERROR_CODE>

Contain the descriptions for possible error codes in the ERROR_CODE box of
Apache Kafka Wire Format protocol responses.

=back

=head2 An Example

    use Kafka qw(
        KAFKA_SERVER_PORT
        DEFAULT_TIMEOUT
        TIMESTAMP_EARLIEST
        DEFAULT_MAX_OFFSETS
        DEFAULT_MAX_SIZE
        );
    use Kafka::IO;
    use Kafka::Producer;
    use Kafka::Consumer;

    #-- IO
    my $io = Kafka::IO->new( host => "localhost" );

    #-- Producer
    my $producer = Kafka::Producer->new( IO => $io );

    # Sending a single message
    $producer->send(
        "test",             # topic
        0,                  # partition
        "Single message"    # message
        );

    # Sending a series of messages
    $producer->send(
        "test",             # topic
        0,                  # partition
        [                   # messages
            "The first message",
            "The second message",
            "The third message",
        ]
        );

    $producer->close;

    #-- Consumer
    my $consumer = Kafka::Consumer->new( IO => $io );

    # Get a list of valid offsets up max_number before the given time
    my $offsets;
    if ( $offsets = $consumer->offsets(
        "test",             # topic
        0,                  # partition
        TIMESTAMP_EARLIEST, # time
        DEFAULT_MAX_OFFSETS # max_number
        ) )
    {
        foreach my $offset ( @$offsets )
        {
            print "Received offset: $offset\n";
        }
    }
    if ( !$offsets or $consumer->last_error )
    {
        print
            "(", $consumer->last_errorcode, ") ",
            $consumer->last_error, "\n";
    }

    # Consuming messages
    if ( my $messages = $consumer->fetch(
        "test",             # topic
        0,                  # partition
        0,                  # offset
        DEFAULT_MAX_SIZE    # Maximum size of MESSAGE(s) to receive
        ) )
    {
        foreach my $message ( @$messages )
        {
            if( $message->valid )
            {
                print "payload    : ", $message->payload,       "\n";
                print "offset     : ", $message->offset,        "\n";
                print "next_offset: ", $message->next_offset,   "\n";
            }
            else
            {
                print "error      : ", $message->error,         "\n";
            }
        }
    }

    $consumer->close;

C<$io>, C<$producer>, and C<$consumer> are created once when the
application starts up.

=head1 DEPENDENCIES

In order to install and use this package you will need Perl version
5.10 or later. Some modules within this package depend on other
packages that are distributed separately from Perl. We recommend that
you have the following packages installed before you install
Kafka:

    Params::Util
    String::CRC32

Kafka package has the following optional dependencies:

    Test::Pod
    Test::Pod::Coverage
    Test::Exception
    CPAN::Meta
    Test::Deep
    Test::Distribution
    Test::Kwalitee

If the optional modules are missing, some "prereq" tests are skipped.

=head1 BUGS AND LIMITATIONS

Currently, the package does not implement send and response of compressed
messages. Also does not implement the MULTIFETCH and MULTIPRODUCE requests.

Use only one C<Kafka::Mock> object at the same time (it has class variables
for the exchange of TCP server processes).

The Kafka package was written, tested, and found working on recent Linux
distributions.

There are no known bugs in this package.

Please report problems to the L</"AUTHOR">.

Patches are welcome.

=head1 MORE DOCUMENTATION

All modules contain detailed information on the interfaces they provide.

=head1 SEE ALSO

The basic operation of the Kafka package modules:

L<Kafka|Kafka> - constants and messages used by the Kafka package modules

L<Kafka::IO|Kafka::IO> - object interface to socket communications with
the Apache Kafka server

L<Kafka::Producer|Kafka::Producer> - object interface to the producer client

L<Kafka::Consumer|Kafka::Consumer> - object interface to the consumer client

L<Kafka::Message|Kafka::Message> - object interface to the Kafka message
properties

L<Kafka::Protocol|Kafka::Protocol> - functions to process messages in the
Apache Kafka's wire format

L<Kafka::Int64|Kafka::Int64> - functions to work with 64 bit elements of the
protocol on 32 bit systems

L<Kafka::Mock|Kafka::Mock> - object interface to the TCP mock server for testing

A wealth of detail about the Apache Kafka and Wire Format:

Main page at L<http://incubator.apache.org/kafka/>

Wire Format at L<http://cwiki.apache.org/confluence/display/KAFKA/Wire+Format/>

Writing a Driver for Kafka at
L<http://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka>

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

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
