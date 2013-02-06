package Kafka;

use 5.010;
use strict;
use warnings;

# Kafka allows you to produce and consume messages using the Apache Kafka distributed publish/subscribe messaging service.

use Exporter qw( import );

our @EXPORT_OK  = qw(
    KAFKA_SERVER_PORT
    DEFAULT_TIMEOUT
    TIMESTAMP_LATEST
    TIMESTAMP_EARLIEST
    DEFAULT_MAX_SIZE
    DEFAULT_MAX_OFFSETS
    MAX_SOCKET_REQUEST_BYTES
    ERROR_MISMATCH_ARGUMENT
    ERROR_WRONG_CONNECT
    ERROR_CANNOT_SEND
    ERROR_CANNOT_RECV
    ERROR_CANNOT_BIND
    ERROR_CHECKSUM_ERROR
    ERROR_COMPRESSED_PAYLOAD
    ERROR_NUMBER_OF_OFFSETS
    ERROR_NOTHING_RECEIVE
    ERROR_IN_ERRORCODE
    ERROR_INVALID_MESSAGE_CODE
    BITS64
    );

# Not used yet
#    ERRORCODE_INVALID_RETCH_SIZE_CODE
#    ERRORCODE_OFFSET_OUT_OF_RANGE
#    ERRORCODE_UNKNOWN
#    ERRORCODE_WRONG_PARTITION_CODE

our $VERSION = '0.10';

use Config;

use constant {
    KAFKA_SERVER_PORT                   => 9092,

    DEFAULT_TIMEOUT                     => 0.5,         # The timeout in secs, for gethostbyname, connect, blocking receive and send calls (could be any integer or floating-point type)

    TIMESTAMP_LATEST                    => -1,
    TIMESTAMP_EARLIEST                  => -2,

    DEFAULT_MAX_SIZE                    => 1024 * 1024, # Maximum size of MESSAGE(s) to receive 1MB
    DEFAULT_MAX_OFFSETS                 => 100,         # the maximum number of offsets to retrieve

    MAX_SOCKET_REQUEST_BYTES            => 104857600,   # The maximum size of a request that the socket server will accept (protection against OOM)

    ERROR_INVALID_MESSAGE_CODE          => 0,
    ERROR_MISMATCH_ARGUMENT             => 1,
    ERROR_WRONG_CONNECT                 => 2,
    ERROR_CANNOT_SEND                   => 3,
    ERROR_CANNOT_RECV                   => 4,
    ERROR_CANNOT_BIND                   => 5,
    ERROR_CHECKSUM_ERROR                => 6,
    ERROR_COMPRESSED_PAYLOAD            => 7,
    ERROR_NUMBER_OF_OFFSETS             => 8,
    ERROR_NOTHING_RECEIVE               => 9,
    ERROR_IN_ERRORCODE                  => 10,

    BITS64                              => ( defined( $Config{use64bitint} ) and $Config{use64bitint} eq "define" ) || $Config{longsize} >= 8,
};

our @ERROR = (
    'Invalid message',
    'Mismatch argument',
    'You must configure a host to connect to!',
    "Can't send",
    "Can't recv",
    "Can't bind",
    'Checksum error',
    'Compressed payload',
    "Amount received offsets does not match 'NUMBER of OFFSETS'",
    'Nothing to receive',
    "Response contains an error in 'ERROR_CODE'",
    );

our %ERROR_CODE = (
    -1  => 'Unknown error',
    1   => 'Offset out of range',
    2   => 'Invalid message code',
    3   => 'Wrong partition code',
    4   => 'Invalid retch size code',
    );

1;

__END__

=head1 NAME

Kafka - constants and messages used by the Kafka package modules

=head1 VERSION

This documentation refers to C<Kafka> package version 0.10

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

1 - Mismatch argument

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

    Digest::CRC
    Params::Util

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
All rights reserved.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut