#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 17;

# Usage - Basic functionalities to include a simple Producer and Consumer
# You need to have access to your Kafka instance and be able to connect through TCP

use Kafka qw(
    KAFKA_SERVER_PORT
    DEFAULT_TIMEOUT
    TIMESTAMP_LATEST
    TIMESTAMP_EARLIEST
    DEFAULT_MAX_OFFSETS
    DEFAULT_MAX_SIZE
    ERROR_CANNOT_BIND
    );
use Kafka::IO;
use Kafka::Producer;
use Kafka::Consumer;
use Kafka::Mock;

# If the reader closes the connection, though, the writer will get a SIGPIPE when it next tries to write there.
$SIG{PIPE} = sub { die };

#-- Mock server

# in a format for pack( 'H*', ... )
my %requests = (
    0   =>                                      # PRODUCE       Request     - "no compression" now
        # Request Header
         '0000005f'                             # REQUEST_LENGTH
        .'0000'                                 # REQUEST_TYPE
        .'0004'                                 # TOPIC_LENGTH
        .'74657374'                             # TOPIC ("test")
        .'00000000'                             # PARTITION
        # PRODUCE Request
        .'0000004f'                             # MESSAGES_LENGTH
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'d94a22be'                             # CHECKSUM
        .'546865206669727374206d657373616765'   # PAYLOAD ("The first message")
        # MESSAGE
        .'00000017'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'a3810845'                             # CHECKSUM
        .'546865207365636f6e64206d657373616765' # PAYLOAD ("The second message")
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'58611780'                             # CHECKSUM
        .'546865207468697264206d657373616765',  # PAYLOAD ("The third message")

    1   =>                                      # FETCH         Request
        # Request Header
         '00000018'                             # REQUEST_LENGTH
        .'0001'                                 # REQUEST_TYPE
        .'0004'                                 # TOPIC_LENGTH
        .'74657374'                             # TOPIC ("test")
        .'00000000'                             # PARTITION
        # FETCH Request
        .'0000000000000000'                     # OFFSET
        .'00100000',                            # MAX_SIZE (1MB)

    2   => '',                                  # MULTIFETCH    Request     - it is not realized yet
    3   => '',                                  # MULTIPRODUCE  Request     - it is not realized yet

    4   =>                                      # OFFSETS       Request
        # Request Header
         '00000018'                             # REQUEST_LENGTH
        .'0004'                                 # REQUEST_TYPE
        .'0004'                                 # TOPIC_LENGTH
        .'74657374'                             # TOPIC ("test")
        .'00000000'                             # PARTITION
        # OFFSETS Request
        .'fffffffffffffffe'                     # TIME (-2 : earliest)
        .'00000064',                            # MAX NUMBER of OFFSETS (100)
    );

# in a format for pack( 'H*', ... )
my %responses = (
    0   => '',                                  # PRODUCE       Response    - None

    1   =>                                      # FETCH         Response    - "no compression" now
        # Response Header
         '00000051'                             # RESPONSE_LENGTH
        .'0000'                                 # ERROR_CODE
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'d94a22be'                             # CHECKSUM
        .'546865206669727374206d657373616765'   # PAYLOAD ("The first message")
        # MESSAGE
        .'00000017'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'a3810845'                             # CHECKSUM
        .'546865207365636f6e64206d657373616765' # PAYLOAD ("The second message")
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'58611780'                             # CHECKSUM
        .'546865207468697264206d657373616765',  # PAYLOAD ("The third message")

    2   => '',                                  # MULTIFETCH    Response    - None
    3   => '',                                  # MULTIPRODUCE  Request     - None

    4   =>                                      # OFFSETS       Response
        # Response Header
         '0000000e'                             # RESPONSE_LENGTH
        .'0000'                                 # ERROR_CODE
        # OFFSETS Response
        .'00000001'                             # NUMBER of OFFSETS
        .'0000000000000000'                     # OFFSET
    );

my $server = Kafka::Mock->new(
    requests    => \%requests,
    responses   => \%responses,
    timeout     => 0.1,                     # Optional, secs float
    );
isa_ok( $server, 'Kafka::Mock');
my $pid = $server->last_request( "pid" );

#-- IO

my $io;

eval { $io = Kafka::IO->new(
    host        => "localhost",
    port        => $server->port,
    timeout     => "nothing",
    RaiseError  => 1
    ) };
ok Kafka::IO::last_errorcode(), "expecting to die: (".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error() if $@;

unless ( $io = Kafka::IO->new(
    host        => "localhost",                 # for work with the mock server use "localhost"
    port        => $server->port,
    timeout     => DEFAULT_TIMEOUT,             # Optional, default = DEFAULT_TIMEOUT
    RaiseError  => 0                            # Optional, default = 0
    ) )
{
    fail "(".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error();
    exit 1;
}
isa_ok( $io, 'Kafka::IO');

$server->delay(
    "request",                              # Operation:    "request" or "response"
    10,                                     # Position:     integer
    0.25                                    # Duration:     secs float
    );
$server->clear();                           # removal of delays
$server->delay( "request",  8,  0.001 );
$server->delay( "request",  18, 0.002 );
$server->delay( "response", 4,  0.003 );
$server->delay( "response", 9,  0.004 );
$server->delay( "response", 15, 0.005 );
like $server->last_request( "note" ), qr/delay response: \d+ \d+.+ delay request: \d+ \d+/, "set the delay";

#-- Producer

my $producer;

eval { $producer = Kafka::Producer->new(
    IO          => "nothing",
    RaiseError  => 1
    ) };
ok Kafka::Producer::last_errorcode(), "expecting to die: (".Kafka::Producer::last_errorcode().") ".Kafka::Producer::last_error() if $@;

$producer = Kafka::Producer->new(
    IO          => $io,
    RaiseError  => 0                            # Optional, default = 0
    );
unless ( $producer )
{
    fail "(".Kafka::Producer::last_errorcode().") ".Kafka::Producer::last_error();
    exit 1;
}
isa_ok( $producer, 'Kafka::Producer');

# Sending a single message
if ( !$producer->send(
    "test",                                     # topic
    0,                                          # partition
    "Single message"                            # message
    ) )
{
    fail "(".$producer->last_errorcode.") ".$producer->last_error;
}
else
{
    pass "message is sent";
}

# Sending a series of messages
if ( !$producer->send(
    "test",                                     # topic
    0,                                          # partition
    [                                           # messages
        "The first message",
        "The second message",
        "The third message",
    ]
    ) )
{
    fail "(".$producer->last_errorcode.") ".$producer->last_error;
}
else
{
    pass "messages sent";
}

# Closes the producer and cleans up
$producer->close;
ok( scalar( keys %$producer ) == 0, "the producer object is an empty" );

#-- Consumer

unless ( $io = Kafka::IO->new(
    host        => "localhost",
    port        => $server->port,
    timeout     => DEFAULT_TIMEOUT,             # Optional, default = DEFAULT_TIMEOUT
    RaiseError  => 0                            # Optional, default = 0
    ) )
{
    fail "(".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error();
    exit 1;
}

my $consumer;

eval { $consumer = Kafka::Consumer->new(
    IO          => "nothing",
    RaiseError  => 1
    ) };
ok Kafka::Consumer::last_errorcode(), "expecting to die: (".Kafka::Consumer::last_errorcode().") ".Kafka::Consumer::last_error() if $@;

$consumer = Kafka::Consumer->new(
    IO          => $io,
    RaiseError  => 0                            # Optional, default = 0
    );
unless ( $consumer )
{
    fail "(".Kafka::Consumer::last_errorcode().") ".Kafka::Consumer::last_error();
    exit 1;
}
isa_ok( $consumer, 'Kafka::Consumer');

# Offsets are monotonically increasing integers unique to a partition.
# Consumers track the maximum offset they have consumed in each partition.

# Get a list of valid offsets (up max_number) before the given time.
my $offsets = $consumer->offsets(
    "test",                                     # topic
    0,                                          # partition
    TIMESTAMP_EARLIEST,                         # time
    DEFAULT_MAX_OFFSETS                         # max_number
    );
if( $offsets )
{
    pass "received offsets";
    foreach my $offset ( @$offsets )
    {
        note "Received offset: $offset";
    }
}
# may be both physical and logical errors ("Response contains an error in 'ERROR_CODE'", "Amount received offsets does not match 'NUMBER of OFFSETS'")
if ( !$offsets or $consumer->last_error )
{
    fail "(".$consumer->last_errorcode.") ".$consumer->last_error;
}

is $server->last_request, $requests{4}, "the last request is correct";

# Consuming messages one by one
my $messages = $consumer->fetch(
    "test",                                     # topic
    0,                                          # partition
    0,                                          # offset
    DEFAULT_MAX_SIZE,                           # Maximum size of MESSAGE(s) to receive ~1MB
    );
if ( $messages )
{
    pass "received messages";
    foreach my $m ( @$messages )
    {
        if( $m->valid )
        {
            note "Payload    : ", $m->payload;
            note "offset     : ", $m->offset;
            note "next_offset: ", $m->next_offset;
        }
        else
        {
            diag "Error: ", $m->error;
        }
    }
}
# may be both physical and logical errors ("Checksum error", "Compressed payload", "Response contains an error in 'ERROR_CODE'")
if ( !$messages or $consumer->last_error )
{
    fail "(".$consumer->last_errorcode.") ".$consumer->last_error;
}

# Closes the consumer and cleans up
$consumer->close;
ok( scalar( keys %$consumer ) == 0, "the consumer object is an empty" );

# to kill the mock server process
$server->close;
ok( scalar( keys %$server ) == 0, "the server object is an empty" );
is( kill( 0, $pid ), 0, "the server process isn't alive" );
