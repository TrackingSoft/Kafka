#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 12;

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

# If the reader closes the connection, though, the writer will get a SIGPIPE when it next tries to write there.
$SIG{PIPE} = sub { die };

my $topic = <DATA>;
chomp $topic;
$topic ||= "test";

#-- IO

my $io;

eval { $io = Kafka::IO->new(
    host        => "localhost",
    port        => KAFKA_SERVER_PORT,
    timeout     => "nothing",
    RaiseError  => 1
    ) };
ok Kafka::IO::last_errorcode(), "expecting to die: (".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error() if $@;

unless ( $io = Kafka::IO->new(
    host        => "localhost",
    port        => KAFKA_SERVER_PORT,
    timeout     => DEFAULT_TIMEOUT,             # Optional, default = DEFAULT_TIMEOUT
    RaiseError  => 0                            # Optional, default = 0
    ) )
{
    fail "(".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error();
    exit 1;
}
isa_ok( $io, 'Kafka::IO');

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
    $topic,                                     # topic
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
    $topic,                                     # topic
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
    port        => KAFKA_SERVER_PORT,
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
    $topic,                                     # topic
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

# Consuming messages one by one
my $messages = $consumer->fetch(
    $topic,                                     # topic
    0,                                          # partition
    0,                                          # offset
    DEFAULT_MAX_SIZE,                           # Maximum size of MESSAGE(s) to receive ~1MB
    );
if ( $messages )
{
    pass "received messages";
    my $cnt = 0;
    foreach my $m ( @$messages )
    {
        if( $m->valid )
        {
#            note "Payload    : ", $m->payload;
#            note "offset     : ", $m->offset;
#            note "next_offset: ", $m->next_offset;
        }
        else
        {
            diag "Message No $cnt, Error: ", $m->error;
            diag "Payload    : ", $m->payload;
            diag "offset     : ", $m->offset;
            diag "next_offset: ", $m->next_offset;
        }
        ++$cnt;
        last if $cnt >100;
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

# DO NOT REMOVE THE FOLLOWING LINES
__DATA__
test