#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
);

use Test::More;

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use Const::Fast;
#use Data::Dumper;

use Kafka qw(
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $KAFKA_SERVER_PORT
    $RECEIVE_LATEST_OFFSETS
    $RECEIVE_EARLIEST_OFFSET
    $REQUEST_TIMEOUT
);
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::Producer;

use Kafka::MockIO;

const my $topic             => 'mytopic';
# Use Kafka::MockIO only with the following information:
const my $partition         => $Kafka::MockIO::PARTITION;

my ( $connect, $producer, $consumer, $response, $offsets );

#-- Connecting to the Kafka mocked server port
my $port = $KAFKA_SERVER_PORT;

Kafka::MockIO::override();

#-- Connection

dies_ok { $connect = Kafka::Connection->new(
    host        => 'localhost',
    port        => $port,
    timeout     => 'nothing',
    dont_load_supported_api_versions => 1,
) } 'expecting to die';

$connect = Kafka::Connection->new(
    host    => 'localhost',
    port    => $port,
    dont_load_supported_api_versions => 1,
);
isa_ok( $connect, 'Kafka::Connection');

#-- Producer

dies_ok { $producer = Kafka::Producer->new(
    Connection  => "nothing",
) } 'expecting to die';

undef $producer;
lives_ok { $producer = Kafka::Producer->new(
    Connection  => $connect,
) } 'expecting to live';
unless ( $producer ) {
    BAIL_OUT 'producer is not created';
}
isa_ok( $producer, 'Kafka::Producer');

# Sending a single message
if ( !( $response = $producer->send(
    $topic,                     # topic
    $partition,                 # partition
    'Single message',           # message
    ) ) ) {
    BAIL_OUT 'response is not received';
}
else {
    pass 'message is sent';
}

# Sending a series of messages
if ( !( $response = $producer->send(
    $topic,                     # topic
    $partition,                 # partition
    [                           # messages
        "The first message",
        "The second message",
        "The third message",
    ],
    ) ) ) {
    BAIL_OUT 'producer is not created';
}
else {
    pass 'messages sent';
}

# Closes the connection producer and cleans up
undef $producer;
ok( !defined( $producer ), 'the producer object is an empty' );
$connect->close;

#-- Consumer

$connect->close;
undef $connect;
unless ( $connect = Kafka::Connection->new(
    host    => 'localhost',
    port    => $port,
    dont_load_supported_api_versions => 1,
    ) ) {
    BAIL_OUT 'connection is not created';
}

dies_ok { $consumer = Kafka::Consumer->new(
    Connection  => "nothing",
    ) } 'expecting to die';

lives_ok { $consumer = Kafka::Consumer->new(
    Connection  => $connect,
) } 'expecting to live';
unless ( $consumer ) {
    BAIL_OUT 'consumer is not created';
}
isa_ok( $consumer, 'Kafka::Consumer');

# Offsets are monotonically increasing integers unique to a partition.
# Consumers track the maximum offset they have consumed in each partition.

# Get a list of valid offsets (up max_number) before the given time.
$offsets = $consumer->offsets(
    $topic,                         # topic
    $partition,                     # partition
    $RECEIVE_LATEST_OFFSETS,         # time
    $DEFAULT_MAX_NUMBER_OF_OFFSETS, # max_number
    );
if ( $offsets ) {
    pass 'received offsets';
    foreach my $offset ( @$offsets ) {
        note "Received offset: $offset";
    }
}
# may be both physical and logical errors
if ( !$offsets ) {
    fail 'offsets are not received';
}

# Consuming messages one by one
my $messages = $consumer->fetch(
    $topic,                         # topic
    $partition,                     # partition
    0,                              # offset
    $DEFAULT_MAX_BYTES,             # Maximum size of MESSAGE(s) to receive
);
if ( $messages ) {
    pass 'received messages';
    my $cnt = 0;
    foreach my $m ( @$messages ) {
        if ( $m->valid ) {
#            note "Payload    : ", $m->payload;
#            note "offset     : ", $m->offset;
#            note "next_offset: ", $m->next_offset;
        } else {
            diag "Message No $cnt, Error: ", $m->error;
            diag 'Payload    : ', $m->payload;
            diag 'offset     : ', $m->offset;
            diag 'next_offset: ', $m->next_offset;
        }
        ++$cnt;
        last if $cnt > 100;         # enough
    }
}
# may be both physical and logical errors
if ( !$messages ) {
    fail 'messages are not received';
}

# Closes the consumer and cleans up
undef $consumer;
ok( !defined( $producer ), 'the consumer object is an empty' );
$connect->close;

