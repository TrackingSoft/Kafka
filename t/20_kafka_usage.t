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
    plan skip_all => 'Unknown base directory of Kafka server'
        unless $ENV{KAFKA_BASE_DIR};
}

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
use Socket;

# Usage - Basic functionalities to include a simple Producer and Consumer
# You need to have access to your Kafka instance and be able to connect through TCP

use Kafka qw(
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $KAFKA_SERVER_PORT
    $RECEIVE_LATEST_OFFSETS
    $RECEIVE_EARLIEST_OFFSET
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::Producer;

# If the reader closes the connection, though, the writer will get a SIGPIPE when it next tries to write there.
$SIG{PIPE} = sub { die };

# port to start the search Kafka server
const my $START_PORT        => 9094;        # Port Number 9094-9099 Unassigned
const my $ITERATIONS        => 100;         # The maximum number of attempts

const my $topic             => $Kafka::Cluster::DEFAULT_TOPIC;
const my $partition         => 0;


my ( $connect, $producer, $consumer, $response, $offsets );

#-- Connecting to the Kafka server port
my $cluster = Kafka::Cluster->new( reuse_existing => 1 );
isa_ok( $cluster, 'Kafka::Cluster' );

my( $port ) = $cluster->servers;  # for example for node_id = 0

for my $host_name ( 'localhost', '127.0.0.1' ) {

pass "Host name: $host_name";

#-- Connection

dies_ok { $connect = Kafka::Connection->new(
    host        => $host_name,
    port        => $port,
    timeout     => 'nothing',
    dont_load_supported_api_versions => 1,
) } 'expecting to die';

$connect = Kafka::Connection->new(
    host            => $host_name,
    port            => $port,
    RETRY_BACKOFF   => $RETRY_BACKOFF * 2,
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
isa_ok( $producer, 'Kafka::Producer');

# Sending a single message
if ( !( $response = $producer->send(
    $topic,                     # topic
    $partition,                 # partition
    'Single message',           # message
    ) ) ) {
    fail 'message is not sent';
} else {
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
    fail 'messages is not sent';
} else {
    pass 'messages sent';
}

# Closes the connection producer and cleans up
undef $producer;
ok( !defined( $producer ), 'the producer object is an empty' );
$connect->close;

#-- Consumer

undef $connect;
unless ( $connect = Kafka::Connection->new(
    host            => $host_name,
    port            => $port,
    RETRY_BACKOFF   => $RETRY_BACKOFF * 2,
    dont_load_supported_api_versions => 1,
    ) ) {
    fail 'connection is not created';
}

dies_ok { $consumer = Kafka::Consumer->new(
    Connection  => "nothing",
) } 'expecting to die';

lives_ok { $consumer = Kafka::Consumer->new(
    Connection  => $connect,
) } 'expecting to live';
unless ( $consumer ) {
    fail 'consumer is not created';
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

}

