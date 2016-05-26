#!/usr/bin/perl -w

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
);

# ENVIRONMENT ------------------------------------------------------------------

use Test::More;

BEGIN {
    plan skip_all => 'Unknown base directory of Kafka server'
        unless $ENV{KAFKA_BASE_DIR};
}

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

#-- verify load the module

BEGIN {
    eval 'use Test::TCP';           ## no critic
    plan skip_all => "because Test::TCP required for testing" if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Const::Fast;
use Net::EmptyPort qw(
    can_bind
);
use Socket;

use Kafka qw(
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $RECEIVE_LATEST_OFFSET
    $RETRY_BACKOFF
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::Producer;

#-- setting up facilities ------------------------------------------------------

if ( eval { Socket::IPV6_V6ONLY } && can_bind( '::1' ) ) {

ok 1, 'starting IPv6 test';

my $CLUSTER = Kafka::Cluster->new(
    kafka_dir               => $ENV{KAFKA_BASE_DIR},    # WARNING: must match the settings of your system
    replication_factor      => 1,
    kafka_properties_file   => 'server.properties.ipv6',
);

#-- Global data ----------------------------------------------------------------

#-- Connecting to the Kafka server port (for example for node_id = 0)
my ( $PORT ) =  $CLUSTER->servers;

my ( $connect, $producer, $consumer, $response, $offsets );

#-- declarations ---------------------------------------------------------------

const my $topic             => $Kafka::Cluster::DEFAULT_TOPIC;
const my $partition         => 0;

# INSTRUCTIONS -----------------------------------------------------------------

$connect = Kafka::Connection->new(
    host            => '::1',
    port            => $PORT,
    RETRY_BACKOFF   => $RETRY_BACKOFF * 2,
);
isa_ok( $connect, 'Kafka::Connection');

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

# Closes the connection
$connect->close;

#-- Consumer

lives_ok { $consumer = Kafka::Consumer->new(
    Connection  => $connect,
) } 'expecting to live';
unless ( $consumer ) {
    fail 'consumer is not created';
}
isa_ok( $consumer, 'Kafka::Consumer');

# Get a list of valid offsets (up max_number) before the given time.
$offsets = $consumer->offsets(
    $topic,                         # topic
    $partition,                     # partition
    $RECEIVE_LATEST_OFFSET,         # time
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
        if( $m->valid ) {
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

# Closes the connection
$connect->close;

# POSTCONDITIONS ---------------------------------------------------------------

$CLUSTER->close;

} else {
    diag 'IPv6 not supported';
}
