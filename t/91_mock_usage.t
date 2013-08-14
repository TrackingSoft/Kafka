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

#-- verify load the module

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Const::Fast;
#use Data::Dumper;

# Usage - Basic functionalities to include a simple Producer and Consumer
# You need to have access to your Kafka instance and be able to connect through TCP

use Kafka qw(
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $KAFKA_SERVER_PORT
    $RECEIVE_LATEST_OFFSET
    $RECEIVE_EARLIEST_OFFSETS
    $REQUEST_TIMEOUT
    );
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::Producer;

use Kafka::MockIO;

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

const my $topic             => 'mytopic';
# Use Kafka::MockIO only with the following information:
const my $partition         => $Kafka::MockIO::PARTITION;

#-- Global data ----------------------------------------------------------------

my ( $port, $connect, $producer, $consumer, $response, $offsets );

# INSTRUCTIONS -----------------------------------------------------------------

#-- Connecting to the Kafka mocked server port

$port = $KAFKA_SERVER_PORT;

Kafka::MockIO::override();

#-- Connection

dies_ok { $connect = Kafka::Connection->new(
    host        => 'localhost',
    port        => $port,
    timeout     => 'nothing',
    RaiseError  => 1,
) } 'expecting to die';
ok( Kafka::Connection->last_errorcode, 'error: ('.Kafka::Connection->last_errorcode.') '.Kafka::Connection->last_error );

$connect = Kafka::Connection->new(
    host    => 'localhost',
    port    => $port,
);
isa_ok( $connect, 'Kafka::Connection');

#-- Producer

dies_ok { $producer = Kafka::Producer->new(
    Connection  => "nothing",
    RaiseError  => 1,
) } 'expecting to die';
ok( Kafka::Producer->last_errorcode, 'error: ('.Kafka::Producer->last_errorcode.') '.Kafka::Producer->last_error );

undef $producer;
lives_ok { $producer = Kafka::Producer->new(
    Connection  => $connect,
    RaiseError  => 0,           # Optional, default = 0
) } 'expecting to live';
unless ( $producer ) {
    BAIL_OUT '('.Kafka::Producer->last_errorcode.') '.Kafka::Producer->last_error;
}
isa_ok( $producer, 'Kafka::Producer');

# Sending a single message
if ( !( $response = $producer->send(
    $topic,                     # topic
    $partition,                 # partition
    'Single message',           # message
    ) ) ) {
    BAIL_OUT '('.$producer->last_errorcode.') '.$producer->last_error;
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
    BAIL_OUT '('.$producer->last_errorcode.') '.$producer->last_error;
}
else {
    pass 'messages sent';
}

# Closes the connection producer and cleans up
undef $producer;
ok( !defined( $producer ), 'the producer object is an empty' );
$connect->close;

#-- Consumer

undef $connect;
unless ( $connect = Kafka::Connection->new(
    host    => 'localhost',
    port    => $port,
    ) ) {
    BAIL_OUT '('.Kafka::Connection->last_errorcode.') '.Kafka::Connection->last_error;
}

dies_ok { $consumer = Kafka::Consumer->new(
    Connection  => "nothing",
    RaiseError  => 1,
    ) } 'expecting to die';
ok( Kafka::Consumer->last_errorcode, 'error: ('.Kafka::Consumer->last_errorcode.') '.Kafka::Consumer->last_error );

lives_ok { $consumer = Kafka::Consumer->new(
    Connection  => $connect,
    RaiseError  => 0,           # Optional, default = 0
) } 'expecting to live';
unless ( $consumer ) {
    BAIL_OUT '('.Kafka::Consumer->last_errorcode.') '.Kafka::Consumer->last_error;
}
isa_ok( $consumer, 'Kafka::Consumer');

# Offsets are monotonically increasing integers unique to a partition.
# Consumers track the maximum offset they have consumed in each partition.

# Get a list of valid offsets (up max_number) before the given time.
$offsets = $consumer->offsets(
    $topic,                         # topic
    $partition,                     # partition
    $RECEIVE_LATEST_OFFSET,         # time
    $DEFAULT_MAX_NUMBER_OF_OFFSETS, # max_number
    );
if( $offsets ) {
    pass 'received offsets';
    foreach my $offset ( @$offsets ) {
        note "Received offset: $offset";
    }
}
# may be both physical and logical errors
if ( !$offsets || $consumer->last_error ) {
    fail '('.$consumer->last_errorcode.') '.$consumer->last_error;
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
        }
        else {
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
if ( !$messages || $consumer->last_error ) {
    fail '('.$consumer->last_errorcode.') '.$consumer->last_error;
}

# Closes the consumer and cleans up
undef $consumer;
ok( !defined( $producer ), 'the consumer object is an empty' );
$connect->close;

# POSTCONDITIONS ---------------------------------------------------------------
