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
use File::HomeDir;
use Params::Util qw(
    _HASH
);

use Kafka qw(
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $RECEIVE_EARLIEST_OFFSETS
    $RECEIVE_LATEST_OFFSET
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Consumer;

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

# port to start the search Kafka server
const my $START_PORT        => 9094;        # Port Number 9094-9099 Unassigned
const my $ITERATIONS        => 100;         # The maximum number of attempts

# WARNING: must match the settings of your system
const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR} || File::Spec->catdir( File::HomeDir->my_home, 'kafka' );

const my $topic             => $Kafka::Cluster::DEFAULT_TOPIC;
const my $partition         => 0;

#-- Global data ----------------------------------------------------------------

my ( $port, $connect, $consumer, $offsets, $messages );

# INSTRUCTIONS -----------------------------------------------------------------

#-- Connecting to the Kafka server port

( $port ) = Kafka::Cluster->new( kafka_dir => $KAFKA_BASE_DIR, does_not_start => 1 )->servers;  # for example for node_id = 0

$connect = Kafka::Connection->new(
    host    => 'localhost',
    port    => $port,
);

$consumer = Kafka::Consumer->new(
    Connection  => $connect,
    RaiseError  => 0            # Optional, default = 0
);

#-- OffsetRequest

# RTFM:
# The Offset API describes the valid offset range available for a set of topic-partitions.
# The response contains the starting offset of each segment for the requested partition
# as well as the "log end offset" i.e. the offset of the next message that would be appended to the given partition.

# Get a list of valid offsets up max_number before the given time
$offsets = $consumer->offsets(
    $topic,                         # topic
    $partition,                     # partition
# RTFM: There are two special values.
# Specify -1 ($RECEIVE_LATEST_OFFSET) to receive the latest offset (this will only ever return one offset).
# Specify -2 ($RECEIVE_EARLIEST_OFFSETS) to receive the earliest available offsets.
#    $RECEIVE_EARLIEST_OFFSETS,      # time
    $RECEIVE_LATEST_OFFSET,         # time
    $DEFAULT_MAX_NUMBER_OF_OFFSETS, # max_number
    );
if( $offsets ) {
    foreach my $offset ( @$offsets ) {
        note "Received offset: $offset";
    }
}
if ( !$offsets || $consumer->last_errorcode ) {
    BAIL_OUT 'Error retrieving data: ('.$consumer->last_errorcode.') '.$consumer->last_error;
}

#-- FetchRequest

# Consuming messages
$messages = $consumer->fetch(
    $topic,             # topic
    $partition,         # partition
    0,                  # offset
    $DEFAULT_MAX_BYTES  # Maximum size of MESSAGE(s) to receive
);
if ( $messages ) {
    foreach my $message ( @$messages ) {
#        note '--------------------';
        if( $message->valid ) {
#            note 'key                : ', $message->key;
#            note 'payload            : ', $message->payload;
#            note 'offset             : ', $message->offset;
#            note 'next_offset        : ', $message->next_offset;
#            note 'HighwaterMarkOffset: ', $message->HighwaterMarkOffset;
        }
        else {
            diag 'error      : ', $message->error;
        }
    }
}

# WARNING: the connections can be used by other instances of the class Kafka::Connection
$connect->close;

# POSTCONDITIONS ---------------------------------------------------------------
