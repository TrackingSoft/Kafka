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
    $BLOCK_UNTIL_IS_COMMITED
    $NOT_SEND_ANY_RESPONSE
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Producer;

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

my ( $port, $connect, $producer, $response );

# INSTRUCTIONS -----------------------------------------------------------------

#-- Connecting to the Kafka server port

( $port ) = Kafka::Cluster->new( kafka_dir => $KAFKA_BASE_DIR, does_not_start => 1 )->servers;  # for example for node_id = 0

$connect = Kafka::Connection->new(
    host    => 'localhost',
    port    => $port,
);

#-- ProduceRequest

for my $mode (
    $NOT_SEND_ANY_RESPONSE,
    $WAIT_WRITTEN_TO_LOCAL_LOG,
    $BLOCK_UNTIL_IS_COMMITED,
    ) {

    $producer = Kafka::Producer->new(
        Connection      => $connect,
        RequiredAcks    => $mode,
        RaiseError      => 0        # Optional, default = 0
    );

    if ( $producer->last_errorcode ) {
        BAIL_OUT '('.$producer->last_errorcode.') ', $producer->last_error."\n";
    }

    # Sending a single message
    $response = $producer->send(
        $topic,                     # topic
        $partition,                 # partition
        "Single message"            # message
    );
    ok _HASH( $response ), 'response is received';
#diag( Data::Dumper->Dump( [ $response ], [ 'produce_response' ] ) );

    # Sending a series of messages
    $response = $producer->send(
        $topic,                     # topic
        $partition,                 # partition
        [                           # messages
            "The first message",
            "The second message",
            "The third message",
        ]
    );
    ok _HASH( $response ), 'response is received';
#diag( Data::Dumper->Dump( [ $response ], [ 'produce_response' ] ) );
}

# WARNING: the connections can be used by other instances of the class Kafka::Connection
$connect->close;

# POSTCONDITIONS ---------------------------------------------------------------
