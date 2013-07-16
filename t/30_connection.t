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
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    $MIN_BYTES_RESPOND_HAS_DATA
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_EARLIEST_OFFSETS
    $RECEIVE_LATEST_OFFSET
    $REQUEST_TIMEOUT
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_PRODUCE
    $APIKEY_OFFSET
    $PRODUCER_ANY_OFFSET
);
use Kafka::Cluster;
use Kafka::Connection;

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

# port to start the search Kafka server
const my $START_PORT        => 9094;    # Port Number 9094-9099 Unassigned
const my $ITERATIONS        => 100;     # The maximum number of attempts

# WARNING: must match the settings of your system
const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR} || File::Spec->catdir( File::HomeDir->my_home, 'kafka' );

const my $topic             => $Kafka::Cluster::DEFAULT_TOPIC;

#-- Global data ----------------------------------------------------------------

my ( $port, $connect, $request, $response );

# INSTRUCTIONS -----------------------------------------------------------------

#-- Connecting to the Kafka server port

( $port ) = Kafka::Cluster->new( kafka_dir => $KAFKA_BASE_DIR, does_not_start => 1 )->servers;  # for example for node_id = 0

$connect = Kafka::Connection->new(
    host    => 'localhost',
    port    => $port,
);

#-- ProduceRequest

# Here and below, the query explicitly indicate ApiKey - producer and consumer must act also

for my $mode (
    $NOT_SEND_ANY_RESPONSE,
    $WAIT_WRITTEN_TO_LOCAL_LOG,
    $BLOCK_UNTIL_IS_COMMITED,
    ) {

    $request = {
        ApiKey                              => $APIKEY_PRODUCE,
        CorrelationId                       => 4,   # for example
        ClientId                            => 'producer',
        RequiredAcks                        => $mode,
        Timeout                             => $REQUEST_TIMEOUT * 1000, # ms
        topics                              => [
            {
                TopicName                   => $topic,
                partitions                  => [
                    {
                        Partition           => 0,
                        MessageSet              => [
                            {
                                Offset          => $PRODUCER_ANY_OFFSET,
                                Key             => q{},
                                Value           => 'Hello!',
                            },
                        ],
                    },
                ],
            },
        ],
    };

    $response = $connect->receive_response_to_request( $request );
    ok _HASH( $response ), 'response is received';
#say Data::Dumper->Dump( [ $response ], [ 'produce_response' ] );
}

#-- FetchRequest

for my $mode (
    $MIN_BYTES_RESPOND_IMMEDIATELY,
    $MIN_BYTES_RESPOND_HAS_DATA,
    ) {

    $request = {
        ApiKey                              => $APIKEY_FETCH,
        CorrelationId                       => 0,
        ClientId                            => 'consumer',
        MaxWaitTime                         => $DEFAULT_MAX_WAIT_TIME,
        MinBytes                            => $mode,
        topics                              => [
            {
                TopicName                   => $topic,
                partitions                  => [
                    {
                        Partition           => 0,
                        FetchOffset         => 0,
                        MaxBytes            => $DEFAULT_MAX_BYTES,
                    },
                ],
            },
        ],
    };

    $response = $connect->receive_response_to_request( $request );
    ok _HASH( $response ), 'response is received';
#say Data::Dumper->Dump( [ $response ], [ 'fetch_response' ] );
}

#-- OffsetRequest

for my $mode (
    $RECEIVE_EARLIEST_OFFSETS,
    $RECEIVE_LATEST_OFFSET,
    ) {

    $request = {
        ApiKey                              => $APIKEY_OFFSET,
        CorrelationId                       => 0,
        ClientId                            => 'consumer',
        topics                              => [
            {
                TopicName                   => $topic,
                partitions                  => [
                    {
                        Partition           => 0,
                        Time                => $mode,
                        MaxNumberOfOffsets  => $DEFAULT_MAX_NUMBER_OF_OFFSETS,
                    },
                ],
            },
        ],
    };

    $response = $connect->receive_response_to_request( $request );
    ok _HASH( $response ), 'response is received';
#say Data::Dumper->Dump( [ $response ], [ 'offset_response' ] );
}

# POSTCONDITIONS ---------------------------------------------------------------
