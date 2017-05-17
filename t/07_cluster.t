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
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use Params::Util qw(
    _HASH
    _SCALAR
);

use Kafka::IO;
use Kafka::Protocol qw(
    decode_metadata_response
    decode_produce_response
);

use Kafka::Cluster;

ok defined( Kafka::Cluster::data_cleanup() ), 'data directory cleaned';

#Kafka::IO->debug_level( 1 );
my $cluster = Kafka::Cluster->new();
isa_ok( $cluster, 'Kafka::Cluster' );

#Kafka::IO->debug_level( 1 );
$cluster->init;
$cluster->start_all;

my ( $response, $decode );

#-- MetadataRequest
#Kafka::IO->debug_level( 1 );
$response = $cluster->request( ( $cluster->servers )[0], '000000230003000000000000000C746573742D726571756573740000000100076D79746F706963' );
ok _SCALAR( $response ), 'correct request';
#diag( 'Hex Stream: ', unpack( 'H*', $$response ) );
$decode = decode_metadata_response( $response );
ok _HASH( $decode ), 'correct decode';
#diag( Data::Dumper->Dump( [ $decode ], [ 'metadata_response' ] ) );

#-- ProduceRequest
#Kafka::IO->debug_level( 1 );
$response = $cluster->request( ( $cluster->servers )[0], '00000049000000000000000400000001000005dc0000000100076d79746f7069630000000100000000000000200000000000000000000000148dc795a20000ffffffff0000000648656c6c6f21' );
ok _SCALAR( $response ), 'correct request';
#diag( 'Hex Stream: ', unpack( 'H*', $$response ) );
$decode = decode_produce_response( $response );
ok _HASH( $decode ), 'correct decode';
#diag( Data::Dumper->Dump( [ $decode ], [ 'produce_response' ] ) );

$cluster->close;

