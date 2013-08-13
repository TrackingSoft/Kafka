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
        unless defined $ENV{KAFKA_BASE_DIR};
}

#-- verify load the module

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Const::Fast;
#use Data::Dumper;
#use File::HomeDir;
use File::Spec;
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

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

# WARNING: must match the settings of your system
#const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR} || File::Spec->catdir( File::HomeDir->my_home, 'kafka' );
const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR};

#-- Global data ----------------------------------------------------------------

my ( $response, $decode );

# INSTRUCTIONS -----------------------------------------------------------------

#$Kafka::IO::DEBUG = 1;
my $cluster = Kafka::Cluster->new( kafka_dir => $KAFKA_BASE_DIR );
isa_ok( $cluster, 'Kafka::Cluster' );

#$Kafka::IO::DEBUG = 1;
$cluster->init;
$cluster->start;

#-- MetadataRequest
#$Kafka::IO::DEBUG = 1;
$response = $cluster->request( ( $cluster->servers )[0], '000000230003000000000000000C746573742D726571756573740000000100076D79746F706963' );
ok _SCALAR( $response ), 'correct request';
#diag( 'Hex Stream: ', unpack( 'H*', $$response ) );
$decode = decode_metadata_response( $response );
ok _HASH( $decode ), 'correct decode';
#diag( Data::Dumper->Dump( [ $decode ], [ 'metadata_response' ] ) );

#-- ProduceRequest
#$Kafka::IO::DEBUG = 1;
$response = $cluster->request( ( $cluster->servers )[0], '00000049000000000000000400000001000005dc0000000100076d79746f7069630000000100000000000000200000000000000000000000148dc795a20000ffffffff0000000648656c6c6f21' );
ok _SCALAR( $response ), 'correct request';
#diag( 'Hex Stream: ', unpack( 'H*', $$response ) );
$decode = decode_produce_response( $response );
ok _HASH( $decode ), 'correct decode';
#diag( Data::Dumper->Dump( [ $decode ], [ 'produce_response' ] ) );

$cluster->close;

# POSTCONDITIONS ---------------------------------------------------------------
