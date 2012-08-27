#!/usr/bin/perl -w

use 5.009004;
use strict;
use warnings;

use lib 'lib';
use bytes;

use Test::More;
plan "no_plan";

use Kafka qw( BITS64 );

SKIP:
{
    skip( "because Kafka::Int64 is used only if no 64-bit int available", 1 ) if BITS64;

    # numbers does not bigint
    my $n4  = 4;
    my $n45 = 4.5;
    my $n0  = 0;
    my $n_1 = -1;
    my $n_2 = -2;
    my $n_3 = -3;

    use Kafka::Int64;

# after the announcement of the new numbers will be bigint
    use bigint;                                 # this allows integers of practially any size at the cost of significant performance drop

    my $ret;
    is( bytes::length( Kafka::Int64::packq( $n4 ) ),    8, 'binary string of length 64 bits (non bigint 4)' );
    is( bytes::length( Kafka::Int64::packq( $n45 ) ),   8, 'binary string of length 64 bits (non bigint 4.5)' );
    is( bytes::length( Kafka::Int64::packq( $n0 ) ),    8, 'binary string of length 64 bits (non bigint 0)' );
    is( bytes::length( Kafka::Int64::packq( $n_1 ) ),   8, 'binary string of length 64 bits (non bigint -1)' );
    is( bytes::length( Kafka::Int64::packq( $n_2 ) ),   8, 'binary string of length 64 bits (non bigint -2)' );
    is( bytes::length( Kafka::Int64::packq( 4 ) ),      8, 'binary string of length 64 bits (bigint 4)' );
    is( bytes::length( Kafka::Int64::packq( 4.5 ) ),    8, 'binary string of length 64 bits (bigint 4.5)' );
    is( bytes::length( Kafka::Int64::packq( 0 ) ),      8, 'binary string of length 64 bits (bigint 0)' );
    is( bytes::length( Kafka::Int64::packq( -1 ) ),     8, 'binary string of length 64 bits (bigint -1)' );
    is( bytes::length( Kafka::Int64::packq( -2 ) ),     8, 'binary string of length 64 bits (bigint -2)' );

    eval { Kafka::Int64::packq( $n_3 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (undef)" );
    eval { Kafka::Int64::packq( $n_3 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (inadmissible negative non bigint number)" );
    eval { Kafka::Int64::packq( -3 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (inadmissible negative bigint number)" );
    eval { Kafka::Int64::packq( "string" ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (argument not the number)" );
}