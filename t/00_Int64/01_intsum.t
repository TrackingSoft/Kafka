#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

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
    my $n_5 = -5;

    use Kafka::Int64;

# after the announcement of the new numbers will be bigint
    use bigint;                                 # this allows integers of practially any size at the cost of significant performance drop

    my $ret;
    is( $ret = Kafka::Int64::intsum( 0, $n0 ),      0,      '0   (bigint)     + 0   (non bigint) is bigint 0' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::intsum( 0, $n4 ),      4,      '0   (bigint)     + 4   (non bigint) is bigint 4' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::intsum( 2, $n4 ),      6,      '2   (bigint)     + 4   (non bigint) is bigint 6' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::intsum( 2, $n0 ),      2,      '2   (bigint)     + 0   (non bigint) is bigint 2' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::intsum( 2, $n_5 ),     -3,     '2   (bigint)     + -5  (non bigint) is bigint -3' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::intsum( $n4, $n4 ),    8,      '4   (non bigint) + 4   (non bigint) is bigint 8' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::intsum( $n0, $n0 ),    0,      '0   (non bigint) + 0   (non bigint) is bigint 0' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::intsum( $n_5, $n_5 ),  -10,    '-5  (non bigint) + -5  (non bigint) is bigint -10' );
    isa_ok( $ret, "Math::BigInt" );

    eval { Kafka::Int64::intsum( undef, $n4 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (the undefined first argument)" );
    eval { Kafka::Int64::intsum( 2, undef ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (the undefined second argument)" );
    eval { Kafka::Int64::intsum( 2, $n45 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (second argument not the integer)" );
    eval { Kafka::Int64::intsum( 2, 4 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (second argument the bigint)" );
    eval { Kafka::Int64::intsum( "string", $n4 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (first argument not the number)" );
    eval { Kafka::Int64::intsum( 2, "string" ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (second argument not the number)" );
}