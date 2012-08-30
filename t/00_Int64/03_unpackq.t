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
    my $n0  = 0;
    my $n4  = 4;
    my $s   = "";
    my $s4  = "4";
    my $ss  = "abcd";
    my $s0  = chr(0) x 8;
    my $sf  = chr(0xff) x 8;
    my $sH1 = chr(1) x 8;
    my $sH2 = chr(0x10) x 8;;

    use Kafka::Int64;

# after the announcement of the new numbers will be bigint
    use bigint;                                 # this allows integers of practially any size at the cost of significant performance drop

    my $ret;
    is( $ret = Kafka::Int64::unpackq( $s0 ),  0, 'bigint zero' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::unpackq( $sf ),  18446744073709551615, 'bigint 18446744073709551615' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::unpackq( $sH1 ), 72340172838076673, 'bigint 72340172838076673' );
    isa_ok( $ret, "Math::BigInt" );
    is( $ret = Kafka::Int64::unpackq( $sH2 ), 1157442765409226768, 'bigint 1157442765409226768' );
    isa_ok( $ret, "Math::BigInt" );

    eval { $ret = Kafka::Int64::unpackq( undef ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (undef)" );
    eval { Kafka::Int64::unpackq( $n0 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (non bigint zero)" );
    eval { Kafka::Int64::unpackq( $n4 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (non bigint number)" );
    eval { Kafka::Int64::unpackq( $s ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (empty string)" );
    eval { Kafka::Int64::unpackq( $s4 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (short digital string)" );
    eval { Kafka::Int64::unpackq( $ss ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (short string)" );
    eval { Kafka::Int64::unpackq( 4 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (bigint number)" );
    eval { Kafka::Int64::unpackq( 4.5 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (bigint number)" );
    eval { Kafka::Int64::unpackq( 0 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (bigint zero)" );
    eval { Kafka::Int64::unpackq( -2 ) };
    like( $@, qr/Mismatch argument/, "threw Exception: Mismatch argument (bigint negative number)" );
}