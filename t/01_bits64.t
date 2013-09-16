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

use Kafka qw (
    $BITS64
    %ERROR
    $ERROR_MISMATCH_ARGUMENT
);

#-- setting up facilities ------------------------------------------------------

SKIP: {
    skip 'You have a 64 bit system', 1, if $BITS64;

#-- declarations ---------------------------------------------------------------

#-- Global data ----------------------------------------------------------------

my $error_mismatch_argument = $ERROR{ $ERROR_MISMATCH_ARGUMENT };
my $qr = qr/$error_mismatch_argument/;

# INSTRUCTIONS -----------------------------------------------------------------

eval { my $ret = unpack( 'Q', 0xff x 8 ) };

if ( !$@ ) {
    ok( $BITS64, 'Your system supports 64-bit integer values' );
}
else {
    ok( !$BITS64, 'Your system not supports 64-bit integer values' );

# numbers does not bigint
    my $n0      = 0;
    my $n4      = 4;
    my $n45     = 4.5;
    my $n_neg1  = -1;
    my $n_neg2  = -2;
    my $n_neg3  = -3;
    my $n_neg5  = -5;

    my $s       = q{};
    my $s4      = '4';
    my $ss      = 'abcd';

    require Kafka::Int64;
    {
# after the announcement of the new numbers will be bigint
        use bigint;

#-- intsum

        foreach my $pair (
                [ 0,        $n0,        0 ],
                [ 0,        $n4,        4 ],
                [ 2,        $n4,        6 ],
                [ 2,        $n0,        2 ],
                [ 2,        $n_neg5,    -3 ],
                [ $n0,      0,          0 ],
                [ $n4,      0,          4 ],
                [ $n4,      2,          6 ],
                [ $n0,      2,          2 ],
                [ $n_neg5,  2,          -3 ],
                [ $n4,      $n4,        8 ],
                [ $n0,      $n0,        0 ],
                [ $n_neg5,  $n_neg5,    -10 ],
            ) {
            my $ret;
            is( $ret = Kafka::Int64::intsum( $pair->[0], $pair->[1] ), $pair->[2],
                $pair->[0].' ('.( ref( $pair->[0] ) eq 'Math::BigInt' ? q{} : 'non ' ).'bigint) + '.$pair->[1].' ('.( ref( $pair->[1] ) eq 'Math::BigInt' ? q{} : 'non ' ).'bigint) is bigint 0' );
            isa_ok( $ret, 'Math::BigInt' );
        }

# NOTE: We presume that the verification of the correctness of the arguments made by the user.

#-- packq

        foreach my $num (
                $n4,
                $n0,
                $n_neg1,
                $n_neg2,
                4,
                4.5,
                0,
                -1,
                -2,
            ) {
            is( length( Kafka::Int64::packq( $num ) ), 8, 'binary string of length 64 bits ('.( ref( $num ) eq 'Math::BigInt' ? q{} : 'non ' )."bigint '$num)" );
        }

# NOTE: We presume that the verification of the correctness of the arguments made by the user.

        throws_ok { Kafka::Int64::packq( -3 ); } 'Kafka::Exception::Int64', 'error thrown';

#-- unpackq

        foreach my $pair (
                [ chr(0)    x 8, 0 ],
                [ chr(0xff) x 8, -1 ],
                [ chr(1)    x 8, 72340172838076673 ],
                [ chr(0x10) x 8, 1157442765409226768 ],
            ) {
            my $ret;
            is( $ret = Kafka::Int64::unpackq( $pair->[0] ), $pair->[1], 'bigint '.$pair->[1] );
            isa_ok( $ret, "Math::BigInt" );
        }

# NOTE: We presume that the verification of the correctness of the arguments made by the user.
    }
}

# POSTCONDITIONS ---------------------------------------------------------------

}
