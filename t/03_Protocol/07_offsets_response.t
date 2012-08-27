#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the function Kafka::Protocol::offsets_response

use lib 'lib';

use Test::More tests => 17;
use Params::Util qw( _HASH );

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

# preload the error code
use Kafka qw(
    ERROR_NUMBER_OF_OFFSETS
    BITS64
    );

# -- verify load the module
BEGIN { use_ok 'Kafka::Protocol', qw( offsets_response ) }

# -- declaration of variables to test

# control response
my $response = pack( "H*",                      # OFFSETS       Response
    # Response Header
     '0000000e'                                 # RESPONSE_LENGTH
    .'0000'                                     # ERROR_CODE
    # OFFSETS Response
    .'00000001'                                 # NUMBER of OFFSETS
    .'0000000000000000'                         # OFFSET
    );

# a control decoded response
my $decoded = {
    'offsets'               => [ 0 ],
    'error'                 => '',
    'number_offsets'        => 1,
    'header'                => {
        'response_length'   => 0xe,
        'error_code' => 0
        }
    };

# control response (bad NUMBER of OFFSETS)
my $response_offsets = pack( "H*",              # OFFSETS       Response
    # Response Header
     '0000000e'                                 # RESPONSE_LENGTH
    .'0000'                                     # ERROR_CODE
    # OFFSETS Response
    .'00000002'                                 # NUMBER of OFFSETS - bad
    .'0000000000000000'                         # OFFSET
    );

# a control decoded response (bad NUMBER of OFFSETS)
my $decoded_offsets = {
    'offsets'               => [ 0 ],
    'error'                 => $Kafka::ERROR[ERROR_NUMBER_OF_OFFSETS],
    'number_offsets'        => 2,
    'header'                => {
        'response_length'   => 0xe,
        'error_code' => 0
        }
    };

# INSTRUCTIONS -----------------------------------------------------------------
# -- verify response to invalid arguments

# without args
throws_ok { offsets_response() } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';

# topic (length > = 6): to see if a value is a normal non-false string of non-zero length
foreach my $response ( ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", [] ) )
{
    throws_ok { offsets_response( $response ) } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# -- verify response
foreach my $pair ( (
    { response => $response,            decoded => $decoded },
    { response => $response_offsets,    decoded => $decoded_offsets },
    ) )
{
    ok defined( _HASH( offsets_response( \$pair->{response} ) ) ), "a raw and unblessed HASH reference with at least one entry";
    ok eq_hash( offsets_response( \$pair->{response} ), $pair->{decoded} ), "contain the correct keys and values";
}

# POSTCONDITIONS ---------------------------------------------------------------
