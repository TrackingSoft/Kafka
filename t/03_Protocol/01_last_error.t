#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the function Kafka::Protocol::last_error

use lib 'lib';

use Test::More tests => 4;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

# -- verify load the module
BEGIN { use_ok 'Kafka::Protocol' }

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify the last error before and after the expected exception
ok( !defined( Kafka::Protocol::last_error ), "not defined last_error" );
throws_ok { Kafka::Protocol::fetch_response( [] ) } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
ok( defined( Kafka::Protocol::last_error ), "defined last_error = ".Kafka::Protocol::last_error );

# POSTCONDITIONS ---------------------------------------------------------------
