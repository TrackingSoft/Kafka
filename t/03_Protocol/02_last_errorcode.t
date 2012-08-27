#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the function Kafka::Protocol::last_errorcode

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
ok( !defined( Kafka::Protocol::last_errorcode ), "not defined last_errorcode" );
throws_ok { Kafka::Protocol::fetch_response( [] ) } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
ok( defined( Kafka::Protocol::last_errorcode ), "defined last_errorcode = ".Kafka::Protocol::last_errorcode );

# POSTCONDITIONS ---------------------------------------------------------------
