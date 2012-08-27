#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Tests load and export by Kafka::Protocol

use lib 'lib';

use Test::More tests => 4;

# PRECONDITIONS ----------------------------------------------------------------

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify load the module
BEGIN { use_ok 'Kafka::Protocol', qw(
    REQUESTTYPE_PRODUCE
    REQUESTTYPE_FETCH
    REQUESTTYPE_MULTIFETCH
    REQUESTTYPE_MULTIPRODUCE
    REQUESTTYPE_OFFSETS
    produce_request
    fetch_request
    offsets_request
    fetch_response
    offsets_response
    ) }

# -- verify import the requesttypes
can_ok( __PACKAGE__, qw(
    REQUESTTYPE_PRODUCE
    REQUESTTYPE_FETCH
    REQUESTTYPE_MULTIFETCH
    REQUESTTYPE_MULTIPRODUCE
    REQUESTTYPE_OFFSETS
    ) );

# -- verify the import of functions
can_ok( __PACKAGE__, qw(
    produce_request
    fetch_request
    offsets_request
    fetch_response
    offsets_response
    ) );

# -- verify the availability of functions
can_ok( 'Kafka::Protocol', qw(
    last_error
    last_errorcode
    ) );

# POSTCONDITIONS ---------------------------------------------------------------
