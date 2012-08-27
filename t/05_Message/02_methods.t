#!/usr/bin/perl -w

use 5.008003;
use strict;
use warnings;

# NAME: Test of the methods Kafka::Message

use lib 'lib';

use Test::More tests => 8;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my $message;

my $msg = {
    payload     => "test",
    valid       => 1,
    error       => '',
    offset      => 0,
    next_offset => 0,
};

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify load the module

BEGIN { use_ok 'Kafka::Message' }

# -- verify methods

lives_ok { $message = Kafka::Message->new( $msg ) } 'expecting to live';
isa_ok( $message, 'Kafka::Message' );

foreach my $method ( qw( payload valid error offset next_offset ) )
{
    is $message->$method, $msg->{ $method }, "proper operation";
}

# POSTCONDITIONS ---------------------------------------------------------------
