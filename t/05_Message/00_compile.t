#!/usr/bin/perl -w

use 5.008003;
use strict;
use warnings;

# NAME: Tests load by Kafka::Message

use lib 'lib';

use Test::More tests => 9;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

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

lives_ok { $message = Kafka::Message->new( $msg ) } 'expecting to live';
isa_ok( $message, 'Kafka::Message' );

# -- verify the availability of functions
can_ok( $message, $_ ) for qw( new payload valid error offset next_offset );


# POSTCONDITIONS ---------------------------------------------------------------
