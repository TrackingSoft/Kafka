#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the method Kafka::Protocol::new

use lib 'lib';

use Test::More tests => 18;

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

# -- verify response to arguments

# correct
lives_ok { $message = Kafka::Message->new( $msg ) } 'expecting to live';
isa_ok( $message, 'Kafka::Message' );

# incorrect
dies_ok { $message = Kafka::Message->new() } "exception because without arg";

$msg->{payload} = undef;
dies_ok { $message = Kafka::Message->new( $msg ) } "exception because an undefined payload";

foreach my $msg ( ( undef, 0, 0.5, 1, -1, "", "0", "1", 9999999999999999, \"scalar", [], {}, { anything => "any" } ) )
{
    throws_ok { Kafka::Message->new( $msg ) } qr/^Mismatch argument/, "expecting to die: Mismatch argument";
}

# POSTCONDITIONS ---------------------------------------------------------------
