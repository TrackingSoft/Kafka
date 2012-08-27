#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the method Kafka::Producer::send

use lib 'lib';

use Test::More tests => 26;

# PRECONDITIONS ----------------------------------------------------------------

use Kafka::Mock;
use Kafka::IO;

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

# -- declaration of variables to test
my ( $server, $io, $producer );

my $topic       = "test";
my $partition   = 0;

# control request to a single message
my $single_message = "The first message";

# control request to a series of messages
my $series_of_messages = [
        "The first message",
        "The second message",
        "The third message",
    ];

sub my_io {
    my $io      = shift;

    $$io = Kafka::IO->new(
        host        => "localhost",
        port        => $server->port,
        );
}

sub my_close {
    $producer->close if $producer;
    $producer  = $io = undef;
}

# -- verification of the objects creation

$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    );
isa_ok( $server, 'Kafka::Mock');

my_io( \$io );
isa_ok( $io, 'Kafka::IO');

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify load the module

BEGIN { use_ok 'Kafka::Producer' }

$producer = Kafka::Producer->new(
    IO          => $io,
    );
isa_ok( $producer, 'Kafka::Producer');

# -- verify response to arguments

ok !$producer->send(),                      "because without args";
ok !$producer->send( anything => "any" ),   "because only unknown arg";

# topic: to see if a value is a normal non-false string of non-zero length
foreach my $topic ( ( undef, 0, "", "0", \"scalar", [] ) )
{
    ok !$producer->send( $topic, $partition, $single_message ), "because mismatch argument";
}

# partition: to see if a value is a non-negative integer (of any length). That is, a positive integer, or zero
foreach my $partition ( ( undef, 0.5, -1, "", "0.5", \"scalar", [] ) )
{
    ok !$producer->send( $topic, $partition, $single_message ), "because mismatch argument";
}

# messages:
#   to see if a value is a normal non-false string of non-zero length
#   or a raw and unblessed ARRAY reference, allowing ARRAY references that contain no elements
foreach my $messages ( ( undef, 0, "", "0", \"scalar" ) )
{
    ok !$producer->send( $topic, $partition, $messages ), "because mismatch argument";
}

# -- verify sent for a single message
is $producer->send( $topic, $partition, $single_message ), 1, "sent a single message";

# -- verify sent for a series of messages
is $producer->send( $topic, $partition, $series_of_messages ), 1, "sent a series of messages";

# POSTCONDITIONS ---------------------------------------------------------------

# -- Closes and cleans up
my_close();
$server->close;
