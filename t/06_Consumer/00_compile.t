#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Tests load by Kafka::Consumer

use lib 'lib';

use Test::More tests => 10;

# PRECONDITIONS ----------------------------------------------------------------

use Kafka::Mock;
use Kafka::IO;

# -- verification of the IO objects creation
my $server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    );
isa_ok( $server, 'Kafka::Mock' );

my $io = Kafka::IO->new(
    host        => "localhost",
    port        => $server->port,
    );
isa_ok( $io, 'Kafka::IO' );

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify load the module
BEGIN { use_ok 'Kafka::Consumer' }

my $consumer = Kafka::Consumer->new(
    IO          => $io,
    );
unless ( $consumer )
{
    fail "(".Kafka::Consumer::last_errorcode().") ".Kafka::Consumer::last_error();
    exit 1;
}
isa_ok( $consumer, 'Kafka::Consumer' );

# -- verify the availability of functions
can_ok( $consumer, $_ ) for qw( new last_error last_errorcode fetch offsets close );


# POSTCONDITIONS ---------------------------------------------------------------

# -- Closes and cleans up
$consumer->close;
$server->close;
