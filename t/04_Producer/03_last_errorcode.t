#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the method Kafka::Producer::last_errorcode

use lib 'lib';

use Test::More tests => 7;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

use Kafka::Mock;
use Kafka::IO;

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my ( $server, $io, $producer );

sub my_io {
    my $io      = shift;

    $$io = Kafka::IO->new(
        host        => "localhost",
        port        => $server->port,
        );
}

sub my_close {
    $producer->close if $producer;
#    $producer  = $io = undef;
    $producer = undef;
}

# -- verification of the IO objects creation

$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    );
isa_ok( $server, 'Kafka::Mock');

my_io( \$io );
isa_ok( $io, 'Kafka::IO');
my_close();

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify load the module

BEGIN { use_ok 'Kafka::Producer' }

# -- verify response to arguments

my_io( \$io );

# -- verify the last error before and after the expected exception
my_io( \$io );
$producer = Kafka::Producer->new(
    IO          => $io,
    RaiseError  => 1
    );
isa_ok( $producer, 'Kafka::Producer');

ok( !defined( $producer->last_errorcode ), "not defined last_errorcode" );
throws_ok { $producer->send( \"scalar" ) } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
ok( defined( $producer->last_errorcode ), "defined last_errorcode = ".$producer->last_errorcode );

# POSTCONDITIONS ---------------------------------------------------------------

# -- Closes and cleans up
my_close();
$server->close;
