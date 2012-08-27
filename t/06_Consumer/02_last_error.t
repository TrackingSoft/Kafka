#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the method Kafka::Consumer::last_error

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

my ( $server, $io, $consumer );

sub my_io {
    my $io      = shift;

    $$io = Kafka::IO->new(
        host        => "localhost",
        port        => $server->port,
        );
}

sub my_close {
    $consumer->close if $consumer;
#    $consumer  = $io = undef;
    $consumer = undef;
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

BEGIN { use_ok 'Kafka::Consumer' }

# -- verify response to arguments

my_io( \$io );

# -- verify the last error before and after the expected exception
my_io( \$io );
$consumer = Kafka::Consumer->new(
    IO          => $io,
    RaiseError  => 1
    );
isa_ok( $consumer, 'Kafka::Consumer');

ok( !defined( $consumer->last_error ), "not defined last_error" );
throws_ok { $consumer->fetch() } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
ok( defined( $consumer->last_error ), "defined last_error = ".$consumer->last_error );

# POSTCONDITIONS ---------------------------------------------------------------

# -- Closes and cleans up
my_close();
$server->close;
