#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use IO::Socket::INET;
use Test::More tests => 5;
use lib 'lib';

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing Kafka::Mock::delay" if $@;
}

use Kafka::Mock;

my $server;

$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.1,
    );
isa_ok( $server, 'Kafka::Mock');

my $port = $server->port;
ok $port, "server port = $port";

SKIP:
{
    my $sock = IO::Socket::INET->new(
        PeerPort => $port,
        PeerAddr => '127.0.0.1',
        Proto    => 'tcp'
        );
    skip( "because Cannot open client socket: $!", 1 ) unless $sock;

    ok defined($sock), "open client socket";

    $server->delay( "response", 1, 1.5 );
    is( $server->last_request( "note" ), "delay response: 1 1.5 delay request: ", "set the delay" );
    $server->close;

    $server = Kafka::Mock->new(
        requests    => {},
        responses   => {},
        timeout     => 0.1,
        );
    $port = $server->port;
    $sock = IO::Socket::INET->new(
        PeerPort => $port,
        PeerAddr => '127.0.0.1',
        Proto    => 'tcp'
        );

    $server->delay( "response", 1, 1.5 );
    $server->delay( "request",  9, 2.5 );
    $server->clear;
    is( $server->last_request( "note" ), "delay response:  delay request: ", "clear the delay" );
}

$server->close;
