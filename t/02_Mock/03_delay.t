#!/usr/bin/perl -w

use 5.008;
use strict;
use warnings;

use IO::Socket::INET;
use Test::More tests => 30;
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
    timeout     => 0.5,
    );
isa_ok( $server, 'Kafka::Mock');

my $port = $server->port;
ok $port, "server port = $port";

SKIP:
{
    {
        my $sock = IO::Socket::INET->new(
            PeerPort => $port,
            PeerAddr => '127.0.0.1',
            Proto    => 'tcp'
            );
        skip( "because Cannot open client socket: $!", 1 ) unless $sock;

        ok defined($sock), "open client socket";
    }

    lives_ok { $server->delay( "response", 10, 0.5 ) } "expecting to live (normal args)";
    lives_ok { $server->delay( "response", 4, 0.5 ) }  "expecting to live (normal args)";
    lives_ok { $server->delay( "request",  10, 0.5 ) } "expecting to live (normal args)";

    dies_ok { $server->delay() } "expecting to die (without args)";

# operation
    foreach my $arg ( ( undef, "", "any", {}, 0, 1, -1 ) )
    {
        dies_ok { $server->delay( $arg, 10, 0.5 ) } "expecting to die (operation = ".( $arg || "" ).")";
    }

# position (request)
    foreach my $arg ( ( undef, "", "any", {}, -1, 0.5, 0, 4 ) )
    {
        dies_ok { $server->delay( "request", $arg, 0.5 ) } "expecting to die (request position = ".( $arg || "" ).")";
    }

# delay
    foreach my $arg ( ( undef, "", "any", {}, -1, 0 ) )
    {
        dies_ok { $server->delay( "request", 10, $arg ) } "expecting to die (delay = ".( $arg || "" ).")";
    }
#    $server->close;

    $server = Kafka::Mock->new(
        requests    => {},
        responses   => {},
        timeout     => 0.1,
        );
    $port = $server->port;
    my $sock = IO::Socket::INET->new(
        PeerPort => $port,
        PeerAddr => '127.0.0.1',
        Proto    => 'tcp'
        );

    $server->delay( "response", 1, 1 );
    is( $server->last_request( "note" ), "delay response: 1 1 delay request: ", "set the delay (response)" );
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

    $server->delay( "request",  8, 0.5 );
    is( $server->last_request( "note" ), "delay response:  delay request: 8 0.5", "set the delay (request)" );
}

$server->close;
