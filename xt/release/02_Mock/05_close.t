#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';
use IO::Socket::INET;
use Test::More tests => 7;

use Kafka::Mock;

my $server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.05,
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

    my $pid = $server->last_request( "pid" );
    is( kill( 0, $pid ), 1, "the server process is alive" );

    ok( scalar( keys %$server ) > 0,    "the server object is not an empty" );
    $server->close;

    ok( scalar( keys %$server ) == 0,   "the server object is an empty" );

    is( kill( 0, $pid ), 0, "the server process isn't alive" );
}
