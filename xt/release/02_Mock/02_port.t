#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use IO::Socket::INET;
use Params::Util qw( _NONNEGINT );
use Test::More tests => 4;
use lib 'lib';

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

    ok( defined( _NONNEGINT( $port = $server->port ) ), "server port = $port" );
}

$server->close;
