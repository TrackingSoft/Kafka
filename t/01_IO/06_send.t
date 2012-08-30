#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 8;

use Kafka::IO;

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

my $io;
my $ret;

$io = Kafka::IO->new(
    host        => "localhost",
    port        => $port,
    timeout     => 0.5,
    RaiseError  => 0
    );
isa_ok( $io, 'Kafka::IO');

# arguments
foreach my $message ( ( "", [], $io ) )
{
    $io->send( $message );
    ok( defined( $io->last_error ), "last_error = ".$io->last_error." (".( $message || "" ).")" );
}

is( $ret = $io->send( "Hello\n" ), 6, "sent bytes = $ret" );

# the server must close the connection
$server->close;

is( $ret = $io->send( "Hello\n" ), undef, "not sent (server close the connection)" );

$io->close;
