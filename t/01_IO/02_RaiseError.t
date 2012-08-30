#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 12;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing Kafka::IO::RaiseError" if $@;
}

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

$io = Kafka::IO->new(
    host        => "localhost",
    port        => $port,
    timeout     => 1.0,
    RaiseError  => 0
    );
isa_ok( $io, 'Kafka::IO');

is( $io->RaiseError, 0, "default RaiseError = 0" );
$io->close;
#$server->close;

$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.1,
    );
$port = $server->port;
foreach my $RaiseError ( ( 1, 0, 10 ) )
{
    $io = Kafka::IO->new(
        host        => "localhost",
        port        => $port,
        timeout     => 1.5,
        RaiseError  => $RaiseError
        );
    is( $io->RaiseError, $RaiseError, "the normal RaiseError = $RaiseError" );
    is( $io->RaiseError, $RaiseError, "RaiseError = $RaiseError" );
    $io->close;
}
$server->close;

$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.1,
    );
$port = $server->port;
$io = Kafka::IO->new( 
    host        => "localhost",
    port        => $port,
    timeout     => 1.5,
    RaiseError  => 0
    );
lives_ok { $io->send( [] ) } 'expecting to live';

$io->close;
$server->close;

$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.1,
    );
$port = $server->port;
$io = Kafka::IO->new( 
    host        => "localhost",
    port        => $port,
    timeout     => 1.5,
    RaiseError  => 1
    );
dies_ok { $io->send( [] ) } 'expecting to die';

$io->close;
$server->close;
