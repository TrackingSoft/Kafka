#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 19;

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
    timeout     => 1.5,
    RaiseError  => 0
    );
isa_ok( $io, 'Kafka::IO');
$io->close;
$server->close;

# host
$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.1,
    );
$port = $server->port;
foreach my $host ( ( "nobody.nothing", "", 0, 1 ) )
{
    $io = Kafka::IO->new( 
        host        => $host,
        port        => $port,
        timeout     => 1.5,
        RaiseError  => 0
        );
    my $err = $@;
    chomp $err;
    isnt( defined( $io ), 1, "threw Exception: $err (host = $host)" );
}
$server->close;

# port
$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.1,
    );
foreach my $test_port ( ( "", "nobody.nothing", 0, -1, 999999 ) )
{
    $server = Kafka::Mock->new(
        requests    => {},
        responses   => {},
        timeout     => 0.1,
        );

    $io = Kafka::IO->new( 
        host        => "localhost",
        port        => $test_port,
        timeout     => 1.5,
        RaiseError  => 0
        );
    my $err = $@;
    chomp $err;
    isnt( defined( $io ), 1, "threw Exception: $err (port = $test_port)" );
}
$server->close;

# timeout
$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.1,
    );
$port = $server->port;
foreach my $timeout ( ( 0, "", "nothing", -1 ) )
{
    $io = Kafka::IO->new( 
        host        => "localhost",
        port        => $port,
        timeout     => $timeout,
        RaiseError  => 0
        );
    my $err = $@;
    chomp $err;
    isnt( defined( $io ), 1, "threw Exception: $err (timeout = $timeout)" );
}
$server->close;

# RaiseError
$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.1,
    );
$port = $server->port;
foreach my $RaiseError ( ( "", "nothing", -1 ) )
{
    $io = Kafka::IO->new( 
        host        => "localhost",
        port        => $port,
        timeout     => 1.5,
        RaiseError  => $RaiseError
        );
    my $err = $@;
    chomp $err;
    isnt( defined( $io ), 1, "threw Exception: $err (RaiseError = $RaiseError)" );
}
$server->close;
