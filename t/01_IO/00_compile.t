#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 11;

BEGIN { use_ok 'Kafka::IO' }

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

my $io = Kafka::IO->new(
    host        => "localhost",
    port        => $port,
    timeout     => 0.5,
    RaiseError  => 0
    );

isa_ok( $io, 'Kafka::IO');

can_ok( $io, $_) for qw( new RaiseError close last_error last_errorcode send receive );

$io->close;

