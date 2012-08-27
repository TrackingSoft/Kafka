#!/usr/bin/perl -w

use 5.008003;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 8;

BEGIN { use_ok 'Kafka::Mock' }

my %requests = (
    0   => '00' x 10,
    1   => '01' x 10,
    2   => '02' x 10,
    3   => '03' x 10,
    4   => '04' x 10
    );

my %responses = (
    0   => 'ff' x 12,
    1   => 'f0' x 12,
    2   => 'e0' x 12,
    3   => 'd0' x 12,
    4   => 'c0' x 12
    );

my $server = Kafka::Mock->new(
    requests    => \%requests,
    responses   => \%responses,
    timeout     => 0.1,
    );

isa_ok( $server, 'Kafka::Mock');

can_ok( $server, $_) for qw( new port delay clear close last_request );

$server->close;
