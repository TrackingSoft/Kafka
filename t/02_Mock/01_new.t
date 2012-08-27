#!/usr/bin/perl -w

use 5.008;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 32;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing Kafka::Mock::new" if $@;
}

use Kafka::Mock;

my $server;

$server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    timeout     => 0.25,
    );
isa_ok( $server, 'Kafka::Mock');
$server->close;

lives_ok { $server = Kafka::Mock->new() } "expecting to live (default args)";
isa_ok( $server, 'Kafka::Mock');
$server->close;

lives_ok { $server = Kafka::Mock->new( anything => "any" ) } 'expecting to live (unknown arg)';
isa_ok( $server, 'Kafka::Mock');
$server->close;

# requests, responses
foreach my $arg ( ( undef, 0, "", "string", 0, 1, \"scalar", [], sub {} ) )
{
    dies_ok { Kafka::Mock->new( requests  => $arg ) } "expecting to die (requests  = ".( $arg || "" ).")";
    dies_ok { Kafka::Mock->new( responses => $arg ) } "expecting to die (responses = ".( $arg || "" ).")";
}

# timeout
lives_ok { $server = Kafka::Mock->new( timeout => 0.1 ) } "expecting to live (timeout = 0)";
isa_ok( $server, 'Kafka::Mock');
$server->close;

foreach my $arg ( ( undef, {}, "", "nobody.nothing", -1 ) )
{
    dies_ok { Kafka::Mock->new( timeout => $arg ) } "expecting to die (timeout = ".( $arg || "" ).")";
}

my %hash1 = (
    0   => '00' x 10,
    1   => '01' x 10,
    2   => '02' x 10,
    3   => '03' x 10,
    );

my %hash2 = (
    0   => 'ff' x 12,
    1   => 'f0' x 12,
    2   => 'e0' x 12,
    3   => 'd0' x 12,
    4   => 'c0' x 12
    );

dies_ok { Kafka::Mock->new( requests => \%hash1, responses => \%hash2 ) } "expecting to die (hashes do not match)";
dies_ok { Kafka::Mock->new( requests => \%hash2, responses => \%hash1 ) } "expecting to die (hashes do not match)";
