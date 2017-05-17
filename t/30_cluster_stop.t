#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
);

use Test::More;

BEGIN {
    plan skip_all => 'Unknown base directory of Kafka server'
        unless $ENV{KAFKA_BASE_DIR};
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use Const::Fast;
use File::HomeDir;
use File::Spec;

use Kafka::Cluster;

my $cluster = Kafka::Cluster->new(
    reuse_existing  => 1,
);
isa_ok( $cluster, 'Kafka::Cluster' );

$cluster->close;

