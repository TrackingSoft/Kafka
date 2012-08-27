#!/usr/bin/perl -w

use 5.008;
use strict;
use warnings;

use lib 'lib';

use Test::More;

eval "use Test::Pod::Coverage";
plan skip_all   => "Test::Pod::Coverage required for testing POD coverage" if $@;
plan tests      => 1;
pod_coverage_ok( "Kafka::Producer");