#!/usr/bin/perl -w



use 5.010;
use strict;
use warnings;

use lib qw(
    lib
);



use Test::More;













eval 'use Test::Pod::Coverage;';    ## no critic
plan skip_all => 'because Test::Pod::Coverage required for testing' if $@;

all_pod_coverage_ok();


