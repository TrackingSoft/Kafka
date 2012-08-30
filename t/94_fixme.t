#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use Test::More;

eval "use Test::Fixme";
plan skip_all => "Test::Fixme required for testing Fixme" if $@;
run_tests(
#    where       => 'lib',      # where to find files to check
    manifest    => 'MANIFEST',
    match       => qr/^#[TODO|FIXME|BUG]/,
#    skip_all    => $ENV{SKIP}  # should all tests be skipped
    );
