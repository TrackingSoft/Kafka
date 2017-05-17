#!/usr/bin/perl -w



use 5.010;
use strict;
use warnings;

use lib qw(
    lib
);



use Test::More;

plan 'no_plan';



BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}











eval { use Test::Dist::VersionSync; };
plan skip_all => 'because Test::Dist::VersionSync required for testing' if $@;

Test::Dist::VersionSync::ok_versions();


