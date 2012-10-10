#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';
use Test::More;

eval "use Test::Vars";
plan( skip_all => "Test::Vars required for testing module versions in the distribution." )
    if $@;

all_vars_ok();  # check libs in Manifest
