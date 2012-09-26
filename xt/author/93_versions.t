#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';
use Test::More;

eval "use Test::Dist::VersionSync";
plan( skip_all => "Test::Dist::VersionSync required for testing module versions in the distribution." )
    if $@;

Test::Dist::VersionSync::ok_versions();