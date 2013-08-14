#!/usr/bin/perl -w

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
);

# ENVIRONMENT ------------------------------------------------------------------

use Test::More;

plan 'no_plan';

#-- verify load the module

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

#-- load the modules -----------------------------------------------------------

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

#-- Global data ----------------------------------------------------------------

# INSTRUCTIONS -----------------------------------------------------------------

eval { use Test::Dist::VersionSync; };
plan skip_all => 'because Test::Dist::VersionSync required for testing' if $@;

Test::Dist::VersionSync::ok_versions();

# POSTCONDITIONS ---------------------------------------------------------------
