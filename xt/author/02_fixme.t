#!/usr/bin/perl -w

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

use Test::More;

#-- verify load the module

#-- load the modules -----------------------------------------------------------

use Cwd qw(
    abs_path
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

#-- Global data ----------------------------------------------------------------

# INSTRUCTIONS -----------------------------------------------------------------

#-- Test::Fixme - check code for FIXMEs.
eval 'use Test::Fixme'; ## no critic
plan skip_all => 'because Test::Fixme required for testing' if $@;

run_tests(
    where       => abs_path( 'lib' ),
    match       => qr/^#[TODO|FIXME|BUG]/,
);

# POSTCONDITIONS ---------------------------------------------------------------

done_testing();
