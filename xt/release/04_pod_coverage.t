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

#-- verify load the module

#-- load the modules -----------------------------------------------------------

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

#-- Global data ----------------------------------------------------------------

# INSTRUCTIONS -----------------------------------------------------------------

eval { use Test::Pod::Coverage; };
plan skip_all => 'because Test::Pod::Coverage required for testing' if $@;

all_pod_coverage_ok();

# POSTCONDITIONS ---------------------------------------------------------------
