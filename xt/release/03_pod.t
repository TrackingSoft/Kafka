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

my $modules_dir = abs_path( 'lib' );

# INSTRUCTIONS -----------------------------------------------------------------

eval { use Test::Pod; };
plan skip_all => 'because Test::Pod required for testing' if $@;

all_pod_files_ok( $modules_dir );

# POSTCONDITIONS ---------------------------------------------------------------
