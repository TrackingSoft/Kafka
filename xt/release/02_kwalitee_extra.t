#!/usr/bin/perl -w

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

use Test::More;

#-- verify load the module

#-- load the modules -----------------------------------------------------------

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

#-- Global data ----------------------------------------------------------------

# INSTRUCTIONS -----------------------------------------------------------------

eval {
    require Test::Kwalitee::Extra;
    Test::Kwalitee::Extra->import( qw(
            :core
            :optional
            :experimental
            !has_separate_license_file
        )
    );
};
plan( skip_all => "Test::Kwalitee::Extra not installed: $@; skipping") if $@;

# POSTCONDITIONS ---------------------------------------------------------------

done_testing();
