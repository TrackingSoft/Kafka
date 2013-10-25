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

# 'prereq' falsely triggered by:
#   Const::Fast
#   String::CRC32
# 'prereq' verification test is performed correctly by ??_kwalitee.t
eval "use Test::Distribution not => 'prereq';"; ## no critic
plan skip_all => 'because Test::Distribution required for testing' if $@;

# POSTCONDITIONS ---------------------------------------------------------------
