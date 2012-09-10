#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use Test::More;
use lib 'lib';

# 'prereq' falsely triggered by:
#   Digest::CRC
#   Params::Util
# 'prereq' verification test is performed correctly by 96_kwalitee.t
eval 'use Test::Distribution not => "prereq"';
plan( skip_all => 'Test::Distribution not installed' ) if $@;
Test::Distribution->import(  );
