#!/usr/bin/perl -w



use 5.010;
use strict;
use warnings;



use Test::More;













# 'prereq' falsely triggered by:
#   Const::Fast
#   String::CRC32
# 'prereq' verification test is performed correctly by ??_kwalitee.t
eval "use Test::Distribution not => 'prereq';"; ## no critic
plan skip_all => 'because Test::Distribution required for testing' if $@;


