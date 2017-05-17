#!/usr/bin/perl -w



use 5.010;
use strict;
use warnings;



use Test::More;





use Cwd qw(
    abs_path
);









#-- Test::Fixme - check code for FIXMEs.
eval 'use Test::Fixme'; ## no critic
plan skip_all => 'because Test::Fixme required for testing' if $@;

run_tests(
    where       => abs_path( 'lib' ),
    match       => qr/^#[TODO|FIXME|BUG]/,
);



done_testing();
