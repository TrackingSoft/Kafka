#!/usr/bin/perl -w



use 5.010;
use strict;
use warnings;



use Test::More;





use Cwd qw(
    abs_path
);







my $modules_dir = abs_path( 'lib' );



eval 'use Test::Pod;';  ## no critic
plan skip_all => 'because Test::Pod required for testing' if $@;

all_pod_files_ok( $modules_dir );


