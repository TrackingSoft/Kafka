#!/usr/bin/perl -w



use 5.010;
use strict;
use warnings;

use lib qw(
    lib
);



use Test::More;





use Cwd qw(
    abs_path
);
use File::Spec::Functions qw(
    catdir
);





my ( $modules_dir, $t_dir, $author_dir, $release_dir, $t_modules_dir, $tools_dir );



$modules_dir    = abs_path( 'lib' );
$t_dir          = abs_path( 't' );
$author_dir     = abs_path( catdir( 'xt', 'author' ) );
$release_dir    = abs_path( catdir( 'xt', 'release' ) );
$t_modules_dir  = catdir( $t_dir, 'lib' );
$tools_dir      = abs_path( 'tools' );



eval 'use Test::PureASCII;';    ## no critic
plan skip_all => 'because Test::PureASCII required for testing' if $@;

all_perl_files_are_pure_ascii(
    {},
    grep {defined $_}
    $modules_dir,
    $t_dir,
    $author_dir,
    $release_dir,
    $t_modules_dir,
    $tools_dir,
);



done_testing();
