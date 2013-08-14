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

use Cwd qw(
    abs_path
);
use File::Spec::Functions qw(
    catdir
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

my ( $modules_dir, $t_dir, $author_dir, $release_dir, $t_modules_dir, $tools_dir );

#-- Global data ----------------------------------------------------------------

$modules_dir    = abs_path( 'lib' );
$t_dir          = abs_path( 't' );
$author_dir     = abs_path( catdir( 'xt', 'author' ) );
$release_dir    = abs_path( catdir( 'xt', 'release' ) );
$t_modules_dir  = catdir( $t_dir, 'lib' );
$tools_dir      = abs_path( 'tools' );

# INSTRUCTIONS -----------------------------------------------------------------

eval { use Test::PureASCII; };
plan skip_all => 'because Test::PureASCII required for testing' if $@;

all_perl_files_are_pure_ascii(
    {},
    $modules_dir,
    $t_dir,
    $author_dir,
    $release_dir,
    $t_modules_dir,
    $tools_dir,
);

# POSTCONDITIONS ---------------------------------------------------------------

done_testing();
