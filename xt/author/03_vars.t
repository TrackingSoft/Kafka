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
use File::Find;
use File::Spec::Functions qw(
    catdir
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

my ( $modules_dir, $t_modules_dir, @modules, @t_modules );

#-- Global data ----------------------------------------------------------------

$modules_dir    = abs_path( 'lib' );
$t_modules_dir  = abs_path( catdir( 't', 'lib' ) );

foreach my $config (
        { dir => $modules_dir,      re => qr(\.pm$),    array => \@modules, },
        { dir => $t_modules_dir,    re => qr(\.pm$),    array => \@t_modules, },
    ) {
    find(
        {
            wanted      => sub {
                my $file = $File::Find::name;
                push( @{ $config->{array} }, abs_path( $file ) ) if -f $file && $file =~ $config->{re};
            },
            preprocess  => sub { sort @_; },
        },
        $config->{dir},
    );
}

# INSTRUCTIONS -----------------------------------------------------------------

eval { use Test::Vars; };
plan( skip_all => 'Test::Vars not installed: $@; skipping' ) if $@;

vars_ok( $_ ) foreach @modules, @t_modules;

# POSTCONDITIONS ---------------------------------------------------------------

done_testing();
