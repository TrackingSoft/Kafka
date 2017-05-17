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
use File::Find;
use File::Spec::Functions qw(
    catdir
);





my ( $modules_dir, $t_modules_dir, @modules, @t_modules );



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



eval 'use Test::Vars;'; ## no critic
plan( skip_all => 'Test::Vars not installed: $@; skipping' ) if $@;

vars_ok( $_ ) foreach @modules, @t_modules;



done_testing();
