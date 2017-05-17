#!/usr/bin/perl -w



use 5.010;
use strict;
use warnings;



use Test::More;













eval {
    require Test::Kwalitee::Extra;
    Test::Kwalitee::Extra->import( qw(
            :core
            :optional
            :experimental
            !has_separate_license_file
            !prereq_matches_use
            !build_prereq_matches_use
        )
    );
};
plan( skip_all => "Test::Kwalitee::Extra not installed: $@; skipping") if $@;



done_testing();
