#!/usr/bin/perl -w

use strict;
use warnings;
use 5.010;

if ( $^O eq 'MSWin32' ) {
    print STDERR "OS unsupported: $^O\n";
    exit( 255 );
}

use ExtUtils::MakeMaker qw( prompt );
use File::Spec::Functions qw(
    catdir
);

my %config = (
    NAME                        => 'Kafka',
    VERSION_FROM                => 'lib/Kafka.pm',  # finds $VERSION
    MIN_PERL_VERSION            => '5.010',
    PREREQ_PM                   => {            # more to the list $Module::CoreList::version{5.010}
        'Const::Fast'           => '0.014',
        'List::MoreUtils'       => 0.33,
        'Params::Util'          => 1.07,
        'Scalar::Util::Numeric' => 0.22,
        'String::CRC32'         => '1.4',
        'Sys::SigAction'        => 0.15,
        },
    BUILD_REQUIRES              => {            # more to the list $Module::CoreList::version{5.010}
        'Capture::Tiny'         => 0.22,
        'Config::IniFiles'      => 2.82,
        'Proc::Daemon'          => 0.14,
        'Sub::Install'          => 0.926,
        'Test::Deep'            => 0.109,
        'Test::Exception'       => 0.31,
        'Test::TCP'             => 2.00,
        },
    ( $] >= 5.005 ?                             ## Add these new keywords supported since 5.005
        (
        ABSTRACT        => 'Apache Kafka low-level synchronous API, which does not use Zookeeper.',
        AUTHOR          => 'TrackingSoft LLC <sgladkov@trackingsoft.com>'
        ) : ()
    ),
    dist        => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean       => { FILES => "Debian_CPANTS.txt Kafka-*.tar.gz" },
    LICENSE     => 'perl',
    NO_MYMETA   => 1,
);

my @patterns = qw( t/*.t );
$config{test} = { TESTS => join ' ', map { glob } @patterns };

if( $ENV{AUTHOR_TESTS} ) {
    @patterns = qw( xt/author/*.t );
    $config{test}->{TESTS} .= ' '.join ' ', map { glob } @patterns;
}

if( $ENV{RELEASE_TESTS} ) {
    @patterns = qw( xt/release/*.t );
    $config{test}->{TESTS} .= ' '.join ' ', map { glob } @patterns;
}

mkdir 't/data' unless -d 't/data';

ExtUtils::MakeMaker::WriteMakefile( %config );
