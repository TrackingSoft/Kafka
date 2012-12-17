#!/usr/bin/perl -w

use strict;
use warnings;
use 5.010;

if ( $^O eq 'MSWin32' ) {
    print STDERR "OS unsupported: $^O\n";
    exit( 255 );
}

use File::Basename;
use ExtUtils::MakeMaker qw( prompt );
use Getopt::Long;

my ( $no_full_tests, $full_tests, $answer, $topic );

GetOptions(
    'no_full_tests!'    => \$no_full_tests,
    'full_tests!'       => \$full_tests,
    'topic=s'           => \$topic,
);
$answer = 'no'  if $no_full_tests;
$answer = 'yes' if $full_tests;

my %config = (
    NAME                        => 'Kafka',
    VERSION_FROM                => 'lib/Kafka.pm',  # finds $VERSION
    MIN_PERL_VERSION            => '5.010',
    PREREQ_PM                   => {            # more to the list $Module::CoreList::version{5.010}
        'Digest::CRC'           => 0.18,
        'Params::Util'          => 1.04,
        },
    BUILD_REQUIRES              => {            # more to the list $Module::CoreList::version{5.010}
        'Test::Deep'            => 0.109,
        'Test::Exception'       => 0.31,
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

my @patterns = qw( t/*.t t/*/*.t );
$config{test} = { TESTS => join ' ', map { glob } @patterns };

print "=  => To run a full tests requires a running local server kafka (port 9092) <=  =\n" unless $answer;
$answer ||= prompt( 'Do you want to run a full tests of the client?', 'no' );

if ( $answer =~ m/^y/i ) 
{
    print "I'm going to run a full tests.\n";
    $answer = $topic || prompt( 'Please specify the topic for use with the kafka server:', 'test' );

    my @files = $config{test}->{TESTS} =~ m| (t\/99_usage\/\d\d_kafka.*?\.t)|g;
    foreach my $f_name ( @files )
    {
        if ( open( my $fh, "+<", $f_name ) )
        {
            my $content = "";
            while ( <$fh> )
            {
                if ( /^__DATA__/ )
                {
                    chomp $_;
                    $content .= "$_\n$answer";
                    seek $fh, 0, 0;
                    print $fh $content if truncate $fh, 0;
                    last;
                }
                else
                {
                    $content .= $_;
                }
            }
            close $fh;
        }
    }
}
else
{
    $config{test}->{TESTS} =~ s| t\/99_usage\/\d\d_kafka.*?\.t||g;
    print "Miss the interaction with the kafka server.\n";
}

if( $ENV{AUTHOR_TESTS} ) {
    @patterns = qw( xt/author/*.t xt/author/*/*.t );
    $config{test}->{TESTS} .= ' '.join ' ', map { glob } @patterns;
}

if( $ENV{RELEASE_TESTS} ) {
    @patterns = qw( xt/release/*.t xt/release/*/*.t );
    $config{test}->{TESTS} .= ' '.join ' ', map { glob } @patterns;
}

ExtUtils::MakeMaker::WriteMakefile( %config );
