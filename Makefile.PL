#!/usr/bin/perl -w

use strict;
use warnings;
use 5.010;

if ( $^O eq 'MSWin32' || $^O eq 'solaris' ||  $^O eq 'cygwin' ) {
    print STDERR "OS unsupported: $^O\n";
    exit( 255 );
}

use ExtUtils::MakeMaker qw(
    prompt
);
use File::Spec::Functions qw(
    catdir
);

my %config = (
    NAME                => 'Kafka',
    VERSION_FROM        => 'lib/Kafka.pm',  # finds $VERSION
    MIN_PERL_VERSION    => '5.010',
    PREREQ_PM           => {            # more to the list $Module::CoreList::version{5.010}
        'Carp'                      => '1.3301',
        'Compress::Snappy'          => '0.23',
        'Const::Fast'               => '0.014',
        'Data::Compare'             => '1.24',
        'Data::HexDump::Range'      => '0.13',
        'Exception::Class'          => '1.37',
        'Exporter'                  => '5.70',
        'IO::Compress::Gzip'        => '2.064',
        'IO::Uncompress::Gunzip'    => '2.064',
        'List::MoreUtils'           => '0.33',
        'List::Util'                => '1.38',
        'Params::Util'              => '1.07',
        'Scalar::Util'              => '1.38',
        'Scalar::Util::Numeric'     => '0.22',
        'Socket'                    => '2.013',
        'String::CRC32'             => '1.5',
        'Sys::SigAction'            => '0.21',
        'Time::HiRes'               => '1.9726',
        'Try::Tiny'                 => '0.21',
    },
    BUILD_REQUIRES      => {            # more to the list $Module::CoreList::version{5.010}
        'Capture::Tiny'             => '0.24',
        'Carp'                      => '1.3301',
        'Clone'                     => '0.36',
        'Config::IniFiles'          => '2.83',
        'Const::Fast'               => '0.014',
        'Cwd'                       => '3.40',
        'Data::Dumper'              => '2.151',
        'Exporter'                  => '5.70',
        'File::HomeDir'             => '1.00',
        'File::Path'                => '2.09',
        'File::Spec'                => '3.40',
        'File::Spec::Functions'     => '3.40',
        'IO::File'                  => '1.16',
        'IO::Socket::INET'          => '1.33',
        'List::Util'                => '1.38',
        'Net::EmptyPort'            => '0.0',
        'Params::Util'              => '1.07',
        'Proc::Daemon'              => '0.14',
        'Scalar::Util'              => '1.38',
        'Scalar::Util::Numeric'     => '0.22',
        'Socket'                    => '2.013',
        'Sub::Install'              => '0.927',
        'Sys::SigAction'            => '0.21',
        'Test::Deep'                => '0.112',
        'Test::Exception'           => '0.32',
        'Test::More'                => '1.001003',
        'Test::NoWarnings'          => '1.04',
        'Test::TCP'                 => '2.02',
        'Time::HiRes'               => '1.9726',
        'Try::Tiny'                 => '0.21',
    },
    META_MERGE          => {
        no_index                => {
            directory               => [
#                't',
#                'inc',
                'xt',
                'tools',
            ]
        },
        provides                => {
            'Kafka'                 => {
                file                    => 'lib/Kafka.pm',
                version                 => '0.8008'
            },
            'Kafka::Connection'     => {
                file                    => 'lib/Kafka/Connection.pm',
                version                 => '0.8008'
            },
            'Kafka::Consumer'       => {
                file                    => 'lib/Kafka/Consumer.pm',
                version                 => '0.8008'
            },
            'Kafka::Exceptions'     => {
                file                    => 'lib/Kafka/Exceptions.pm',
                version                 => '0.8008'
            },
            'Kafka::IO'             => {
                file                    => 'lib/Kafka/IO.pm',
                version                 => '0.8008'
            },
            'Kafka::Int64'          => {
                file                    => 'lib/Kafka/Int64.pm',
                version                 => '0.8008'
            },
            'Kafka::Internals'      => {
                file                    => 'lib/Kafka/Internals.pm',
                version                 => '0.8008'
            },
            'Kafka::Message'        => {
                file                    => 'lib/Kafka/Message.pm',
                version                 => '0.8008'
            },
            'Kafka::Producer'       => {
                file                    => 'lib/Kafka/Producer.pm',
                version                 => '0.8008'
            },
            'Kafka::Protocol'       => {
                file                    => 'lib/Kafka/Protocol.pm',
                version                 => '0.8008'
            },
        },
    },
    ( $] >= 5.005 ?                             ## Add these new keywords supported since 5.005
        (
            ABSTRACT            => 'Apache Kafka low-level synchronous API, which does not use Zookeeper.',
            AUTHOR              => 'TrackingSoft LLC <sgladkov@trackingsoft.com>'
        ) : ()
    ),
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => "Debian_CPANTS.txt Kafka-*.tar.gz" },
    LICENSE             => 'perl',
#    NO_MYMETA           => 1,
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
mkdir 'inc' unless -d 'inc';

ExtUtils::MakeMaker::WriteMakefile( %config );
