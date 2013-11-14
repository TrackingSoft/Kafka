#!/usr/bin/perl -w

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
);

# ENVIRONMENT ------------------------------------------------------------------

use Test::More;

#-- verify load the module

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

sub plan_skip {
    my ( $module ) = @_;

    plan( skip_all => "$module not installed: $@; skipping" ) if $@;
}

plan 'no_plan';

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

my ( $modules_dir, $t_dir, $author_dir, $release_dir, $t_modules_dir, $tools_dir, @modules, @t_modules, @t_scripts, @tools_scripts, @author_scripts, @release_scripts );

#-- Global data ----------------------------------------------------------------

$modules_dir    = abs_path( 'lib' );
$t_dir          = abs_path( 't' );
$author_dir     = abs_path( catdir( 'xt', 'author' ) );
$release_dir    = abs_path( catdir( 'xt', 'release' ) );
$t_modules_dir  = catdir( $t_dir, 'lib' );
$tools_dir      = abs_path( 'tools' );

foreach my $config (
        { dir => $modules_dir,      re => qr(\.pm$),    array => \@modules, },
        { dir => $t_modules_dir,    re => qr(\.pm$),    array => \@t_modules, },
        { dir => $t_dir,            re => qr(\.t$),     array => \@t_scripts, },
        { dir => $author_dir,       re => qr(\.t$),     array => \@author_scripts, },
        { dir => $release_dir,      re => qr(\.t$),     array => \@release_scripts, },
        { dir => $tools_dir,        re => qr(\.pl$),    array => \@tools_scripts, },
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

#-- Test::Version - Check to see that version's in modules are sane
eval {
    use Test::Version qw( version_ok ),
        {
            is_strict   => 0,
            has_version => 1,
        };
    };
plan_skip( 'Test::Version' );

find( sub {
        version_ok( $File::Find::name ) if -f $File::Find::name;
    },
    $modules_dir,
    $t_modules_dir,
);

#-- Test::Synopsis - Test your SYNOPSIS code
eval { use Test::Synopsis; };
plan_skip( 'Test::Synopsis' );

synopsis_ok( $_ ) foreach @modules;

#-- Test::Strict - Check syntax, presence of use strict; and test coverage
eval { use Test::Strict; };
plan_skip( 'Test::Strict' );

find( sub {
        if ( -f $File::Find::name ) {
#            syntax_ok( $File::Find::name );
            warnings_ok( $File::Find::name );
        }
    },
    @modules,
    @t_modules,
    @t_scripts,
    @tools_scripts,
    @author_scripts,
    @release_scripts,
);

#-- Test::Spelling - check for spelling errors in POD files
eval { use Test::Spelling; };
plan_skip( 'Test::Spelling' );

if ( has_working_spellchecker ) {
    add_stopwords( <DATA> );
    find( sub {
            pod_file_spelling_ok( $File::Find::name )
                if -f $File::Find::name;
        },
        $modules_dir,
        $t_modules_dir,
    );
}

#-- Test::Mojibake - check your source for encoding misbehavior.
eval { use Test::Mojibake; };
plan_skip( 'Test::Mojibake' );

find( sub {
    file_encoding_ok( $File::Find::name )
        if -f $File::Find::name;
    },
    $modules_dir,
    $t_modules_dir,
    @t_scripts,
    @tools_scripts,
    @author_scripts,
    @release_scripts,
);

#-- Test::MinimumVersion - does your code require newer perl than you think?
eval { use Test::MinimumVersion; };
plan_skip( 'Test::MinimumVersion' );

#note "Test::MinimumVersion as skipping it runs a bit slow";
minimum_version_ok( $_, 5.010 ) foreach
    @modules,
    @t_modules,
    @t_scripts,
    @tools_scripts,
    @author_scripts,
    @release_scripts,
    ;

# POSTCONDITIONS ---------------------------------------------------------------

__END__
analyse
Analysing
api
API
ApiKey
APIs
ApiVersion
AutoCreateTopicsEnable
bigint
Backoff
BigInteger
bla
Checksum
ClientId
codec
codecs
COMMITTED
CorrelationId
Crc
CRC
ErrorCode
FetchOffset
Gladkov
HighwaterMarkOffset
hostname
ie
inconsistence
intra
IP
ISR
Isr
kafka
LeaderNotAvailable
LLC
MagicByte
Marchenko
MaxBytes
MaxNumberOfOffsets
MaxWaitTime
MERCHANTABILITY
MessageSets
MessageSetSize
MessageSize
metadata
Metadata
METADATA
MinBytes
NodeId
occured
perlartistic
prereq
ReplicaId
RequiredAcks
sec
Sergey
Simplistically
Solovey
subclasses
TCP
timestamp
TopicName
TrackingSoft
unblessed
ZooKeeper
