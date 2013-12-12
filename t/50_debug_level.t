#!/usr/bin/perl -w

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
);

# ENVIRONMENT ------------------------------------------------------------------

use Test::More;

#-- verify load the module

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Kafka::Connection;
use Kafka::IO;

#-- setting up facilities ------------------------------------------------------

my $PERL_KAFKA_DEBUG    = $ENV{PERL_KAFKA_DEBUG};
my $DEBUG_Connection    = $Kafka::Connection::DEBUG;
my $DEBUG_IO            = $Kafka::IO::DEBUG;

delete $ENV{PERL_KAFKA_DEBUG};

#-- declarations ---------------------------------------------------------------

package Kafka::TestDebugLevel;

    use 5.010;
    use strict;
    use warnings;

    use Kafka::Internals qw(
        debug_level
    );

    our $DEBUG = 0;

    sub new {
        my ( $class ) = @_;

        my $self = bless {}, $class;

        return $self;
    }

package main;

#-- Global data ----------------------------------------------------------------

# INSTRUCTIONS -----------------------------------------------------------------

#-- direct control

$Kafka::TestDebugLevel::DEBUG                                   = 0;
is( $Kafka::TestDebugLevel::DEBUG,                              0, 'debug level not set' );
$Kafka::TestDebugLevel::DEBUG                                   = 1;
is( $Kafka::TestDebugLevel::DEBUG,                              1, 'debug level set' );

#----- control through a method/function

#-- establish a simple value

$Kafka::TestDebugLevel::DEBUG                                   = 0;
is( Kafka::TestDebugLevel->debug_level(),                       0, 'debug level not set' );
is( Kafka::TestDebugLevel->debug_level( 1 ),                    1, 'debug level set' );
is( $Kafka::TestDebugLevel::DEBUG,                              1, 'debug level set' );
is( Kafka::TestDebugLevel->debug_level(),                       1, 'debug level set' );
is( Kafka::TestDebugLevel->debug_level( undef ),                1, 'debug level set' );
is( $Kafka::TestDebugLevel::DEBUG,                              1, 'debug level set' );

#-- set for the "correct" module

$Kafka::TestDebugLevel::DEBUG                                   = 0;
is( Kafka::TestDebugLevel->debug_level( 'TestDebugLevel:1' ),   1, 'debug level set' );
is( Kafka::TestDebugLevel->debug_level(),                       1, 'debug level set' );
is( $Kafka::TestDebugLevel::DEBUG,                              1, 'debug level set' );

#-- set for the "another" module

$Kafka::Connection::DEBUG                                       = 0;
$Kafka::IO::DEBUG                                               = 0;
$Kafka::TestDebugLevel::DEBUG                                   = 0;

is( Kafka::TestDebugLevel->debug_level( 'IO:1' ),               0, 'debug level not set' );

is( Kafka::IO->debug_level(),                                   1, 'debug level set' );
is( $Kafka::IO::DEBUG,                                          1, 'debug level set' );

is( Kafka::TestDebugLevel->debug_level(),                       0, 'debug level not set' );
is( $Kafka::TestDebugLevel::DEBUG,                              0, 'debug level notset' );

is( Kafka::Connection->debug_level(),                           0, 'debug level not set' );
is( $Kafka::Connection::DEBUG,                                  0, 'debug level not set' );

$Kafka::IO::DEBUG                                               = 0;

#-- set for the "incorrect" module

is( Kafka::TestDebugLevel->debug_level( 'SomethingBad:1' ),     0, 'debug level not set' );

is( $Kafka::IO::DEBUG,                                          0, 'debug level not set' );
is( $Kafka::TestDebugLevel::DEBUG,                              0, 'debug level not set' );
is( $Kafka::Connection::DEBUG,                                  0, 'debug level not set' );

#----- control through an environment variable

$ENV{PERL_KAFKA_DEBUG}                                          = 1;

is( Kafka::TestDebugLevel->debug_level(),                       1, 'debug level set' );
is( $Kafka::TestDebugLevel::DEBUG,                              1, 'debug level set' );

is( $Kafka::IO::DEBUG,                                          0, 'debug level not set' );
is( $Kafka::Connection::DEBUG,                                  0, 'debug level not set' );

$Kafka::TestDebugLevel::DEBUG                                   = 0;

$ENV{PERL_KAFKA_DEBUG}                                          = 'IO:1';

is( Kafka::TestDebugLevel->debug_level(),                       0, 'debug level not set' );

is( $Kafka::IO::DEBUG,                                          1, 'debug level set' );
is( $Kafka::TestDebugLevel::DEBUG,                              0, 'debug level not set' );
is( $Kafka::Connection::DEBUG,                                  0, 'debug level not set' );

$Kafka::IO::DEBUG                                               = 0;

#----- several specifications

is( Kafka::TestDebugLevel->debug_level( '1,IO:2' ),             1, 'debug level set' );

is( $Kafka::TestDebugLevel::DEBUG,                              1, 'debug level set' );
is( $Kafka::IO::DEBUG,                                          2, 'debug level not set' );
is( $Kafka::Connection::DEBUG,                                  0, 'debug level set' );

$Kafka::TestDebugLevel::DEBUG                                   = 0;
$Kafka::IO::DEBUG                                               = 0;

is( Kafka::TestDebugLevel->debug_level( 'IO:1,2' ),             2, 'debug level set' );

is( $Kafka::TestDebugLevel::DEBUG,                              2, 'debug level set' );
is( $Kafka::IO::DEBUG,                                          1, 'debug level not set' );
is( $Kafka::Connection::DEBUG,                                  0, 'debug level set' );

$Kafka::TestDebugLevel::DEBUG                                   = 0;
$Kafka::IO::DEBUG                                               = 0;

#----- control via an object method

delete $ENV{PERL_KAFKA_DEBUG};

my $obj = Kafka::TestDebugLevel->new;

is( $obj->debug_level(),                                        0, 'debug level not set' );
is( $obj->debug_level( 'TestDebugLevel:1,IO:2' ),               1, 'debug level set' );
is( $obj->debug_level(),                                        1, 'debug level set' );

is( $Kafka::TestDebugLevel::DEBUG,                              1, 'debug level set' );
is( $Kafka::IO::DEBUG,                                          2, 'debug level set' );
is( $Kafka::Connection::DEBUG,                                  0, 'debug level not set' );

#-- Closes and cleans up

# POSTCONDITIONS ---------------------------------------------------------------

$Kafka::Connection::DEBUG   = $DEBUG_Connection;
$Kafka::IO::DEBUG           = $DEBUG_IO;
$ENV{PERL_KAFKA_DEBUG}      = $PERL_KAFKA_DEBUG;
