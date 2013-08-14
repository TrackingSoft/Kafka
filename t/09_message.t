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

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Params::Util qw(
    _HASH
);

use Kafka::Message;
use Kafka::TestInternals qw(
    @not_hash
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

my ( $message, $msg );

#-- Global data ----------------------------------------------------------------

$msg = {
    Attributes          => 0,
    error               => 0,
    HighwaterMarkOffset => 0,
    key                 => q{},
    MagicByte           => 0,
    next_offset         => 0,
    payload             => q{},
    offset              => 0,
    valid               => 1,
};

# INSTRUCTIONS -----------------------------------------------------------------

#-- new

# NOTE: We presume that the verification of the correctness of the arguments made by the user.

#foreach my $bad_arg ( @not_hash ) {
#    dies_ok { $message = Kafka::Message->new( $bad_arg ) } 'exception to die';
#}

lives_ok { $message = Kafka::Message->new( $msg ) } 'expecting to live';
isa_ok( $message, 'Kafka::Message' );

#-- methods

foreach my $method ( @Kafka::Message::_standard_fields )
{
    is $message->$method, $msg->{ $method }, "proper operation";
}

# POSTCONDITIONS ---------------------------------------------------------------
