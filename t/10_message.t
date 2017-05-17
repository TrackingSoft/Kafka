#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
);

use Test::More;

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use Params::Util qw(
    _HASH
);

use Kafka::Message;
use Kafka::TestInternals qw(
    @not_hash
);

my $msg = {
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

# NOTE: We presume that the verification of the correctness of the arguments made by the user.

#foreach my $bad_arg ( @not_hash ) {
#    dies_ok { $message = Kafka::Message->new( $bad_arg ) } 'exception to die';
#}

my $message;

lives_ok { $message = Kafka::Message->new( $msg ) } 'expecting to live';
isa_ok( $message, 'Kafka::Message' );

foreach my $method ( @Kafka::Message::_standard_fields )
{
    is $message->$method, $msg->{ $method }, "proper operation";
}

