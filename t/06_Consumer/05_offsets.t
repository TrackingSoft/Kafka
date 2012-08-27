#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the method Kafka::Consumer::offsets

use lib 'lib';

use Test::More tests => 67;
use Params::Util qw( _ARRAY );

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

use Kafka::Mock;
use Kafka::IO;

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

# -- declaration of variables to test

my ( $server, $io, $consumer, $ret );

my $topic       = "test";
my $partition   = 0;
my $time        = -2;                           # TIMESTAMP_EARLIEST
my $max_number  = 100;                          # DEFAULT_MAX_OFFSETS

# control request
my $request     =                               # OFFSETS       Request
    # Request Header
     '00000018'                                 # REQUEST_LENGTH
    .'0004'                                     # REQUEST_TYPE
    .'0004'                                     # TOPIC_LENGTH
    .'74657374'                                 # TOPIC ("test")
    .'00000000'                                 # PARTITION
    # OFFSETS Request
    .'fffffffffffffffe'                         # TIME (-2 : earliest)
    .'00000064'                                 # MAX NUMBER of OFFSETS (100)
    ;

# control sponse
my $response    =                               # OFFSETS       Response
    # Response Header
     '0000000e'                                 # RESPONSE_LENGTH
    .'0000'                                     # ERROR_CODE
    # OFFSETS Response
    .'00000001'                                 # NUMBER of OFFSETS
    .'0000000000000000'                         # OFFSET
    ;

# a control decoded response
my $decoded = [ 0 ];

sub my_io {
    my $io      = shift;

    $$io = Kafka::IO->new(
        host        => "localhost",
        port        => $server->port,
        );
}

sub my_close {
    $consumer->close if $consumer;
    $consumer  = $io = undef;
}

# -- verification of the IO objects creation

$server = Kafka::Mock->new(
    requests    => {
        0   => '',                              # PRODUCE Request
        1   => '',                              # FETCH Request
        2   => '',                              # MULTIFETCH Request
        3   => '',                              # MULTIPRODUCE Request
        4   => $request,                        # OFFSETS Request
        },
    responses   => {
        0   => '',                              # PRODUCE Response
        1   => '',                              # FETCH Response
        2   => '',                              # MULTIFETCH Response
        3   => '',                              # MULTIPRODUCE Response
        4   => $response,                       # OFFSETS Response
        },
    );
isa_ok( $server, 'Kafka::Mock');

my_io( \$io );
isa_ok( $io, 'Kafka::IO');
my_close();

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify load the module

BEGIN { use_ok 'Kafka::Consumer' }

my_io( \$io );
$consumer = Kafka::Consumer->new(
    IO          => $io,
    RaiseError  => 0
    );
isa_ok( $consumer, 'Kafka::Consumer');

# -- verify response to invalid arguments

# without args
lives_ok { $ret = $consumer->offsets() } 'expecting to live';
like( $consumer->last_error, qr/^Mismatch argument/, "Mismatch argument" );

# topic: to see if a value is a normal non-false string of non-zero length
foreach my $topic ( ( undef, 0, "", "0", \"scalar", [] ) )
{
    lives_ok { $ret = $consumer->offsets( $topic, $partition, $time, $max_number ) } 'expecting to live';
    like( $consumer->last_error, qr/^Mismatch argument/, "Mismatch argument" );
}

# partition: to see if a value is a non-negative integer (of any length). That is, a positive integer, or zero
foreach my $partition ( ( undef, 0.5, -1, "", "0.5", \"scalar", [] ) )
{
    lives_ok { $ret = $consumer->offsets( $topic, $partition, $time, $max_number ) } 'expecting to live';
    like( $consumer->last_error, qr/^Mismatch argument/, "Mismatch argument" );
}

# time (truncated to an integer):
#   to see if a value is a number. That is, it is defined and perl thinks it's a number
#   or "Math::BigInt" object
foreach my $time ( ( undef, -3, "", \"scalar", [] ) )
{
    lives_ok { $ret = $consumer->offsets( $topic, $partition, $time, $max_number ) } 'expecting to live';
    like( $consumer->last_error, qr/^Mismatch argument/, "Mismatch argument" );
}

# max_number: to see if a value is a positive integer (of any length)
foreach my $max_number ( ( undef, 0, 0.5, -1, "", "0", "0.5", \"scalar", [] ) )
{
    lives_ok { $ret = $consumer->offsets( $topic, $partition, $time, $max_number ) } 'expecting to live';
    like( $consumer->last_error, qr/^Mismatch argument/, "Mismatch argument" );
}

my_close();

# -- verify sent for a request

my_io( \$io );
isa_ok( $io, 'Kafka::IO');
$consumer = Kafka::Consumer->new(
    IO          => $io,
    RaiseError  => 0
    );
isa_ok( $consumer, 'Kafka::Consumer');

$ret = $consumer->offsets( $topic, $partition, $time, $max_number );
is $server->last_request, $request, "sent a valid request";
ok defined( _ARRAY( $ret ) ), "a raw and unblessed ARRAY reference with at least one entry";
ok eq_array( $ret, $decoded ), "contain the correct entry";

# -- validation error response

$server->close;
lives_ok { $ret = $consumer->offsets( $topic, $partition, $time, $max_number ) } 'expecting to live';
like( $consumer->last_error, qr/^Can't recv/, "last error: Can't recv" );

# POSTCONDITIONS ---------------------------------------------------------------

# -- Closes and cleans up
my_close();
$server->close;
