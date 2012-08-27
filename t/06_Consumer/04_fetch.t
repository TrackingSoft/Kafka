#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the method Kafka::Consumer::fetch

use lib 'lib';

use Test::More tests => 71;
use Params::Util qw( _ARRAY );

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}
BEGIN {
    eval "use Test::Deep";
    plan skip_all => "because Test::Deep required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

use Kafka qw ( BITS64 );
use Kafka::Mock;
use Kafka::IO;

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

# -- declaration of variables to test

my ( $server, $io, $consumer, $ret );

my $topic       = "test";
my $partition   = 0;
my $offset      = 0;
my $max_size    = 1024 * 1024;                  # DEFAULT_MAX_SIZE

# control request
my $request     =                               # FETCH         Request
    # Request Header
     '00000018'                                 # REQUEST_LENGTH
    .'0001'                                     # REQUEST_TYPE
    .'0004'                                     # TOPIC_LENGTH
    .'74657374'                                 # TOPIC ("test")
    .'00000000'                                 # PARTITION
    # FETCH Request
    .'0000000000000000'                         # OFFSET
    .'00100000'                                 # MAX_SIZE (1MB)
    ;

# control sponse
my $response    =                               # FETCH         Response    - "no compression" now
    # Response Header
     '00000051'                                 # RESPONSE_LENGTH
    .'0000'                                     # ERROR_CODE
    # MESSAGE
    .'00000016'                                 # LENGTH
    .'00'                                       # MAGIC
    .''                                         # COMPRESSION
    .'d94a22be'                                 # CHECKSUM
    .'546865206669727374206d657373616765'       # PAYLOAD ("The first message")
    # MESSAGE
    .'00000017'                                 # LENGTH
    .'00'                                       # MAGIC
    .''                                         # COMPRESSION
    .'a3810845'                                 # CHECKSUM
    .'546865207365636f6e64206d657373616765'     # PAYLOAD ("The second message")
    # MESSAGE
    .'00000016'                                 # LENGTH
    .'00'                                       # MAGIC
    .''                                         # COMPRESSION
    .'58611780'                                 # CHECKSUM
    .'546865207468697264206d657373616765'       # PAYLOAD ("The third message")
    ;

# a control decoded response
my $decoded = BITS64 ?
    [
        bless( {
            'magic'         => 0,
            'valid'         => 1,
            'compression'   => 0,
            'checksum'      => 0xd94a22be,
            'length'        => 22,              # 0x16
            'payload'       => 'The first message',
            'error'         => '',
            'next_offset'   => 26,
            'offset'        => 0
            }, 'Kafka::Message' ),
        bless( {
            'magic'         => 0,
            'valid'         => 1,
            'compression'   => 0,
            'checksum'      => 0xa3810845,
            'length'        => 23,              # 0x17
            'payload'       => 'The second message',
            'error'         => '',
            'next_offset'   => 53,
            'offset'        => 26
            }, 'Kafka::Message' ),
        bless( {
            'magic'         => 0,
            'valid'         => 1,
            'compression'   => 0,
            'checksum'      => 0x58611780,
            'length'        => 22,              # 0x16
            'payload'       => 'The third message',
            'error'         => '',
            'next_offset'   => 79,
            'offset'        => 53
            }, 'Kafka::Message' )
        ]
    : [
        bless( {
            'magic'         => 0,
            'valid'         => 1,
            'compression'   => 0,
            'checksum'      => 0xd94a22be,
            'length'        => 22,              # 0x16
            'payload'       => 'The first message',
            'error'         => '',
            'next_offset'   => bless( {
                'value'     => [
                    26
                    ],
                'sign'      => '+'
                }, 'Math::BigInt' ),
            'offset' => bless( {
                'value'     => [
                    0
                ],
                'sign'      => '+'
                }, 'Math::BigInt' )
            }, 'Kafka::Message' ),
        bless( {
            'magic'         => 0,
            'valid'         => 1,
            'compression'   => 0,
            'checksum'      => 0xa3810845,
            'length'        => 23,              # 0x17
            'payload'       => 'The second message',
            'error'         => '',
            'next_offset'   => bless( {
                'value'     => [
                    53
                    ],
                    'sign'  => '+'
                }, 'Math::BigInt' ),
            'offset'        => bless( {
                'value'     => [
                    26
                    ],
                    'sign'  => '+'
                }, 'Math::BigInt' )
            }, 'Kafka::Message' ),
        bless( {
            'magic'         => 0,
            'valid'         => 1,
            'compression'   => 0,
            'checksum'      => 0x58611780,
            'length'        => 22,              # 0x16
            'payload'       => 'The third message',
            'error'         => '',
            'next_offset'   => bless( {
                'value'     => [
                    79
                    ],
                'sign'      => '+'
                }, 'Math::BigInt' ),
            'offset'        => bless( {
                'value'     => [
                    53
                    ],
                'sign'      => '+'
                }, 'Math::BigInt' )
            }, 'Kafka::Message' )
        ];

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
        1   => $request,                        # FETCH Request
        2   => '',                              # MULTIFETCH Request
        3   => '',                              # MULTIPRODUCE Request
        4   => '',                              # OFFSETS Request
        },
    responses   => {
        0   => '',                              # PRODUCE Response
        1   => $response,                       # FETCH Response
        2   => '',                              # MULTIFETCH Response
        3   => '',                              # MULTIPRODUCE Response
        4   => '',                              # OFFSETS Response
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
lives_ok { $ret = $consumer->fetch() } 'expecting to live';
like( $consumer->last_error, qr/^Mismatch argument/, "Mismatch argument" );

# topic: to see if a value is a normal non-false string of non-zero length
foreach my $topic ( ( undef, 0, "", "0", \"scalar", [] ) )
{
    lives_ok { $ret = $consumer->fetch( $topic, $partition, $offset, $max_size ) } 'expecting to live';
    like( $consumer->last_error, qr/^Mismatch argument/, "Mismatch argument" );
}

# partition: to see if a value is a non-negative integer (of any length). That is, a positive integer, or zero
foreach my $partition ( ( undef, 0.5, -1, "", "0.5", \"scalar", [] ) )
{
    lives_ok { $ret = $consumer->fetch( $topic, $partition, $offset, $max_size ) } 'expecting to live';
    like( $consumer->last_error, qr/^Mismatch argument/, "Mismatch argument" );
}

# offset:
#   to see if a value is a non-negative integer (of any length). That is, a positive integer, or zero
#   or "Math::BigInt" object
foreach my $offset ( ( undef, 0.5, -1, "", "0.5", \"scalar", [] ) )
{
    lives_ok { $ret = $consumer->fetch( $topic, $partition, $offset, $max_size ) } 'expecting to live';
    like( $consumer->last_error, qr/^Mismatch argument/, "Mismatch argument" );
}

# max_size: to see if a value is a positive integer (of any length)
foreach my $max_size ( ( undef, 0, 0.5, -1, "", "0", "0.5", \"scalar", [] ) )
{
    lives_ok { $ret = $consumer->fetch( $topic, $partition, $offset, $max_size ) } 'expecting to live';
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

$ret = $consumer->fetch( $topic, $partition, $offset, $max_size );
is $server->last_request, $request, "sent a valid request";
ok defined( _ARRAY( $ret ) ), "a raw and unblessed ARRAY reference with at least one entry";
cmp_deeply $ret, $decoded, "contain the correct entries";

# -- validation error response

$server->close;
lives_ok { $ret = $consumer->fetch( $topic, $partition, $offset, $max_size ) } 'expecting to live';
like( $consumer->last_error, qr/^Can't recv/, "last error: Can't recv" );

# POSTCONDITIONS ---------------------------------------------------------------

# -- Closes and cleans up
my_close();
$server->close;
