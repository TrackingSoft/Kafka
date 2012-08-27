#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the function Kafka::Protocol::fetch_response

use lib 'lib';

use Test::More tests => 21;
use Params::Util qw( _HASH );

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

# preload the error codes
use Kafka qw(
    ERROR_CHECKSUM_ERROR
    ERROR_COMPRESSED_PAYLOAD
    );

# -- verify load the module
BEGIN { use_ok 'Kafka::Protocol', qw( fetch_response ) }

# -- declaration of variables to test

# control response
my $response = pack( "H*",                      # FETCH Response - "no compression" now
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
    .'546865207468697264206d657373616765',      # PAYLOAD ("The third message")
    );

# a control decoded response
my $decoded = {
    'messages' => [
        {
            'checksum'      => 0xd94a22be,
            'compression'   => 0,
            'magic'         => 0,
            'length'        => 0x16,
            'error'         => '',
            'payload'       => 'The first message',
            'valid'         => 1
        },
        {
            'checksum'      => 0xa3810845,
            'compression'   => 0,
            'magic'         => 0,
            'length'        => 0x17,
            'error'         => '',
            'payload'       => 'The second message',
            'valid'         => 1
        },
        {
            'checksum'      => 0x58611780,
            'compression'   => 0,
            'magic'         => 0,
            'length'        => 0x16,
            'error'         => '',
            'payload'       => 'The third message',
            'valid'         => 1
        }
    ],
    'header' => {
        'response_length'   => 0x51,
        'error_code'        => 0
    }
    };

# control response (not valid CHECKSUM)
my $response_not_valid = pack( "H*",            # FETCH Response - "no compression" now
    # Response Header
     '00000051'                                 # RESPONSE_LENGTH
    .'0000'                                     # ERROR_CODE
    # MESSAGE
    .'00000016'                                 # LENGTH
    .'00'                                       # MAGIC
    .''                                         # COMPRESSION
    .'00000000'                                 # CHECKSUM - not valid
    .'546865206669727374206d657373616765'       # PAYLOAD ("The first message")
    # MESSAGE
    .'00000017'                                 # LENGTH
    .'00'                                       # MAGIC
    .''                                         # COMPRESSION
    .'00000000'                                 # CHECKSUM - not valid
    .'546865207365636f6e64206d657373616765'     # PAYLOAD ("The second message")
    # MESSAGE
    .'00000016'                                 # LENGTH
    .'00'                                       # MAGIC
    .''                                         # COMPRESSION
    .'00000000'                                 # CHECKSUM - not valid
    .'546865207468697264206d657373616765',      # PAYLOAD ("The third message")
    );

# a control decoded response (not valid CHECKSUM)
my $decoded_not_valid = {
    'messages' => [
        {
            'checksum'      => 0,
            'compression'   => 0,
            'magic'         => 0,
            'length'        => 0x16,
            'error'         => $Kafka::ERROR[ERROR_CHECKSUM_ERROR],
            'payload'       => 'The first message',
            'valid'         => ''
        },
        {
            'checksum'      => 0,
            'compression'   => 0,
            'magic'         => 0,
            'length'        => 0x17,
            'error'         => $Kafka::ERROR[ERROR_CHECKSUM_ERROR],
            'payload'       => 'The second message',
            'valid'         => ''
        },
        {
            'checksum'      => 0,
            'compression'   => 0,
            'magic'         => 0,
            'length'        => 0x16,
            'error'         => $Kafka::ERROR[ERROR_CHECKSUM_ERROR],
            'payload'       => 'The third message',
            'valid'         => ''
        }
    ],
    'header' => {
        'response_length'   => 0x51,
        'error_code'        => 0
    }
    };

# control response (bad MAGIC)
my $response_bad_magic = pack( "H*",            # FETCH Response - "no compression" now
    # Response Header
     '00000054'                                 # RESPONSE_LENGTH
    .'0000'                                     # ERROR_CODE
    # MESSAGE
    .'00000017'                                 # LENGTH
    .'01'                                       # MAGIC - bad
    .'00'                                       # COMPRESSION - no compression
    .'d94a22be'                                 # CHECKSUM
    .'546865206669727374206d657373616765'       # PAYLOAD ("The first message")
    # MESSAGE
    .'00000018'                                 # LENGTH
    .'01'                                       # MAGIC - bad
    .'00'                                       # COMPRESSION - no compression
    .'a3810845'                                 # CHECKSUM
    .'546865207365636f6e64206d657373616765'     # PAYLOAD ("The second message")
    # MESSAGE
    .'00000017'                                 # LENGTH
    .'01'                                       # MAGIC -bad
    .'00'                                       # COMPRESSION - no compression
    .'58611780'                                 # CHECKSUM
    .'546865207468697264206d657373616765',      # PAYLOAD ("The third message")
    );

# a control decoded response (bad MAGIC)
my $decoded_bad_magic = {
    'messages' => [
        {
            'checksum'      => 0xd94a22be,
            'compression'   => 0,
            'magic'         => 1,
            'length'        => 0x17,
            'error'         => $Kafka::ERROR[ERROR_COMPRESSED_PAYLOAD],
            'payload'       => 'The first message',
            'valid'         => ''
        },
        {
            'checksum'      => 0xa3810845,
            'compression'   => 0,
            'magic'         => 1,
            'length'        => 0x18,
            'error'         => $Kafka::ERROR[ERROR_COMPRESSED_PAYLOAD],
            'payload'       => 'The second message',
            'valid'         => ''
        },
        {
            'checksum'      => 0x58611780,
            'compression'   => 0,
            'magic'         => 1,
            'length'        => 0x17,
            'error'         => $Kafka::ERROR[ERROR_COMPRESSED_PAYLOAD],
            'payload'       => 'The third message',
            'valid'         => ''
        }
    ],
    'header' => {
        'response_length'   => 0x54,
        'error_code'        => 0
    }
    };

# control response (not valid CHECKSUM & bad MAGIC)
my $response_two_errors = pack( "H*",           # FETCH Response - "no compression" now
    # Response Header
     '00000054'                                 # RESPONSE_LENGTH
    .'0000'                                     # ERROR_CODE
    # MESSAGE
    .'00000017'                                 # LENGTH
    .'01'                                       # MAGIC - bad
    .'00'                                       # COMPRESSION - no compression
    .'00000000'                                 # CHECKSUM
    .'546865206669727374206d657373616765'       # PAYLOAD ("The first message")
    # MESSAGE
    .'00000018'                                 # LENGTH
    .'01'                                       # MAGIC - bad
    .'00'                                       # COMPRESSION - no compression
    .'00000000'                                 # CHECKSUM
    .'546865207365636f6e64206d657373616765'     # PAYLOAD ("The second message")
    # MESSAGE
    .'00000017'                                 # LENGTH
    .'01'                                       # MAGIC - bad
    .'00'                                       # COMPRESSION - no compression
    .'00000000'                                 # CHECKSUM
    .'546865207468697264206d657373616765',      # PAYLOAD ("The third message")
    );

# a control decoded response (not valid CHECKSUM & bad MAGIC)
my $decoded_two_errors = {
    'messages' => [
        {
            'checksum'      => 0,
            'compression'   => 0,
            'magic'         => 1,
            'length'        => 0x17,
            'error'         => $Kafka::ERROR[ERROR_CHECKSUM_ERROR]."\n".$Kafka::ERROR[ERROR_COMPRESSED_PAYLOAD],
            'payload'       => 'The first message',
            'valid'         => ''
        },
        {
            'checksum'      => 0,
            'compression'   => 0,
            'magic'         => 1,
            'length'        => 0x18,
            'error'         => $Kafka::ERROR[ERROR_CHECKSUM_ERROR]."\n".$Kafka::ERROR[ERROR_COMPRESSED_PAYLOAD],
            'payload'       => 'The second message',
            'valid'         => ''
        },
        {
            'checksum'      => 0,
            'compression'   => 0,
            'magic'         => 1,
            'length'        => 0x17,
            'error'         => $Kafka::ERROR[ERROR_CHECKSUM_ERROR]."\n".$Kafka::ERROR[ERROR_COMPRESSED_PAYLOAD],
            'payload'       => 'The third message',
            'valid'         => ''
        }
    ],
    'header' => {
        'response_length'   => 0x54,
        'error_code'        => 0
    }
    };

# INSTRUCTIONS -----------------------------------------------------------------
# -- verify response to invalid arguments

# without args
throws_ok { fetch_response() } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';

# topic (length > = 6): to see if a value is a normal non-false string of non-zero length
foreach my $response ( ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", [] ) )
{
    throws_ok { fetch_response( $response ) } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# -- verify response
foreach my $pair ( (
    { response => $response,            decoded => $decoded },
    { response => $response_not_valid,  decoded => $decoded_not_valid },
    { response => $response_bad_magic,  decoded => $decoded_bad_magic },
    { response => $response_two_errors, decoded => $decoded_two_errors },
    ) )
{
    ok defined( _HASH( fetch_response( \$pair->{response} ) ) ), "a raw and unblessed HASH reference with at least one entry";
    ok eq_hash( fetch_response( \$pair->{response} ), $pair->{decoded} ), "contain the correct keys and values";
}

# POSTCONDITIONS ---------------------------------------------------------------
