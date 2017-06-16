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
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use Kafka qw(
    $COMPRESSION_NONE
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_LATEST_OFFSETS
    $REQUEST_TIMEOUT
);
use Kafka::Internals qw(
    $PRODUCER_ANY_OFFSET
);
use Kafka::Protocol qw(
    decode_fetch_response
);

# NOTE: We presume that the verification of the correctness of the arguments made by the user.

my $request = {};

# CorrelationId
$request->{CorrelationId} = 0;

# ClientId
$request->{ClientId} = q{};

# RequiredAcks
$request->{RequiredAcks} = $NOT_SEND_ANY_RESPONSE;

# Timeout
$request->{Timeout} = $REQUEST_TIMEOUT * 1000;  # ms

# MaxWaitTime
$request->{MaxWaitTime} = $DEFAULT_MAX_WAIT_TIME;

# MinBytes
$request->{MinBytes} = $MIN_BYTES_RESPOND_IMMEDIATELY;

# topics

# TopicName
$request->{topics} = [ { TopicName => 'mytopic' } ];

# partitions

# Partition
$request->{topics}->[0]->{partitions}->[0]->{Partition} = 0;

# MessageSet
$request->{topics}->[0]->{partitions}->[0]->{MessageSet} = [ {} ];

# FetchOffset
$request->{topics}->[0]->{partitions}->[0]->{FetchOffset} = 0;

# MaxBytes
$request->{topics}->[0]->{partitions}->[0]->{MaxBytes} = $DEFAULT_MAX_BYTES;

# Time
$request->{topics}->[0]->{partitions}->[0]->{Time} = $RECEIVE_LATEST_OFFSETS;

# MaxNumberOfOffsets
$request->{topics}->[0]->{partitions}->[0]->{MaxNumberOfOffsets} = $DEFAULT_MAX_NUMBER_OF_OFFSETS;

#- MessageSet internal

# Offset
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Offset} = $PRODUCER_ANY_OFFSET;

# NOTE: MagicByte, Attributes should not be assigned by the user
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{MagicByte}  = 0;
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Attributes} = $COMPRESSION_NONE;

# Key, Value
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Key} = q{};
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Value} = q{};

#-- decode_fetch_response (_decode_MessageSet_template)

# According to Apache Kafka documentation:
# As an optimization the server is allowed to return a partial message at the end of the message set.
# Clients should handle this case.

# description of responses, see t/??_decode_encode.t

# the correct encoded fetch response hex stream
my $encoded_response = pack( "H*", '0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000470000000000000000000000148dc795a20000ffffffff0000000648656c6c6f2100000000000000010000001b989feb390000ffffffff0000000d48656c6c6f2c20576f726c6421' );
# the correct decoded fetch response
#$decoded_response = {
#    CorrelationId                           => 0,
#    topics                                  => [
#        {
#            TopicName                       => 'mytopic',
#            partitions                      => [
#                {
#                    Partition               => 0,
#                    ErrorCode               => 0,
#                    HighwaterMarkOffset     => 2,
#                    MessageSet              => [
#                        {
#                            Offset          => 0,
#                            MagicByte       => 0,
#                            Attributes      => 0,
#                            Key             => q{},
#                            Value           => 'Hello!',
#                        },
#                        {
#                            Offset          => 1,
#                            MagicByte       => 0,
#                            Attributes      => 0,
#                            Key             => q{},
#                            Value           => 'Hello, World!',
#                        },
#                    ],
#                },
#            ],
#        },
#    ],
#};

my $decoded = decode_fetch_response( \$encoded_response );
is scalar( @{ $decoded->{topics}->[0]->{partitions}->[0]->{MessageSet} } ), 2, 'all messages are decoded';

foreach my $corrupted (
        # Not the full MessageSet: $MessageSetSize < 22;
                # [q] Offset
                # [l] MessageSize
                # [l] Crc
                # [c] MagicByte
                # [c] Attributes
                # [l] Key length
                # [l] Value length
        {
            hex_stream      => '0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000150000000000000000000000148dc795a20000ffffffff0000000648656c6c6f2100000000000000010000001b989feb390000ffffffff0000000d48656c6c6f2c20576f726c6421',
            received        => 0,
            description     => 'no decoded messages',
        },
        {
            hex_stream      => '0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000150000000000000000000000148dc795a20000ffffff',
            received        => 0,
            description     => 'no decoded messages',
        },
        # Not the full MessageSet: not the full Key or Value length
        {
            hex_stream      => '0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000470000000000000000000000148dc795a20000ffffffff000000',
            received        => 0,
            description     => 'no decoded messages',
        },
        {
            hex_stream      => '0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000470000000000000000000000148dc795a20000ffffffff0000000648656c6c6f2100000000000000010000001b989feb390000ffffffff0000',
            received        => 1,
            description     => 'the first message is decoded',
        },
        # Not the full MessageSet: not the full Value
        {
            hex_stream      => '0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000470000000000000000000000148dc795a20000ffffffff0000000648656c6c6f',
            received        => 0,
            description     => 'no decoded messages',
        },
        {
            hex_stream      => '0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000470000000000000000000000148dc795a20000ffffffff0000000648656c6c6f2100000000000000010000001b989feb390000ffffffff0000000d48656c6c6f2c20576f726c64',
            received        => 1,
            description     => 'the first message is decoded',
        },
    ) {
    $encoded_response = pack( "H*", $corrupted->{hex_stream} );
    $decoded = decode_fetch_response( \$encoded_response );
    is scalar( @{ $decoded->{topics}->[0]->{partitions}->[0]->{MessageSet} } ), $corrupted->{received}, $corrupted->{description};
}

