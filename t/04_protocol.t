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

use Kafka qw(
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    %ERROR
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NO_ERROR
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_LATEST_OFFSET
    $REQUEST_TIMEOUT
);
use Kafka::Internals qw(
    $PRODUCER_ANY_OFFSET
);
use Kafka::Protocol qw(
    $COMPRESSION_NONE
    decode_fetch_response
    decode_metadata_response
    decode_offset_response
    decode_produce_response
    encode_fetch_request
    encode_metadata_request
    encode_offset_request
    encode_produce_request
);
use Kafka::MockProtocol qw(
    decode_fetch_request
    decode_metadata_request
    decode_offset_request
    decode_produce_request
    encode_fetch_response
    encode_metadata_response
    encode_offset_response
    encode_produce_response
);
use Kafka::TestInternals qw(
    @not_array
    @not_empty_string
    @not_hash
    @not_isint
    @not_string
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

#sub expected_error {
#    my ( $function, $bad_arg ) = @_;
#
#    Kafka::Protocol::_protocol_error( $ERROR_NO_ERROR );
#    ok !Kafka::Protocol::last_errorcode(), 'no error';
#    {
#        no strict 'refs';                   ## no critic
#        &$function( $bad_arg );
#    }
#    ok Kafka::Protocol::last_errorcode(), 'an error is detected';
#    ok Kafka::Protocol::last_error(), 'expected error';
#}

my ( @decode_functions, @encode_functions, $request, $encoded_response, $decoded );

#-- Global data ----------------------------------------------------------------

@decode_functions = qw(
    decode_fetch_response
    decode_metadata_response
    decode_offset_response
    decode_produce_response
    decode_fetch_request
    decode_metadata_request
    decode_offset_request
    decode_produce_request
);

@encode_functions = qw(
    encode_fetch_request
    encode_metadata_request
    encode_offset_request
    encode_produce_request
    encode_fetch_response
    encode_metadata_response
    encode_offset_response
    encode_produce_response
);

# INSTRUCTIONS -----------------------------------------------------------------

# NOTE: We presume that the verification of the correctness of the arguments made by the user.

#-- last_errorcode, last_error

#foreach my $function ( @decode_functions, @encode_functions ) {
#    foreach my $bad_arg (
#            @not_hash,
#            $function =~ /^decode/ ? { foo => 'bar' } : \'scalar',
#        ) {
#        expected_error( $function, $bad_arg );
#    }
#}

#-- The encode requests structure only

# CorrelationId

#foreach my $function ( @encode_functions ) {
#    foreach my $bad_CorrelationId ( @not_isint ) {
#        $request->{CorrelationId} = $bad_CorrelationId;
#        expected_error( $function, $request );
#    }
#}
$request->{CorrelationId} = 0;

# ClientId

#foreach my $function ( grep /request$/, @encode_functions ) {
#    foreach my $bad_ClientId ( @not_string ) {
#        $request->{ClientId} = $bad_ClientId;
#        expected_error( $function, $request );
#    }
#}
$request->{ClientId} = q{};

# RequiredAcks

#foreach my $bad_RequiredAcks ( @not_isint ) {
#    $request->{RequiredAcks} = $bad_RequiredAcks;
#    expected_error( 'encode_produce_request', $request );
#}
$request->{RequiredAcks} = $NOT_SEND_ANY_RESPONSE;

# Timeout

#foreach my $bad_Timeout ( @not_isint ) {
#    $request->{Timeout} = $bad_Timeout;
#    expected_error( 'encode_produce_request', $request );
#}
$request->{Timeout} = $REQUEST_TIMEOUT * 1000;  # ms

# MaxWaitTime

#foreach my $bad_MaxWaitTime ( @not_isint ) {
#    $request->{MaxWaitTime} = $bad_MaxWaitTime;
#    expected_error( 'encode_fetch_request', $request );
#}
$request->{MaxWaitTime} = $DEFAULT_MAX_WAIT_TIME;

# MinBytes

#foreach my $bad_MinBytes ( @not_isint ) {
#    $request->{MinBytes} = $bad_MinBytes;
#    expected_error( 'encode_fetch_request', $request );
#}
$request->{MinBytes} = $MIN_BYTES_RESPOND_IMMEDIATELY;

# topics

#foreach my $function ( grep /request$/, @encode_functions ) {
#    foreach my $bad_topics (
#            @not_array,
#            $function =~ /metadata/ ? () : [ 'scalar' ],
#            [ \'scalar' ],
#            [ [] ],
#        ) {
#        $request->{topics} = $bad_topics;
#        expected_error( $function, $request );
#    }
#}
#
#foreach my $function (
#        'encode_metadata_request'
#    ) {
#    foreach my $bad_topic ( @not_empty_string ) {
#        $request->{topics} = [ $bad_topic ];
#        expected_error( $function, $request );
#    }
#}

# TopicName

#foreach my $function (
#        'encode_fetch_request',
#        'encode_offset_request',
#        'encode_produce_request',
#    ) {
#    foreach my $bad_TopicName ( @not_string ) {
#        $request->{topics} = [ { TopicName => $bad_TopicName } ];
#        expected_error( $function, $request );
#    }
#}
$request->{topics} = [ { TopicName => 'mytopic' } ];

# partitions

#foreach my $function (
#        'encode_fetch_request',
#        'encode_offset_request',
#        'encode_produce_request',
#    ) {
#    foreach my $bad_partitions (
#            @not_array,
#            [ 'scalar' ],
#            [ \'scalar' ],
#            [ [] ],
#        ) {
#        $request->{topics}->[0]->{partitions} = $bad_partitions;
#        expected_error( $function, $request );
#    }
#}

# Partition

#foreach my $function (
#        'encode_fetch_request',
#        'encode_offset_request',
#        'encode_produce_request',
#    ) {
#    foreach my $bad_partition ( @not_isint ) {
#        $request->{topics}->[0]->{partitions} = [ { Partition => $bad_partition } ];
#        expected_error( $function, $request );
#    }
#}
$request->{topics}->[0]->{partitions}->[0]->{Partition} = 0;

# MessageSet

#foreach my $function (
#        'encode_produce_request',
#    ) {
#    foreach my $bad_MessageSet (
#            @not_array,
#            [ 'scalar' ],
#            [ \'scalar' ],
#            [ [] ],
#        ) {
#        $request->{topics}->[0]->{partitions}->[0]->{MessageSet} = $bad_MessageSet;
#        expected_error( $function, $request );
#    }
#}
$request->{topics}->[0]->{partitions}->[0]->{MessageSet} = [ {} ];

# FetchOffset

#foreach my $function (
#        'encode_fetch_request',
#    ) {
#    foreach my $bad_FetchOffset ( @not_isint ) {
#        $request->{topics}->[0]->{partitions}->[0]->{FetchOffset} = $bad_FetchOffset;
#        expected_error( $function, $request );
#    }
#}
$request->{topics}->[0]->{partitions}->[0]->{FetchOffset} = 0;

# MaxBytes

#foreach my $function (
#        'encode_fetch_request',
#    ) {
#    foreach my $bad_MaxBytes ( @not_isint ) {
#        $request->{topics}->[0]->{partitions}->[0]->{MaxBytes} = $bad_MaxBytes;
#        expected_error( $function, $request );
#    }
#}
$request->{topics}->[0]->{partitions}->[0]->{MaxBytes} = $DEFAULT_MAX_BYTES;

# Time

#foreach my $function (
#        'encode_offset_request',
#    ) {
#    foreach my $bad_Time ( @not_isint ) {
#        $request->{topics}->[0]->{partitions}->[0]->{Time} = $bad_Time;
#        expected_error( $function, $request );
#    }
#}
$request->{topics}->[0]->{partitions}->[0]->{Time} = $RECEIVE_LATEST_OFFSET;

# MaxNumberOfOffsets

#foreach my $function (
#        'encode_offset_request',
#    ) {
#    foreach my $bad_MaxNumberOfOffsets ( @not_isint ) {
#        $request->{topics}->[0]->{partitions}->[0]->{MaxNumberOfOffsets} = $bad_MaxNumberOfOffsets;
#        expected_error( $function, $request );
#    }
#}
$request->{topics}->[0]->{partitions}->[0]->{MaxNumberOfOffsets} = $DEFAULT_MAX_NUMBER_OF_OFFSETS;

#- MessageSet internal

# Offset

#foreach my $function (
#        'encode_produce_request',
#    ) {
#    foreach my $bad_Offset ( @not_isint ) {
#        $request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Offset} = $bad_Offset;
#        expected_error( $function, $request );
#    }
#}
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Offset} = $PRODUCER_ANY_OFFSET;

# NOTE: MagicByte, Attributes should not be assigned by the user
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{MagicByte}  = 0;
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Attributes} = $COMPRESSION_NONE;

# Key, Value

#foreach my $field (
#        'Key',
#        'Value',
#    ) {
#    foreach my $bad_value ( @not_empty_string ) {
#        $request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{ $field } = $bad_value;
#        expected_error( 'encode_produce_request', $request );
#        $request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Key} = q{};
#    }
#}
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Key} = q{};
$request->{topics}->[0]->{partitions}->[0]->{MessageSet}->[0]->{Value} = q{};

#-- decode_fetch_response (_decode_MessageSet_template)

# RTFM: As an optimization the server is allowed to return a partial message at the end of the message set.
# Clients should handle this case.

# description of responses, see t/??_decode_encode.t

# the correct encoded fetch response hex stream
$encoded_response = pack( "H*", '0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000470000000000000000000000148dc795a20000ffffffff0000000648656c6c6f2100000000000000010000001b989feb390000ffffffff0000000d48656c6c6f2c20576f726c6421' );
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
$decoded = decode_fetch_response( \$encoded_response );
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

# POSTCONDITIONS ---------------------------------------------------------------
