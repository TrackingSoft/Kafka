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

#use Data::Dumper;

use Kafka qw(
    $COMPRESSION_GZIP
    $COMPRESSION_NONE
    $COMPRESSION_SNAPPY
    $COMPRESSION_LZ4
    $RECEIVE_EARLIEST_OFFSET
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_PRODUCE
    $APIKEY_OFFSET
    $PRODUCER_ANY_OFFSET
);
use Kafka::Protocol qw(
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


my ( $encoded, $decoded );


#-- ProduceRequest -------------------------------------------------------------

=for The ProduceRequest example

***** A ProduceRequest
Hex Stream: 00000049000000000000000400000000000005dc0000000100076d79746f7069630000000100000000000000200000000000000000000000148dc795a20000ffffffff0000000648656c6c6f21

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:49:                    # MessageSize => int32 (a size 0x49 = 73 bytes)
*** Request header
RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
00:00:                          # ApiKey => int16
00:00:                          # ApiVersion => int16
00:00:00:04:                    # CorrelationId => int32
00:00:                          # ClientId => string (a length 0 bytes)
**** ProduceRequest
ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
00:01:                          # RequiredAcks => int16 (1, the server will wait the data is written to the local log before sending a response)
00:00:05:dc:                    # Timeout => int32 (0x5dc = 1_500 ms)
*** Array data for 'topics':
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63:           #   content = 'mytopic'
    ** Array data for 'partitions':
    00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
        [ the first element of the 'partitions' array
        00:00:00:00:                    # Partition => int32
        00:00:00:20:                    # MessageSetSize => int32 (a size 0x20 = 32 bytes)
        MessageSet
MessageSet => [Offset MessageSize Message]
        00:00:00:00:00:00:00:00:        # Offset => int64 (any value here)
        00:00:00:14:                    # MessageSize => int32 (a size 0x14 = 20 bytes)
        Message
Message => Crc MagicByte Attributes Key Value
        8d:c7:95:a2:                    # Crc => int32
        00:                             # MagicByte => int8
        00:                             # Attributes => int8 (the lowest 2 bits - Compression None)
        ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
        00:00:00:06:                    # Value => bytes (a length 0x6 = 6 bytes)
        48:65:6c:6c:6f:21               #   content = 'Hello!'
        ] the end of the first element of the 'partitions' array
    ] the end of the first element of 'topics' the array

***** A Response
Hex Stream: 00000023000000040000000100076d79746f706963000000010000000000000000000000000000

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:23:                    # Size => int32 (a size 0x23 = 35 bytes)
Response => CorrelationId ResponseMessage
00:00:00:04:                    # CorrelationId => int32
ProduceResponse => [TopicName [Partition ErrorCode Offset]]
*** Array data for topics:
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63:           #   content = 'mytopic'
    ** Array data for 'partitions':
    00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
        [ the first element of the 'partitions' array
        00:00:00:00:                    # Partition => int32
        00:00:                          # ErrorCode => int16
        00:00:00:00:00:00:00:00         # Offset => int64
        ] the end of the first element of the 'partitions' array
    ] the end of the first element of 'topics' the array

=cut

# a encoded produce request hex stream
$encoded = pack( "H*", '00000049000000000000000400000001000005dc0000000100076d79746f7069630000000100000000000000200000000000000000000000148dc795a20000ffffffff0000000648656c6c6f21' );

# a decoded produce request
$decoded = {
    CorrelationId                       => 4,
    ClientId                            => q{},
    RequiredAcks                        => $WAIT_WRITTEN_TO_LOCAL_LOG,
    Timeout                             => 1_500,
    topics                              => [
        {
            TopicName                   => 'mytopic',
            partitions                  => [
                {
                    Partition           => 0,
                    MessageSet              => [
                        {
                            Offset          => $PRODUCER_ANY_OFFSET,
                            MagicByte       => 0,
                            Attributes      => 0,
                            Key             => q{},
                            Value           => 'Hello!',
                        },
                    ],
                },
            ],
        },
    ],
};

is_deeply( decode_produce_request( \$encoded ), $decoded, 'decoded correctly' );
is( encode_produce_request( $decoded ), $encoded, 'encoded correctly' );

# a encoded produce response hex stream
$encoded = pack( "H*", '00000023000000040000000100076d79746f706963000000010000000000000000000000000000' );

# a decoded produce response
$decoded = {
    CorrelationId                           => 4,
    topics                                  => [
        {
            TopicName                       => 'mytopic',
            partitions                      => [
                {
                    Partition               => 0,
                    ErrorCode               => 0,
                    Offset                  => 0,
                },
            ],
        },
    ],
};

is_deeply( decode_produce_response( \$encoded ), $decoded, 'decoded correctly' );
is( encode_produce_response( $decoded ), $encoded, 'encoded correctly' );

### Compression ################################################################

# a decoded produce request
$decoded = {
    CorrelationId                       => 4,
    ClientId                            => q{},
    RequiredAcks                        => $WAIT_WRITTEN_TO_LOCAL_LOG,
    Timeout                             => 1_500,
    topics                              => [
        {
            TopicName                   => 'mytopic',
            partitions                  => [
                {
                    Partition           => 0,
                    MessageSet              => [
                        {
                            Offset          => $PRODUCER_ANY_OFFSET,
                            MagicByte       => 0,
                            Attributes      => $COMPRESSION_NONE,
                            Key             => q{},
                            Value           => 'Hello 1!',
                        },
                        {
                            Offset          => $PRODUCER_ANY_OFFSET,
                            MagicByte       => 0,
                            Attributes      => $COMPRESSION_NONE,
                            Key             => q{},
                            Value           => 'Hello 2!',
                        },
                    ],
                },
            ],
        },
    ],
};

# GZIP #########################################################################

=for The ProduceRequest example

***** A ProduceRequest (GZIP)
Hex Stream: 0000007a000000000000000400000001000005dc0000000100076d79746f706963000000010000000000000051000000000000000000000045a5c2da7f0001ffffffff000000371f8b0800d7054f5300036360800331b99d0d171918fe030190c7e1919a9393af60a8c880a4c274ca65217415468a002d91792a44000000

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:7a:                    # MessageSize => int32 (a size 0x7a = 122 bytes)
*** Request header
RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
00:00:                          # ApiKey => int16
00:00:                          # ApiVersion => int16
00:00:00:04:                    # CorrelationId => int32
00:00:                          # ClientId => string (a length 0 bytes)
**** ProduceRequest
ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
00:01:                          # RequiredAcks => int16 (1, the server will wait the data is written to the local log before sending a response)
00:00:05:dc:                    # Timeout => int32 (0x5dc = 1_500 ms)
*** Array data for 'topics':
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63:           #   content = 'mytopic'
    ** Array data for 'partitions':
    00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
        [ the first element of the 'partitions' array
        00:00:00:00:                    # Partition => int32
        00:00:00:51:                    # MessageSetSize => int32 (a size 0x51 = 81 bytes)
        MessageSet
MessageSet => [Offset MessageSize Message]
        00:00:00:00:00:00:00:00:        # Offset => int64 (any value here)
        00:00:00:45:                    # MessageSize => int32 (a size 0x45 = 69 bytes)
        Message
Message => Crc MagicByte Attributes Key Value
        a5:c2:da:7f:                    # Crc => int32
        00:                             # MagicByte => int8
        01:                             # Attributes => int8 (the lowest 2 bits - Compression GZIP)
        ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
        00:00:00:37:                    # Value => bytes (a length 0x37 = 55 bytes)
                                        #   content = compressed MessageSet
        1f:8b:08:00:d7:05:4f:53:00:03:63:60:80:03:31:b9:9d:0d:17:19:18:fe:03:01:90:c7:e1:91:9a:93:93:af:60:a8:c8:80:a4:c2:74:ca:65:21:74:15:46:8a:00:2d:91:79:2a:44:00:00:00
#    MessageSet => [Offset MessageSize Message]
#
#            00:00:00:00:00:00:00:00:        # Offset => int64 (any value here)
#            00:00:00:16:                    # MessageSize => int32 (a size 0x16 = 22 bytes)
#            Message
#    Message => Crc MagicByte Attributes Key Value
#            1e:b9:80:d1:                    # Crc => int32
#            00:                             # MagicByte => int8
#            00:                             # Attributes => int8 (the lowest 2 bits - Compression NONE)
#            ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
#            00:00:00:08:                    # Value => bytes (a length 0x8 = 8 bytes)
#            48:65:6C:6C:6F:20:31:21:        #   content = 'Hello 1!'
#
#            00:00:00:00:00:00:00:00:        # Offset => int64 (any value here)
#            00:00:00:16:                    # MessageSize => int32 (a size 0x16 = 22 bytes)
#            Message
#    Message => Crc MagicByte Attributes Key Value
#            35:94:d3:12:                    # Crc => int32
#            00:                             # MagicByte => int8
#            00:                             # Attributes => int8 (the lowest 2 bits - Compression NONE)
#            ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
#            00:00:00:08:                    # Value => bytes (a length 0x8 = 8 bytes)
#            48:65:6C:6C:6F:20:32:21         #   content = 'Hello 2!'
#
        ] the end of the first element of the 'partitions' array
    ] the end of the first element of 'topics' the array

=cut

# a encoded produce request hex stream
my $encoded_GZIP = pack( 'H*', '0000007a000000000000000400000001000005dc0000000100076d79746f706963000000010000000000000051000000000000000000000045a5c2da7f0001ffffffff000000371f8b0800d7054f5300036360800331b99d0d171918fe030190c7e1919a9393af60a8c880a4c274ca65217415468a002d91792a44000000' );

is_deeply( decode_produce_request( \$encoded_GZIP ), $decoded, 'decoded correctly' );

my $compared_orig;
$compared_orig = $encoded_GZIP;
substr( $compared_orig, 57,  4, '' );   # remove CRC
substr( $compared_orig, 67, 10, '' );   # remove GZIP header (ID1, ID2, CM, FLG, MTIME, XFL, OS  = 1+1+1+1+4+1+1)

my $compared_new;
$compared_new = encode_produce_request( $decoded, $COMPRESSION_GZIP );
substr( $compared_new,  57,  4, '' );   # remove CRC
substr( $compared_new,  67, 10, '' );   # remove GZIP header (ID1, ID2, CM, FLG, MTIME, XFL, OS  = 1+1+1+1+4+1+1)

is( $compared_orig, $compared_new, 'encoded correctly' );

# SNAPPY #######################################################################

=for The ProduceRequest example

***** A ProduceRequest (SNAPPY)
Hex Stream: 0000006f000000000000000400000001000005dc0000000100076d79746f70696300000001000000000000004600000000000000000000003a2ce564450002ffffffff0000002c440000190158161eb980d10000ffffffff0000000848656c6c6f20312119211400163594d3123e2200043221

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:6f:                    # MessageSize => int32 (a size 0x6f = 111 bytes)
*** Request header
RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
00:00:                          # ApiKey => int16
00:00:                          # ApiVersion => int16
00:00:00:04:                    # CorrelationId => int32
00:00:                          # ClientId => string (a length 0 bytes)
**** ProduceRequest
ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
00:01:                          # RequiredAcks => int16 (1, the server will wait the data is written to the local log before sending a response)
00:00:05:dc:                    # Timeout => int32 (0x5dc = 1_500 ms)
*** Array data for 'topics':
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63:           #   content = 'mytopic'
    ** Array data for 'partitions':
    00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
        [ the first element of the 'partitions' array
        00:00:00:00:                    # Partition => int32
        00:00:00:46:                    # MessageSetSize => int32 (a size 0x46 = 70 bytes)
        MessageSet
MessageSet => [Offset MessageSize Message]
        00:00:00:00:00:00:00:00:        # Offset => int64 (any value here)
        00:00:00:3a:                    # MessageSize => int32 (a size 0x3a = 58 bytes)
        Message
Message => Crc MagicByte Attributes Key Value
        2c:e5:64:45:                    # Crc => int32
        00:                             # MagicByte => int8
        02:                             # Attributes => int8 (the lowest 2 bits - Compression SNAPPY)
        ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
        00:00:00:2c:                    # Value => bytes (a length 0x2c = 44 bytes)
                                        #   content = compressed MessageSet
        44:00:00:19:01:58:16:1e:b9:80:d1:00:00:ff:ff:ff:ff:00:00:00:08:48:65:6c:6c:6f:20:31:21:19:21:14:00:16:35:94:d3:12:3e:22:00:04:32:21
#    MessageSet => [Offset MessageSize Message]
#
#            00:00:00:00:00:00:00:00:        # Offset => int64 (any value here)
#            00:00:00:16:                    # MessageSize => int32 (a size 0x16 = 22 bytes)
#            Message
#    Message => Crc MagicByte Attributes Key Value
#            1e:b9:80:d1:                    # Crc => int32
#            00:                             # MagicByte => int8
#            00:                             # Attributes => int8 (the lowest 2 bits - Compression NONE)
#            ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
#            00:00:00:08:                    # Value => bytes (a length 0x8 = 8 bytes)
#            48:65:6C:6C:6F:20:31:21:        #   content = 'Hello 1!'
#
#            00:00:00:00:00:00:00:00:        # Offset => int64 (any value here)
#            00:00:00:16:                    # MessageSize => int32 (a size 0x16 = 22 bytes)
#            Message
#    Message => Crc MagicByte Attributes Key Value
#            35:94:d3:12:                    # Crc => int32
#            00:                             # MagicByte => int8
#            00:                             # Attributes => int8 (the lowest 2 bits - Compression NONE)
#            ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
#            00:00:00:08:                    # Value => bytes (a length 0x8 = 8 bytes)
#            48:65:6C:6C:6F:20:32:21         #   content = 'Hello 2!'
#
        ] the end of the first element of the 'partitions' array
    ] the end of the first element of 'topics' the array

=cut

# a encoded produce request hex stream
my $encoded_SNAPPY_raw = pack( 'H*', '0000006f000000000000000400000001000005dc0000000100076d79746f70696300000001000000000000004600000000000000000000003a2ce564450002ffffffff0000002c440000190158161eb980d10000ffffffff0000000848656c6c6f20312119211400163594d3123e2200043221' );
is_deeply( decode_produce_request( \$encoded_SNAPPY_raw ), $decoded, 'raw decoded correctly' );

# now it will by default encode not in a raw snappy, but with xenial (as Kafka do), so roundtrip is not posiible this way
# is( encode_produce_request( $decoded, $COMPRESSION_SNAPPY ), $encoded_SNAPPY_raw, 'xerial encoded correctly' );

my $encoded_SNAPPY_xerial = pack( 'H*','00000083000000000000000400000001000005dc0000000100076d79746f70696300000001000000000000005a00000000000000000000004e26d5ffb00002ffffffff0000004082534e415050590000000001000000010000002c440000190158161eb980d10000ffffffff0000000848656c6c6f20312119211400163594d3123e2200043221');
is_deeply( decode_produce_request( \$encoded_SNAPPY_xerial ), $decoded, 'xerial decoded correctly' );
is( encode_produce_request( $decoded, $COMPRESSION_SNAPPY ), $encoded_SNAPPY_xerial, 'xerial encoded correctly' );

my $snappy_data_2_frames = pack( 'H*', '82534e41505059000000000100000001000006218080020000150160cc73831516e101000000015e7b3c4a4cffffffff0000cc5d58fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe01007201000000039bff98010058fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100f60100');
my $uncomplessed = Kafka::Protocol::_snappy_xerial_decompress($snappy_data_2_frames);
is length($uncomplessed), 52351, 'xerial unpacker unpack multiple frames data correctly';
is Kafka::Protocol::_snappy_xerial_compress($uncomplessed), $snappy_data_2_frames, 'xerial packer roundtrip ok';

#-- Compression Errors ---------------------------------------------------------

# NOTE: Checked only part possibility errors now

my ( $encoded_with_error_str, $encoded_with_error );

# Unknown compression codec (SNAPPY)
$encoded_with_error_str = '0000006f000000000000000400000001000005dc0000000100076d79746f70696300000001000000000000004600000000000000000000003a2ce564450002ffffffff0000002c440000190158161eb980d10000ffffffff0000000848656c6c6f20312119211400163594d3123e2200043221';
                          #0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0
                          #                                                                                                                            ff
substr( $encoded_with_error_str, 124, 2, 'ff' );    # compressed codec
                             #0000006f000000000000000400000001000005dc0000000100076d79746f70696300000001000000000000004600000000000000000000003a2ce5644500ffffffffff0000002c440000190158161eb980d10000ffffffff0000000848656c6c6f20312119211400163594d3123e2200043221
$encoded_with_error = pack( 'H*', $encoded_with_error_str );
throws_ok {
    decode_produce_request( \$encoded_with_error );
} 'Kafka::Exception::Protocol', 'error thrown';

# gunzip failed: unexpected end of file (GZIP)
$encoded_with_error_str = '0000007a000000000000000400000001000005dc0000000100076d79746f706963000000010000000000000051000000000000000000000045a5c2da7f0001ffffffff000000371f8b0800d7054f5300036360800331b99d0d171918fe030190c7e1919a9393af60a8c880a4c274ca65217415468a002d91792a44000000';
                          #0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0         0
                          #                                                                                                                                                                                                ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
substr( $encoded_with_error_str, -( 30 * 2 ), 30 * 2, 'ff' x 30 );  # compressed MessageSet
                          #0000007a000000000000000400000001000005dc0000000100076d79746f706963000000010000000000000051000000000000000000000045a5c2da7f0001ffffffff000000371f8b0800d7054f5300036360800331b99d0d171918fe030190ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
$encoded_with_error = pack( 'H*', $encoded_with_error_str );
throws_ok {
    decode_produce_request( \$encoded_with_error );
} 'Kafka::Exception::Protocol', 'error thrown';

##### TODO: LZ4


#-- FetchRequest ---------------------------------------------------------------

=for The FetchRequest example

***** A FetchRequest
Hex Stream: 0000004d00010000000000000016636f6e736f6c652d636f6e73756d65722d3235353535ffffffff00000064000000010000000100076d79746f7069630000000100000000000000000000000000100000

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:4d:                    # MessageSize => int32 (a size 0x4d = 77 bytes)
*** Request header
RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
00:01:                          # ApiKey => int16
00:00:                          # ApiVersion => int16
00:00:00:00:                    # CorrelationId => int32
00:16:                          # ClientId => string (a length 0x16 = 22 bytes)
63:6f:6e:73:6f:6c:65:2d:63:6f:  #   content = 'console-consumer-25555'
6e:73:75:6d:65:72:2d:32:35:35:
35:35:
**** FetchRequest
FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
ff:ff:ff:ff:                    # ReplicaId => int32 (-1)
00:00:00:64:                    # MaxWaitTime => int32 (x64 = 100 ms)
00:00:00:01:                    # MinBytes => int32
*** Array data for 'topics':
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63:           #   content = 'mytopic'
    ** Array data for 'partitions':
    00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
        [ the first element of the 'partitions' array
        00:00:00:00:                    # Partition => int32
        00:00:00:00:00:00:00:00:        # FetchOffset => int64
        00:10:00:00                     # MaxBytes => int32 (x100000 = 1_048_576)
        ] the end of the first element of the 'partitions' array
    ] the end of the first element of 'topics' the array

***** A Response
Hex Stream: 0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000470000000000000000000000148dc795a20000ffffffff0000000648656c6c6f2100000000000000010000001b989feb390000ffffffff0000000d48656c6c6f2c20576f726c6421

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:6e:                    # Size => int32 (a size 0x6e = 110 bytes)
Response => CorrelationId ResponseMessage
00:00:00:00:                    # CorrelationId => int32
FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
*** Array data for topics:
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63:           #   content = 'mytopic'
    ** Array data for 'partitions':
    00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
        [ the first element of the 'partitions' array
        00:00:00:00:                    # Partition => int32
        00:00:                          # ErrorCode => int16
        00:00:00:00:00:00:00:02:        # HighwaterMarkOffset => int64
        00:00:00:47:                    # MessageSetSize => int32 (a size 0x47 = 71 bytes)
        MessageSet
MessageSet => [Offset MessageSize Message]
            [ the first element of the 'MessageSet' array
            00:00:00:00:00:00:00:00:        # Offset => int64
            00:00:00:14:                    # MessageSize => int32 (a size 0x14 = 20 bytes)
            Message
Message => Crc MagicByte Attributes Key Value
            8d:c7:95:a2:                    # Crc => int32
            00:                             # MagicByte => int8
            00:                             # Attributes => int8 (the lowest 2 bits - Compression None)
            ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
            00:00:00:06:                    # Value => bytes (a length 0x6 = 6 bytes)
            48:65:6c:6c:6f:21               #   content = 'Hello!'
            ] the end of the first element of the 'MessageSet' array
            [ the second element of the 'MessageSet' array
            00:00:00:00:00:00:00:01:        # Offset => int64
            00:00:00:1b:                    # MessageSize => int32 (a size 0x1b = 27 bytes)
            Message
Message => Crc MagicByte Attributes Key Value
            98:9f:eb:39:                    # Crc => int32
            00:                             # MagicByte => int8
            00:                             # Attributes => int8 (the lowest 2 bits - Compression None)
            ff:ff:ff:ff:                    # Key => bytes (a length -1 = null bytes)
            00:00:00:0d:                    # Value => bytes (a length 0xd = 13 bytes)
            48:65:6c:6c:6f:2c:20:57:6f:72:  #   content = 'Hello, World!'
            6c:64:21
            ] the end of the second element of the 'MessageSet' array
        ] the end of the first element of the 'partitions' array
    ] the end of the first element of 'topics' the array

=cut

# a encoded fetch request hex stream
$encoded = pack( "H*", '0000004d00010000000000000016636f6e736f6c652d636f6e73756d65722d3235353535ffffffff00000064000000010000000100076d79746f7069630000000100000000000000000000000000100000' );

# a decoded fetch request
$decoded = {
    CorrelationId                       => 0,
    ClientId                            => 'console-consumer-25555',
    MaxWaitTime                         => 100,
    MinBytes                            => 1,
    topics                              => [
        {
            TopicName                   => 'mytopic',
            partitions                  => [
                {
                    Partition           => 0,
                    FetchOffset         => 0,
                    MaxBytes            => 1_048_576,
                },
            ],
        },
    ],
};

is_deeply( decode_fetch_request( \$encoded ), $decoded, 'decoded correctly' );
is( encode_fetch_request( $decoded ), $encoded, 'encoded correctly' );

# a encoded fetch response hex stream
$encoded = pack( "H*", '0000006e000000000000000100076d79746f706963000000010000000000000000000000000002000000470000000000000000000000148dc795a20000ffffffff0000000648656c6c6f2100000000000000010000001b989feb390000ffffffff0000000d48656c6c6f2c20576f726c6421' );

# a decoded fetch response
$decoded = {
    CorrelationId                           => 0,
    topics                                  => [
        {
            TopicName                       => 'mytopic',
            partitions                      => [
                {
                    Partition               => 0,
                    ErrorCode               => 0,
                    HighwaterMarkOffset     => 2,
                    MessageSet              => [
                        {
                            Offset          => 0,
                            MagicByte       => 0,
                            Attributes      => 0,
                            Key             => q{},
                            Value           => 'Hello!',
                        },
                        {
                            Offset          => 1,
                            MagicByte       => 0,
                            Attributes      => 0,
                            Key             => q{},
                            Value           => 'Hello, World!',
                        },
                    ],
                },
            ],
        },
    ],
};

is_deeply( decode_fetch_response( \$encoded ), $decoded, 'decoded correctly' );
is( encode_fetch_response( $decoded ), $encoded, 'encoded correctly' );

#-- OffsetRequest --------------------------------------------------------------

=for The OffsetRequest example

***** A OffsetRequest
Hex Stream: 0000004500020000000000000016636f6e736f6c652d636f6e73756d65722d3235353535ffffffff0000000100076d79746f7069630000000100000000fffffffffffffffe00000001

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:45:                    # MessageSize => int32 (a size 0x45 = 69 bytes)
*** Request header
RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
00:02:                          # ApiKey => int16
00:00:                          # ApiVersion => int16
00:00:00:00:                    # CorrelationId => int32
00:16:                          # ClientId => string (a length 0x16 = 22 bytes)
63:6f:6e:73:6f:6c:65:2d:63:6f:  #   content = 'console-consumer-25555'
6e:73:75:6d:65:72:2d:32:35:35:
35:35:
**** OffsetRequest
OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
ff:ff:ff:ff:                    # ReplicaId => int32 (-1)
*** Array data for topics:
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63:           #   content = 'mytopic'
    ** Array data for 'partitions':
    00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
        [ the first element of the 'partitions' array
        00:00:00:00:                    # Partition => int32
        ff:ff:ff:ff:ff:ff:ff:fe:        # Time => int64 (-2)
        00:00:00:01                     # MaxNumberOfOffsets => int32
        ] the end of the first element of the 'partitions' array
    ] the end of the first element of 'topics' the array

***** A Response
Hex Stream: 00000027000000000000000100076d79746f70696300000001000000000000000000010000000000000000

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:27:                    # Size => int32 (a size 0x27 = 39 bytes)
Response => CorrelationId ResponseMessage
There is also a small issue with the CorrelationId in the offset response always being 0.
00:00:00:00:                    # CorrelationId => int32
OffsetResponse => [TopicName [PartitionOffsets]]
*** Array data for 'topics':
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63:           #   content = 'mytopic'
    ** Array data for 'PartitionOffsets':
    00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
        [ the first element of the 'PartitionOffsets' array
PartitionOffsets => Partition ErrorCode [Offset]
        00:00:00:00:                    # Partition => int32
        00:00:                          # ErrorCode => int16
        ** Array data for 'Offset':
        00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
            [ the first element of the 'Offset' array
            00:00:00:00:00:00:00:00         # Offset => int64
            ] the end of the first element of the 'Offset' array
        ] the end of the first element of the 'PartitionOffsets' array
    ] the end of the first element of 'topics' the array

=cut

# a encoded offset request hex stream
$encoded = pack( "H*", '0000004500020000000000000016636f6e736f6c652d636f6e73756d65722d3235353535ffffffff0000000100076d79746f7069630000000100000000fffffffffffffffe00000001' );

# a decoded offset request
$decoded = {
    CorrelationId                       => 0,
    ClientId                            => 'console-consumer-25555',
    topics                              => [
        {
            TopicName                   => 'mytopic',
            partitions                  => [
                {
                    Partition           => 0,
                    Time                => $RECEIVE_EARLIEST_OFFSET,
                    MaxNumberOfOffsets  => 1,
                },
            ],
        },
    ],
};

is_deeply( decode_offset_request( \$encoded ), $decoded, 'decoded correctly' );
is( encode_offset_request( $decoded ), $encoded, 'encoded correctly' );

# a encoded offset response hex stream
$encoded = pack( "H*", '00000027000000000000000100076d79746f70696300000001000000000000000000010000000000000000' );

# a decoded offset response
$decoded = {
    CorrelationId                       => 0,
    topics                              => [
        {
            TopicName                   => 'mytopic',
            PartitionOffsets            => [
                {
                    Partition           => 0,
                    ErrorCode           => 0,
                    Offset              => [
                                           0,
                    ],
                },
            ],
        },
    ],
};

is_deeply( decode_offset_response( \$encoded ), $decoded, 'decoded correctly' );
is( encode_offset_response( $decoded ), $encoded, 'encoded correctly' );

#-- MetadataRequest ------------------------------------------------------------

=for The MetadataRequest example

***** A MetadataRequest
Hex Stream: 0000002d00030000000000000016636f6e736f6c652d636f6e73756d65722d32353535350000000100076d79746f706963

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:2d:                    # MessageSize => int32 (a size 0x2d = 45 bytes)
*** Request header
RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
00:03:                          # ApiKey => int16
00:00:                          # ApiVersion => int16
00:00:00:00:                    # CorrelationId => int32
00:16:                          # ClientId => string (a length 0x16 = 22 bytes)
63:6f:6e:73:6f:6c:65:2d:63:6f:  #   content = 'console-consumer-25555'
6e:73:75:6d:65:72:2d:32:35:35:
35:35:
**** MetadataRequest
MetadataRequest => [TopicName]
*** Array data for 'topics':
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'topics' array
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63            #   content = 'mytopic'
    ] the end of the first element of 'topics' the array

***** A Response
Hex Stream: 0000009c00000000000000030000000200137365726765792d6d696e7431342d6b64653634000023890000000000137365726765792d6d696e7431342d6b64653634000023870000000100137365726765792d6d696e7431342d6b646536340000238800000001000000076d79746f70696300000001000000000000000000020000000300000002000000000000000100000003000000020000000000000001

**** Common Request and Response
RequestOrResponse => Size (RequestMessage | ResponseMessage)
00:00:00:9c:                    # Size => int32 (a size 0x9c = 156 bytes)
Response => CorrelationId ResponseMessage
00:00:00:00:                    # CorrelationId => int32
MetadataResponse => [Broker][TopicMetadata]
*** Array data for 'Broker':
00:00:00:03:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'Broker' array
Broker => NodeId Host Port
    00:00:00:02:                    # NodeId => int32
    00:13:                          # Host => string (a length 0x13 = 19 bytes)
    73:65:72:67:65:79:2d:6d:69:6e:  #   content = 'sergey-mint14-kde64'
    74:31:34:2d:6b:64:65:36:34:
    00:00:23:89:                    # Port => int32 (9097)
    ] the end of the first element of 'Broker' the array
    [ the second element of the 'Broker' array
Broker => NodeId Host Port
    00:00:00:00:                    # NodeId => int32
    00:13:                          # Host => string (a length 0x13 = 19 bytes)
    73:65:72:67:65:79:2d:6d:69:6e:  #   content = 'sergey-mint14-kde64'
    74:31:34:2d:6b:64:65:36:34:
    00:00:23:87:                    # Port => int32 (9095)
    ] the end of the second element of 'Broker' the array
    [ the third element of the 'Broker' array
Broker => NodeId Host Port
    00:00:00:01:                    # NodeId => int32
    00:13:                          # Host => string (a length 0x13 = 19 bytes)
    73:65:72:67:65:79:2d:6d:69:6e:  #   content = 'sergey-mint14-kde64'
    74:31:34:2d:6b:64:65:36:34:
    00:00:23:88:                    # Port => int32 (9096)
    ] the end of the third element of 'Broker' the array
*** Array data for 'TopicMetadata':
00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
    [ the first element of the 'TopicMetadata' array
TopicMetadata => ErrorCode TopicName [PartitionMetadata]
    00:00:                          # ErrorCode => int16
    00:07:                          # TopicName => string (a length 0x7 = 7 bytes)
    6d:79:74:6f:70:69:63:           #   content = 'mytopic'
    ** Array data for 'PartitionMetadata':
    00:00:00:01:                    # int32 array size containing the length N (repetitions of the structure)
        [ the first element of the 'PartitionMetadata' array
PartitionMetadata => ErrorCode Partition Leader Replicas Isr
        00:00:                          # ErrorCode => int16
        00:00:00:00:                    # Partition => int32
        00:00:00:02:                    # Leader => int32
Replicas => [ReplicaId]
        ** Array data for 'Replicas':
        00:00:00:03:                    # int32 array size containing the length N (repetitions of the structure)
            [ the first element of the 'Replicas' array
            00:00:00:02:                    # ReplicaId => int32
            ] the end of the first element of the 'Replicas' array
            [ the second element of the 'Replicas' array
            00:00:00:00:                    # ReplicaId => int32
            ] the end of the second element of the 'Replicas' array
            [ the third element of the 'Replicas' array
            00:00:00:01:                    # ReplicaId => int32
            ] the end of the third element of the 'Replicas' array
Isr => [ReplicaId]
        ** Array data for 'Isr':
        00:00:00:03:                    # int32 array size containing the length N (repetitions of the structure)
            [ the first element of the 'Isr' array
            00:00:00:02:                    # ReplicaId => int32
            ] the end of the first element of the 'Isr' array
            [ the second element of the 'Isr' array
            00:00:00:00:                    # ReplicaId => int32
            ] the end of the second element of the 'Isr' array
            [ the third element of the 'Isr' array
            00:00:00:01:                    # ReplicaId => int32
            ] the end of the third element of the 'Isr' array
        ] the end of the first element of the 'PartitionMetadata' array
    ] the end of the first element of 'TopicMetadata' the array

=cut

# a encoded metadata request hex stream
$encoded = pack( "H*", '0000002d00030000000000000016636f6e736f6c652d636f6e73756d65722d32353535350000000100076d79746f706963' );

# a decoded metadata request
$decoded = {
    CorrelationId                       => 0,
    ClientId                            => 'console-consumer-25555',
    topics                              => [
                                        'mytopic',
    ],
};

is_deeply( decode_metadata_request( \$encoded ), $decoded, 'decoded correctly' );
is( encode_metadata_request( $decoded ), $encoded, 'encoded correctly' );

# a encoded metadata response hex stream
$encoded = pack( "H*", '0000009c00000000000000030000000200137365726765792d6d696e7431342d6b64653634000023890000000000137365726765792d6d696e7431342d6b64653634000023870000000100137365726765792d6d696e7431342d6b646536340000238800000001000000076d79746f70696300000001000000000000000000020000000300000002000000000000000100000003000000020000000000000001' );

# a decoded metadata response
$decoded = {
    CorrelationId                       => 0,
    Broker                              => [
        {
            NodeId                      => 2,
            Host                        => 'sergey-mint14-kde64',
            Port                        => 9097,
        },
        {
            NodeId                      => 0,
            Host                        => 'sergey-mint14-kde64',
            Port                        => 9095,
        },
        {
            NodeId                      => 1,
            Host                        => 'sergey-mint14-kde64',
            Port                        => 9096,
        },
    ],
    TopicMetadata                       => [
        {
            ErrorCode                   => 0,
            TopicName                   => 'mytopic',
            PartitionMetadata           => [
                {
                    ErrorCode           => 0,
                    Partition           => 0,
                    Leader              => 2,
                    Replicas            => [    # of ReplicaId
                                           2,
                                           0,
                                           1,
                    ],
                    Isr                 => [    # of ReplicaId
                                           2,
                                           0,
                                           1,
                    ],
                },
            ],
        },
    ],
};

is_deeply( decode_metadata_response( \$encoded ), $decoded, 'decoded correctly' );
is( encode_metadata_response( $decoded ), $encoded, 'encoded correctly' );

