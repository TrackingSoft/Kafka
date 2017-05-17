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

use Const::Fast;
#use Data::Dumper;
use Params::Util qw(
    _STRING
);

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    $COMPRESSION_NONE
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_WAIT_TIME
    $KAFKA_SERVER_PORT
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $MIN_BYTES_RESPOND_HAS_DATA
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_EARLIEST_OFFSET
    $RECEIVE_LATEST_OFFSETS
    $REQUEST_TIMEOUT
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Internals qw(
    $APIKEY_PRODUCE
    $APIKEY_FETCH
    $APIKEY_OFFSET
    $APIKEY_METADATA
    $PRODUCER_ANY_OFFSET
);
use Kafka::MockIO;
use Kafka::MockProtocol qw(
    encode_fetch_response
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

const my $TOPIC             => 'mytopic';
# Use Kafka::MockIO only with the following information:
const my $PARTITION         => $Kafka::MockIO::PARTITION;

my ( $io, $decoded_request, $encoded_request, $decoded_response, $encoded_response, $len );

sub fulfill_request {
    $len = $io->send( $encoded_request );
    is $len, length $encoded_request, 'request sent correctly';
    $len = 4;
    undef $encoded_response;
    $encoded_response = $io->receive( $len );
    $len = unpack( 'l>', $$encoded_response );
    $$encoded_response .= ${ $io->receive( $len ) };
    ok _STRING( $$encoded_response ), 'response received';
}

Kafka::MockIO::override();

$io = Kafka::IO->new(
    host        => 'localhost',
    port        => $KAFKA_SERVER_PORT,
    timeout     => $REQUEST_TIMEOUT,
    );
isa_ok( $io, 'Kafka::IO' );

#Kafka::IO->debug_level( 1 );

#-- Special cases --------------------------------------------------------------

# a decoded fetch request
$decoded_request = {
    ApiKey                              => $APIKEY_FETCH,
    CorrelationId                       => 0,
    ClientId                            => 'console-consumer-25555',
    MaxWaitTime                         => 100,
    MinBytes                            => $MIN_BYTES_RESPOND_HAS_DATA,
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
$encoded_request = encode_fetch_request( $decoded_request );

# a decoded fetch response
$decoded_response = {
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
$encoded_response = encode_fetch_response( $decoded_response );

is scalar( keys %{ Kafka::MockIO::special_cases() } ), 0, 'special case not present';
Kafka::MockIO::add_special_case( { $encoded_request => $encoded_response } );
is scalar( keys %{ Kafka::MockIO::special_cases() } ), 1, 'special case present';

fulfill_request();
is_deeply( decode_fetch_response( $encoded_response ), $decoded_response, 'decoded correctly' );

Kafka::MockIO::del_special_case( $encoded_request );
is scalar( keys %{ Kafka::MockIO::special_cases() } ), 0, 'special case deleted';

#-- MetadataRequest ------------------------------------------------------------

# a decoded metadata request
$decoded_request = {
    ApiKey                              => $APIKEY_METADATA,
    CorrelationId                       => 0,
    ClientId                            => 'console-consumer-25555',
    topics                              => [
                                        $TOPIC,
    ],
};

$encoded_request = encode_metadata_request( $decoded_request );
fulfill_request();
$decoded_response = decode_metadata_response( $encoded_response );

#-- ProduceRequest --------------------------------------------------------------

# a decoded produce request
$decoded_request = {
    ApiKey                              => $APIKEY_PRODUCE,
    CorrelationId                       => 4,
    ClientId                            => q{},
    RequiredAcks                        => $NOT_SEND_ANY_RESPONSE,
    Timeout                             => int( $REQUEST_TIMEOUT * 1000 ),
    topics                              => [
        {
            TopicName                   => $TOPIC,
            partitions                  => [
                {
                    Partition           => $PARTITION,
                    MessageSet              => [
                        {
                            Offset          => $PRODUCER_ANY_OFFSET,
                            MagicByte       => 0,
                            Attributes      => $COMPRESSION_NONE,
                            Key             => q{},
                            Value           => 'Hello!',
                        },
                    ],
                },
            ],
        },
    ],
};

$encoded_request = encode_produce_request( $decoded_request );
fulfill_request();
$decoded_response = decode_produce_response( $encoded_response );

#-- FetchRequest ---------------------------------------------------------------

# a decoded fetch request
$decoded_request = {
    ApiKey                              => $APIKEY_FETCH,
    CorrelationId                       => 0,
    ClientId                            => 'console-consumer-25555',
    MaxWaitTime                         => int( $DEFAULT_MAX_WAIT_TIME * 1000 ),
    MinBytes                            => $MIN_BYTES_RESPOND_HAS_DATA,
    topics                              => [
        {
            TopicName                   => $TOPIC,
            partitions                  => [
                {
                    Partition           => $PARTITION,
                    FetchOffset         => 0,
                    MaxBytes            => $DEFAULT_MAX_BYTES,
                },
            ],
        },
    ],
};

$encoded_request = encode_fetch_request( $decoded_request );
fulfill_request();
$decoded_response = decode_fetch_response( $encoded_response );

#-- OffsetRequest --------------------------------------------------------------

# a decoded offset request
$decoded_request = {
    ApiKey                              => $APIKEY_OFFSET,
    CorrelationId                       => 0,
    ClientId                            => 'console-consumer-25555',
    topics                              => [
        {
            TopicName                   => $TOPIC,
            partitions                  => [
                {
                    Partition           => $PARTITION,
#                    Time                => $RECEIVE_EARLIEST_OFFSET,
                    Time                => $RECEIVE_LATEST_OFFSETS,
                    MaxNumberOfOffsets  => $DEFAULT_MAX_NUMBER_OF_OFFSETS,
                },
            ],
        },
    ],
};

$encoded_request = encode_offset_request( $decoded_request );
fulfill_request();
$decoded_response = decode_offset_response( $encoded_response );

$io->close;

Kafka::MockIO::restore();

