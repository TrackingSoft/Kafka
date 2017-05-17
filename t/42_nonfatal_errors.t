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

use Const::Fast;
#use Data::Dumper;
use Sub::Install;

use Kafka qw(
    %ERROR

    $ERROR_NO_ERROR
    $ERROR_UNKNOWN
    $ERROR_OFFSET_OUT_OF_RANGE
    $ERROR_INVALID_MESSAGE
    $ERROR_UNKNOWN_TOPIC_OR_PARTITION
    $ERROR_INVALID_FETCH_SIZE
    $ERROR_LEADER_NOT_AVAILABLE
    $ERROR_NOT_LEADER_FOR_PARTITION
    $ERROR_REQUEST_TIMED_OUT
    $ERROR_BROKER_NOT_AVAILABLE
    $ERROR_REPLICA_NOT_AVAILABLE
    $ERROR_MESSAGE_TOO_LARGE
    $ERROR_STALE_CONTROLLER_EPOCH
    $ERROR_OFFSET_METADATA_TOO_LARGE
    $ERROR_NETWORK_EXCEPTION
    $ERROR_GROUP_LOAD_IN_PROGRESS
    $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE
    $ERROR_NOT_COORDINATOR_FOR_GROUP
    $ERROR_INVALID_TOPIC_EXCEPTION
    $ERROR_RECORD_LIST_TOO_LARGE
    $ERROR_NOT_ENOUGH_REPLICAS
    $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND
    $ERROR_INVALID_REQUIRED_ACKS
    $ERROR_ILLEGAL_GENERATION
    $ERROR_INCONSISTENT_GROUP_PROTOCOL
    $ERROR_INVALID_GROUP_ID
    $ERROR_UNKNOWN_MEMBER_ID
    $ERROR_INVALID_SESSION_TIMEOUT
    $ERROR_REBALANCE_IN_PROGRESS
    $ERROR_INVALID_COMMIT_OFFSET_SIZE
    $ERROR_TOPIC_AUTHORIZATION_FAILED
    $ERROR_GROUP_AUTHORIZATION_FAILED
    $ERROR_CLUSTER_AUTHORIZATION_FAILED
    $ERROR_INVALID_TIMESTAMP
    $ERROR_UNSUPPORTED_SASL_MECHANISM
    $ERROR_ILLEGAL_SASL_STATE
    $ERROR_UNSUPPORTED_VERSION

    $ERROR_CANNOT_GET_METADATA
    $ERROR_LEADER_NOT_FOUND
    $ERROR_MISMATCH_ARGUMENT

    $ERROR_CANNOT_BIND
    $ERROR_CANNOT_RECV
    $ERROR_CANNOT_SEND
    $ERROR_SEND_NO_ACK
    $ERROR_NO_CONNECTION
    $MIN_BYTES_RESPOND_HAS_DATA
    $RECEIVE_EARLIEST_OFFSET
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
    $SEND_MAX_ATTEMPTS
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Connection qw(
    %RETRY_ON_ERRORS
);
use Kafka::Consumer;
use Kafka::Producer;

use Kafka::Internals qw(
    $APIKEY_OFFSET
    $APIKEY_PRODUCE
    $PRODUCER_ANY_OFFSET
);
use Kafka::MockProtocol qw(
    encode_metadata_response
    encode_offset_response
    encode_produce_response
);
use Kafka::Protocol qw(
    encode_metadata_request
    encode_offset_request
    encode_produce_request
);

use Kafka::MockIO;

Kafka::MockIO::override();
#$Kafka::Connection::DEBUG = 1;

const my $host              => $Kafka::MockIO::KAFKA_MOCK_HOSTNAME;
const my $port              => $Kafka::MockIO::KAFKA_MOCK_SERVER_PORT;
const my $topic             => $Kafka::MockIO::TOPIC;
const my $partition         => $Kafka::MockIO::PARTITION;
const my $CorrelationId     => 0;

my $decoded_produce_request = {
    ApiKey                              => $APIKEY_PRODUCE,
    CorrelationId                       => $CorrelationId,
    ClientId                            => 'producer',
    RequiredAcks                        => $MIN_BYTES_RESPOND_HAS_DATA,
    Timeout                             => $REQUEST_TIMEOUT * 1000, # ms
    topics                              => [
        {
            TopicName                   => $topic,
            partitions                  => [
                {
                    Partition           => $partition,
                    MessageSet              => [
                        {
                            Offset          => $PRODUCER_ANY_OFFSET,
                            Key             => q{},
                            Value           => 'Hello!',
                        },
                    ],
                },
            ],
        },
    ],
};
my $normal_encoded_produce_request = encode_produce_request( $decoded_produce_request );

my $decoded_offset_request = {
    ApiKey                              => $APIKEY_OFFSET,
    CorrelationId                       => $CorrelationId,
    ClientId                            => 'consumer',
    topics                              => [
        {
            TopicName                   => $topic,
            partitions                  => [
                {
                    Partition           => $partition,
                    Time                => $RECEIVE_EARLIEST_OFFSET,
                    MaxNumberOfOffsets  => 1,
                },
            ],
        },
    ],
};
my $normal_encoded_offset_request = encode_offset_request( $decoded_offset_request );

my $data_exchange->{ $ERROR_NO_ERROR } = {
    decoded_metadata_request    => {
            CorrelationId           => $CorrelationId,
            ClientId                => q{},
            topics                  => [
                                        $topic,
            ],
    },
    decoded_metadata_response   => {
        CorrelationId               => $CorrelationId,
        Broker                      => [
            {
                NodeId                  => 2,
                Host                    => $host,
                Port                    => $port + 2,
            },
            {
                NodeId                  => 0,
                Host                    => $host,
                Port                    => $port,
            },
            {
                NodeId                  => 1,
                Host                    => $host,
                Port                    => $port + 1,
            },
        ],
        TopicMetadata               => [
            {
                ErrorCode               => 0,
                TopicName               => $topic,
                PartitionMetadata       => [
                    {
                        ErrorCode           => 0,
                        Partition           => $partition,
                        Leader              => 2,
                        Replicas            => [
                                                   2,
                                                   0,
                                                   1,
                        ],
                        Isr                 => [
                                                   2,
                                                   0,
                                                   1,
                        ],
                    },
                ],
            },
        ],
    },
};
my $normal_encoded_metadata_request  = encode_metadata_request( $data_exchange->{ $ERROR_NO_ERROR }->{decoded_metadata_request} );
my $normal_encoded_metadata_response = encode_metadata_response( $data_exchange->{ $ERROR_NO_ERROR }->{decoded_metadata_response} );

#-- NON-FATAL errors

$data_exchange->{ $ERROR_LEADER_NOT_FOUND } = {
    decoded_metadata_response   => {
        CorrelationId               => $CorrelationId,
        Broker                      => [
            {
                NodeId                  => 2,
                Host                    => $host,
                Port                    => $port + 2,
            },
            {
                NodeId                  => 0,
                Host                    => $host,
                Port                    => $port,
            },
            {
                NodeId                  => 1,
                Host                    => $host,
                Port                    => $port + 1,
            },
        ],
        TopicMetadata               => [
            {
                ErrorCode               => 0,
                TopicName               => $topic,
                PartitionMetadata       => [
                    {
                        ErrorCode           => 0,
                        Partition           => $partition,
# reason for error $ERROR_LEADER_NOT_FOUND
                        Leader              => 3,
                        Replicas            => [
                                                   2,
                                                   0,
                                                   1,
                        ],
                        Isr                 => [
                                                   2,
                                                   0,
                                                   1,
                        ],
                    },
                ],
            },
        ],
    },
};

$data_exchange->{RETRY_ON_ERRORS} = {
    decoded_produce_response   => {
        CorrelationId                           => $CorrelationId,
        topics                                  => [
            {
                TopicName                       => $topic,
                partitions                      => [
                    {
                        Partition               => $partition,
# reason for error 'RETRY_ON_ERRORS'
                        ErrorCode               => 0,
                        Offset                  => 0,
                    },
                ],
            },
        ],
    },
};

our ( $replaced_method, $skip_calls );
sub Kafka_IO_error {
    my $method_name         = shift;
    $skip_calls             = shift;
    my $expected_error_code = shift;
    my $expected_nonfatals  = shift;
    my $decoded_request     = shift;
    my $throw_error         = shift // $ERROR_CANNOT_SEND;

    my $replaced_method_name = 'Kafka::IO::'.$method_name;
    $replaced_method = \&$replaced_method_name;

    Sub::Install::reinstall_sub(
        {
            code    => sub {
                if ( $main::skip_calls ) {
                    --$main::skip_calls;
                    return $main::replaced_method->( @_ );
                } else {
                    my ( $self ) = @_;
                    $self->_error( $throw_error );
                }
            },
            into    => 'Kafka::IO',
            as      => $method_name,
        }
    );

    my $connection = Kafka::Connection->new(
        host                => $host,
        port                => $port,
        RETRY_BACKOFF       => $RETRY_BACKOFF * 2,
        dont_load_supported_api_versions => 1,
    );

    is scalar( @{ $connection->nonfatal_errors } ), 0, 'non-fatal errors are not fixed';
    eval { $connection->receive_response_to_request( $decoded_request ); };
    my $result_error = $@;
    isa_ok( $result_error, 'Kafka::Exception::Connection' );
    is $result_error->code, $expected_error_code, 'non-fatal error: '.$ERROR{ $expected_error_code };
    # because connection is available, but you can not send a request for metadata
    is scalar( @{ $connection->nonfatal_errors } ), $expected_nonfatals, "$expected_nonfatals non-fatal errors are fixed";

    Sub::Install::reinstall_sub(
        {
            code    => $replaced_method,
            into    => 'Kafka::IO',
            as      => $method_name,
        }
    );
}



my ( $connection, $error );

#-- Connecting to the Kafka mocked server port

#-- Connection

$connection = Kafka::Connection->new(
    host                => $host,
    port                => $port,
    RETRY_BACKOFF       => $RETRY_BACKOFF * 2,
    dont_load_supported_api_versions => 1,
);

#-- $ERROR_LEADER_NOT_FOUND
if( 0 ) { # cannot mock request with unknown CorrelationId
Kafka::MockIO::add_special_case(
    {
        $normal_encoded_metadata_request => encode_metadata_response( $data_exchange->{ $ERROR_LEADER_NOT_FOUND }->{decoded_metadata_response} ),
    }
);

is scalar( @{ $connection->nonfatal_errors } ), 0, 'non-fatal errors are not fixed';
eval { $connection->receive_response_to_request( $decoded_produce_request ); };
$error = $@;
isa_ok( $error, 'Kafka::Exception::Connection' );
is $error->code, $ERROR_LEADER_NOT_FOUND, 'non-fatal error: '.$ERROR{ $ERROR_LEADER_NOT_FOUND };
is scalar( @{ $connection->nonfatal_errors } ), $SEND_MAX_ATTEMPTS, 'non-fatal errors are fixed';

is scalar( @{ $connection->clear_nonfatals } ), 0, 'non-fatal errors are not fixed now';
is scalar( @{ $connection->nonfatal_errors } ), 0, 'non-fatal errors are not fixed';

Kafka::MockIO::add_special_case( { $normal_encoded_metadata_request => $normal_encoded_metadata_response, } );
}

#-- connect IO

Kafka_IO_error(
    'new',              # method causes an error
    # skip connection with the initial preparation of metadata
    1,                  # skip calls
    $ERROR_CANNOT_BIND, # expected error code
    # because connection is not available
    $SEND_MAX_ATTEMPTS,                  # expected non-fatal errors
    $decoded_produce_request,
    $ERROR_CANNOT_BIND, # error to throw from IO
);

#-- send IO

Kafka_IO_error(
    'send',             # method name
    # skip sending the request for the initial preparation of metadata
    1,                  # skip calls
    $ERROR_CANNOT_SEND, # expected error code
    # because connection is available, but you can not send a request for metadata
    1,                  # expected non-fatal errors
    $decoded_offset_request,
);

#-- receive IO

Kafka_IO_error(
    'receive',          # method name
    # skip to receive a response for the initial preparation of metadata (consists of two consecutive readings from the socket)
    2,                  # skip calls
    $ERROR_SEND_NO_ACK, # expected error code
    # because connection is available, but you can not receive a response for metadata
    0,                  # expected non-fatal errors
    $decoded_produce_request,
);

Kafka_IO_error(
    'receive',          # method name
    # skip to receive a response for the initial preparation of metadata (consists of two consecutive readings from the socket)
    2,                  # skip calls
    $ERROR_CANNOT_RECV, # expected error code
    # because connection is available, but you can not receive a response for metadata
    1,                  # expected non-fatal errors
    $decoded_offset_request,
);

#-- %Kafka::Connection::RETRY_ON_ERRORS
my $partition_data = $data_exchange->{RETRY_ON_ERRORS}->{decoded_produce_response}->{topics}->[0]->{partitions}->[0];
foreach my $ErrorCode (
        $ERROR_NO_ERROR,
        $ERROR_UNKNOWN,
        $ERROR_OFFSET_OUT_OF_RANGE,
        $ERROR_INVALID_MESSAGE,
        $ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        $ERROR_INVALID_FETCH_SIZE,
        $ERROR_LEADER_NOT_AVAILABLE,
        $ERROR_NOT_LEADER_FOR_PARTITION,
        $ERROR_BROKER_NOT_AVAILABLE,
        $ERROR_REPLICA_NOT_AVAILABLE,
        $ERROR_MESSAGE_TOO_LARGE,
        $ERROR_STALE_CONTROLLER_EPOCH,
        $ERROR_NETWORK_EXCEPTION,
        $ERROR_OFFSET_METADATA_TOO_LARGE,
        $ERROR_GROUP_LOAD_IN_PROGRESS,
        $ERROR_GROUP_COORDINATOR_NOT_AVAILABLE,
        $ERROR_NOT_COORDINATOR_FOR_GROUP,
        $ERROR_INVALID_TOPIC_EXCEPTION,
        $ERROR_RECORD_LIST_TOO_LARGE,
        $ERROR_NOT_ENOUGH_REPLICAS,
        $ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        $ERROR_INVALID_REQUIRED_ACKS,
        $ERROR_ILLEGAL_GENERATION,
        $ERROR_INCONSISTENT_GROUP_PROTOCOL,
        $ERROR_INVALID_GROUP_ID,
        $ERROR_UNKNOWN_MEMBER_ID,
        $ERROR_INVALID_SESSION_TIMEOUT,
        $ERROR_REBALANCE_IN_PROGRESS,
        $ERROR_INVALID_COMMIT_OFFSET_SIZE,
        $ERROR_TOPIC_AUTHORIZATION_FAILED,
        $ERROR_GROUP_AUTHORIZATION_FAILED,
        $ERROR_CLUSTER_AUTHORIZATION_FAILED,
        $ERROR_INVALID_TIMESTAMP,
        $ERROR_UNSUPPORTED_SASL_MECHANISM,
        $ERROR_ILLEGAL_SASL_STATE,
        $ERROR_UNSUPPORTED_VERSION,
        $ERROR_NO_CONNECTION,
    ) {

    $partition_data->{ErrorCode} = $ErrorCode;
    Kafka::MockIO::add_special_case(
        {
            $normal_encoded_produce_request => encode_produce_response( $data_exchange->{RETRY_ON_ERRORS}->{decoded_produce_response} ),
        }
    );

    $connection = Kafka::Connection->new(
        host                => $host,
        port                => $port,
        RETRY_BACKOFF       => $RETRY_BACKOFF * 2,
        dont_load_supported_api_versions => 1,
    );

    is scalar( @{ $connection->nonfatal_errors } ), 0, 'non-fatal errors are not fixed';

    if ( $ErrorCode == $ERROR_NO_ERROR ) {
        lives_ok { $connection->receive_response_to_request( $decoded_produce_request ); } 'expecting to live';
    } else {
        eval { $connection->receive_response_to_request( $decoded_produce_request ); };
        $error = $@;
        isa_ok( $error, 'Kafka::Exception::Connection' );

        if ( exists $RETRY_ON_ERRORS{ $ErrorCode } ) {
            is $error->code, $ErrorCode, 'non-fatal error: '.$ERROR{ $ErrorCode };
            is scalar( @{ $connection->nonfatal_errors } ), $SEND_MAX_ATTEMPTS, 'non-fatal errors are fixed';
        } else {
            is $error->code, $ErrorCode, 'FATAL error: '.$ERROR{ $ErrorCode };
            is scalar( @{ $connection->nonfatal_errors } ), 0, 'non-fatal errors are not fixed';
        }
    }
}

$partition_data->{ErrorCode} = $ERROR_NO_ERROR;
Kafka::MockIO::add_special_case(
    {
        $normal_encoded_produce_request => encode_produce_response( $data_exchange->{RETRY_ON_ERRORS}->{decoded_produce_response} ),
    }
);

$connection->close;

Kafka::MockIO::restore();

