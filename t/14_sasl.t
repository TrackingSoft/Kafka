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

use Kafka::MockIO;

Kafka::MockIO::override();
#$Kafka::Connection::DEBUG = 1;

const my $host              => $Kafka::MockIO::KAFKA_MOCK_HOSTNAME;
const my $port              => $Kafka::MockIO::KAFKA_MOCK_SERVER_PORT;
const my $topic             => $Kafka::MockIO::TOPIC;
const my $partition         => $Kafka::MockIO::PARTITION;
const my $CorrelationId     => 0;

Sub::Install::reinstall_sub(
    {
        code    => sub { 0 },
        into    => 'Kafka::Internals',
        as      => '_get_CorrelationId',
    }
);
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
    sasl_username       => 'test_user',
    sasl_password       => 'test_password',
    sasl_mechanizm      => 'SCRAM-SHA-512',
    RETRY_BACKOFF       => $RETRY_BACKOFF * 2,
    dont_load_supported_api_versions => 1,
);

my $meta = eval { $connection->get_metadata() };
isnt($@, 'Exception on auth');
ok(exists $meta->{mytopic}, "Get topic");
