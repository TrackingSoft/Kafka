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

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Const::Fast;
#use File::HomeDir;
use Params::Util qw(
    _HASH
    _INSTANCE
);
use Sub::Install;

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_WAIT_TIME
    $ERROR_MISMATCH_ARGUMENT
    $KAFKA_SERVER_PORT
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $MIN_BYTES_RESPOND_HAS_DATA
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_EARLIEST_OFFSETS
    $RECEIVE_LATEST_OFFSET
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
    $SEND_MAX_RETRIES
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_PRODUCE
    $APIKEY_OFFSET
    $PRODUCER_ANY_OFFSET
);
use Kafka::MockIO;
use Kafka::TestInternals qw(
    @not_array0
    @not_hash
    @not_is_like_server_list
    @not_isint
    @not_nonnegint
    @not_number
    @not_posint
    @not_string
    @not_topics_array
    $topic
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

# WARNING: must match the settings of your system
#const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR} || File::Spec->catdir( File::HomeDir->my_home, 'kafka' );
const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR};

my ( $port, $connect, $server, $request, $response, $tmp );

sub new_ERROR_MISMATCH_ARGUMENT {
    my ( $field, @bad_values ) = @_;

    foreach my $bad_value ( @bad_values ) {
        undef $connect;
        throws_ok {
            $connect = Kafka::Connection->new(
                host                => 'localhost',
                port                => $port,
                CorrelationId       => undef,
                SEND_MAX_RETRIES    => $SEND_MAX_RETRIES,
                RETRY_BACKOFF       => $RETRY_BACKOFF,
                $field              => $bad_value,
            );
        } 'Kafka::Exception::Connection', 'error thrown';
        ok !defined( $connect ), 'connection object is not created';
    }
}

sub is_ERROR_MISMATCH_ARGUMENT {
    my ( $function ) = @_;

    foreach my $bad_value ( @not_is_like_server_list ) {
        $connect = Kafka::Connection->new(
            host        => 'localhost',
            port        => $port,
        );

        throws_ok { $connect->$function( $bad_value->[0] ) } 'Kafka::Exception::Connection', 'error thrown';
    }
}

#-- Global data ----------------------------------------------------------------

# INSTRUCTIONS -----------------------------------------------------------------

testing();
testing( $KAFKA_BASE_DIR ) if $KAFKA_BASE_DIR;

communication_error();

sub testing {
    my ( $kafka_base_dir ) = @_;

    if ( $kafka_base_dir ) {
        #-- Connecting to the Kafka server port (for example for node_id = 0)
        ( $port ) =  Kafka::Cluster->new( kafka_dir => $KAFKA_BASE_DIR, does_not_start => 1 )->servers;
    }
    else {
        $port = $Kafka::MockIO::KAFKA_MOCK_SERVER_PORT;
        Kafka::MockIO::override();
    }

#-- simple start

    $connect = Kafka::Connection->new(
        host    => 'localhost',
        port    => $port,
    );
    isa_ok( $connect, 'Kafka::Connection' );

#-- get_known_servers
    is scalar( $connect->get_known_servers() ), 1, 'Known only one server';
    ( $server ) = $connect->get_known_servers();
    ok $connect->is_server_known( $server ), 'known server';
    # requests to the server has not yet been
    ok !$connect->is_server_alive( $server ), 'server is not alive';

    undef $connect;
    ok !$connect, 'connection object is destroyed';

#-- new

# host
    new_ERROR_MISMATCH_ARGUMENT( 'host', @not_string );

# port
    new_ERROR_MISMATCH_ARGUMENT( 'port', @not_posint );

# timeout
    new_ERROR_MISMATCH_ARGUMENT( 'timeout', grep { defined $_ } @not_number );

# broker_list
    new_ERROR_MISMATCH_ARGUMENT( 'broker_list', @not_array0 );
    new_ERROR_MISMATCH_ARGUMENT( 'broker_list', @not_is_like_server_list );

# CorrelationId
    new_ERROR_MISMATCH_ARGUMENT( 'CorrelationId', @not_isint );

# SEND_MAX_RETRIES
    new_ERROR_MISMATCH_ARGUMENT( 'SEND_MAX_RETRIES', @not_posint );

# RETRY_BACKOFF
    new_ERROR_MISMATCH_ARGUMENT( 'RETRY_BACKOFF', @not_posint );

#-- receive_response_to_request

#-- ProduceRequest

    $connect = Kafka::Connection->new(
        host    => 'localhost',
        port    => $port,
    );

# Here and below, the query explicitly indicate ApiKey - producer and consumer must act also

    for my $mode (
            $NOT_SEND_ANY_RESPONSE,
            $WAIT_WRITTEN_TO_LOCAL_LOG,
            $BLOCK_UNTIL_IS_COMMITTED,
        ) {

        $request = {
            ApiKey                              => $APIKEY_PRODUCE,
            CorrelationId                       => 4,   # for example
            ClientId                            => 'producer',
            RequiredAcks                        => $mode,
            Timeout                             => $REQUEST_TIMEOUT * 1000, # ms
            topics                              => [
                {
                    TopicName                   => $topic,
                    partitions                  => [
                        {
                            Partition           => 0,
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

        $response = $connect->receive_response_to_request( $request );
        ok _HASH( $response ), 'response is received';
#use Data::Dumper;
#say Data::Dumper->Dump( [ $response ], [ 'produce_response' ] );
    }

#-- FetchRequest

    for my $mode (
        $MIN_BYTES_RESPOND_IMMEDIATELY,
        $MIN_BYTES_RESPOND_HAS_DATA,
        ) {

        $request = {
            ApiKey                              => $APIKEY_FETCH,
            CorrelationId                       => 0,
            ClientId                            => 'consumer',
            MaxWaitTime                         => $DEFAULT_MAX_WAIT_TIME,
            MinBytes                            => $mode,
            topics                              => [
                {
                    TopicName                   => $topic,
                    partitions                  => [
                        {
                            Partition           => 0,
                            FetchOffset         => 0,
                            MaxBytes            => $DEFAULT_MAX_BYTES,
                        },
                    ],
                },
            ],
        };

        $response = $connect->receive_response_to_request( $request );
        ok _HASH( $response ), 'response is received';
#use Data::Dumper;
#say Data::Dumper->Dump( [ $response ], [ 'fetch_response' ] );
    }

#-- OffsetRequest

    for my $mode (
        $RECEIVE_EARLIEST_OFFSETS,
        $RECEIVE_LATEST_OFFSET,
        ) {

        $request = {
            ApiKey                              => $APIKEY_OFFSET,
            CorrelationId                       => 0,
            ClientId                            => 'consumer',
            topics                              => [
                {
                    TopicName                   => $topic,
                    partitions                  => [
                        {
                            Partition           => 0,
                            Time                => $mode,
                            MaxNumberOfOffsets  => $DEFAULT_MAX_NUMBER_OF_OFFSETS,
                        },
                    ],
                },
            ],
        };

        $response = $connect->receive_response_to_request( $request );
        ok _HASH( $response ), 'response is received';
#use Data::Dumper;
#say Data::Dumper->Dump( [ $response ], [ 'offset_response' ] );
    }

#-- get_known_servers
    ok scalar( $connect->get_known_servers() ), 'Known some servers';

#-- is_server_alive
    foreach my $server ( $connect->get_known_servers() ) {
        if ( $connect->is_server_alive( $server ) ) {
            ok $connect->is_server_alive( $server ), 'server is alive';
            ok $connect->close_connection( $server ), 'close connection';
            ok !$connect->is_server_alive( $server ), 'server is not alive';
        }
    }
    is_ERROR_MISMATCH_ARGUMENT( 'is_server_alive' );

#-- is_server_known
    is_ERROR_MISMATCH_ARGUMENT( 'is_server_known' );

    foreach my $server ( $connect->get_known_servers() ) {
        ok $connect->is_server_known( $server ), 'known server';
    }

#-- close_connection
    is_ERROR_MISMATCH_ARGUMENT( 'close_connection' );

#-- close
    $connect->receive_response_to_request( $request );
    $tmp = 0;
    foreach my $server ( $connect->get_known_servers() ) {
        ++$tmp if $connect->is_server_alive( $server );
    }
    ok( $tmp, 'server is alive' );
    $connect->close;
    $tmp = 0;
    foreach my $server ( $connect->get_known_servers() ) {
        ++$tmp if $connect->is_server_alive( $server );
    }
    ok !$tmp, 'server is not alive';

#-- finish
    Kafka::MockIO::restore()
        unless $kafka_base_dir;
}

sub communication_error {

    $port = $Kafka::MockIO::KAFKA_MOCK_SERVER_PORT;
    Kafka::MockIO::override();
    my $method = \&Kafka::IO::send;

    our $_attempt;
    Sub::Install::reinstall_sub( {
        code    => sub {
            my ( $self ) = @_;
            if ( $main::_attempt++ ) {
                $self->_error( $ERROR_MISMATCH_ARGUMENT );
            } else {
                return &$method( @_ );
            }
        },
        into    => 'Kafka::IO',
        as      => 'send',
    } );

    $connect = Kafka::Connection->new(
        host        => 'localhost',
        port        => $port,
    );

    my $errors = $connect->cluster_errors;
    ok !%$errors, 'no errors';
    eval { $response = $connect->receive_response_to_request( $request ); };
    isa_ok( $@, 'Kafka::Exception' );
    $errors = $connect->cluster_errors;
    is scalar( keys %$errors ), scalar( $connect->get_known_servers ), 'communication errors';

    Sub::Install::reinstall_sub( {
        code    => $method,
        into    => 'Kafka::IO',
        as      => 'send',
    } );

    Kafka::MockIO::restore();
}

# POSTCONDITIONS ---------------------------------------------------------------
