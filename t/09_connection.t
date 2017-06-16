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

plan 'no_plan';

use Const::Fast;
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
    $ERROR_CANNOT_SEND
    $ERROR_RESPONSEMESSAGE_NOT_RECEIVED
    $IP_V4
    $IP_V6
    $KAFKA_SERVER_PORT
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $MIN_BYTES_RESPOND_HAS_DATA
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_EARLIEST_OFFSET
    $RECEIVE_LATEST_OFFSETS
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
    $SEND_MAX_ATTEMPTS
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

const my $KAFKA_BASE_DIR => $ENV{KAFKA_BASE_DIR};

my ( $port, $connect, $server, $request, $response, $tmp );

sub new_ERROR_MISMATCH_ARGUMENT {
    my ( $field, @bad_values ) = @_;

    foreach my $bad_value ( @bad_values ) {
        $connect->close if $connect;
        undef $connect;
        throws_ok {
            $connect = Kafka::Connection->new(
                host                => 'localhost',
                port                => $port,
                SEND_MAX_ATTEMPTS   => $SEND_MAX_ATTEMPTS,
                RETRY_BACKOFF       => $RETRY_BACKOFF,
                $field              => $bad_value,
                dont_load_supported_api_versions => 1,
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
            dont_load_supported_api_versions => 1,
        );

        throws_ok { $connect->$function( $bad_value->[0] ) } 'Kafka::Exception::Connection', 'error thrown';
    }
}

my $ip_version_verified;
testing();
testing( $KAFKA_BASE_DIR ) if $KAFKA_BASE_DIR;
ok( $ip_version_verified, 'ip_version verified' ) if $KAFKA_BASE_DIR;

communication_error();

sub testing {
    my ( $kafka_base_dir ) = @_;

    $ip_version_verified = undef;

    if ( $kafka_base_dir ) {
        #-- Connecting to the Kafka server port (for example for node_id = 0)
        ( $port ) =  Kafka::Cluster->new( kafka_dir => $KAFKA_BASE_DIR, reuse_existing => 1 )->servers;
    } else {
        $port = $Kafka::MockIO::KAFKA_MOCK_SERVER_PORT;
        Kafka::MockIO::override();
    }

    #-- simple start

    $connect = Kafka::Connection->new(
        host    => 'localhost',
        port    => $port,
        dont_load_supported_api_versions => 1,
    );
    isa_ok( $connect, 'Kafka::Connection' );
    ok !defined( $connect->{ip_version} ), 'ip_version OK';

    $connect = Kafka::Connection->new(
        host        => 'localhost',
        port        => $port,
        ip_version  => $IP_V4,
        dont_load_supported_api_versions => 1,
    );
    isa_ok( $connect, 'Kafka::Connection' );
    is $connect->{ip_version}, $IP_V4, 'ip_version OK';

    #-- get_known_servers
    is scalar( $connect->get_known_servers() ), 1, 'Known only one server';
    ( $server ) = $connect->get_known_servers();
    ok $connect->is_server_known( $server ), 'known server';
    # requests to the server has not yet been
    ok !$connect->_is_server_connected( $server ), 'server is not alive';

    $connect->close;
    undef $connect;
    ok !$connect, 'connection object is destroyed';

    #-- new

    new_ERROR_MISMATCH_ARGUMENT( 'host', @not_string );
    new_ERROR_MISMATCH_ARGUMENT( 'port', @not_posint );
    new_ERROR_MISMATCH_ARGUMENT( 'timeout', grep { defined $_ } @not_number );
    new_ERROR_MISMATCH_ARGUMENT( 'broker_list', @not_array0 );
    new_ERROR_MISMATCH_ARGUMENT( 'broker_list', @not_is_like_server_list );
    new_ERROR_MISMATCH_ARGUMENT( 'ip_version', -1, $IP_V6 * 2 );
    new_ERROR_MISMATCH_ARGUMENT( 'SEND_MAX_ATTEMPTS', @not_posint );
    new_ERROR_MISMATCH_ARGUMENT( 'RETRY_BACKOFF', @not_posint );

    #-- receive_response_to_request

    #-- ProduceRequest

    $connect = Kafka::Connection->new(
        host        => 'localhost',
        port        => $port,
        ip_version  => $IP_V4,
        dont_load_supported_api_versions => 1,
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
            Timeout                             => int( $REQUEST_TIMEOUT * 1000 ), # ms
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
            MaxWaitTime                         => int( $DEFAULT_MAX_WAIT_TIME * 1000 ),
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
        $RECEIVE_EARLIEST_OFFSET,
        $RECEIVE_LATEST_OFFSETS,
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

    #-- is_server_connected
    foreach my $server ( keys %{ $connect->{_IO_cache} } ) {
        my $io = $connect->{_IO_cache}->{ $server }->{IO};
        if ( exists $io->{ip_version} ) {
            my $ip_version = $io->{ip_version};
            ok defined( $ip_version ), 'server is connected already';
            is $ip_version, $IP_V4, 'ip_version OK';
            ++$ip_version_verified;
        }
    }

    foreach my $server ( $connect->get_known_servers() ) {
        if ( $connect->_is_server_connected( $server ) ) {
            ok $connect->_is_server_connected( $server ), 'server is connected';
            ok $connect->close_connection( $server ), 'close connection';
            ok !$connect->_is_server_connected( $server ), 'server is not connected';
        }
    }
    is_ERROR_MISMATCH_ARGUMENT( '_is_server_connected' );

    #-- _is_server_alive
    foreach my $server ( $connect->get_known_servers() ) {
        ok $connect->_is_server_alive( $server ), 'server is alive';
        ok $connect->close_connection( $server ), 'close connection';
        ok $connect->_is_server_alive( $server ), 'server is alive';
    }
    is_ERROR_MISMATCH_ARGUMENT( '_is_server_alive' );
    throws_ok { $connect->_is_server_alive( 'nothing:9999' ) } 'Kafka::Exception::Connection', 'error thrown';

    #-- get_metadata
    my $metadata = $connect->get_metadata( $topic );
    ok $metadata, 'metadata received';
    ok scalar( keys %$metadata ) == 1 && exists( $metadata->{ $topic } ), "metadata for '$topic' only";
    if ( $kafka_base_dir ) {
        $metadata = $connect->get_metadata();
        ok $metadata, 'metadata received (all topics)';
        ok scalar( keys %$metadata ) >= 1 && exists( $metadata->{ $topic } ), 'metadata for all topics';
        $metadata = $connect->get_metadata( $topic );
        ok $metadata, 'metadata received';
        ok scalar( keys %$metadata ) == 1 && exists( $metadata->{ $topic } ), "metadata for '$topic' only";
        ok scalar( keys %{ $connect->{_metadata} } ) >= 1 && exists( $connect->{_metadata}->{ $topic } ), 'metadata for all topics present';
        delete $connect->{_metadata}->{ $topic };
        $metadata = $connect->get_metadata( $topic );
        ok scalar( keys %{ $connect->{_metadata} } ) >= 1 && exists( $connect->{_metadata}->{ $topic } ), 'metadata for all topics';
        ok $metadata, 'metadata received';
        throws_ok { $connect->get_metadata( '' ) } 'Kafka::Exception::Connection', 'error thrown';
    }

    #-- exists_topic_partition
    ok $connect->exists_topic_partition( $topic, 0 ), 'existing topic';
    ok !$connect->exists_topic_partition( 99999, 0 ), 'not yet existing topic';
    ok !$connect->exists_topic_partition( $topic, 99999 ), 'not yet existing topic';

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
        ++$tmp if $connect->_is_server_connected( $server );
    }
    ok( $tmp, 'server is alive' );
    $connect->close;
    $tmp = 0;
    foreach my $server ( $connect->get_known_servers() ) {
        ++$tmp if $connect->_is_server_connected( $server );
    }
    ok !$tmp, 'server is not connected';

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
                $self->_error( $ERROR_CANNOT_SEND );
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
        dont_load_supported_api_versions => 1,
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

    #-- $ERROR_RESPONSEMESSAGE_NOT_RECEIVED

    $method = \&Kafka::IO::receive;

    Sub::Install::reinstall_sub( {
        code    => sub {
            my ( $self, $length ) = @_;
            my $only_MessageSize;
            if ( $length == 4 ) {
                $only_MessageSize = pack( q{l>}, 0 );
            } else {
                $only_MessageSize = q{};
            }
            return \$only_MessageSize;
        },
        into    => 'Kafka::IO',
        as      => 'receive',
    } );

    $request = {
        ApiKey                              => $APIKEY_FETCH,
        CorrelationId                       => 0,
        ClientId                            => 'consumer',
        MaxWaitTime                         => int( $DEFAULT_MAX_WAIT_TIME * 1000 ),
        MinBytes                            => $MIN_BYTES_RESPOND_IMMEDIATELY,
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

    eval { $response = $connect->receive_response_to_request( $request ); };
    isa_ok( $@, 'Kafka::Exception' );
    is $@->code, $ERROR_RESPONSEMESSAGE_NOT_RECEIVED, '$ERROR_RESPONSEMESSAGE_NOT_RECEIVED OK';

    Sub::Install::reinstall_sub( {
        code    => $method,
        into    => 'Kafka::IO',
        as      => 'receive',
    } );

    Kafka::MockIO::restore();
}

