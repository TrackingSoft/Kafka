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
use Params::Util qw(
    _ARRAY0
    _HASH
);
use Sub::Install;

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    $COMPRESSION_GZIP
    $COMPRESSION_NONE
    $COMPRESSION_SNAPPY
    $COMPRESSION_LZ4
    $ERROR_MISMATCH_ARGUMENT
    $NOT_SEND_ANY_RESPONSE
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::MockIO;
use Kafka::Producer;
use Kafka::TestInternals qw(
    @not_empty_string
    @not_isint
    @not_nonnegint
    @not_number
    @not_right_object
    @not_string_array
    $topic
);

# WARNING: must match the settings of your system
const my $KAFKA_BASE_DIR => $ENV{KAFKA_BASE_DIR};

my ( $port, $connect, $partition, $producer, $response );

sub new_ERROR_MISMATCH_ARGUMENT {
    my ( $field, @bad_values ) = @_;

    foreach my $bad_value ( @bad_values ) {
        undef $producer;
        throws_ok {
            $producer = Kafka::Producer->new(
                Connection      => $connect,
                ClientId        => 'producer',
                RequiredAcks    => $WAIT_WRITTEN_TO_LOCAL_LOG,
                Timeout         => $REQUEST_TIMEOUT * 1000, # This provides a maximum time (ms) the server can await the receipt of the number of acknowledgements in RequiredAcks
                $field          => $bad_value,
            );
        } 'Kafka::Exception::Producer', 'error thrown';
    }
}

sub send_ERROR_MISMATCH_ARGUMENT {
    my ( $topic, $partition, $messages, $key, $compression_codec ) = @_;

    $producer = Kafka::Producer->new(
        Connection  => $connect,
    );
    undef $response;
    throws_ok {
        $response = $producer->send(
            $topic,
            $partition,
            $messages,
            $key,
            $compression_codec,
        );
    } 'Kafka::Exception', 'error thrown';
}

sub communication_error {
    my ( $module, $name ) = @_;

    my $method_name = "${module}::${name}";
    my $method = \&$method_name;

    $connect = Kafka::Connection->new(
        host            => 'localhost',
        port            => $port,
        RETRY_BACKOFF   => $RETRY_BACKOFF * 2,
    );
    $producer = Kafka::Producer->new(
        Connection  => $connect,
    );

    Sub::Install::reinstall_sub( {
        code    => sub {
            my ( $self ) = @_;
            $self->_error( $ERROR_MISMATCH_ARGUMENT );
        },
        into    => $module,
        as      => $name,
    } );

    undef $response;
    throws_ok {
        $response = $producer->send(
            $topic,
            $partition,
            'Single message',
        );
    } 'Kafka::Exception', 'error thrown';

    Sub::Install::reinstall_sub( {
        code    => $method,
        into    => $module,
        as      => $name,
    } );
}

$partition = $Kafka::MockIO::PARTITION;;

testing();
testing( $KAFKA_BASE_DIR ) if $KAFKA_BASE_DIR;

sub testing {
    my ( $kafka_base_dir ) = @_;

    my $no_api_versions = 0;

    if ( $kafka_base_dir ) {
        #-- Connecting to the Kafka server port (for example for node_id = 0)
        ( $port ) =  Kafka::Cluster->new( kafka_dir => $KAFKA_BASE_DIR, reuse_existing => 1 )->servers;
    } else {
        $port = $Kafka::MockIO::KAFKA_MOCK_SERVER_PORT;
        Kafka::MockIO::override();
        $no_api_versions = 1; # no API versions support in Mock protocol
    }

    $connect = Kafka::Connection->new(
        host            => 'localhost',
        port            => $port,
        RETRY_BACKOFF   => $RETRY_BACKOFF * 2,
        dont_load_supported_api_versions => $no_api_versions,
    );

    #-- simple start

    $producer = Kafka::Producer->new(
        Connection  => $connect,
    );
    isa_ok( $producer, 'Kafka::Producer' );

    undef $producer;
    ok !$producer, 'producer object is destroyed';

    #-- new

    new_ERROR_MISMATCH_ARGUMENT( 'Connection', @not_right_object );
    new_ERROR_MISMATCH_ARGUMENT( 'ClientId', @not_empty_string );
    new_ERROR_MISMATCH_ARGUMENT( 'RequiredAcks', @not_isint );
    new_ERROR_MISMATCH_ARGUMENT( 'Timeout', grep { defined $_ } @not_number ); # undef is allowed and replaced with connection timeout or $REQUEST_TIMEOUT

    #-- send
    send_ERROR_MISMATCH_ARGUMENT( $_, $partition, 'Some value', 'Some key' )
        foreach @not_empty_string;
    send_ERROR_MISMATCH_ARGUMENT( $topic, $_, 'Some value', 'Some key' )
        foreach @not_isint;

    foreach my $bad_message (
            grep( { !_ARRAY0( $_ ) } @not_empty_string ),
            @not_string_array,
        ) {
        send_ERROR_MISMATCH_ARGUMENT( $topic, $partition, $bad_message, 'Some key' );
    }

    send_ERROR_MISMATCH_ARGUMENT( $topic, $partition, 'Some value', $_ )
        foreach grep { !( _ARRAY0( $_ ) && @$_ == 1 ) } @not_empty_string;

    send_ERROR_MISMATCH_ARGUMENT( $topic, $partition, 'Some value', 'Some key', $_ )
        foreach ( @not_isint, $COMPRESSION_NONE - 1, $COMPRESSION_LZ4 + 1 );
    # Valid values for $compression_codec checked in the test *_consumer.t

    #-- ProduceRequest

    for my $mode (
        $NOT_SEND_ANY_RESPONSE,
        $WAIT_WRITTEN_TO_LOCAL_LOG,
        $BLOCK_UNTIL_IS_COMMITTED,
        ) {

        $producer = Kafka::Producer->new(
            Connection      => $connect,
            RequiredAcks    => $mode,
        );
        isa_ok( $producer, 'Kafka::Producer' );

        # Sending a single message
        $response = $producer->send(
            $topic,
            $partition,
            'Single message'            # message
        );
        ok _HASH( $response ), 'response is received';

        # Sending a series of messages
        $response = $producer->send(
            $topic,
            $partition,
            [                           # messages
                'The first message',
                'The second message',
                'The third message',
            ]
        );
        ok _HASH( $response ), 'response is received';

        # TODO: MockIO supports only v0, for timestamps v2 is used
        if ( $kafka_base_dir ) {
            # Sending a single message with timestamp
            $response = $producer->send(
                $topic,
                $partition,
                'Single message',            # message
                undef,
                undef,
                time()*1000
            );
            ok _HASH( $response ), 'response is received';

            # Sending a series of messages with timestamp
            $response = $producer->send(
                $topic,
                $partition,
                [                           # messages
                    'The first message',
                    'The second message',
                    'The third message',
                ],
                undef,
                undef,
                time()*1000
            );
            ok _HASH( $response ), 'response is received';

            # Sending a series of messages with series of timestamps
            $response = $producer->send(
                $topic,
                $partition,
                [                           # messages
                    'The first message',
                    'The second message',
                    'The third message',
                ],
                undef,
                undef,
                [time()*1000 - 3, time()*1000 - 2, time()*1000 - 1]
            );
            ok _HASH( $response ), 'response is received';
        }

    }

    #-- Response to errors in communication modules

    # Kafka::IO
    communication_error( 'Kafka::IO', 'send' );

    # Kafka::Connection
    communication_error( 'Kafka::Connection', 'receive_response_to_request' );

    Kafka::MockIO::restore()
        unless $kafka_base_dir;
}

