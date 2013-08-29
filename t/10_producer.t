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
#use Data::Dumper;
#use File::HomeDir;
use Params::Util qw(
    _ARRAY
    _HASH
);
use Sub::Install;

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    $ERROR_MISMATCH_ARGUMENT
    $NOT_SEND_ANY_RESPONSE
    $REQUEST_TIMEOUT
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

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

# WARNING: must match the settings of your system
#const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR} || File::Spec->catdir( File::HomeDir->my_home, 'kafka' );
const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR};

my ( $port, $connect, $partition, $producer, $response );

sub new_ERROR_MISMATCH_ARGUMENT {
    my ( $field, @bad_values ) = @_;

    foreach my $RaiseError ( 0, 1 ) {
        foreach my $bad_value ( @bad_values ) {
            undef $producer;
            $@ = undef;
            $producer = eval { Kafka::Producer->new(
                Connection      => $connect,
                RaiseError      => $RaiseError,
                CorrelationId   => undef,
                ClientId        => 'producer',
                RequiredAcks    => $WAIT_WRITTEN_TO_LOCAL_LOG,
                Timeout         => $REQUEST_TIMEOUT * 1000, # This provides a maximum time (ms) the server can await the receipt of the number of acknowledgements in RequiredAcks
                $field          => $bad_value,
            ) };
            ok $@, "\$@ changed";
            ok !defined( $producer ), 'producer object is not created';
        }
    }
}

sub send_ERROR_MISMATCH_ARGUMENT {
    my ( $topic, $partition, $messages, $key ) = @_;

    foreach my $RaiseError ( 0, 1 ) {
        $producer = Kafka::Producer->new(
            Connection  => $connect,
            RaiseError  => $RaiseError,
        );
        undef $response;
        $@ = undef;
        ok !$producer->last_errorcode(), 'no error';
        $response = eval { $producer->send(
            $topic,
            $partition,
            $messages,
            $key,
        ) };
        ok $@, "\$@ changed"
            if $RaiseError;
        ok $producer->last_errorcode(), 'an error is detected';
        ok $producer->last_error(), 'expected error';
        ok !defined( $response ), 'response is not received';
    }
}

sub communication_error {
    my ( $module, $name ) = @_;

    my $method_name = "${module}::${name}";
    my $method = \&$method_name;
    for my $RaiseError ( 0, 1 ) {
        $connect = Kafka::Connection->new(
            host        => 'localhost',
            port        => $port,
            RaiseError  => $RaiseError,
        );
        $producer = Kafka::Producer->new(
            Connection  => $connect,
            RaiseError  => $RaiseError,
        );

        Sub::Install::reinstall_sub( {
            code    => sub {
                my ( $self ) = @_;
                return $self->_error( $ERROR_MISMATCH_ARGUMENT );
            },
            into    => $module,
            as      => $name,
        } );

        undef $response;
        $@ = undef;
        ok !$producer->last_errorcode(), 'no error';
        $response = eval { $producer->send(
            $topic,
            $partition,
            'Single message',
        ) };
        if ( $RaiseError ) {
            ok $@, "\$@ changed";
        }
        ok $producer->last_errorcode(), 'an error is detected';
        ok $producer->last_error(), 'expected error';
        ok !defined( $response ), 'response is not received';

        Sub::Install::reinstall_sub( {
            code    => $method,
            into    => $module,
            as      => $name,
        } );
    }
}

#-- Global data ----------------------------------------------------------------

$partition = $Kafka::MockIO::PARTITION;;

# INSTRUCTIONS -----------------------------------------------------------------

testing();
testing( $KAFKA_BASE_DIR ) if $KAFKA_BASE_DIR;

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

#-- Connecting to the Kafka server port

    $connect = Kafka::Connection->new(
        host    => 'localhost',
        port    => $port,
    );

#-- simple start

    $producer = Kafka::Producer->new(
        Connection  => $connect,
    );
    isa_ok( $producer, 'Kafka::Producer' );

    ok !$producer->last_errorcode, 'No errorcode';
    ok !$producer->last_error, 'No error';

    undef $producer;
    ok !$producer, 'producer object is destroyed';

#-- new

# RaiseError ($DEFAULT_RAISE_ERROR => 0;)
    new_ERROR_MISMATCH_ARGUMENT( 'RaiseError', @not_nonnegint );

# Connection
    new_ERROR_MISMATCH_ARGUMENT( 'Connection', @not_right_object );

# CorrelationId
    new_ERROR_MISMATCH_ARGUMENT( 'CorrelationId', @not_isint );

# ClientId
    new_ERROR_MISMATCH_ARGUMENT( 'ClientId', @not_empty_string );

# RequiredAcks
    new_ERROR_MISMATCH_ARGUMENT( 'RequiredAcks', @not_isint );

# Timeout
    new_ERROR_MISMATCH_ARGUMENT( 'Timeout', @not_number );

#-- send

# topic
    send_ERROR_MISMATCH_ARGUMENT( $_, $partition, 'Some value', 'Some key' )
        foreach @not_empty_string;

# partition
    send_ERROR_MISMATCH_ARGUMENT( $topic, $_, 'Some value', 'Some key' )
        foreach @not_isint;

# messages
    foreach my $bad_message (
            grep( { !_ARRAY( $_ ) } @not_empty_string ),
            @not_string_array,
        ) {
        send_ERROR_MISMATCH_ARGUMENT( $topic, $partition, $_, 'Some key' );
    }

# key
    send_ERROR_MISMATCH_ARGUMENT( $topic, $partition, 'Some value', $_ )
        foreach @not_empty_string;

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

        if ( $producer->last_errorcode ) {
            BAIL_OUT '('.$producer->last_errorcode.') ', $producer->last_error."\n";
        }

        # Sending a single message
        $response = $producer->send(
            $topic,
            $partition,
            'Single message'            # message
        );
        ok _HASH( $response ), 'response is received';
#use Data::Dumper;
#diag( Data::Dumper->Dump( [ $response ], [ 'produce_response' ] ) );

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
#use Data::Dumper;
#diag( Data::Dumper->Dump( [ $response ], [ 'produce_response' ] ) );
    }

#-- Response to errors in communication modules

# Kafka::IO
    communication_error( 'Kafka::IO', 'send' );

# Kafka::Connection
    communication_error( 'Kafka::Connection', 'receive_response_to_request' );

#-- finish
    Kafka::MockIO::restore()
        unless $kafka_base_dir;
}

# POSTCONDITIONS ---------------------------------------------------------------
