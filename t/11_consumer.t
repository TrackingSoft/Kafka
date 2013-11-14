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
use Params::Util qw(
    _ARRAY
    _HASH
);
use Sub::Install;

use Kafka qw(
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    $ERROR_MISMATCH_ARGUMENT
    $MESSAGE_SIZE_OVERHEAD
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $RECEIVE_EARLIEST_OFFSETS
    $RECEIVE_LATEST_OFFSET
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::MockIO;
use Kafka::Producer;
use Kafka::TestInternals qw(
    @not_empty_string
    @not_isint
    @not_nonnegint
    @not_posint
    @not_right_object
    $topic
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

# WARNING: must match the settings of your system
const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR};

my ( $port, $connect, $partition, $consumer, $offsets, $messages );

sub new_ERROR_MISMATCH_ARGUMENT {
    my ( $field, @bad_values ) = @_;

    foreach my $bad_value ( @bad_values ) {
        undef $consumer;
        throws_ok {
            $consumer = Kafka::Consumer->new(
                Connection          => $connect,
                CorrelationId       => undef,
                ClientId            => undef,
                MaxWaitTime         => $DEFAULT_MAX_WAIT_TIME,
                MinBytes            => $MIN_BYTES_RESPOND_IMMEDIATELY,
                MaxBytes            => $DEFAULT_MAX_BYTES,
                MaxNumberOfOffsets  => $DEFAULT_MAX_NUMBER_OF_OFFSETS,
                $field              => $bad_value,
            );
        } 'Kafka::Exception::Consumer', 'error thrown';
    }
}

sub fetch_ERROR_MISMATCH_ARGUMENT {
    my ( $topic, $partition, $offset, $max_size ) = @_;

    $consumer = Kafka::Consumer->new(
        Connection  => $connect,
    );
    undef $messages;
    throws_ok {
        $messages = $consumer->fetch(
            $topic,
            $partition,
            $offset,
            $max_size,
        );
    } 'Kafka::Exception::Consumer', 'error thrown';
}

sub offsets_ERROR_MISMATCH_ARGUMENT {
    my ( $topic, $partition, $time, $max_number ) = @_;

    $consumer = Kafka::Consumer->new(
        Connection  => $connect,
    );
    undef $offsets;
    throws_ok {
        $messages = $consumer->offsets(
            $topic,
            $partition,
            $time,
            $max_number,
        );
    } 'Kafka::Exception::Consumer', 'error thrown';
}

sub communication_error {
    my ( $module, $name ) = @_;

    my $method_name = "${module}::${name}";
    my $method = \&$method_name;
    $connect = Kafka::Connection->new(
        host        => 'localhost',
        port        => $port,
    );

    Sub::Install::reinstall_sub( {
        code    => sub {
            my ( $self ) = @_;
            $self->_error( $ERROR_MISMATCH_ARGUMENT );
        },
        into    => $module,
        as      => $name,
    } );

# fetch
    $consumer = Kafka::Consumer->new(
        Connection  => $connect,
    );

    undef $messages;
    throws_ok {
        $messages = $consumer->fetch(
            $topic,
            $partition,
            0,
        );
    } 'Kafka::Exception::Connection', 'error thrown';

# offsets
    $consumer = Kafka::Consumer->new(
        Connection  => $connect,
    );

    undef $offsets;
    throws_ok {
        $offsets = $consumer->offsets(
            $topic,
            $partition,
            $RECEIVE_LATEST_OFFSET,
        );
    } 'Kafka::Exception::Connection', 'error thrown';

    Sub::Install::reinstall_sub( {
        code    => $method,
        into    => $module,
        as      => $name,
    } );
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

    $consumer = Kafka::Consumer->new(
        Connection  => $connect,
    );
    isa_ok( $consumer, 'Kafka::Consumer' );

    undef $consumer;
    ok !$consumer, 'consumer object is destroyed';

#-- new

# Connection
    new_ERROR_MISMATCH_ARGUMENT( 'Connection', @not_right_object );

# CorrelationId
    new_ERROR_MISMATCH_ARGUMENT( 'CorrelationId', @not_isint );

# ClientId
    new_ERROR_MISMATCH_ARGUMENT( 'ClientId', @not_empty_string );

# MaxWaitTime
    new_ERROR_MISMATCH_ARGUMENT( 'MaxWaitTime', @not_isint );

# MinBytes
    new_ERROR_MISMATCH_ARGUMENT( 'MinBytes', @not_nonnegint );

# MaxBytes
    new_ERROR_MISMATCH_ARGUMENT( 'MaxBytes', @not_posint, $MESSAGE_SIZE_OVERHEAD - 1 );

# MaxNumberOfOffsets
    new_ERROR_MISMATCH_ARGUMENT( 'MaxNumberOfOffsets', @not_posint );

#-- fetch

# topic
    fetch_ERROR_MISMATCH_ARGUMENT( $_, $partition, 0, $DEFAULT_MAX_BYTES )
        foreach @not_empty_string;

# partition
    fetch_ERROR_MISMATCH_ARGUMENT( $topic, $_, 0, $DEFAULT_MAX_BYTES )
        foreach @not_isint;

# offset
    fetch_ERROR_MISMATCH_ARGUMENT( $topic, $partition, $_, $DEFAULT_MAX_BYTES )
        foreach @not_nonnegint;

# max_size
    fetch_ERROR_MISMATCH_ARGUMENT( $topic, $partition, 0, $_ )
        foreach grep( { defined $_ } @not_nonnegint ), $MESSAGE_SIZE_OVERHEAD - 1;

#-- offsets

# topic
    offsets_ERROR_MISMATCH_ARGUMENT( $_, $partition, $RECEIVE_EARLIEST_OFFSETS, $DEFAULT_MAX_NUMBER_OF_OFFSETS )
        foreach @not_empty_string;

# partition
    offsets_ERROR_MISMATCH_ARGUMENT( $topic, $_, $RECEIVE_EARLIEST_OFFSETS, $DEFAULT_MAX_NUMBER_OF_OFFSETS )
        foreach @not_isint;

# time
    offsets_ERROR_MISMATCH_ARGUMENT( $topic, $partition, $_, $DEFAULT_MAX_NUMBER_OF_OFFSETS )
        foreach @not_isint, -3;

# max_number
    offsets_ERROR_MISMATCH_ARGUMENT( $topic, $partition, $RECEIVE_EARLIEST_OFFSETS, $_ )
        foreach grep( { defined $_ } @not_posint );

#-- Preparing data

    my $producer = Kafka::Producer->new(
        Connection      => $connect,
    );
    # Sending a series of messages
    $producer->send(
        $topic,
        $partition,
        [                           # messages
            'The first message',
            'The second message',
            'The third message',
        ]
    );

    $consumer = Kafka::Consumer->new(
        Connection  => $connect,
    );
    isa_ok( $consumer, 'Kafka::Consumer' );

#-- OffsetRequest

# According to Apache Kafka documentation:
# The Offset API describes the valid offset range available for a set of topic-partitions.
# The response contains the starting offset of each segment for the requested partition
# as well as the "log end offset" i.e. the offset of the next message that would be appended to the given partition.

    # Get a list of valid offsets up max_number before the given time
    $offsets = $consumer->offsets(
        $topic,
        $partition,
# According to Apache Kafka documentation:
# There are two special values.
# Specify -1 ($RECEIVE_LATEST_OFFSET) to receive the latest offset (this will only ever return one offset).
# Specify -2 ($RECEIVE_EARLIEST_OFFSETS) to receive the earliest available offsets.
#        $RECEIVE_EARLIEST_OFFSETS,      # time
        $RECEIVE_LATEST_OFFSET,         # time
        $DEFAULT_MAX_NUMBER_OF_OFFSETS, # max_number
    );
    ok _ARRAY( $offsets ), 'offsets are received';
    if( $offsets ) {
        foreach my $offset ( @$offsets ) {
            note "Received offset: $offset";
        }
    }

#-- FetchRequest

    # Consuming messages
    $messages = $consumer->fetch(
        $topic,
        $partition,
        0,                  # offset
        $DEFAULT_MAX_BYTES  # Maximum size of MESSAGE(s) to receive
    );
    ok _ARRAY( $messages ), 'messages are received';
    if ( $messages ) {
        foreach my $message ( @$messages ) {
#            note '--------------------';
            if( $message->valid ) {
#                note 'key                : ', $message->key;
#                note 'payload            : ', $message->payload;
#                note 'offset             : ', $message->offset;
#                note 'next_offset        : ', $message->next_offset;
#                note 'HighwaterMarkOffset: ', $message->HighwaterMarkOffset;
            } else {
                diag 'error              : ', $message->error;
            }
        }
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
