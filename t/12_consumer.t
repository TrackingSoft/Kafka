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
    _ARRAY
    _HASH
);
use Sub::Install;

use Kafka qw(
    $COMPRESSION_GZIP
    $COMPRESSION_NONE
    $COMPRESSION_SNAPPY
    $COMPRESSION_LZ4
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    $ERROR_CANNOT_RECV
    $MESSAGE_SIZE_OVERHEAD
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $RECEIVE_EARLIEST_OFFSET
    $RECEIVE_LATEST_OFFSETS
    $RETRY_BACKOFF
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
    @not_posnumber
    @not_right_object
    $topic
);



STDOUT->autoflush;

# WARNING: must match the settings of your system
const my $KAFKA_BASE_DIR => $ENV{KAFKA_BASE_DIR};

my ( $port, $connect, $partition, $consumer, $offsets, $messages );

sub new_ERROR_MISMATCH_ARGUMENT {
    my ( $field, @bad_values ) = @_;

    foreach my $bad_value ( @bad_values ) {
        undef $consumer;
        throws_ok {
            $consumer = Kafka::Consumer->new(
                Connection          => $connect,
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
        host            => 'localhost',
        port            => $port,
        RETRY_BACKOFF   => $RETRY_BACKOFF * 2,
    );

    Sub::Install::reinstall_sub( {
        code    => sub {
            my ( $self ) = @_;
            $self->_error( $ERROR_CANNOT_RECV );
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
            $RECEIVE_LATEST_OFFSETS,
        );
    } 'Kafka::Exception::Connection', 'error thrown';

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

    $consumer = Kafka::Consumer->new(
        Connection  => $connect,
    );
    isa_ok( $consumer, 'Kafka::Consumer' );

    undef $consumer;
    ok !$consumer, 'consumer object is destroyed';

    #-- new

    new_ERROR_MISMATCH_ARGUMENT( 'Connection', @not_right_object );
    new_ERROR_MISMATCH_ARGUMENT( 'ClientId', @not_empty_string );
    new_ERROR_MISMATCH_ARGUMENT( 'MaxWaitTime', @not_posnumber );
    new_ERROR_MISMATCH_ARGUMENT( 'MinBytes', @not_nonnegint );
    new_ERROR_MISMATCH_ARGUMENT( 'MaxBytes', @not_posint, $MESSAGE_SIZE_OVERHEAD - 1 );
    new_ERROR_MISMATCH_ARGUMENT( 'MaxNumberOfOffsets', @not_posint );

    #-- fetch

    fetch_ERROR_MISMATCH_ARGUMENT( $_, $partition, 0, $DEFAULT_MAX_BYTES )
        foreach @not_empty_string;

    fetch_ERROR_MISMATCH_ARGUMENT( $topic, $_, 0, $DEFAULT_MAX_BYTES )
        foreach @not_isint;

    fetch_ERROR_MISMATCH_ARGUMENT( $topic, $partition, $_, $DEFAULT_MAX_BYTES )
        foreach @not_nonnegint;

    fetch_ERROR_MISMATCH_ARGUMENT( $topic, $partition, 0, $_ )
        foreach grep( { defined $_ } @not_nonnegint ), $MESSAGE_SIZE_OVERHEAD - 1;

    #-- offsets

    offsets_ERROR_MISMATCH_ARGUMENT( $_, $partition, $RECEIVE_EARLIEST_OFFSET, $DEFAULT_MAX_NUMBER_OF_OFFSETS )
        foreach @not_empty_string;

    offsets_ERROR_MISMATCH_ARGUMENT( $topic, $_, $RECEIVE_EARLIEST_OFFSET, $DEFAULT_MAX_NUMBER_OF_OFFSETS )
        foreach @not_isint;

    offsets_ERROR_MISMATCH_ARGUMENT( $topic, $partition, $_, $DEFAULT_MAX_NUMBER_OF_OFFSETS )
        foreach @not_isint, -3;

    offsets_ERROR_MISMATCH_ARGUMENT( $topic, $partition, $RECEIVE_EARLIEST_OFFSET, $_ )
        foreach grep( { defined $_ } @not_posint );

    #-- Preparing data

    my $producer = Kafka::Producer->new(
        Connection      => $connect,
    );

    my @codecs = ($COMPRESSION_GZIP, $COMPRESSION_NONE, $COMPRESSION_SNAPPY );

    # TODO: $COMPRESSION_LZ4 uses api v2 which is not supported in MockIO
    # so testing that only with real server
    push @codecs, $COMPRESSION_LZ4 if $kafka_base_dir;

    foreach my $compression_codec (@codecs)
    {
        # Sending a series of messages
        $producer->send(
            $topic,
            $partition,
            [                           # messages
# WARN: Next commented messages lead to error under kafka 0.10.0.0 with $COMPRESSION_SNAPPY
# also look at https://issues.apache.org/jira/browse/KAFKA-3789
#                'The first message',
#                'The second message',
#                'The third message',
                'Message #1',
                'Message #2',
                'Message #3',
            ],
            undef,
            $compression_codec,
        );
    }

    $consumer = Kafka::Consumer->new(
        Connection  => $connect,
        $kafka_base_dir ? ( ApiVersion => 2 ) : (), # TODO: $COMPRESSION_LZ4 uses api v2 which is not supported in MockIO
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
# Specify -1 ($RECEIVE_LATEST_OFFSETS) to receive the latest offset (this will only ever return one offset).
# Specify -2 ($RECEIVE_EARLIEST_OFFSET) to receive the earliest available offsets.
#        $RECEIVE_EARLIEST_OFFSET,      # time
        $RECEIVE_LATEST_OFFSETS,         # time
        $DEFAULT_MAX_NUMBER_OF_OFFSETS, # max_number
    );
    ok _ARRAY( $offsets ), 'offsets are received';
    if ( $offsets ) {
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
            if ( $message->valid ) {
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

    communication_error( 'Kafka::IO', 'send' );

    communication_error( 'Kafka::Connection', 'receive_response_to_request' );

    Kafka::MockIO::restore()
        unless $kafka_base_dir;
}


