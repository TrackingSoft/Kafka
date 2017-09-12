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
    plan skip_all => 'Unknown base directory of Kafka server'
        unless $ENV{KAFKA_BASE_DIR};
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    $COMPRESSION_GZIP
    $COMPRESSION_NONE
    $COMPRESSION_SNAPPY
    $COMPRESSION_LZ4
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $RECEIVE_LATEST_OFFSETS
    $RETRY_BACKOFF
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::MockIO;
use Kafka::Producer;
use Kafka::TestInternals qw(
    $topic
);

STDOUT->autoflush;

my $cluster = Kafka::Cluster->new(
    replication_factor => 1,
);

my $partition = $Kafka::MockIO::PARTITION;

#-- Connecting to the Kafka server port (for example for node_id = 0)
my( $port ) =  $cluster->servers;

#-- Connecting to the Kafka server port

my $connect = Kafka::Connection->new(
    host            => 'localhost',
    port            => $port,
    RETRY_BACKOFF   => $RETRY_BACKOFF * 2,
    dont_load_supported_api_versions => 1,
);

my $producer = Kafka::Producer->new(
    Connection      => $connect,
    # Require verification the number of messages sent and recorded
    RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
);

my $consumer = Kafka::Consumer->new(
    Connection  => $connect,
    ApiVersion  => 2,
);

my $messages_to_send;
push @$messages_to_send, "Message #$_" foreach ( 1..3 );

my @compession_codecs = (
    [ $COMPRESSION_NONE,    'NONE' ],
    [ $COMPRESSION_GZIP,    'GZIP' ],
    [ $COMPRESSION_SNAPPY,  'SNAPPY' ],
    [ $COMPRESSION_LZ4,     'LZ4' ],
);

foreach my $codec ( @compession_codecs )
{
    # Sending a series of messages
    $producer->send(
        $topic,
        $partition,
        $messages_to_send,
        undef,
        $codec->[0],
    );
}

# Get a list of valid offsets up max_number before the given time
my $offsets = $consumer->offsets(
    $topic,
    $partition,
    $RECEIVE_LATEST_OFFSETS,         # time
    $DEFAULT_MAX_NUMBER_OF_OFFSETS, # max_number
);
if ( $offsets ) {
    foreach my $offset ( @$offsets ) {
        note "Received offset: $offset";
    }
}

foreach my $return_all ( 0, 1 ) {
    foreach my $start_offset ( $offsets->[1] .. ( $offsets->[0] - 1 ) ) {
        my $compression_codec = int( $start_offset / scalar( @compession_codecs ) );

        # Consuming messages
        my $messages = $consumer->fetch(
            $topic,
            $partition,
            $start_offset,
            $DEFAULT_MAX_BYTES, # Maximum size of MESSAGE(s) to receive
            $return_all,
        );
        if ( $messages ) {
            note '--------------------';
            note "Start offset = $start_offset, return_all = $return_all, codec when sending = ".$compession_codecs[ $compression_codec ]->[1];
            foreach my $message ( @$messages ) {
                if ( $message->valid ) {
                    note 'consumed offset: ', $message->offset;
                } else {
                    diag 'error          : ', $message->error;
                }

                if ( $message->offset == $start_offset ) {
                    pass 'Starting Message is present';
                } elsif ( $message->offset > $start_offset ) {
                    pass 'additional message is present';
                } else {    # $message->offset < $start_offset
                    if ( $return_all ) {
                        if ( $compression_codec != $COMPRESSION_NONE ) {
                            pass 'returned redundant message';
                        } else {
                            fail 'returned invalid data';
                        }
                    } else {
                        fail 'returned invalid data';
                    }
                }
            }
        }
    }
}

$cluster->close;

