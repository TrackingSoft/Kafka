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

BEGIN {
    plan skip_all => 'Unknown base directory of Kafka server'
        unless defined $ENV{KAFKA_BASE_DIR};
}

#-- verify load the module

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Kafka qw(
    $COMPRESSION_GZIP
    $COMPRESSION_NONE
    $COMPRESSION_SNAPPY
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $RECEIVE_LATEST_OFFSET
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::MockIO;
use Kafka::Producer;
use Kafka::TestInternals qw(
    $topic
);

#-- setting up facilities ------------------------------------------------------

STDOUT->autoflush;

my $cluster = Kafka::Cluster->new(
    kafka_dir           => $ENV{KAFKA_BASE_DIR},    # WARNING: must match the settings of your system
    replication_factor  => 1,
);

#-- declarations ---------------------------------------------------------------

my ( $port, $connect, $partition, $producer, $consumer, $offsets, $messages );

#-- Global data ----------------------------------------------------------------

$partition = $Kafka::MockIO::PARTITION;;

# INSTRUCTIONS -----------------------------------------------------------------

#-- Connecting to the Kafka server port (for example for node_id = 0)
( $port ) =  $cluster->servers;

#-- Connecting to the Kafka server port

$connect = Kafka::Connection->new(
    host    => 'localhost',
    port    => $port,
);

#-- Preparing data

$producer = Kafka::Producer->new(
    Connection      => $connect,
);

$consumer = Kafka::Consumer->new(
    Connection  => $connect,
);

my $messages_to_send;
push @$messages_to_send, "Message #$_" foreach ( 1..3 );

my @compession_codecs = (
    [ $COMPRESSION_NONE,    'NONE' ],
    [ $COMPRESSION_GZIP,    'GZIP' ],
    [ $COMPRESSION_SNAPPY,  'SNAPPY' ],
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
$offsets = $consumer->offsets(
    $topic,
    $partition,
    $RECEIVE_LATEST_OFFSET,         # time
    $DEFAULT_MAX_NUMBER_OF_OFFSETS, # max_number
);
if( $offsets ) {
    foreach my $offset ( @$offsets ) {
        note "Received offset: $offset";
    }
}

foreach my $return_all ( 0, 1 ) {
    foreach my $start_offset ( $offsets->[1] .. ( $offsets->[0] - 1 ) ) {
        my $compression_codec = int( $start_offset / scalar( @compession_codecs ) );

        # Consuming messages
        $messages = $consumer->fetch(
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
                if( $message->valid ) {
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

# POSTCONDITIONS ---------------------------------------------------------------

$cluster->close;
