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
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use Const::Fast;
use FindBin qw(
    $Bin
);
use File::Spec::Functions qw(
    catdir
    catfile
);
use Params::Util qw(
    _ARRAY
    _ARRAY0
    _HASH
);

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    $DEFAULT_MAX_BYTES
    $RECEIVE_LATEST_OFFSETS
    $RETRY_BACKOFF
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::MockIO;
use Kafka::Producer;

const my $TOPIC_PATTERN         => 'stranger0';

my ( $connection, $topic, $partition, $producer, $response, $consumer, $offsets, $messages );

sub sending {
    return $producer->send(
        ++$topic,                   # unknown topic
        $partition,
        'Single message'            # message
    );
}

sub getting_offsets {
    return $consumer->offsets(
        ++$topic,
        $partition,
        $RECEIVE_LATEST_OFFSETS,         # time
    );
}

sub fetching {
    return $consumer->fetch(
        ++$topic,
        $partition,
        0,                  # offset
        $DEFAULT_MAX_BYTES  # Maximum size of MESSAGE(s) to receive
    );
}

$partition  = $Kafka::MockIO::PARTITION;;
$topic      = $TOPIC_PATTERN;

for my $auto_create_topics_enable ( 'true', 'false' ) {
    my $cluster = Kafka::Cluster->new(
        properties => {
            'auto.create.topics.enable' => $auto_create_topics_enable,
        },
    );
    isa_ok( $cluster, 'Kafka::Cluster' );

    #-- Connecting to the Kafka server port (for example for node_id = 0)
    my( $port ) =  $cluster->servers;

    for my $AutoCreateTopicsEnable ( 0, 1 ) {
        #-- Connecting to the Kafka server port
        $connection = Kafka::Connection->new(
            host                    => 'localhost',
            port                    => $port,
            AutoCreateTopicsEnable  => $AutoCreateTopicsEnable,
            RETRY_BACKOFF           => $RETRY_BACKOFF * 2,
            dont_load_supported_api_versions => 1,
        );
        $producer = Kafka::Producer->new(
            Connection      => $connection,
            # Require verification of the number of messages sent and recorded
            RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
        );
        $consumer = Kafka::Consumer->new(
            Connection  => $connection,
        );

        # Sending a single message
        undef $response;
        if ( $auto_create_topics_enable eq 'true' && $AutoCreateTopicsEnable ) {
            ok $connection->exists_topic_partition( $topic, $partition ), 'existing topic';
            my $next_topic = $topic;
            ++$next_topic;
            ok !$connection->exists_topic_partition( $next_topic, $partition ), 'not yet existing topic';
            lives_ok    { $response = sending() } 'expecting to live';
            ok _HASH( $response ), 'response is not received';
            $connection->get_metadata( $topic );
            ok $connection->exists_topic_partition( $next_topic, $partition ), 'autocreated topic';
        } else {
            if ( $auto_create_topics_enable ne 'true' ) {
                dies_ok     { $response = sending() } 'expecting to die';
                ok !defined( $response ), 'response is not received';
            }
        }

        # Get a list of valid offsets up max_number before the given time
        undef $offsets;
        if ( $auto_create_topics_enable eq 'true' && $AutoCreateTopicsEnable ) {
            lives_ok    { $offsets = getting_offsets() } 'expecting to live';
            ok _ARRAY( $offsets ), 'offsets are received';
        } else {
            if ( $auto_create_topics_enable ne 'true' ) {
                dies_ok     { $offsets = getting_offsets() } 'expecting to die';
                ok !defined( $offsets ), 'offsets are not received';
            }
        }

        # Consuming messages
        undef $messages;
        if ( $auto_create_topics_enable eq 'true' && $AutoCreateTopicsEnable ) {
            lives_ok    { $messages = fetching() } 'expecting to live';
            ok _ARRAY0( $messages ), 'messages are received';
        } else {
            if ( $auto_create_topics_enable ne 'true' ) {
                dies_ok     { $messages = fetching() } 'expecting to die';
                ok !defined( $messages ), 'messages are not received';
            }
        }
    }

    $cluster->close;
}

