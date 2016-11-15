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
        unless $ENV{KAFKA_BASE_DIR};
}

#-- verify load the module

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Const::Fast;

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    %ERROR
    $ERROR_SEND_NO_ACK
    $RECEIVE_LATEST_OFFSET
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
    $SEND_MAX_ATTEMPTS
);
use Kafka::Cluster qw(
    $DEFAULT_TOPIC
);
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::MockIO;
use Kafka::Producer;

#-- setting up facilities ------------------------------------------------------

STDOUT->autoflush;

my $cluster = Kafka::Cluster->new(
    kafka_dir           => $ENV{KAFKA_BASE_DIR},    # WARNING: must match the settings of your system
    replication_factor  => 3,
);

#-- declarations ---------------------------------------------------------------

const my $PARTITION             => $Kafka::MockIO::PARTITION;
const my $TOPIC                 => $DEFAULT_TOPIC;
const my $MESSAGE               => 'simple message';
const my $SEND_NO_ACK_REPEATS   => 200;
const my $SEND_NO_ACK_ERROR     => $ERROR{ $ERROR_SEND_NO_ACK };

my ( $CONNECTION, $PRODUCER, $CONSUMER, $TIMEOUT );
my ( $port, $response, $previous_offset, $next_offset, $send_no_ack_errors, $success_sendings );

$send_no_ack_errors = 0;
$TIMEOUT            = $REQUEST_TIMEOUT;
$success_sendings   = 0;

sub sending {
    my $retries = 0;
    my $response;
    ++$retries;
    eval {
        $response = $PRODUCER->send(
            $TOPIC,
            $PARTITION,
            $MESSAGE,
        );
    };
    my $error = $@;

    if ( $error ) {
        $TIMEOUT = $REQUEST_TIMEOUT;
        get_new_objects();
        my $stored_messages = fetching();
        my $_received = scalar( @$stored_messages );

        if ( $error->message =~ /$SEND_NO_ACK_ERROR/ ) {
            ++$send_no_ack_errors;
            pass "expected sending error: $error";

            if ( $_received == $success_sendings ) {
                fail 'unexpected no received on SEND_NO_ACK_ERROR';
                return;
            } elsif ( $_received == $success_sendings + 1 ) {
                ok 'success receive on SEND_NO_ACK_ERROR';
                $success_sendings = $_received;
                return 1;
            } elsif ( $_received > $success_sendings + 1 ) {
                fail 'unexpected additional receives on SEND_NO_ACK_ERROR: '.( $_received - $success_sendings + 1 );
                $success_sendings = $_received;
            }
        } else {
            pass "acceptable not SEND_NO_ACK_ERROR sending error = '$error'";

            if ( $_received == $success_sendings + 1 ) {
                ++$success_sendings;
                return 1;
            } elsif ( $_received > $success_sendings + 1 ) {
                fail 'unexpected additional receives: '.( $_received - $success_sendings + 1 );
                $success_sendings = $_received;
            } else {
                return;
            }
        }
    } else {
        ++$success_sendings;
        return 1;
    }
}

sub next_offset {
    my $offsets;
    eval {
        $offsets = $CONSUMER->offsets(
            $TOPIC,
            $PARTITION,
            $RECEIVE_LATEST_OFFSET,             # time
        );
    };
    fail "offsets are not received: $@" if $@;

    if ( $offsets ) {
        return $offsets->[0];
    } else {
        return;
    }
}

sub fetching {
    my $messages;
    eval {
        $messages = $CONSUMER->fetch( $TOPIC, $PARTITION, 0 );
    };
    fail "messages are not fetched: $@" if $@;

    if ( $messages ) {
        foreach my $i ( 0 .. $#$messages ) {
            my $message = $messages->[ $i ];
            return unless $message->valid && $message->payload;
        }
    } else {
        return;
    }

    return $messages;
}

sub get_new_objects {
    pass "TIMEOUT = ".sprintf( "%.6f", $TIMEOUT );

    $CONNECTION->close if $CONNECTION;
    undef $CONSUMER;
    undef $PRODUCER;
    undef $CONNECTION;

    $CONNECTION = Kafka::Connection->new(
        host                    => 'localhost',
        port                    => $port,
        timeout                 => $TIMEOUT,
        RECEIVE_MAX_ATTEMPTS    => 1,
    );
    $PRODUCER = Kafka::Producer->new(
        Connection      => $CONNECTION,
        # Ensure that all messages sent and recorded
        RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
        Timeout         => $TIMEOUT,
    );
    $CONSUMER = Kafka::Consumer->new(
        Connection  => $CONNECTION,
    );
}

#-- Global data ----------------------------------------------------------------

#-- Connecting to the Kafka server port (for example for node_id = 0)
( $port ) =  $cluster->servers;

# INSTRUCTIONS -----------------------------------------------------------------

my $stored_messages;
while ( $send_no_ack_errors < $SEND_NO_ACK_REPEATS ) {
    my $prev_success_sendings = $success_sendings;

    get_new_objects();
    my $response = sending();

    my $work_timeout = $TIMEOUT;
    $TIMEOUT = $REQUEST_TIMEOUT;
    get_new_objects();
    if ( $response ) {
        ok( $stored_messages = fetching(), 'fetching ok' );
        is scalar( @$stored_messages ), $success_sendings, 'only registered receives';
        is $prev_success_sendings + 1, $success_sendings, 'one sending';
    }

    $TIMEOUT = $work_timeout / 2;
}

$TIMEOUT = $REQUEST_TIMEOUT;
get_new_objects();
ok( $stored_messages = fetching(), 'fetching ok' );
is $send_no_ack_errors, $SEND_NO_ACK_REPEATS, "SEND_NO_ACK_ERROR found: $send_no_ack_errors";
ok $stored_messages, 'messages are stored';
is scalar( @$stored_messages ), $success_sendings, 'receives on SEND_NO_ACK_ERROR registered';

# POSTCONDITIONS ---------------------------------------------------------------

$cluster->close;
