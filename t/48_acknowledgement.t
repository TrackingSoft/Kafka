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
use Data::Dumper;
use Time::HiRes ();
use Try::Tiny;

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    %ERROR
    $ERROR_SEND_NO_ACK
    $RECEIVE_LATEST_OFFSETS
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

STDOUT->autoflush;

my $cluster = Kafka::Cluster->new(
    replication_factor => 3,
);

const my $PARTITION             => $Kafka::MockIO::PARTITION;
const my $TOPIC                 => $DEFAULT_TOPIC;
const my $MESSAGE               => '*' x 200;
const my $SEND_NO_ACK_REPEATS   => 20;
const my $SEND_NO_ACK_ERROR     => $ERROR{ $ERROR_SEND_NO_ACK };

const my $TIMEOUT_DIVIDER       => 2;
const my $RETRIES               => 2;

my ( $CONNECTION, $PRODUCER, $CONSUMER, $TIMEOUT );
my ( $port, $response, $previous_offset, $next_offset, $success_sendings );

$TIMEOUT                        = $REQUEST_TIMEOUT; # normal timeout
$success_sendings               = 0;

# report variables
my $TOTAL_SENDINGS              = 0;
my $send_with_NO_ACK_errors     = 0;
my $NO_ACK_message_stored       = 0;
my $NO_ACK_message_not_stored   = 0;
my $send_with_other_errors      = 0;
my $other_message_stored        = 0;
my $other_message_not_stored    = 0;
my $not_stored_without_error    = 0;
my %found_ERRORS;

sub sending {
    my $response;
    my $error;
    ++$TOTAL_SENDINGS;
    try {
        $response = $PRODUCER->send(
            $TOPIC,
            $PARTITION,
            $MESSAGE,
        );
    } catch {
        $error = $_;
    };

    # control fetching stored messages
    my $prev_timeout = $TIMEOUT;
    $TIMEOUT = $REQUEST_TIMEOUT;    # restore normal timeout
    my $stored_messages;
    my $retries = $RETRIES;
    while ( $retries-- ) {
        Time::HiRes::sleep 0.5;
        get_new_objects();
#        last if $stored_messages = fetching();
        last if $stored_messages = next_offset();
    }
    BAIL_OUT( 'sending - Cannot fetch messages' ) unless $stored_messages;

#    my $stored = scalar @$stored_messages;
    my $stored = $stored_messages;
    my $prev_success_sendings = $success_sendings;
    $success_sendings = $stored;

    unless ( $error ) {
        if ( $stored == $prev_success_sendings + 1 ) {
            return 1;
        } else {
            ++$not_stored_without_error;
            diag( sprintf( "\n%s WARN: data not stored without error! Sending %d, expected %d but got %d stored records. Timeout %.5f",
                    localtime().'',
                    $TOTAL_SENDINGS,
                    $prev_success_sendings + 1,
                    $stored,
                    $prev_timeout,
                )
            );
            return -1;
        }
    }

    ++$found_ERRORS{ $error }->{total};

    if ( $error->message =~ /$SEND_NO_ACK_ERROR/ ) {
        ++$send_with_NO_ACK_errors;
        diag( sprintf( "\r[%d/%d] %s: stored %d, not stored without error %d, timeout %.5f\r",
                $send_with_NO_ACK_errors,
                $SEND_NO_ACK_REPEATS,
                localtime().'',
                $success_sendings,
                $not_stored_without_error,
                $prev_timeout,
            )
        );

        if ( $stored == $prev_success_sendings ) {
            ++$NO_ACK_message_not_stored;
            pass 'possible not stored on SEND_NO_ACK_ERROR';
            ++$found_ERRORS{ $error }->{not_stored};
            $found_ERRORS{ $error }->{max_not_stored_timeout} = $prev_timeout if !exists( $found_ERRORS{ $error }->{max_not_stored_timeout} ) || $prev_timeout > $found_ERRORS{ $error }->{max_not_stored_timeout};
        } elsif ( $stored == $prev_success_sendings + 1 ) {
            ++$NO_ACK_message_stored;
            pass 'success stored on SEND_NO_ACK_ERROR';
            ++$found_ERRORS{ $error }->{stored};
            $found_ERRORS{ $error }->{max_stored_timeout} = $prev_timeout if !exists( $found_ERRORS{ $error }->{max_stored_timeout} ) || $prev_timeout > $found_ERRORS{ $error }->{max_stored_timeout};
        } else {
            fail "unexpected stored on SEND_NO_ACK_ERROR: fetched $stored, prev_success_sendings $prev_success_sendings";
        }
    } else {
        ++$send_with_other_errors;
#        diag "sending - ignore possible not SEND_NO_ACK_ERROR error: '$error'";

        if ( $stored == $prev_success_sendings ) {
            ++$other_message_not_stored;
            pass 'possible not stored on error';
            ++$found_ERRORS{ $error }->{not_stored};
            $found_ERRORS{ $error }->{max_not_stored_timeout} = $prev_timeout if !exists( $found_ERRORS{ $error }->{max_not_stored_timeout} ) || $prev_timeout > $found_ERRORS{ $error }->{max_not_stored_timeout};
        } elsif ( $stored == $prev_success_sendings + 1 ) {
            pass 'possible stored on error';
            ++$other_message_stored;
            ++$found_ERRORS{ $error }->{stored};
            $found_ERRORS{ $error }->{max_stored_timeout} = $prev_timeout if !exists( $found_ERRORS{ $error }->{max_stored_timeout} ) || $prev_timeout > $found_ERRORS{ $error }->{max_stored_timeout};
        } else {
            fail "unexpected stored on error: fetched $stored, prev_success_sendings $prev_success_sendings";
        }
    }

    return;
}

sub next_offset {
    $TIMEOUT = $REQUEST_TIMEOUT;    # restore normal timeout
    my ( $error, $offsets );
    my $retries = $RETRIES;
    while ( $retries-- ) {
        get_new_objects();
        try {
            $offsets = $CONSUMER->offsets(
                $TOPIC,
                $PARTITION,
                $RECEIVE_LATEST_OFFSETS,
            );
        } catch {
            $error = $_;
        };
        last if $offsets && @$offsets;
        sleep 1;
    }
    BAIL_OUT( 'next_offset - offsets are not received' ) unless $offsets && @$offsets;

    return $offsets->[0];
}

#sub fetching {
#    my $messages;
#    my $error;
#    try {
#        $messages = $CONSUMER->fetch( $TOPIC, $PARTITION, 0 );
#    } catch {
#        $error = $_;
#    };
#    fail "fetching - messages are not fetched: '$error'" if $error;
#
#    return unless @$messages;
#
#    foreach my $i ( 0 .. $#$messages ) {
#        my $message = $messages->[ $i ];
#        unless ( $message->valid && $message->payload ) {
#            fail "fetching - not valid message: message error '".$message->error."'";
#            return;
#        }
#    }
#
#    return $messages;
#}

sub get_new_objects {
    pass "get_new_objects - TIMEOUT = ".sprintf( "%.6f", $TIMEOUT );

    $CONNECTION->close if $CONNECTION;
    undef $CONSUMER;
    undef $PRODUCER;
    undef $CONNECTION;

    lives_ok {
        $CONNECTION = Kafka::Connection->new(
            host                    => 'localhost',
            port                    => $port,
            timeout                 => $TIMEOUT,
            dont_load_supported_api_versions => 1,
        );
    } 'Expecting to live new CONNECTION';
    lives_ok {
        $PRODUCER = Kafka::Producer->new(
            Connection      => $CONNECTION,
            # Ensure that all messages sent and recorded
            RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
            Timeout         => $TIMEOUT,
        );
    } 'Expecting to live new PRODUCER';
    lives_ok {
        $CONSUMER = Kafka::Consumer->new(
            Connection  => $CONNECTION,
        );
    } 'Expecting to live new CONSUMER';
}



#-- Connecting to the Kafka server port (for example for node_id = 0)
( $port ) =  $cluster->servers;

diag 'Started at '.localtime()."\n";
my $stored_messages;
my $work_timeout = $TIMEOUT;
my $max_error_timeout = 0;
while ( $send_with_NO_ACK_errors < $SEND_NO_ACK_REPEATS ) {
    my $prev_success_sendings = $success_sendings;

    $TIMEOUT = $work_timeout;
    get_new_objects();
    my $success_sending = sending();

    if ( $success_sending ) {
        last if $success_sending == -1;
        $work_timeout /= $TIMEOUT_DIVIDER;
        $work_timeout = 0.001 if $work_timeout < 0.001; # minimum timeout is 1ms
    } else {
        $max_error_timeout = $work_timeout if $work_timeout > $max_error_timeout;
        $work_timeout = $TIMEOUT;   # return to normal timeout
    }
}
diag "\nFinished at ".localtime();

ok $success_sendings, 'messages stored';
is $TOTAL_SENDINGS,
      $success_sendings
    - $not_stored_without_error
    + $NO_ACK_message_not_stored
    + $other_message_not_stored,
    'all sendings accounted';
is $send_with_other_errors,
      $other_message_stored
    + $other_message_not_stored,
    'all other errors accounted';
is $send_with_NO_ACK_errors,
      $NO_ACK_message_stored
    + $NO_ACK_message_not_stored,
    'all NO_ACK_ERROR sendings accounted';

# report
diag "total sendings $TOTAL_SENDINGS";
diag "stored messages $success_sendings";
#fail( "NOT STORED WITHOUT ERROR $not_stored_without_error" ) if $not_stored_without_error;
diag( "NOT STORED WITHOUT ERROR $not_stored_without_error" ) if $not_stored_without_error;
diag "max error timeout $max_error_timeout";
diag "sendings with NO_ACK_ERROR $send_with_NO_ACK_errors";
diag "sendings with NO_ACK_ERROR stored $NO_ACK_message_stored";
diag "sendings with NO_ACK_ERROR not stored $NO_ACK_message_not_stored";
diag "sendings with other errors $send_with_other_errors";
diag "sendings with other errors stored $other_message_stored";
diag "sendings with other errors not stored $other_message_not_stored";

$Data::Dumper::Sortkeys = 1;
diag( Data::Dumper->Dump( [ \%found_ERRORS ], [ 'found_ERRORS' ] ) );

$cluster->close;

