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

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Kafka::Cluster;
use Const::Fast;
use Sub::Install;

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    %ERROR
    $ERROR_SEND_NO_ACK
    $MESSAGE_SIZE_OVERHEAD
    $RECEIVE_LATEST_OFFSET
);
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::Producer;

#-- setting up facilities ------------------------------------------------------

STDOUT->autoflush;

#Kafka::Connection->debug_level( 1 );

#-- declarations ---------------------------------------------------------------

const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR};    # WARNING: must match the settings of your system
const my $TOPIC             => $Kafka::Cluster::DEFAULT_TOPIC;
const my $HOST              => $Kafka::Cluster::HOST;
const my $PARTITION         => 0;

my ( $cluster, $port, $connection, $producer, $consumer, $error );

sub next_offset {
    my $offsets;
    eval {
        $offsets = $consumer->offsets(
            $TOPIC,
            $PARTITION,
            $RECEIVE_LATEST_OFFSET,             # time
        );
    };
    if ( $@ ) {
        fail "'offsets' FATAL error: $@";
        return;
    } else {
        if ( $offsets ) {
            return $offsets->[0];
        }
        if ( !$offsets ) {
            fail 'offsets are not received';
            return;
        }
    }
}

{
    my $start_offset;

    # counting on the fact that all messages have the same length
    my @transmitted_messages = (
        '1111111111',
        '2222222222',
        '3333333333',
    );

    my $original_send = \&Kafka::IO::send;

    sub prepare_messages {
        foreach my $i ( 0 .. $#transmitted_messages ) {
            ++$transmitted_messages[ $i ];
        }
    }

    sub send_without_response {
        prepare_messages();
        $start_offset = next_offset();

        Sub::Install::reinstall_sub(
            {
                code    => sub {
                    my ( $self, $message ) = @_;

                    my $ret = $original_send->( $self, $message );
                    $self->close;
                    ok !$self->is_alive, 'is not alive';
                    return $ret;
                },
                into    => 'Kafka::IO',
                as      => 'send',
            }
        );

        my $response;
        eval {
                $response = $producer->send(
                    $TOPIC,
                    $PARTITION,
                    \@transmitted_messages,
                );
        };
        my $error = $@;
        my $error_message = $ERROR{$ERROR_SEND_NO_ACK};
        like $error->message, qr/$error_message/, "'send' FATAL error";

        Sub::Install::reinstall_sub(
            {
                code    => $original_send,
                into    => 'Kafka::IO',
                as      => 'send',
            }
        );
    }

    sub fetching_all_messages {
        my $msgs_to_receive     = scalar @transmitted_messages;
        my $bytes_to_receive    = $msgs_to_receive * ( length( $transmitted_messages[0] ) + $MESSAGE_SIZE_OVERHEAD );

        my $messages;
        eval {
            $messages = $consumer->fetch(
                $TOPIC,
                $PARTITION,
                $start_offset,
                $bytes_to_receive,  # Maximum size of MESSAGE(s) to receive
            );
        };
        if ( $@ ) {
            fail "'fetch' FATAL error: $@";
            return;
        };

        my $i = 0;
        foreach my $message ( @$messages ) {
            if( $message->valid ) {
                is $message->payload, $transmitted_messages[ $i++ ], 'message ok';
            } else {
                fail 'message error: '.$message->error;
            }
        }
        is $i, $msgs_to_receive, 'all messages recorded';
    }

    # not real situation
    sub send_not_complete_messages_without_lost_connection {
        prepare_messages();
        $start_offset = next_offset();

        Sub::Install::reinstall_sub(
            {
                code    => sub {
                    my ( $self, $message ) = @_;

                    # simply truncate message
                    substr $message, -3, 3, q{};
                    return $original_send->( $self, $message );
                },
                into    => 'Kafka::IO',
                as      => 'send',
            }
        );

        my $response;
        eval {
                $response = $producer->send(
                    $TOPIC,
                    $PARTITION,
                    \@transmitted_messages,
                );
        };
        my $error = $@;
        my $error_message = $ERROR{$ERROR_SEND_NO_ACK};
        like $error->message, qr/$error_message/, "'send' FATAL error";

        Sub::Install::reinstall_sub(
            {
                code    => $original_send,
                into    => 'Kafka::IO',
                as      => 'send',
            }
        );
    }

    sub send_not_complete_messages_with_lost_connection {
        prepare_messages();
        $start_offset = next_offset();

        Sub::Install::reinstall_sub(
            {
                code    => sub {
                    my ( $self, $message ) = @_;

                    # simply truncate message
                    substr $message, -3, 3, q{};
                    my $ret  = $original_send->( $self, $message );
                    $self->close;
                    return $ret;
                },
                into    => 'Kafka::IO',
                as      => 'send',
            }
        );

        my $response;
        eval {
                $response = $producer->send(
                    $TOPIC,
                    $PARTITION,
                    \@transmitted_messages,
                );
        };
        my $error = $@;
        my $error_message = $ERROR{$ERROR_SEND_NO_ACK};
        like $error->message, qr/$error_message/, "'send' FATAL error";

        Sub::Install::reinstall_sub(
            {
                code    => $original_send,
                into    => 'Kafka::IO',
                as      => 'send',
            }
        );
    }

    sub fetching_no_messages {
        my $msgs_to_receive     = scalar @transmitted_messages;
        my $bytes_to_receive    = $msgs_to_receive * ( length( $transmitted_messages[0] ) + $MESSAGE_SIZE_OVERHEAD );

        my $messages;
        eval {
            $messages = $consumer->fetch(
                $TOPIC,
                $PARTITION,
                $start_offset,
                $bytes_to_receive,  # Maximum size of MESSAGE(s) to receive
            );
        };
        if ( $@ ) {
            fail "'fetch' FATAL error: $@";
            return;
        };

        my $i = 0;
        foreach my $message ( @$messages ) {
            if( $message->valid ) {
                is $message->payload, $transmitted_messages[ $i++ ], 'message ok';
            } else {
                fail 'message error: '.$message->error;
            }
        }
        is $i, 0, 'not all messages recorded';
    }
}

#-- Global data ----------------------------------------------------------------

# INSTRUCTIONS -----------------------------------------------------------------

# Demonstrate the following conclusions:
# - When the server receives the not complete request, the messages is not stored
# - NOTE: errors in the server logfiles are not seen

$cluster = Kafka::Cluster->new(
    kafka_dir           => $KAFKA_BASE_DIR,
);
isa_ok( $cluster, 'Kafka::Cluster' );

( $port )   =  $cluster->servers;

$connection = Kafka::Connection->new(
    host            => $HOST,
    port            => $port,
);
$producer = Kafka::Producer->new(
    Connection      => $connection,
    # For sure that all messages sent and recorded
    RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
);
$consumer = Kafka::Consumer->new(
    Connection  => $connection,
);

#- not receive a response to send messages (sending is successful, but no response is received)
send_without_response();
fetching_all_messages();

#- server received a not complete message (the connection is not lost)
send_not_complete_messages_without_lost_connection();   # not real situation
fetching_no_messages();

#- server received a not complete message (server not received the full message (connection is lost before receiving the response))
send_not_complete_messages_with_lost_connection();
fetching_no_messages();

#-- Closes and cleans up

$connection->close;

$cluster->close;

# POSTCONDITIONS ---------------------------------------------------------------
