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

use Const::Fast;
use Data::Dumper;
use List::Util qw(
    min
    shuffle
);
use POSIX ':sys_wait_h';
use Time::HiRes ();
use Try::Tiny;

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    $MESSAGE_SIZE_OVERHEAD
    $RECEIVE_LATEST_OFFSETS
    $RETRY_BACKOFF
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::Internals qw(
    $MAX_CORRELATIONID
);
use Kafka::MockIO;
use Kafka::Producer;

STDOUT->autoflush;

# Restrictions:
# All pairs clients work with a common topic
# Each pair of clients (Producer + Consumer) works with a separate partition
# All messages have the same length

const my $PAIRS_CLIENTS         => 10;
const my $MAX_DATA              => 20 * 1024 *1024; # MBs
const my $MAX_DATA_RECORS       => 1_000_000;
const my $MSG_LEN               => int( $MAX_DATA / $MAX_DATA_RECORS );
const my $MAX_MSGS_SENT         => min( 10_000, int( $MAX_CORRELATIONID / ( $MSG_LEN + $MESSAGE_SIZE_OVERHEAD ) ) ),
const my $MAX_MSGS_RECV         => min( 10_000, int( $MAX_CORRELATIONID / ( $MSG_LEN + $MESSAGE_SIZE_OVERHEAD ) ) ),
const my $CLIENT_MSGS           => int( $MAX_DATA_RECORS / $PAIRS_CLIENTS );
const my $SECS_TO_WAIT          => 600;

const my $AUTO_CREATE_TOPICS    => 'true';
const my $REPLICATION_FACTOR    => 3;

const my $KAFKA_BASE_DIR        => $ENV{KAFKA_BASE_DIR};    # WARNING: must match the settings of your system
const my $TOPIC                 => $Kafka::MockIO::TOPIC;
const my $HOST                  => 'localhost';

my ( @msg_pool, $cluster, $port, @clients, $pid, $ppid, $connection, $producer, $consumer, $is_ready );

$SIG{USR1} = sub { ++$is_ready };

sub create_msg_pool {
    my @chars               = ( " ", "A" .. "Z", "a" .. "z", 0 .. 9, qw(! @ $ % ^ & *) );

    note 'generation of messages can take a while';
    $msg_pool[ $MAX_MSGS_SENT - 1 ] = undef;
    foreach my $i ( 0 .. ( $MAX_MSGS_SENT - 1 ) ) {
        $msg_pool[ $i ] = join( q{}, @chars[ map { rand( scalar( @chars ) ) } ( 1 .. $MSG_LEN ) ] );
    }
    note 'generation of messages complited';
}

sub sending {
    my ( $partition ) = @_;

    my $msgs_to_send    = int( rand( $MAX_MSGS_SENT ) + 1 );
    my $first_msg_idx   = int( rand( $MAX_MSGS_SENT - $msgs_to_send ) );
    my $last_msg_idx    = $first_msg_idx + $msgs_to_send - 1;
    my $messages        = [ @msg_pool[ $first_msg_idx .. $last_msg_idx ] ];

    eval {
        $producer->send(
            $TOPIC,
            $partition,
            $messages,
        );
    };
    if ( $@ ) {
        diag "'send' FATAL error: $@";
        return 0;
    } else {
        return $msgs_to_send;
    }
}

sub next_offset {
    my ( $partition ) = @_;

    my $offsets;
    eval {
        $offsets = $consumer->offsets(
            $TOPIC,
            $partition,
            $RECEIVE_LATEST_OFFSETS,             # time
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

sub fetching {
    my ( $partition ) = @_;

    my $next_offset = next_offset( $partition );
    return 0 unless $next_offset;

    my $msgs_to_receive     = min( $next_offset, int( rand( $MAX_MSGS_RECV ) + 1 ) );
    my $start_offset        = int( rand( $next_offset - $msgs_to_receive ) );
    my $bytes_to_receive    = $msgs_to_receive * ( $MSG_LEN + $MESSAGE_SIZE_OVERHEAD );

    Time::HiRes::sleep( 1.5 );
    my $error;
    try {
        $consumer->fetch(
            $TOPIC,
            $partition,
            $start_offset,
            $bytes_to_receive,  # Maximum size of MESSAGE(s) to receive
        );
    } catch {
        $error = $_;
    };
    if ( $error ) {
        fail "'fetch' FATAL error: $error";
        return;
    } else {
        return $msgs_to_receive;
    }
}

sub is_died {
    my ( $pid ) = @_;

    my $rc = waitpid( $pid, &WNOHANG );
    return( ( $rc == 0 ) ? 0 : 1 );
}

sub wait_child_ready {
    my $secs_to_wait = $SECS_TO_WAIT;

    my $all_alive;
    my $alive_pid;
    while ( $secs_to_wait-- ) {
        $all_alive = 1;
        foreach my $pid ( @clients ) {
            if ( is_died( $pid ) ) {
                $all_alive = 0;
                $alive_pid = $pid;
                last;
            }
        }
        last if $all_alive;
        sleep 1;
    }
    unless ( $all_alive ) {
        kill 'KILL' => @clients;
        fail "too long a wait for ready $alive_pid, waitpid = ".waitpid( $alive_pid, &WNOHANG );
    }

    return $all_alive;
}

sub wait_parent_ready {
    my $secs_to_wait = $SECS_TO_WAIT;
    my $ok = 1;
    until ( $is_ready ) {
        unless ( $secs_to_wait-- ) {
            fail 'too long a wait for a parent';
            $ok = 0;
            last;
        }
        sleep 1;
    }
    return $ok;
}

sub wait_child_success {
    my $secs_to_wait = $SECS_TO_WAIT;
    my $all_died;
    my $alive_pid;
    while ( $secs_to_wait-- ) {
        $all_died = 1;
        foreach my $pid ( @clients ) {
            unless ( is_died( $pid ) ) {
                $all_died = 0;
                $alive_pid = $pid;
                pass 'There are clients working';
                last;
            }
        }
        last if $all_died;
        sleep 1;
    }
    unless ( $all_died ) {
        kill 'KILL' => @clients;
        fail "too long a wait for success $alive_pid, waitpid = ".waitpid( $alive_pid, &WNOHANG );
    }
    return $all_died;
}

sub note_nonfatals {
    my $arr_ref = $connection->nonfatal_errors();

    if ( @$arr_ref ) {
        note "Non-fatal errors:";
    }

    foreach my $txt ( @$arr_ref ) {
        note "\t$txt";
    }
}

sub create_client {
    my ( $client_type, $partition ) = @_;

    if ( $pid = fork ) {                # herein the parent
        return $pid;
    } elsif ( defined $pid ) {          # herein the child process
        $is_ready = 0;
        $ppid = getppid();
        note "Started '$client_type', pid = $$, partition = $partition";

        $connection = Kafka::Connection->new(
            host                    => $HOST,
            port                    => $port,
            timeout                 => 30,
            AutoCreateTopicsEnable  => 1,
            RETRY_BACKOFF           => $RETRY_BACKOFF * 2,
            dont_load_supported_api_versions => 1,
        );

        if ( $client_type eq 'producer' ) {
            $producer = Kafka::Producer->new(
                Connection  => $connection,
                # Require verification the number of messages sent and recorded
                RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
                Timeout         => 30,
            );
        } elsif ( $client_type eq 'consumer' ) {
            $consumer = Kafka::Consumer->new(
                Connection  => $connection,
                MaxWaitTime => 30,
            );
        } else {
            # nothing to do now
        }

        my $count = 0;
        if ( wait_parent_ready() ) {
            if ( $client_type eq 'producer' ) {
                while ( $count < $CLIENT_MSGS ) {
                    $count += sending( $partition );
                }
            } elsif ( $client_type eq 'consumer' ) {
                while ( $count < $CLIENT_MSGS ) {
                    $count += fetching( $partition );
                }
            } else {
                # nothing to do now
            }
        }

        note "Finished '$client_type', pid = $$, partition = $partition";
        note_nonfatals();

        exit;
    } else {
        fail 'An unexpected error (fork 1)';
    }
}

create_msg_pool();

ok defined( Kafka::Cluster::data_cleanup() ), 'data directory cleaned';

$cluster = Kafka::Cluster->new(
    replication_factor  => $REPLICATION_FACTOR,
    partition           => $PAIRS_CLIENTS,
    properties          => {
        'auto.create.topics.enable' => $AUTO_CREATE_TOPICS,
    },
);
isa_ok( $cluster, 'Kafka::Cluster' );

#-- Connecting to the Kafka server port (for example for node_id = 0)
( $port ) = $cluster->servers;

foreach my $i ( 1 .. $PAIRS_CLIENTS ) {
    foreach my $client_type ( 'producer', 'consumer' ) {
        my $pid = create_client( $client_type, $i - 1 );
        ok $pid, "client started: $pid";
        push @clients, $pid;    # partitions are 0 based
    }
}

if ( wait_child_ready() ) {
    sleep 1;
    kill 'USR1' => @clients;
}
wait_child_success();

$cluster->close;

ok defined( Kafka::Cluster::data_cleanup() ), 'data directory cleaned';

#-- Inside a process competition

$cluster = Kafka::Cluster->new(
    replication_factor  => $REPLICATION_FACTOR,
    partition           => $PAIRS_CLIENTS,
    properties          => {
        'auto.create.topics.enable' => $AUTO_CREATE_TOPICS,
    },
);
isa_ok( $cluster, 'Kafka::Cluster' );
#-- Connecting to the Kafka server port (for example for node_id = 0)
( $port ) = $cluster->servers;

$connection = Kafka::Connection->new(
    host                    => $HOST,
    port                    => $port,
    AutoCreateTopicsEnable  => 1,
    RETRY_BACKOFF           => $RETRY_BACKOFF * 2,
    dont_load_supported_api_versions => 1,
);
$producer = Kafka::Producer->new(
    Connection      => $connection,
    # Require verification the number of messages sent and recorded
    RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
);
$consumer = Kafka::Consumer->new(
    Connection      => $connection,
);

@clients = ();

foreach my $i ( 1 .. $PAIRS_CLIENTS ) {
    foreach my $client_type ( 'producer', 'consumer' ) {
        push @clients,
            {
                client_type => $client_type,
                partition   => $i - 1,          # partitions are 0 based
                count       => 0,
            }
        ;
    }
}

@clients = shuffle @clients;

my $tm = time;
my $nonfatals = 0;
while ( scalar @clients ) {
    my $i           = int( rand scalar @clients );
    my $client_type = $clients[ $i ]->{client_type};
    my $partition   = $clients[ $i ]->{partition};
    my $count       = $clients[ $i ]->{count};

    if ( $count == 0 ) {
        note "Started '$client_type', partition = $partition";
    }

    if ( $client_type eq 'producer' ) {
        $clients[ $i ]->{count} += sending( $partition );
    } else {    # consumer
        $clients[ $i ]->{count} += fetching( $partition );
    }
    $count = $clients[ $i ]->{count};

    if ( ( my $new_nonfatals = $connection->nonfatal_errors() ) != $nonfatals ) {
        note "New non-fatal errors fixed: '$client_type', partition = $partition";
        note_nonfatals();
        $nonfatals = $new_nonfatals;
    }

    if ( $count > $CLIENT_MSGS ) {
        note "Finished '$client_type', partition = $partition";

        #-- total
        if ( $client_type eq 'producer' ) {
            is $count, next_offset( $partition ), "total number of recorded messages matches the number of messages sent ($count)";
        }

        splice( @clients, $i, 1 );
    }

    if ( $tm < time ) {
        pass 'There are clients working';
        $tm = time;
    }
}
note_nonfatals();

$cluster->close;
