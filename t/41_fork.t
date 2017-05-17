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

use Clone qw(
    clone
);
use Const::Fast;

use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    $RECEIVE_LATEST_OFFSETS
    $RETRY_BACKOFF
);
use Kafka::Cluster qw(
    $DEFAULT_TOPIC
);
use Kafka::Connection;
use Kafka::MockIO;
use Kafka::Consumer;
use Kafka::Producer;

STDOUT->autoflush;

my $cluster = Kafka::Cluster->new(
    replication_factor  => 1,
);
isa_ok( $cluster, 'Kafka::Cluster' );

my ( $connection, $topic, $partition, $producer, $response, $consumer, $is_ready, $pid, $ppid, $success, $etalon_messages, $starting_offset );

sub random_strings {
    my @chars               = ( " ", "A" .. "Z", "a" .. "z", 0 .. 9, qw(! @ $ % ^ & *) );
    my $msg_len             = 100;
    my $number_of_messages  = 500;

    note 'generation of messages can take a while';
    my ( @strings, $size );
    $strings[ $number_of_messages - 1 ] = undef;
    foreach my $i ( 0 .. ( $number_of_messages - 1 ) ) {
        my $len = int( rand( $msg_len ) ) + 1;
        $strings[ $i ] = join( q{}, @chars[ map { rand( scalar( @chars ) ) } ( 1 .. $len ) ] );
    }
    note 'generation of messages complited';
    return \@strings;
}

sub sending {
    my ( $messages ) = @_;

    my $response;
    eval {
        foreach my $message ( @$messages ) {
            undef $response;
            $response = $producer->send(
                $topic,
                $partition,
                $message
            );
        }
    };
    fail "sending error: $@" if $@;

    return $response;
}

sub next_offset {
    my ( $consumer, $topic, $partition ) = @_;

    my $offsets;
    eval {
        $offsets = $consumer->offsets(
            $topic,
            $partition,
            $RECEIVE_LATEST_OFFSETS,             # time
        );
    };
    fail "offsets are not received: $@" if $@;

    if ( $offsets ) {
        return $offsets->[0];
    } else {
        return;
    }
}

sub testing_sending {
    my $first_offset;

    return
        unless defined( $first_offset = next_offset( $consumer, $topic, $partition ) );
    return
        unless sending( $etalon_messages );

    ++$success;

    return $first_offset;
}

sub testing_fetching {
    my ( $first_offset ) = @_;

    my $messages;
    eval {
        $messages = $consumer->fetch( $topic, $partition, $first_offset );
    };
    fail "messages are not fetched: $@" if $@;

    if ( $messages ) {
        foreach my $i ( 0 .. $#$etalon_messages ) {
            my $message = $messages->[ $i ];
            return unless $message->valid && $message->payload eq $etalon_messages->[ $i++ ];
        }
    } else {
        return;
    }

    ++$success;

    return $messages;
}

sub wait_until_ready {
    my ( $level, $pid ) = @_;

    my $count = 0;
    while ( ( $is_ready // 0 ) != $level ) {
        if ( ++$count > 5 ) {
            kill 'KILL' => $pid;
            fail "too long a wait for $pid";
            last;
        }
        sleep 1;
    }
}

$SIG{USR1} = sub { ++$is_ready };
$SIG{USR2} = sub { ++$success };

$partition  = $Kafka::MockIO::PARTITION;
$topic      = $DEFAULT_TOPIC;

#-- Connecting to the Kafka server port (for example for node_id = 0)
my( $port ) =  $cluster->servers;

# connecting to the Kafka server port
$connection = Kafka::Connection->new(
    host            => 'localhost',
    port            => $port,
    RETRY_BACKOFF   => $RETRY_BACKOFF * 2,
    dont_load_supported_api_versions => 1,
);
$producer = Kafka::Producer->new(
    Connection      => $connection,
    # Ensure that all messages sent and recorded
    RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
);
$consumer = Kafka::Consumer->new(
    Connection  => $connection,
);

# simple sending
ok sending( [ 'single message' ] ), 'simple sending ok';

my $clone_connection = clone( $connection );
# the clients are destroyed
undef $producer;
is_deeply( $connection, $clone_connection, 'connection is not destroyed' );
undef $consumer;
is_deeply( $connection, $clone_connection, 'connection is not destroyed' );

# recreating clients
$producer = Kafka::Producer->new(
    Connection  => $connection,
    # Ensure that all messages sent and recorded
    RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
);
$consumer = Kafka::Consumer->new(
    Connection  => $connection,
);

$success = 0;

$etalon_messages = random_strings();
$starting_offset = testing_sending();
ok $success == 1, 'sending OK';
testing_fetching( $starting_offset );
ok $success == 2, 'fetching OK';

#-- the producer and connection are destroyed in the child

$is_ready = 0;

$etalon_messages = random_strings();
if ( $pid = fork ) {                # herein the parent
    $success = 0;

    # producer destroyed in the child
    $starting_offset = testing_sending();
    # $success == 1

    wait_until_ready( 1, $pid );    # expect readiness of the child process

    testing_fetching( $starting_offset );
    # $success == 2
    kill 'USR1' => $pid;

    wait_until_ready( 2, $pid );    # expect readiness of the child process

    # connection destroyed in the child
    $producer = Kafka::Producer->new(
        Connection  => $connection, # potentially destroyed connection
        # Ensure that all messages sent and recorded
        RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
    );
    $consumer = Kafka::Consumer->new(
        Connection  => $connection,
    );
    # $connection ok

    $etalon_messages = random_strings();
    $starting_offset = testing_sending();
    # $success == 3
    testing_fetching( $starting_offset );
    # $success == 4
    kill 'USR1' => $pid;

    wait;                           # forward to the completion of a child process
} elsif ( defined $pid ) {          # herein the child process
    $ppid = getppid();

    undef $producer;
    kill 'USR1' => $ppid;

    wait_until_ready( 1, $ppid );   # expect readiness of the parent process
    $connection->close;
    undef $connection;
    kill 'USR1' => $ppid;

    wait_until_ready( 2, $ppid );   # expect readiness of the parent process
    exit;
} else {
    fail 'An unexpected error (fork 1)';
}

ok $success == 4, 'Testing of destruction in the child';

#-- the producer and connection are destroyed in the parent

$is_ready = 0;

if ( $pid = fork ) {                # herein the parent
    $success = 0;

    wait_until_ready( 1, $pid );    # expect readiness of the child process
    undef $producer;
    kill 'USR1' => $pid;

    wait_until_ready( 2, $pid );    # expect readiness of the child process
    $connection->close;
    undef $connection;
    kill 'USR1' => $pid;

    wait;                           # forward to the completion of a child process
} elsif ( defined $pid ) {          # herein the child process
    $success = 0;
    $ppid = getppid();

    # producer is not destroyed in the parent
    $etalon_messages = random_strings();
    $starting_offset = testing_sending();
    # $success == 1
    testing_fetching( $starting_offset );
    # $success == 2

    $etalon_messages = random_strings();
    kill 'USR1' => $ppid;
    # producer destroyed in the parent
    $starting_offset = testing_sending();
    # $success == 3
    testing_fetching( $starting_offset );
    # $success == 4
    wait_until_ready( 1, $ppid );   # expect readiness of the parent process

    $etalon_messages = random_strings();
    kill 'USR1' => $ppid;
    wait_until_ready( 2, $ppid );   # expect readiness of the parent process

    # connection destroyed in the parent
    $producer = Kafka::Producer->new(
        Connection  => $connection, # potentially destroyed connection
        # Ensure that all messages sent and recorded
        RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
    );
    $consumer = Kafka::Consumer->new(
        Connection  => $connection,
    );
    # $connection ok

    $starting_offset = testing_sending();
    # $success == 5
    testing_fetching( $starting_offset );
    # $success == 6

    kill 'USR2' => $ppid if $success == 6;  # parent $success increment

    exit;
} else {
    fail 'An unexpected error (fork 2)';
}

ok $success == 1, 'Testing of destruction in the parent';

$cluster->close;

