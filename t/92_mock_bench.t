#!/usr/bin/perl -w

# Performance test

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
    eval 'use Test::Deep';          ## no critic
    plan skip_all => 'because Test::Deep required for testing' if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use Const::Fast;
#use Data::Dumper;
use Params::Util qw(
    _ARRAY0
    _HASH
);
use Time::HiRes qw(
    gettimeofday
);

use Kafka qw(
    $RECEIVE_LATEST_OFFSETS
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $ERROR_CANNOT_BIND
    $KAFKA_SERVER_PORT
    $MESSAGE_SIZE_OVERHEAD
    $REQUEST_TIMEOUT
    $NOT_SEND_ANY_RESPONSE
    $WAIT_WRITTEN_TO_LOCAL_LOG
    $BLOCK_UNTIL_IS_COMMITTED
);
use Kafka::Connection;
use Kafka::Producer;
use Kafka::Consumer;

use Kafka::MockIO;

const my $TOPIC             => 'mytopic';
# Use Kafka::MockIO only with the following information:
const my $PARTITION         => $Kafka::MockIO::PARTITION;

my (
    $connect,
    $producer,
    $consumer,
    $first_offset,
    $messages,
    $total,
    $fetch,
    $request_size,
    $delta,
    @copy,
    $in_single,
    $in_package,
    $number_of_package_mix,
    $number_of_package_ser,
);

my $timeout = $ENV{KAFKA_BENCHMARK_TIMEOUT} || $REQUEST_TIMEOUT;

my @chars               = ( " ", "A" .. "Z", "a" .. "z", 0 .. 9, qw(! @ $ % ^ & *) );
my $min_len             = $ENV{KAFKA_BENCHMARK_LEN_MIN} || 200;
my $max_len             = $ENV{KAFKA_BENCHMARK_LEN_MAX} || 200;
my $number_of_package   = $ENV{KAFKA_BENCHMARK_PACKAGE} || 5_000;
my $number_of_single    = $ENV{KAFKA_BENCHMARK_SINGLE}  || 5;
my $max_size            = $DEFAULT_MAX_BYTES * 512; # ~512 MB

my %bench;

#-- Connection

my $port = $KAFKA_SERVER_PORT;

Kafka::MockIO::override();

unless ( $connect = Kafka::Connection->new(
    host    => 'localhost',
    port    => $port,
    dont_load_supported_api_versions => 1,
    ) ) {
    BAIL_OUT 'connection is not created';
}
isa_ok( $connect, 'Kafka::Connection');

unless ( $producer = Kafka::Producer->new(
        Connection      => $connect,
        RequiredAcks    => $NOT_SEND_ANY_RESPONSE,
#        RequiredAcks    => $WAIT_WRITTEN_TO_LOCAL_LOG,
#        RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
    ) ) {
    BAIL_OUT 'producer is not created';
}
isa_ok( $producer, 'Kafka::Producer');

unless ( $consumer = Kafka::Consumer->new(
    Connection  => $connect,
) ) {
    BAIL_OUT 'consumer is not created';
}
isa_ok( $consumer, 'Kafka::Consumer');

sub next_offset {
    my ( $consumer, $topic, $partition, $is_package ) = @_;

    my $offsets = $consumer->offsets(
        $topic,
        $partition,
        $RECEIVE_LATEST_OFFSETS,             # time
        $DEFAULT_MAX_NUMBER_OF_OFFSETS,     # max_number
    );
    if ( $offsets ) {
        ok( _ARRAY0( $offsets ), 'offsets are obtained' ) if $is_package;
        return $offsets->[0];
    }
    if ( !$offsets ) {
        fail 'offsets are not received';
        return;
    }
}

sub send_messages {
    my ( $producer, $topic, $partition, $messages ) = @_;

    my ( $time_before, $time_after );
    $time_before = gettimeofday;
    my $response = $producer->send( $topic, $partition, $messages );
    $time_after = gettimeofday;
    if ( $response ) {
        ok( _HASH( $response ), 'sent a series of messages' ) if $#{ $messages };
        return $time_after - $time_before;
    } else {
        fail 'response is not received';
        return;
    }
}

sub fetch_messages {
    my ( $consumer, $topic, $partition, $offset, $max_size, $is_package ) = @_;

    my ( $time_before, $time_after );
    $time_before = gettimeofday;
    my $messages = $consumer->fetch( $topic, $partition, $offset, $max_size );
    $time_after = gettimeofday;
    if ( $messages ) {
        ok( _ARRAY0( $messages ), 'messages are received' ) if $is_package;
        my @fetch;
        my $cnt = 0;
        foreach my $m ( @$messages ) {
            push @fetch, $m->payload;
            unless ( $m->valid ) {
                diag "Message No $cnt, Error: ", $m->error;
#                diag 'Payload    : ', length( $m->payload ) > 100 ? substr( $m->payload, 0, 100 ).'...' : $m->payload;
                diag 'Payload    : ', $m->payload;
                diag 'offset     : ', $m->offset;
                diag 'next_offset: ', $m->next_offset;
                fail 'received not valid message';
            }
            ++$cnt;
        }
        return ( \@fetch, $time_after - $time_before );
    }
    if ( !$messages ) {
        fail 'messages are not received';
        return;
    }
}

sub random_strings {
    my ( $chars, $min_len, $max_len, $number_of ) = @_;

    note 'generation of messages can take a while';
    my ( @strings, $size );
    $strings[ $number_of - 1 ] = undef;
    my $delta = $max_len - $min_len + 1;
    foreach my $i ( 0 .. ( $number_of - 1 ) ) {
        my $len = $delta ? int( rand( $delta ) ) + $min_len : $min_len;
        $strings[ $i ] = join( q{}, @chars[ map { rand @$chars } ( 1 .. $len ) ] );
        $size += $len;
    }
    note 'generation of messages complited';
    return \@strings, $size;
}

sub report {
#    note 'Legend:';
    note "Message length: $min_len .. $max_len";
    note "Messages      : package - $number_of_package, single - $number_of_single";
    note "IO timeout    : $timeout";

    my @indicators = qw(
        send_package
        send_single
        send_mix
        fetch_package
        fetch_single
        fetch_mix
        fetch_inseries
    );

    note 'Total:';
    foreach my $k ( @indicators ) {
        note sprintf( '%-14s ', $k ), sprintf( '%.4f', $bench{ $k } );
    }

    foreach my $result ( (
        $bench{send_package},
        $bench{fetch_package}
        ) ) {
        $result /= $number_of_package;
    }

    foreach my $result ( (
        $bench{send_mix},
        $bench{fetch_mix}
        ) ) {
        $result /= $number_of_package_mix;
    }

    foreach my $result ( (
        $bench{fetch_inseries}
        ) ) {
        $result /= $number_of_package_ser;
    }

    foreach my $result ( (
        $bench{send_single},
        $bench{fetch_single}
        ) ) {
        $result /= $number_of_single;
    }

    note 'Seconds per message:';
    foreach my $k ( @indicators ) {
        note sprintf( '%-14s ', $k ), sprintf( '%.4f', $bench{ $k } );
    }

    note 'Messages per second:';
    foreach my $k ( @indicators ) {
        note sprintf( '%-14s ', $k ), $bench{ $k } ? sprintf( '%4d', int( 1 / $bench{ $k } ) ) : 'N/A';
    }
}



$in_package = $number_of_package;

#-- Package
( $messages, $total ) = random_strings( \@chars, $min_len, $max_len, $in_package );
@copy = (); push @copy, @$messages;
# To calculate the traffic in the protocol 0.8 should consider the size of the encoded sendings.
# Consider only the size of the data.
$request_size = $total;

$fetch = [];
$bench{fetch_package} = $bench{send_package} = 0;

# to wait for forcing a flush of previous data to disk
$first_offset = next_offset( $consumer, $TOPIC, $PARTITION, 1 );
while (1) {
    sleep 1;
    if ( $first_offset != next_offset( $consumer, $TOPIC, $PARTITION, 1 ) ) {
        note 'to wait for forcing a flush of previous data to disk';
        $first_offset = next_offset( $consumer, $TOPIC, $PARTITION, 1 );
    } else {
        last;
    }
}

while (1) {
    note "PRODUCE Request transfer size $request_size bytes, please wait...";
    $first_offset = next_offset( $consumer, $TOPIC, $PARTITION, 1 );
    $bench{send_package} = $bench{send_package}
        + send_messages( $producer, $TOPIC, $PARTITION, $messages );
    note 'PRODUCE Request transmitted';

    note 'waiting for messages to get ready...';
    1 while next_offset( $consumer, $TOPIC, $PARTITION ) < $first_offset + $in_package;

    my ( $fetched, $to_bench );

    note 'trying to get FETCH Response to all messages, please wait...';
    ( $fetched, $to_bench ) = fetch_messages( $consumer, $TOPIC, $PARTITION, $first_offset, $max_size, 1 );
    $bench{fetch_package} += $to_bench;
    push @$fetch, @$fetched;

    last if $bench{send_package} and $bench{fetch_package};

    $number_of_package += $in_package;
    push @$messages, @copy;
}

cmp_deeply( $fetch, $messages, 'all messages are received correctly' );

#-- Single message
( $messages, $total ) = random_strings( \@chars, $min_len, $max_len, $number_of_single );
@copy = (); push @copy, @$messages;

$fetch = [];
$bench{fetch_single} = $bench{send_single} = 0;

$in_single = $number_of_single;
while (1)
{
    note 'message processing one by one, please wait...';
    foreach my $idx ( 0 .. ( $in_single - 1 ) ) {
        $first_offset = next_offset( $consumer, $TOPIC, $PARTITION );

        $bench{send_single} += send_messages( $producer, $TOPIC, $PARTITION, [ $copy[ $idx ] ] );

        my ( $fetched, $to_bench );

        1 while next_offset( $consumer, $TOPIC, $PARTITION ) == $first_offset;

        ( $fetched, $to_bench ) = fetch_messages( $consumer, $TOPIC, $PARTITION, $first_offset, $max_size );
        push @$fetch, $$fetched[0];
        $bench{fetch_single} += $to_bench;
    }

    last if $bench{send_single} and $bench{fetch_single};

    $number_of_single += $in_single;
    push @$messages, @copy;
}
cmp_deeply( $fetch, $messages, 'all messages are processed correctly' );

#-- Mix
$number_of_package_mix = $in_package;
( $messages, $total ) = random_strings( \@chars, $min_len, $max_len, $in_package );
@copy = (); push @copy, @$messages;
# Consider only the size of the data.
$request_size = $total;

$fetch = [];
$bench{fetch_mix} = $bench{send_mix} = 0;

while (1) {
    note 'message sending one by one, please wait...';
    $first_offset = next_offset( $consumer, $TOPIC, $PARTITION, 1 );

    foreach my $idx ( 0 .. ( $in_package - 1 ) ) {
        diag( sprintf( "Error: copy ne message: idx = %d, copy = '%s', message = '%s'", $idx, $copy[ $idx ] ne $messages->[ $idx ] ) )
            if $copy[ $idx ] ne $messages->[ $idx ];
        $bench{send_mix} += send_messages( $producer, $TOPIC, $PARTITION, [ $copy[ $idx ] ] );
    }

    note 'waiting for messages to get ready...';
    1 while next_offset( $consumer, $TOPIC, $PARTITION ) < $first_offset + $in_package;

    my ( $fetched, $to_bench );

    note 'trying to get FETCH Response to all messages, please wait...';
    ( $fetched, $to_bench ) = fetch_messages( $consumer, $TOPIC, $PARTITION, $first_offset, $max_size, 1 );
    $bench{fetch_mix} += $to_bench;
    push @$fetch, @$fetched;

    last if $bench{send_mix} and $bench{fetch_mix};

    $number_of_package_mix += $in_package;
    push @$messages, @copy;
}

cmp_deeply( $fetch, $messages, 'all messages are received correctly' );

#-- Consuming messages one by one
# Uses Mix section data
$number_of_package_ser = $in_package;

$fetch = [];
$bench{fetch_inseries} = 0;

while (1) {
    note 'trying to get FETCH Response to all messages one by one, please wait...';
    $delta = 0;
    foreach my $idx ( 0 .. ( $in_package - 1 ) ) {
        my ( $fetched, $to_bench );

        ( $fetched, $to_bench ) = fetch_messages(
            $consumer,
            $TOPIC,
            $PARTITION,
            $first_offset + $delta,
            length( $$messages[ $idx ] ) + $MESSAGE_SIZE_OVERHEAD,
            );
        ++$delta;

        push @$fetch, $$fetched[0];
        $bench{fetch_inseries} += $to_bench;
    }

    last if $bench{fetch_inseries};

    $number_of_package_ser += $in_package;
    push @$messages, @copy;
}

cmp_deeply( $fetch, $messages, 'all messages are received correctly' );



# Closes and cleans up
undef $producer;
ok( !$producer, 'the producer object is an empty' );
undef $consumer;
ok( !$consumer, 'the consumer object is an empty' );

$connect->close;

# Statistics
report();

