#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Performance test

use lib 'lib';
use bytes;
use Params::Util qw( _ARRAY0 );
use Benchmark qw( timediff timesum timestr );

use Test::More;
plan "no_plan";

BEGIN {
    eval "use Test::Deep";
    plan skip_all => "because Test::Deep required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

#-- verify load the module
BEGIN { use_ok 'Kafka', qw(
    KAFKA_SERVER_PORT
    DEFAULT_TIMEOUT
    TIMESTAMP_LATEST
    TIMESTAMP_EARLIEST
    DEFAULT_MAX_OFFSETS
    DEFAULT_MAX_SIZE
    ERROR_CANNOT_BIND
    ) }
BEGIN { use_ok 'Kafka::IO' }
BEGIN { use_ok 'Kafka::Producer' }
BEGIN { use_ok 'Kafka::Consumer' }

#-- declaration of variables to test
my (
    $io,
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

my $topic = <DATA>;
chomp $topic;
$topic ||= "test";

my $partition           = 0;
my $timeout             = $ENV{KAFKA_BENCHMARK_TIMEOUT} || DEFAULT_TIMEOUT;

my @chars = ( " ", "A" .. "Z", "a" .. "z", 0 .. 9, qw(! @ $ % ^ & *) );
my $min_len             = $ENV{KAFKA_BENCHMARK_LEN_MIN} || 200;
my $max_len             = $ENV{KAFKA_BENCHMARK_LEN_MAX} || 200;
my $number_of_package   = $ENV{KAFKA_BENCHMARK_PACKAGE} || 5_000;
my $number_of_single    = $ENV{KAFKA_BENCHMARK_SINGLE}  || 5;
my $max_size            = DEFAULT_MAX_SIZE * 512;   # ~512 MB

my %bench = ();

#-- setting up facilities
unless ( $io = Kafka::IO->new(
    host    => "localhost",
    timeout => $timeout,
    ) )
{
    fail "(".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error();
    exit 1;
}
isa_ok( $io, 'Kafka::IO');

unless ( $producer = Kafka::Producer->new( IO => $io ) )
{
    fail "(".Kafka::Producer::last_errorcode().") ".Kafka::Producer::last_error();
    exit 1;
}
isa_ok( $producer, 'Kafka::Producer');

unless ( $consumer = Kafka::Consumer->new( IO => $io ) )
{
    fail "(".Kafka::Consumer::last_errorcode().") ".Kafka::Consumer::last_error();
    exit 1;
}
isa_ok( $consumer, 'Kafka::Consumer');

#-- definition of the functions

sub next_offset {
    my $consumer    = shift;
    my $topic       = shift;
    my $partition   = shift;
    my $is_package  = shift;

    my $offsets = $consumer->offsets(
        $topic,
        $partition,
        TIMESTAMP_LATEST,                           # time
        DEFAULT_MAX_OFFSETS                         # max_number
        );
    if( $offsets )
    {
        ok( defined( _ARRAY0( $offsets ) ), "offsets are obtained" ) if $is_package;
        return $$offsets[0];
    }
    if ( !$offsets or $consumer->last_error )
    {
        fail "(".$consumer->last_errorcode.") ".$consumer->last_error;
        return;
    }
}

sub send_messages {
    my $producer    = shift;
    my $topic       = shift;
    my $partition   = shift;
    my $messages    = shift;

    my ( $timestamp1, $timestamp2 );
    $timestamp1 = new Benchmark;
    my $ret = $producer->send( $topic, $partition, $messages );
    $timestamp2 = new Benchmark;
    if ( $ret )
    {
        is( $ret, 1, "sent a series of messages" ) if $#{$messages};
        return timediff( $timestamp2, $timestamp1 );
    }
    else
    {
        fail "(".$producer->last_errorcode.") ".$producer->last_error;
        return;
    }
}

sub fetch_messages {
    my $consumer    = shift;
    my $topic       = shift;
    my $partition   = shift;
    my $offset      = shift;
    my $max_size    = shift;
    my $is_package  = shift;

    my ( $timestamp1, $timestamp2 );
    $timestamp1 = new Benchmark;
    my $messages = $consumer->fetch( $topic, $partition, $offset, $max_size );
    $timestamp2 = new Benchmark;
    if ( $messages )
    {
        ok( defined( _ARRAY0( $messages ) ), "messages are received" ) if $is_package;
        my @fetch;
        my $cnt = 0;
        foreach my $m ( @$messages )
        {
            push @fetch, $m->payload;
            unless ( $m->valid )
            {
                diag "Message No $cnt, Error: ", $m->error;
                diag "Payload    : ", bytes::length( $m->payload ) > 100 ? substr( $m->payload, 0, 100 )."..." : $m->payload;
                diag "offset     : ", $m->offset;
                diag "next_offset: ", $m->next_offset;
            }
            ++$cnt;
        }
        return ( \@fetch, timediff( $timestamp2, $timestamp1 ) );
    }
    if ( !$messages or $consumer->last_error )
    {
        fail "(".$consumer->last_errorcode.") ".$consumer->last_error;
        return;
    }
}

sub random_strings {
    my $chars       = shift;
    my $min_len     = shift;
    my $max_len     = shift;
    my $number_of   = shift;

    note "generation of messages can take a while";
    my ( @strings, $size );
    $strings[ $number_of - 1 ] = undef;
    my $delta = $max_len - $min_len + 1;
    foreach my $i ( 0 .. ( $number_of - 1 ) )
    {
        my $len = $delta ? int( rand( $delta ) ) + $min_len : $min_len;
        $strings[ $i ] = join( "", @chars[ map { rand @$chars } ( 1 .. $len ) ]);
        $size += $len;
    }
    note "generation of messages complited";
    return \@strings, $size;
}

sub report {
    diag "Legend:";
    diag "Message length: $min_len .. $max_len";
    diag "Messages      : package - $number_of_package, single - $number_of_single";
    diag "IO timeout    : $timeout";

    diag "Total:";
    foreach my $k ( qw( send_package send_single send_mix fetch_package fetch_single fetch_mix fetch_inseries ) )
    {
        diag sprintf( "%-14s ", $k ), timestr( $bench{ $k }, "noc", ".4f" );
    }

    foreach my $result ( (
        $bench{send_package}, $bench{fetch_package}
        ) )
    {
        for ( my $i = 0; $i < @$result; ++$i )
        {
            ${$result}[ $i ] /= $number_of_package;
        }
    }

    foreach my $result ( (
        $bench{send_mix}, $bench{fetch_mix}
        ) )
    {
        for ( my $i = 0; $i < @$result; ++$i )
        {
            ${$result}[ $i ] /= $number_of_package_mix;
        }
    }

    foreach my $result ( (
        $bench{fetch_inseries}
        ) )
    {
        for ( my $i = 0; $i < @$result; ++$i )
        {
            ${$result}[ $i ] /= $number_of_package_ser;
        }
    }

    foreach my $result ( ( $bench{send_single}, $bench{fetch_single} ) )
    {
        for ( my $i = 0; $i < @$result; ++$i )
        {
            ${$result}[ $i ] /= $number_of_single;
        }
    }

    diag "Seconds per message:";
    foreach my $k ( qw( send_package send_single send_mix fetch_package fetch_single fetch_mix fetch_inseries ) )
    {
        diag sprintf( "%-14s ", $k ), timestr( $bench{ $k }, "noc", ".4f" );
    }

    diag "Messages per second:";
    foreach my $k ( qw( send_package send_single send_mix fetch_package fetch_single fetch_mix fetch_inseries ) )
    {
        my( undef, $pu, $ps, undef, undef, undef ) = @{$bench{ $k }};
        diag sprintf( "%-14s ", $k ), ( $pu + $ps ) ? sprintf( "%4d", int( 1 / ( $pu + $ps ) ) ) : "N/A";
    }
}

# INSTRUCTIONS -----------------------------------------------------------------

$in_package = $number_of_package;

#-- Package
( $messages, $total ) = random_strings( \@chars, $min_len, $max_len, $in_package );
@copy = (); push @copy, @$messages;
$request_size = $in_package * 9 + $total;

$fetch = [];
$bench{send_package} = new Benchmark;
$bench{fetch_package} = $bench{send_package} = timediff( $bench{send_package}, $bench{send_package} );

while (1)
{
    note "PRODUCE Request transfer size $request_size bytes, please wait...";
    $first_offset = next_offset( $consumer, $topic, $partition, 1 );
    $bench{send_package} = timesum( $bench{send_package},
        send_messages( $producer, $topic, $partition, $messages ) );
    note "PRODUCE Request transmitted";

    note "waiting for messages to get ready...";
    1 while next_offset( $consumer, $topic, $partition ) < $first_offset + $request_size;

    my ( $fetched, $to_bench );

    note "trying to get FETCH Response to all messages, please wait...";
    ( $fetched, $to_bench ) = fetch_messages( $consumer, $topic, $partition, $first_offset, $max_size, 1 );
    $bench{fetch_package} = timesum( $bench{fetch_package}, $to_bench );
    push @$fetch, @$fetched;

    my( undef, $pu_send,  $ps_send,  undef, undef, undef ) = @{$bench{send_package}};
    my( undef, $pu_fetch, $ps_fetch, undef, undef, undef ) = @{$bench{fetch_package}};

    last if ( $pu_send + $ps_send ) and ( $pu_fetch + $ps_fetch );

    $number_of_package += $in_package;
    push @$messages, @copy;
}

cmp_deeply $fetch, $messages, "all messages are received correctly";

#-- Single message
( $messages, $total ) = random_strings( \@chars, $min_len, $max_len, $number_of_single );
@copy = (); push @copy, @$messages;

$fetch = [];
$bench{send_single} = new Benchmark;
$bench{fetch_single} = $bench{send_single} = timediff( $bench{send_single}, $bench{send_single} );

$in_single = $number_of_single;
while (1)
{
    note "message processing one by one, please wait...";
    foreach my $idx ( 0 .. ( $in_single - 1 ) )
    {
        $first_offset = next_offset( $consumer, $topic, $partition );

        $bench{send_single} = timesum( $bench{send_single},
            send_messages( $producer, $topic, $partition, [ $copy[ $idx ] ] )
            );

        my ( $fetched, $to_bench );

        1 while next_offset( $consumer, $topic, $partition ) == $first_offset;

        ( $fetched, $to_bench ) = fetch_messages( $consumer, $topic, $partition, $first_offset, $max_size );
        push @$fetch, $$fetched[0];
        $bench{fetch_single} = timesum( $bench{fetch_single}, $to_bench );
    }

    my( undef, $pu_send,  $ps_send,  undef, undef, undef ) = @{$bench{send_single}};
    my( undef, $pu_fetch, $ps_fetch, undef, undef, undef ) = @{$bench{fetch_single}};

    last if ( $pu_send + $ps_send ) and ( $pu_fetch + $ps_fetch );

    $number_of_single += $in_single;
    push @$messages, @copy;
}
cmp_deeply $fetch, $messages, "all messages are processed correctly";

#-- Mix
$number_of_package_mix = $in_package;
( $messages, $total ) = random_strings( \@chars, $min_len, $max_len, $in_package );
@copy = (); push @copy, @$messages;
$request_size = $in_package * 9 + $total;

$fetch = [];
$bench{send_mix} = new Benchmark;
$bench{fetch_mix} = $bench{send_mix} = timediff( $bench{send_mix}, $bench{send_mix} );

while (1)
{
    note "message sending one by one, please wait...";
    $first_offset = next_offset( $consumer, $topic, $partition, 1 );

    foreach my $idx ( 0 .. ( $in_package - 1 ) )
    {
        $bench{send_mix} = timesum( $bench{send_mix},
            send_messages( $producer, $topic, $partition, [ $copy[ $idx ] ] )
            );
    }

    note "waiting for messages to get ready...";
    1 while next_offset( $consumer, $topic, $partition ) < $first_offset + $request_size;

    my ( $fetched, $to_bench );

    note "trying to get FETCH Response to all messages, please wait...";
    ( $fetched, $to_bench ) = fetch_messages( $consumer, $topic, $partition, $first_offset, $max_size, 1 );
    $bench{fetch_mix} = timesum( $bench{fetch_mix}, $to_bench );
    push @$fetch, @$fetched;

    my( undef, $pu_send,  $ps_send,  undef, undef, undef ) = @{$bench{send_mix}};
    my( undef, $pu_fetch, $ps_fetch, undef, undef, undef ) = @{$bench{fetch_mix}};

    last if ( $pu_send + $ps_send ) and ( $pu_fetch + $ps_fetch );

    $number_of_package_mix += $in_package;
    push @$messages, @copy;
}

cmp_deeply $fetch, $messages, "all messages are received correctly";

#-- Consuming messages one by one
# Uses Mix section data
$number_of_package_ser = $in_package;

$fetch = [];
$bench{fetch_inseries} = new Benchmark;
$bench{fetch_inseries} = timediff( $bench{fetch_inseries}, $bench{fetch_inseries} );

while (1)
{
    note "trying to get FETCH Response to all messages one by one, please wait...";
    $delta = 0;
    foreach my $idx ( 0 .. ( $in_package - 1 ) )
    {
        my ( $fetched, $to_bench );

        ( $fetched, $to_bench ) = fetch_messages(
            $consumer,
            $topic,
            $partition,
            $first_offset + $delta,
            9 + bytes::length( $$messages[ $idx ] )
            );
        $delta += 9 + bytes::length( $$messages[ $idx ] );

        push @$fetch, $$fetched[0];
        $bench{fetch_inseries} = timesum( $bench{fetch_inseries}, $to_bench );
    }

    my( undef, $pu_fetch, $ps_fetch, undef, undef, undef ) = @{$bench{fetch_inseries}};

    last if ( $pu_fetch + $ps_fetch );

    $number_of_package_ser += $in_package;
    push @$messages, @copy;
}

cmp_deeply $fetch, $messages, "all messages are received correctly";

# POSTCONDITIONS ---------------------------------------------------------------

# Closes and cleans up
$producer->close;
ok( scalar( keys %$producer ) == 0, "the producer object is an empty" );
$consumer->close;
ok( scalar( keys %$consumer ) == 0, "the consumer object is an empty" );

# Statistics
report();

# DO NOT REMOVE THE FOLLOWING LINES
__DATA__
test