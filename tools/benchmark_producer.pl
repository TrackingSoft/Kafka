#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Producer test

use lib 'lib';
use bytes;
use Time::HiRes     qw( gettimeofday );
use Getopt::Long;

# PRECONDITIONS ----------------------------------------------------------------

#-- verify load the module
use Kafka qw(
    DEFAULT_TIMEOUT
    );
use Kafka::IO;
use Kafka::Producer;

#-- declaration of variables to test
my (
    $io,
    $producer,
    $messages,
    $total,
    $request_size,
    $delta,
    @copy,
    $in_package,
    $number_of_package_mix,
    @tmp_bench,
    );

my $host                = "localhost",
my $topic               = undef;
my $partitions          = undef;
my $timeout             = DEFAULT_TIMEOUT;

my @chars = ( " ", "A" .. "Z", "a" .. "z", 0 .. 9, qw(! @ $ % ^ & *) );
my $msg_len             = 200;
my $number_of_package   = undef;

my %bench = ();

my $help;

#-- setting up facilities

my $ret = GetOptions(
    "host=s"        => \$host,
    "topic=s"       => \$topic,
    "partitions=i"  => \$partitions,
    "messages=i"    => \$number_of_package,
    "length=i"      => \$msg_len,
    "help|?"        => \$help,
    );

if ( !$ret or $help or !$topic or !$partitions or !$number_of_package )
{
    print <<HELP;
Usage: $0 --topic="..." --partitions=... --messages=... [--host="..."] [--length=...]

Send messages to random paritions of a given topic

Options:
    --help
        Display this help and exit

    --topic="..."
        topic name
    --partitions=...
        number of partitions to use
    --host="..."
        Apache Kafka host to connect to
    --messages=...
        number of messages to send
    --length=...
        length of messages
HELP
    exit 1;
}

unless ( $io = Kafka::IO->new(
    host    => $host,
    timeout => $timeout,
    ) )
{
    print STDERR "Kafka::IO->new: (".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error();
    exit 1;
}

unless ( $producer = Kafka::Producer->new( IO => $io ) )
{
    print STDERR "Kafka::Producer->new: (".Kafka::Producer::last_errorcode().") ".Kafka::Producer::last_error();
    exit 1;
}

#-- definition of the functions

sub send_messages {
    my $producer    = shift;
    my $topic       = shift;
    my $partition   = shift;
    my $messages    = shift;

    my ( $time_before, $time_after );
    $time_before = gettimeofday;
    my $ret = $producer->send( $topic, $partition, $messages );
    $time_after = gettimeofday;
    if ( $ret )
    {
        return $time_after - $time_before;
    }
    else
    {
        print STDERR "send: (".$producer->last_errorcode.") ".$producer->last_error;
        exit 1;
    }
}

sub random_strings {
    my $chars       = shift;
    my $msg_len     = shift;
    my $number_of   = shift;

    print STDERR "generation of messages can take a while\n";
    my @strings;
    $strings[ $number_of - 1 ] = undef;
    foreach my $i ( 0 .. ( $number_of - 1 ) )
    {
        $strings[ $i ] = join( "", @chars[ map { rand @$chars } ( 1 .. $msg_len ) ]);
    }
    return \@strings, $number_of * $msg_len;
}

# INSTRUCTIONS -----------------------------------------------------------------

$in_package = $number_of_package;

#-- Mix
$number_of_package_mix = $in_package;
( $messages, $total ) = random_strings( \@chars, $msg_len, $in_package );
@copy = (); push @copy, @$messages;
$request_size = $in_package * 9 + $total;

$bench{send_mix} = 0;

while (1)
{
    print STDERR "message sending one by one, please wait...\n";
    my $secs = 0;
    foreach my $idx ( 0 .. ( $in_package - 1 ) )
    {
        $bench{send_mix} += send_messages( $producer, $topic, int( rand( $partitions ) ), [ $copy[ $idx ] ] );

# decoration
        my $num = $idx + 1;
        unless ( $num % 1000 )
        {
            $secs = $bench{send_mix};
            my $mbs = ( $num * $msg_len ) / ( 1024 * 1024 );

            print STDERR "[", scalar localtime, "] ",
                "Sent $num messages ",
                "(".sprintf( "%.3f", $mbs )." MB), ",
                ( $secs ) ? sprintf( "%d", int( $num / $secs ) ) : "N/A", " messages/sec ",
                "(".( $secs ? ( sprintf( "%.3f", $mbs / $secs ) ) : "N/A" )." MB/sec)",
                " " x 10;
            print STDERR ( $num % 10000 ) ? "\r" : "\n";
        }
    }

    my $_send = $bench{send_mix};
    last if $_send;

    $number_of_package_mix += $in_package;
    push @$messages, @copy;
}

# POSTCONDITIONS ---------------------------------------------------------------

# Closes and cleans up
$producer->close;

# Statistics
my $msgs = scalar @$messages;
my $mbs = 0; map { $mbs += bytes::length( $_ ) } @$messages; $mbs /= 1024 * 1024;
my $secs = $bench{send_mix};
print STDERR "[", scalar localtime, "] Total: ",
    "Sent $msgs messages ",
    "(".sprintf( "%.3f", $mbs )." MB), ",
    $secs ? sprintf( "%d", int( $msgs / $secs ) ) : "N/A", " messages/sec ",
    "(".( $secs ? ( sprintf( "%.3f", $mbs / $secs ) ) : "N/A" )." MB/sec)",
    "\n";
