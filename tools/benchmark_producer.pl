#!/usr/bin/perl -w

# NAME: Producer test

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    ../lib
);

# ENVIRONMENT ------------------------------------------------------------------

#-- load the modules -----------------------------------------------------------

use Getopt::Long;
use Time::HiRes qw(
    gettimeofday
);

use Kafka::Connection;
use Kafka::Producer;

#-- setting up facilities ------------------------------------------------------

my $host                = 'localhost',
my $port                = undef;
my $topic               = 'mytopic';
my $partitions          = 1;
my $msg_len             = 200;
my $number_of_messages  = 10_000;

my ( $ret, $help );

$ret = GetOptions(
    'host=s'        => \$host,
    'port=i'        => \$port,
    'topic=s'       => \$topic,
    'partitions=i'  => \$partitions,
    'messages=i'    => \$number_of_messages,
    'length=i'      => \$msg_len,
    'help|?'        => \$help,
);

if ( !$ret || $help || !$host || !$port || !$topic || !$partitions || !$number_of_messages || !$msg_len ) {
    print <<HELP;
Usage: $0 [--host="..."] --port=... [--topic="..."] [--partitions=...] [--messages=...] [--length=...]

Send messages to random paritions of a given topic

Options:
    --help
        Display this help and exit

    --host="..."
        Apache Kafka host to connect to
    --port=...
        Apache Kafka port to connect to
    --topic="..."
        topic name
    --partitions=...
        number of partitions to use
    --messages=...
        number of messages to send
    --length=...
        length of messages
HELP

    exit 1;
}

#-- declarations ---------------------------------------------------------------

my ( $connect, $producer, $messages, $messages_sent, $dispatch_time, $mbs );

sub exit_on_error {
    my ( $message ) = @_;

    say STDERR $message;
    exit 1;
}

sub random_strings {
    my @chars = ( ' ', 'A'..'Z', 'a'..'z', 0..9, qw( ! @ $ % ^ & * ) );

    print STDERR 'generation of messages can take a while';
    my @strings;
    $strings[ $number_of_messages - 1 ] = undef;
    foreach my $i ( 0..( $number_of_messages - 1 ) ) {
        $strings[ $i ] = join( q{}, @chars[ map { rand @chars } ( 1..$msg_len ) ] );
    }

    return \@strings;
}

sub send_message {
    my ( $partition, $message ) = @_;

    my ( $ret, $time_before, $time_after );
    $time_before = gettimeofday();
    $ret = $producer->send( $topic, $partition, $message );
    $time_after = gettimeofday();
    exit_on_error( 'send: ('.$producer->last_errorcode.') '.$producer->last_error )
        unless $ret;

    return $time_after - $time_before;
}

#-- Global data ----------------------------------------------------------------

!( $connect  = Kafka::Connection->new( host => $host, port => $port ) )->last_errorcode || exit_on_error( 'Kafka::Connect->new: ('.$connect->last_errorcode.') '.$connect->last_error );
!( $producer = Kafka::Producer->new( Connection => $connect ) )->last_errorcode || exit_on_error( 'Kafka::Producer->new: ('.$producer->last_errorcode.') '.$producer->last_error );

# INSTRUCTIONS -----------------------------------------------------------------

$messages       = random_strings();
$messages_sent  = 0;
$dispatch_time  = 0;

while (1) {
    print STDERR "\rmessage sending one by one, please wait...\r";
    foreach my $idx ( 0..( $number_of_messages - 1 ) ) {
        $dispatch_time += send_message( int( rand( $partitions ) ), $messages->[ $idx ] );

        # decoration
        unless ( ( my $num = $idx + 1 ) % 1000 ) {
            $mbs = ( $num * $msg_len ) / ( 1024 * 1024 );
            print( STDERR
                sprintf( '[%s] Sent %d messages (%.3f MB) %s messages/sec (%s MB/sec)',
                    scalar localtime,
                    $num,
                    $mbs,
                    $dispatch_time ? sprintf( '%d',   int( $num / $dispatch_time ) ) : 'N/A',
                    $dispatch_time ? sprintf( '%.3f', $mbs / $dispatch_time )        : 'N/A'
                ),
                ' ' x 10,
                "\r",
            );
        }
    }
    $messages_sent += $number_of_messages;

    last if $dispatch_time; # achieved significant time
}

# POSTCONDITIONS ---------------------------------------------------------------

# Closes and cleans up

undef $producer;
undef $connect;

# Statistics

$mbs = ( $messages_sent * $msg_len ) / ( 1024 * 1024 );
say( STDERR sprintf( '[%s] Total: Sent %d messages (%.3f MB), %d messages/sec (%.3f MB/sec)',
        scalar localtime,
        $messages_sent,
        $mbs,
        int( $messages_sent / $dispatch_time ),
        $mbs / $dispatch_time,
    ),
);
