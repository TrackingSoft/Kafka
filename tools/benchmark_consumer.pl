#!/usr/bin/perl -w

# NAME: Consuming messages



use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    ../lib
);





use Getopt::Long;
use Scalar::Util qw(
    blessed
);
use Time::HiRes qw(
    gettimeofday
);
use Try::Tiny;

use Kafka qw(
    $MESSAGE_SIZE_OVERHEAD
    $RECEIVE_EARLIEST_OFFSET
    $RECEIVE_LATEST_OFFSETS
);
use Kafka::Connection;
use Kafka::Consumer;



my $host                = 'localhost',
my $port                = undef;
my $topic               = 'mytopic';
my $partition           = 0;
my $msg_len             = 200;
my $number_of_messages  = 20_000;
my $re_read             = 0;
my $no_infinite         = 0;

my ( $ret, $help );

$ret = GetOptions(
    'host=s'        => \$host,
    'port=i'        => \$port,
    'topic=s'       => \$topic,
    'partition=i'   => \$partition,
    'package=i'     => \$number_of_messages,
    'length=i'      => \$msg_len,
    're_read'       => \$re_read,
    'no_infinite'   => \$no_infinite,
    'help|?'        => \$help,
);

if ( !$ret || $help || !$host || !$port || !$topic || !$msg_len || !$number_of_messages ) {
    print <<HELP;
Usage: $0 [--host="..."] --port=... [--topic="..."] [--partition=...] [--package=...] [--length=...] [--re_read] [--no_infinite]

Consume messages from parition of a given topic

Options:
    --help
        Display this help and exit

    --host="..."
        Apache Kafka host to connect to
    --port=...
        Apache Kafka port to connect to
    --topic="..."
        topic name
    --partition=...
        partition to use
    --package=...
        number of messages in single fetch request
    --length=...
        length of messages
    --re_read
        re-read all the available data
    --no_infinite
        no an infinite loop
HELP

    exit 1;
}

my $ctrl_c  = 0;
$SIG{INT}   = \&tsk;

sub tsk {
    $SIG{INT} = \&tsk;
    $ctrl_c = 1;
}



my ( $connect, $consumer, $desired_size, $first_offset, $fetch, $dispatch_time, $messages_recv, $mbs );

sub exit_on_error {
    my ( $error ) = @_;

    my $message;
    if ( !blessed( $error ) || !$_->isa( 'Kafka::Exception' ) ) {
        $message = $error;
    } else {
        $message = $_->message;
    }
    say STDERR $message;
    exit 1;
}

sub fetch_messages {
    my ( $offset, $max_size ) = @_;

    my ( $messages, $time_before, $time_after );
    $time_before = gettimeofday();
    try {
        $messages = $consumer->fetch( $topic, $partition, $offset, $max_size );
    } catch {
        exit_on_error( $_ );
    };
    $time_after = gettimeofday();

    my $cnt = 0;
    foreach my $m ( @$messages ) {
        unless ( $m->valid ) {
            say STDERR "Message No $cnt, Error: ", $m->error;
            say STDERR 'Payload    : ', length( $m->payload ) > 100 ? substr( $m->payload, 0, 100 ).'...' : $m->payload;
            say STDERR 'offset     : ', $m->offset;
            say STDERR 'next_offset: ', $m->next_offset;
        }
        ++$cnt;
    }

    return $messages, $time_after - $time_before;
}



try {
    $connect  = Kafka::Connection->new( host => $host, port => $port );
    $consumer = Kafka::Consumer->new( Connection => $connect );
} catch {
    exit_on_error( $_ );
};

$desired_size = ( $MESSAGE_SIZE_OVERHEAD + $msg_len ) * $number_of_messages;



$fetch          = [];
$messages_recv  = 0;
$dispatch_time  = 0;
my $cnt         = 0;

INFINITE:                                       # an infinite loop
{
    $first_offset = ( $re_read || !@$fetch ) ? 0 : $fetch->[ @$fetch - 1 ]->next_offset;
    $fetch  = [];

    CONSUMPTION:
    while (1) {
        # until all messages
        FETCH:
        {
            last INFINITE if $ctrl_c;

            # useful work
            my ( $fetched, $to_bench ) = fetch_messages(
                @$fetch ? $fetch->[ @$fetch - 1 ]->next_offset : $first_offset,
                $desired_size,
            );
            last FETCH unless @$fetched;    # all messages fetched

            $dispatch_time += $to_bench;
            $messages_recv += scalar @$fetched;
            push @$fetch, @$fetched;

            # decoration
            $mbs = ( $messages_recv * $msg_len ) / ( 1024 * 1024 );
            print( STDERR
                sprintf( '[%s] Received %d messages (%.3f MB) %s messages/sec (%s  MB/sec)',
                    scalar localtime,
                    $messages_recv,
                    $mbs,
                    $dispatch_time ? sprintf( '%d', int( $messages_recv / $dispatch_time ) ) : 'N/A',
                    $dispatch_time ? sprintf( '%.3f', $mbs / $dispatch_time ) : 'N/A',
                ),
                ' ' x 10,
            );
            if ( ( $cnt += scalar @$fetched ) < 200_000 ) {
                print STDERR "\r";
            } else {
                print STDERR "\n";
                $cnt = 0;
            }

            redo FETCH;                     # could still remain unread messages
        }
        last CONSUMPTION if $dispatch_time; # achieved significant time
    }
    redo INFINITE unless $no_infinite;
}



# Closes and cleans up

undef $consumer;
$connect->close;
undef $connect;

# Statistics

$mbs = ( $messages_recv * $msg_len ) / ( 1024 * 1024 );
say( STDERR sprintf( '[%s] Total: Received %d messages (%.3f MB), %s messages/sec (%s MB/sec)',
        scalar localtime,
        $messages_recv,
        $mbs,
        $dispatch_time ? sprintf( '%d', int( $messages_recv / $dispatch_time ) ) : 'N/A',
        $dispatch_time ? sprintf( '%.3f', $mbs / $dispatch_time ) : 'N/A',
    ),
);
