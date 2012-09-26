#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Consuming messages

use lib 'lib';
use bytes;
use Time::HiRes     qw( gettimeofday );
use Getopt::Long;

# PRECONDITIONS ----------------------------------------------------------------

#-- verify load the module
use Kafka qw(
    DEFAULT_TIMEOUT
    DEFAULT_MAX_SIZE
    TIMESTAMP_LATEST
    TIMESTAMP_EARLIEST
    DEFAULT_MAX_OFFSETS
    );
use Kafka::IO;
use Kafka::Consumer;

#-- declaration of variables to test
my $host                = "localhost",
my $topic               = undef;
my $partition           = undef;
my $timeout             = DEFAULT_TIMEOUT;
my $number_of_package   = 1_000;
my $msg_len             = 200;

my $re_read             = 0;
my $no_infinite         = 0;
my $ctrl_c              = 0;

my $help;

#-- setting up facilities

my $ret = GetOptions(
    "host=s"        => \$host,
    "topic=s"       => \$topic,
    "partition=i"   => \$partition,
    "length=i"      => \$msg_len,
    "package=i"     => \$number_of_package,
    "re_read"       => \$re_read,
    "no_infinite"   => \$no_infinite,
    "help|?"        => \$help,
    );

if ( !$ret or $help or !$topic or !defined( $partition ) )
{
    print <<HELP;
Usage: $0 --topic="..." --partition=... [--host="..."] [--package=...] [--length=...] [--re_read] [--no_infinite]

Consume messages from parition of a given topic

Options:
    --help
        Display this help and exit

    --topic="..."
        topic name
    --partition=...
        partition to use
    --host="..."
        Apache Kafka host to connect to
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
my (
    $io,
    $consumer,
    $first_offset,
    $fetch,
    @tmp_bench,
    );
my %bench = ();

unless ( $io = Kafka::IO->new(
    host    => $host,
    timeout => $timeout,
    ) )
{
    print STDERR "Kafka::IO->new: (".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error();
    exit 1;
}

unless ( $consumer = Kafka::Consumer->new( IO => $io ) )
{
    print STDERR "Kafka::Consumer->new: (".Kafka::Consumer::last_errorcode().") ".Kafka::Consumer::last_error();
    exit 1;
}

#-- definition of the functions

$SIG{INT} = \&tsk;

sub tsk {
    $SIG{INT} = \&tsk;
    $ctrl_c = 1;
}

sub fetch_messages {
    my $consumer    = shift;
    my $topic       = shift;
    my $partition   = shift;
    my $offset      = shift;
    my $max_size    = shift;

    my ( $time_before, $time_after );
    $time_before = gettimeofday;
    my $messages = $consumer->fetch( $topic, $partition, $offset, $max_size );
    $time_after = gettimeofday;
    if ( $messages )
    {
        my $cnt = 0;
        foreach my $m ( @$messages )
        {
            unless ( $m->valid )
            {
                print STDERR "Message No $cnt, Error: ", $m->error, "\n";
                print STDERR "Payload    : ", bytes::length( $m->payload ) > 100 ? substr( $m->payload, 0, 100 )."..." : $m->payload, "\n";
                print STDERR "offset     : ", $m->offset, "\n";
                print STDERR "next_offset: ", $m->next_offset, "\n";
            }
            ++$cnt;
        }
        return ( $messages, $time_after - $time_before );
    }
    if ( !$messages or $consumer->last_error )
    {
        print STDERR "fetch: (".$consumer->last_errorcode.") ".$consumer->last_error;
        exit 1;
    }
    return;
}

# INSTRUCTIONS -----------------------------------------------------------------

#-- Mix
$fetch = [];
my $wanted_size = ( 9 + $msg_len ) * $number_of_package;
my %total = (
    messages    => 0,
    seconds     => 0,
    );

# an infinite loop
INFINITE: {
    $first_offset = ( $re_read or !@$fetch ) ? 0 : $fetch->[ @$fetch - 1 ]->next_offset;
    $fetch = [];
    $bench{fetch_mix} = 0;
    my $cnt = 0;
    my $all_bytes = 0;

# to determine the number of messages per second
    MIX: while (1)
    {
# until all messages
        {
            last INFINITE if $ctrl_c;

# useful work
            my ( $fetched, $to_bench ) = fetch_messages(
                $consumer,
                $topic,
                $partition,
                @$fetch ? $fetch->[ @$fetch - 1 ]->next_offset : $first_offset,
                $wanted_size,
                );
            last unless @$fetched;

# decoration
            $bench{fetch_mix} += $to_bench;
            $total{seconds}     += $to_bench;
            $total{messages}    += scalar @$fetched;
            foreach my $m ( @$fetched )
            {
                my $len = bytes::length( $m->payload );
                $all_bytes      += $len;
                $total{bytes}   += $len;
            }

            push @$fetch, @$fetched;
            my $already = scalar @$fetch;
            my $mbs = $all_bytes / ( 1024 * 1024 );
            my $tmp_bench = $bench{fetch_mix};

            $cnt += scalar @$fetched;

            my $secs = $bench{fetch_mix};

            $tmp_bench /= ( scalar $already );

            print STDERR "[", scalar localtime, "] ",
                "Received $already messages ",
                "(".sprintf( "%.3f", $mbs )." MB), ",
                $tmp_bench ? sprintf( "%d", int( 1 / $tmp_bench ) ) : "N/A",
                " messages/sec ",
                "(".( $secs ? ( sprintf( "%.3f", $mbs / $secs ) ) : "N/A" )." MB/sec)",
                " " x 10;
            if ( $cnt < 10_000 )
            {
                print STDERR "\r";
            }
            else
            {
                print STDERR "\n";
                $cnt = 0;
            }

            redo;
        }

        last if $bench{fetch_mix};
    }

    redo unless $no_infinite;
}

# POSTCONDITIONS ---------------------------------------------------------------

# Closes and cleans up
$consumer->close;

# Statistics
my $mbs = $total{bytes} / ( 1024 * 1024 );
print STDERR "\n[", scalar localtime, "] Total: ",
    "Received $total{messages} messages ",
    "(".sprintf( "%.3f", $mbs )." MB), ",
    ( $total{seconds} ) ? sprintf( "%d", int( $total{messages} / $total{seconds} ) ) : "N/A",
    " messages/sec ",
    "(".( $total{seconds} ? ( sprintf( "%.3f", $mbs / $total{seconds} ) ) : "N/A" )." MB/sec)\n";
