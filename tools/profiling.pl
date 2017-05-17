#!/usr/bin/perl -w

# WARNING: Ensure kafka cluster is started before executing this program (see t/??_cluster_start.t for examples)



use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
    ../t/lib
);



defined( $ENV{KAFKA_BASE_DIR} ) or exit_on_error( 'Unknown base directory of Kafka server' );



use Const::Fast;
#use File::HomeDir;
use Cwd;
use File::Spec::Functions qw(
    catdir
);
use Getopt::Long;
use Scalar::Util qw(
    blessed
);
use Try::Tiny;

use Kafka qw (
    $MESSAGE_SIZE_OVERHEAD
    $RECEIVE_EARLIEST_OFFSET
    $RECEIVE_LATEST_OFFSETS
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::MockIO;
use Kafka::Producer;
use Kafka::TestInternals;





const my @T_DIRS                => ( 't', catdir( '..', 't' ) );
const my @TEST_OFFSETS          => ( $RECEIVE_LATEST_OFFSETS, $RECEIVE_EARLIEST_OFFSET );

my ( $ret, $help, $base_dir, $topic, $partition, $msg_len, $number_of_messages );

$base_dir           = $ENV{KAFKA_BASE_DIR};     # WARNING: must match the settings of your system
$topic              = $Kafka::TestInternals::topic;
$partition          = $Kafka::MockIO::PARTITION;
$msg_len            = 200;
$number_of_messages = 20_000;

$ret = GetOptions(
    'kafka=s'       => \$base_dir,
    'topic=s'       => \$topic,
    'partition=i'   => \$partition,
    'length=i'      => \$msg_len,
    'messages=i'    => \$number_of_messages,
    'help|?'        => \$help,
);

if ( !$ret || $help )
{
    print <<HELP;
Usage: $0 [--kafka="..."] [--topic="..."] [--partition=...] [--length=...] [--messages=...]

Easy sending and consume messages for parition of a given topic

Options:
    --help
        Display this help and exit

    --kafka="..."
        path to base directory of kafka installation
    --topic="..."
        topic name
    --partition=...
        partition to use
    --length=...
        length of messages
    --messages=...
        the number of messages sent and consumed
HELP
    exit 1;
}

my ( $port, $connect, $producer, $consumer, $offsets, $messages, $strings );

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

sub random_strings {
    my ( $msg_len, $number_of ) = @_;

    my @chars = ( " ", "A" .. "Z", "a" .. "z", 0 .. 9, qw(! @ $ % ^ & *) );

    my @strings;
    $strings[ $number_of - 1 ] = undef;
    foreach my $i ( 0 .. ( $number_of - 1 ) )
    {
        $strings[ $i ] = join( q{}, @chars[ map { rand @chars } ( 1..$msg_len ) ] );
    }
    return \@strings, $number_of * $msg_len;
}



foreach my $t_dir ( @T_DIRS ) {
    if ( -d $t_dir ) {
        my $cwd = getcwd();
        chdir $t_dir;
#-- the Kafka server port (for example for node_id = 0)
        try {
            ( $port ) = Kafka::Cluster->new(
                kafka_dir       => $base_dir,
                reuse_existing  => 1,
                t_dir           => $t_dir,
            )->servers;
        } catch {
            exit_on_error( "Running Kafka server not found: $_" );
        };
        chdir $cwd;
        last;
    }
}

try {
    $connect  = Kafka::Connection->new(
        host    => 'localhost',
        port    => $port,
    );
    $producer = Kafka::Producer->new( Connection => $connect );
    $consumer = Kafka::Consumer->new( Connection => $connect );
} catch {
    exit_on_error( $_ );
};



try {
    $offsets = $consumer->offsets( $topic, $partition, $RECEIVE_LATEST_OFFSETS );
} catch {
    exit_on_error( $_ );
};
my $start_offset = $offsets->[0];

say STDERR 'generation of messages can take a while ...';
( $strings ) = random_strings( $msg_len, $number_of_messages );

say STDERR 'send messages (one by one):';
foreach my $num ( 1..$number_of_messages )
{
    try {
        $producer->send( $topic, $partition, $strings->[ $num - 1 ] );
    } catch {
        exit_on_error( $_ );
    };
    print STDERR '.' unless $num % 1000;
}
print STDERR "\n";

say STDERR 'consume offsets (for a set of statistics):';
foreach my $num ( 1..$number_of_messages )
{
    try {
        $offsets = $consumer->offsets( $topic, $partition, $TEST_OFFSETS[ int( rand 2 ) ] );
    } catch {
        exit_on_error( $_ );
    };
    scalar( @$offsets ) or exit_on_error( 'no offsets' );
    print STDERR '.' unless $num % 1000;
}
print STDERR "\n";

say STDERR 'fetch messages (one by one):';
foreach my $num ( 1..$number_of_messages )
{
    try {
        $messages = $consumer->fetch( $topic, $partition, $start_offset + $num - 1, $msg_len + $MESSAGE_SIZE_OVERHEAD );
    } catch {
        exit_on_error( $_ );
    };
    $messages->[0]->payload eq $strings->[ $num - 1 ] or exit_on_error( 'the received message does not match the original' );
    print STDERR '.' unless $num % 1000;
}
print STDERR "\n";



$connect->close;
$connect = $consumer = $producer = undef;
