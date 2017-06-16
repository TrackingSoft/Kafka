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

plan 'no_plan';

BEGIN {
    plan skip_all => 'Unknown base directory of Kafka server'
        unless $ENV{KAFKA_BASE_DIR};
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}


use Const::Fast;
use Kafka qw(
    $BLOCK_UNTIL_IS_COMMITTED
    $RETRY_BACKOFF
);
use Kafka::Cluster;
use Kafka::Connection;
use Kafka::Producer;
use Kafka::TestInternals qw(
    $topic
);

ok defined( Kafka::Cluster::data_cleanup() ), 'data directory cleaned';

const my $HOST          => 'localhost'; # use only 'localhost' for test
const my $PARTITIONS    => 5;
const my $SENDINGS      => 1_000;

my $CLUSTER = Kafka::Cluster->new(
    replication_factor  => 1,
    partition           => $PARTITIONS,
);

my ( $PORT ) = $CLUSTER->servers;

my $connection = get_new_connection();

for ( my $i = 0; $i < $SENDINGS; ++$i ) {
    send_beacon( $connection, "Some beacon #$i" );
}

# the same connection but new producer
for ( my $i = 0; $i < $SENDINGS; ++$i ) {
    send_beacon( $connection, "Other beacon #$i" );
}

my @first_used_sockets = get_used_socket_ids( $connection );

$connection->close;
undef $connection;

# renew connection
$connection = get_new_connection();

# the new connection
for ( my $i = 0; $i < $SENDINGS; ++$i ) {
    send_beacon( $connection, "Next beacon #$i" );
}

my @second_used_sockets = get_used_socket_ids( $connection );

is scalar( @first_used_sockets ), scalar( @second_used_sockets ), "used socket number not changed";
is scalar( @second_used_sockets ), 1, 'only one socket used';
ok "@first_used_sockets" ne "@second_used_sockets", 'the new socket used';



$CLUSTER->close;

exit;

sub get_new_connection {
    return Kafka::Connection->new(
        host                    => $HOST,
        port                    => $PORT,
        AutoCreateTopicsEnable  => 1,
        RETRY_BACKOFF           => $RETRY_BACKOFF * 2,
        dont_load_supported_api_versions => 1,
    );
}

sub get_used_socket_ids {
    my $connection = shift;

    my @sockets;
    foreach my $server ( keys %{ $connection->{_IO_cache} } ) {
        if ( my $io = $connection->{_IO_cache}->{ $server }->{IO} ) {
            my $socket = $io->{socket};
            push @sockets, ''.$socket;
        }
    }

    return( sort @sockets );
}

sub send_beacon {
    my $connection  = shift;
    my @beacons     = @_;

    my $producer = Kafka::Producer->new(
        Connection      => $connection,
        RequiredAcks    => $BLOCK_UNTIL_IS_COMMITTED,
    );

    my @array = ( 0 .. $PARTITIONS - 1 );

    my $random_partition = $array[ rand scalar @array ];

    my @start_used_sockets = get_used_socket_ids( $connection );

    ok $producer->send(
        $topic,             # topic
        $random_partition,  # partition
        [                   # message
            @beacons,
        ]
    ), "sent OK: @beacons";

    my @finish_used_sockets = get_used_socket_ids( $connection );

    if ( @start_used_sockets ) {
        is scalar( @start_used_sockets ), scalar( @finish_used_sockets ), "used socket number not changed";
        is scalar( @start_used_sockets ), 1, 'only one socket used';
        is "@start_used_sockets", "@finish_used_sockets", "the same sockets used: @start_used_sockets";
    }

    undef $producer;
}

