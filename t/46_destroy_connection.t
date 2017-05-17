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

use Kafka qw(
    $RETRY_BACKOFF
);
use Kafka::Cluster;
use Kafka::Connection;

my $CLUSTER = Kafka::Cluster->new(
    replication_factor => 1,
);
isa_ok( $CLUSTER, 'Kafka::Cluster' );

#-- Connecting to the Kafka server port (for example for node_id = 0)
my ( $PORT ) =  $CLUSTER->servers;

my ( $CONNECTION, $HOSTS, $IO_CACHE );

sub new_connection {
    my ( $port ) = @_;

    # connecting to the Kafka server port
    my $connection = Kafka::Connection->new(
        host            => 'localhost',
        port            => $port,
        RETRY_BACKOFF   => $RETRY_BACKOFF * 2,
        dont_load_supported_api_versions => 1,
    );
    isa_ok( $connection, 'Kafka::Connection' );

    # simple communication
    is scalar( $connection->get_known_servers() ), 1, 'Known only one server';
    my ( $server ) = $connection->get_known_servers();
    ok $connection->is_server_known( $server ), 'known server';
    my $metadata = $connection->get_metadata;
    ok $metadata, 'communication OK';

    my $IO_cache = $connection->{_IO_cache};
    my @hosts = keys %$IO_cache;
    ok scalar( @hosts ), 'IO cache filled';

    is_sockets_opened( \@hosts, $IO_cache );

    return( $connection, \@hosts, $IO_cache );
}

sub is_sockets_opened {
    my ( $hosts, $IO_cache ) = @_;

    foreach my $host_port ( @$hosts ) {
        my $io = $IO_cache->{ $host_port }->{IO};
        is ref( $io ), 'Kafka::IO', 'Kafka::IO';
        my $socket = $io->{socket};
        ok defined( $socket ), 'socket exists';
        my $fn = fileno $socket;
        ok defined( $fn ), 'socket opened';
    }
}

sub is_sockets_closed {
    my ( $hosts, $IO_cache ) = @_;

    foreach my $host_port ( @$hosts ) {
        my $io = $IO_cache->{ $host_port }->{IO};
        ok !defined( $io ), 'IO (socket) closed';
    }
}



( $CONNECTION, $HOSTS, $IO_CACHE ) = new_connection( $PORT );

undef $CONNECTION;
is_sockets_opened( $HOSTS, $IO_CACHE );

( $CONNECTION, $HOSTS, $IO_CACHE ) = new_connection( $PORT );

$CONNECTION->close;
is_sockets_closed( $HOSTS, $IO_CACHE );

$CLUSTER->close;

