#!/usr/bin/perl -w

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
);

# ENVIRONMENT ------------------------------------------------------------------

use Test::More;

BEGIN {
    plan skip_all => 'Unknown base directory of Kafka server'
        unless $ENV{KAFKA_BASE_DIR};
}

#-- verify load the module

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Const::Fast;

use Kafka::Cluster;
use Kafka::Connection;
use Kafka::MockIO;
use Kafka::TestInternals qw(
    $topic
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

# WARNING: must match the settings of your system
const my $KAFKA_BASE_DIR    => $ENV{KAFKA_BASE_DIR};

#-- Global data ----------------------------------------------------------------

my $partition = $Kafka::MockIO::PARTITION;;

# INSTRUCTIONS -----------------------------------------------------------------

testing();

sub testing {
    #-- Connecting to the Kafka server port (for example for node_id = 0)
    my $cluster =  Kafka::Cluster->new( kafka_dir => $KAFKA_BASE_DIR, reuse_existing => 1 );
    my @server_ports = $cluster->servers;
    my $port =  $server_ports[0];

#-- simple start
    my $connect = Kafka::Connection->new(
        host    => 'localhost',
        port    => $port,
    );
    isa_ok( $connect, 'Kafka::Connection' );

#-- stop leader
    my ( $leader_server, $leader_port ) = get_leader( $connect );
    ok $connect->is_server_alive( $leader_server ), 'leader is alive';
    ok $connect->is_server_connected( $leader_server ), 'leader is connected';
    $cluster->stop( $leader_port );
    ok !$connect->is_server_alive( $leader_server ), 'leader is not alive';
    ok !$connect->is_server_connected( $leader_server ), 'leader is connected';
    my ( $next_leader_server ) = get_leader( $connect );
    ok $connect->is_server_alive( $next_leader_server ), 'new leader is alive';
    ok $connect->is_server_connected( $next_leader_server ), 'new leader is connected';

#-- start previous leader
    $cluster->_remove_log_tree( $leader_port );
    $cluster->start( $leader_port );
    ok $connect->is_server_alive( $leader_server ), 'leader is alive';

#-- close
    $connect->close;
    my $tmp = 0;
    foreach my $server ( $connect->get_known_servers() ) {
        ++$tmp if $connect->is_server_connected( $server );
    }
    ok !$tmp, 'server is not connected';
}

sub get_leader {
    my ( $connect ) = @_;

    my $metadata = $connect->get_metadata( $topic );
    my $leader_id = $metadata->{ $topic }->{ $partition }->{Leader};
    my $leader_server = $connect->_find_leader_server( $leader_id );
    my ( $leader_port ) = $leader_server =~ /:(\d{1,5})$/;

    return( $leader_server, $leader_port );
}

# POSTCONDITIONS ---------------------------------------------------------------
