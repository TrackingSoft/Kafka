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

#-- verify load the module

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval 'use Test::TCP';           ## no critic
    plan skip_all => "because Test::TCP required for testing" if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use IO::Socket::INET;

use Kafka qw(
    $REQUEST_TIMEOUT
);
use Kafka::IO;
use Kafka::TestInternals qw(
    @not_posint
    @not_posnumber
    @not_string
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

my $server_code = sub {
    my ( $port ) = @_;

    my $sock = IO::Socket::INET->new(
        LocalPort   => $port,
        LocalAddr   => 'localhost',
        Proto       => 'tcp',
        Listen      => 5,
        Type        => SOCK_STREAM,
        ReuseAddr   => 1,
    ) or die "Cannot open server socket: $!";

    $SIG{TERM} = sub { exit };

    while ( my $remote = $sock->accept ) {
        while ( my $line = <$remote> ) {
            print { $remote } $line;
        }
    }
};

my ( $server, $port, $io, $sent, $resp, $test_message );

#-- Global data ----------------------------------------------------------------

$server = Test::TCP->new( code => $server_code );
$port = $server->port;
ok $port, "server port = $port";
wait_port( $port );

$test_message = "Test message\n";

# INSTRUCTIONS -----------------------------------------------------------------

throws_ok {
    $io = Kafka::IO->new(
        host    => 'something bad',
        port    => $port,
        timeout => $REQUEST_TIMEOUT,
    );
} 'Kafka::Exception::IO', 'error thrown';

$io = Kafka::IO->new(
    host    => 'localhost',
    port    => $port,
    timeout => $REQUEST_TIMEOUT,
);
isa_ok( $io, 'Kafka::IO' );

#-- is_alive

ok $io->is_alive, 'socket alive';

#-- close

ok $io->{socket}, 'socket defined';
$io->close;
ok !$io->{socket}, 'socket not defined';

#-- is_alive

ok !$io->is_alive, 'socket not alive';

#-- send

$io = Kafka::IO->new(
    host    => 'localhost',
    port    => $port,
    timeout => $REQUEST_TIMEOUT,
);

lives_ok { $sent = $io->send( $test_message ); } 'expecting to live';
is $sent, length( $test_message ), 'sent '.length( $test_message ).' bytes';

#-- receive

lives_ok { $resp = $io->receive( length( $test_message ) ); } 'expecting to live';
is( $$resp, $test_message, 'receive OK' );

undef $server;
ok $io, 'IO exists';
throws_ok { $sent = $io->send( $test_message ); } 'Kafka::Exception::IO', 'error thrown';

# POSTCONDITIONS ---------------------------------------------------------------

undef $server;
