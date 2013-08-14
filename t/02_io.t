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

my ( $server, $port, $io, $test_message );

#-- Global data ----------------------------------------------------------------

$server = Test::TCP->new( code => $server_code );
$port = $server->port;
ok $port, "server port = $port";

$test_message = "Test message\n";

# INSTRUCTIONS -----------------------------------------------------------------

# NOTE: Code is duplicated in the test t/03_mockio.t partially (Section Kafka::IO API)

$io = Kafka::IO->new(
    host    => 'localhost',
    port    => $port,
    timeout => $REQUEST_TIMEOUT,
);
isa_ok( $io, 'Kafka::IO' );

#-- last_errorcode, last_error

ok !$io->last_errorcode, 'No errorcode';
ok !$io->last_error, 'No error';

#-- is_alive

ok $io->is_alive, 'socket alive';

#-- close

ok $io->{socket}, 'socket defined';
$io->close;
ok !$io->{socket}, 'socket not defined';

#-- is_alive

ok !$io->is_alive, 'socket not alive';

#-- last_errorcode, last_error

# NOTE: We presume that the verification of the correctness of the arguments made by the user.

#undef $io;
#$io = Kafka::IO->new(
#    host    => 'incorrect host',
#    port    => 'incorrect port',
#    timeout => 'incorrect timeout',
#);
#ok $io->last_errorcode, 'errorcode present: '.$io->last_errorcode;
#ok $io->last_error, 'error present: '.$io->last_error;
#
##-- new
#
## host
#
#$@ = $test_message;
#
#foreach my $bad_host ( @not_string ) {
#    undef $io;
#    $io = Kafka::IO->new(
#        host    => $bad_host,
#        port    => $port,
#        timeout => $REQUEST_TIMEOUT,
#    );
#    is $@, $test_message, '$@ not changed';
#    isa_ok( $io, 'Kafka::IO');
#    ok $io->last_errorcode, 'Invalid host: '.$io->last_error;
#}
#
## port
#
#foreach my $bad_port ( @not_posint ) {
#    undef $io;
#    $io = Kafka::IO->new(
#        host    => 'localhost',
#        port    => $bad_port,
#        timeout => $REQUEST_TIMEOUT,
#    );
#    is $@, $test_message, '$@ not changed';
#    isa_ok( $io, 'Kafka::IO');
#    ok $io->last_errorcode, 'Invalid port: '.$io->last_error;
#}
#
## timeout
#
#foreach my $bad_timeout ( @not_posnumber ) {
#    undef $io;
#    $io = Kafka::IO->new(
#        host        => 'localhost',
#        port        => $port,
#        timeout     => $bad_timeout,
#    );
#    is $@, $test_message, '$@ not changed';
#    isa_ok( $io, 'Kafka::IO');
#    ok $io->last_errorcode, 'Invalid timeout: '.$io->last_error;
#}

#-- send

$io = Kafka::IO->new(
    host    => 'localhost',
    port    => $port,
    timeout => $REQUEST_TIMEOUT,
);

is( $io->send( $test_message ), length( $test_message ), 'sent '.length( $test_message ).' bytes' );
ok !$io->last_errorcode, 'No errorcode';

#-- receive

is( ${ $io->receive( length( $test_message ) ) }, $test_message, 'receive OK' );

# NOTE: We presume that the verification of the correctness of the arguments made by the user.

##-- send
#
#foreach my $bad_message ( @not_string ) {
#    $io = Kafka::IO->new(
#        host    => 'localhost',
#        port    => $port,
#        timeout => $REQUEST_TIMEOUT,
#    );
#    ok $io->is_alive, 'socket alive';
#
#    $io->send( $bad_message );
#    is $@, $test_message, '$@ not changed';
#    ok $io->last_errorcode, 'send error: '.$io->last_error;
#}
#
##-- receive
#
#ok $io->is_alive, 'socket alive';
#
#foreach my $bad_len ( @not_posint ) {
#    $io->receive( $bad_len );
#    is $@, $test_message, '$@ not changed';
#    ok $io->last_errorcode, 'receive error: '.$io->last_error;
#}

$@ = undef;

# POSTCONDITIONS ---------------------------------------------------------------

undef $server;  # kill child process on DESTROY
