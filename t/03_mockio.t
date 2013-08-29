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

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Params::Util qw(
    _STRING
);

use Kafka qw(
    $KAFKA_SERVER_PORT
    $REQUEST_TIMEOUT
);
use Kafka::MockIO;
use Kafka::TestInternals qw(
    @not_posint
    @not_posnumber
    @not_string
);

#-- setting up facilities ------------------------------------------------------

#-- declarations ---------------------------------------------------------------

my ( %original_IO_API, $io, $test_message, $topic, $partition );

#-- Global data ----------------------------------------------------------------

my @IO_API_names = qw(
    new
    send
    receive
    close
    is_alive
);

$test_message   = "Test message\n";

$topic      = 'mytopic';
$partition  = $Kafka::MockIO::PARTITION;

# description of requests, see t/??_decode_encode.t
my $encoded_produce_request     = pack( "H*", '00000049000000000000000400000001000005dc0000000100076d79746f7069630000000100000000000000200000000000000000000000148dc795a20000ffffffff0000000648656c6c6f21' );
my $encoded_fetch_request       = pack( "H*", '0000004d00010000000000000016636f6e736f6c652d636f6e73756d65722d3235353535ffffffff00000064000000010000000100076d79746f7069630000000100000000000000000000000000100000' );
my $encoded_offset_request      = pack( "H*", '0000004500020000000000000016636f6e736f6c652d636f6e73756d65722d3235353535ffffffff0000000100076d79746f7069630000000100000000fffffffffffffffe00000001' );
my $encoded_metadata_request    = pack( "H*", '0000002d00030000000000000016636f6e736f6c652d636f6e73756d65722d32353535350000000100076d79746f706963' );

# INSTRUCTIONS -----------------------------------------------------------------

#-- Mocking Kafka::IO ----------

$original_IO_API{ $_ } = \&{ "Kafka::IO::$_" } foreach @IO_API_names;

#-- override

Kafka::MockIO::override();
ok( \&{ "Kafka::IO::$_" } ne $original_IO_API{ $_ }, "IO API mocked: $_" ) foreach @IO_API_names;

#-- restore

Kafka::MockIO::restore();
ok( \&{ "Kafka::IO::$_" } eq $original_IO_API{ $_ }, "IO API restored: $_" ) foreach @IO_API_names;

#-- Kafka::MockIO API ----------

Kafka::MockIO::override();

#-- special_cases

ok !%{ Kafka::IO::special_cases() }, 'There are no special cases';

#-- add_special_case

Kafka::IO::add_special_case( { $test_message => $test_message } );
ok( scalar( keys( %{ Kafka::IO::special_cases() } ) ) == 1 && Kafka::IO::special_cases()->{ $test_message } eq $test_message, 'The special case added' );

#-- del_special_case

Kafka::IO::del_special_case( $test_message );
ok !%{ Kafka::IO::special_cases() }, 'The special case deleted';

#-- Kafka::IO API ----------

Kafka::IO::add_special_case( { $test_message => $test_message } );

# NOTE: Is duplicated test code t/02_io.t partially (Section INSTRUCTIONS)

$io = Kafka::IO->new(
    host    => 'localhost',
    port    => $KAFKA_SERVER_PORT,
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

undef $io;
$io = Kafka::IO->new(
    host        => 'incorrect host',
    port        => 'incorrect port',
    timeout     => 'incorrect timeout',
);
ok $io->last_errorcode, 'errorcode present: '.$io->last_errorcode;
ok $io->last_error, 'error present: '.$io->last_error;

#-- new

# host

$@ = $test_message;

foreach my $bad_host ( @not_string ) {
    undef $io;
    $io = Kafka::IO->new(
        host    => $bad_host,
        port    => $KAFKA_SERVER_PORT,
        timeout => $REQUEST_TIMEOUT,
    );
    is $@, $test_message, '$@ not changed';
    isa_ok( $io, 'Kafka::IO');
    ok $io->last_errorcode, 'Invalid host: '.$io->last_error;
}

# port

foreach my $bad_port ( @not_posint ) {
    undef $io;
    $io = Kafka::IO->new(
        host    => 'localhost',
        port    => $bad_port,
        timeout => $REQUEST_TIMEOUT,
    );
    is $@, $test_message, '$@ not changed';
    isa_ok( $io, 'Kafka::IO');
    ok $io->last_errorcode, 'Invalid port: '.$io->last_error;
}

# port

foreach my $bad_timeout ( @not_posnumber ) {
    undef $io;
    $io = Kafka::IO->new(
        host    => 'localhost',
        port    => $KAFKA_SERVER_PORT,
        timeout => $bad_timeout,
    );
    is $@, $test_message, '$@ not changed';
    isa_ok( $io, 'Kafka::IO');
    ok $io->last_errorcode, 'Invalid timeout: '.$io->last_error;
}

#-- send

$io = Kafka::IO->new(
    host    => 'localhost',
    port    => $KAFKA_SERVER_PORT,
    timeout => $REQUEST_TIMEOUT,
);

is( $io->send( $test_message ), length( $test_message ), 'sent '.length( $test_message ).' bytes' );
ok !$io->last_errorcode, 'No errorcode';

#-- receive

is( ${ $io->receive( length( $test_message ) ) }, $test_message, 'receive OK' );

#-- send

foreach my $bad_message ( @not_string ) {
    $io = Kafka::IO->new(
        host    => 'localhost',
        port    => $KAFKA_SERVER_PORT,
        timeout => $REQUEST_TIMEOUT,
    );
    ok $io->is_alive, 'socket alive';

    $io->send( $bad_message );
    is $@, $test_message, '$@ not changed';
    ok $io->last_errorcode, 'send error: '.$io->last_error;
}

#-- receive

ok $io->is_alive, 'socket alive';

foreach my $bad_len ( @not_posint ) {
    $io->receive( $bad_len );
    is $@, $test_message, '$@ not changed';
    ok $io->last_errorcode, 'receive error: '.$io->last_error;
}

$@ = undef;

#-- Kafka server capabilities ----------

#-- APIKEY_PRODUCE

ok !exists( $Kafka::MockIO::_received_data{ $topic }->{ $partition } ), 'data is not received';
$io->send( $encoded_produce_request );
is( scalar( @{ $Kafka::MockIO::_received_data{ $topic }->{ $partition } } ), 1, 'data is received' );

#-- all requests

foreach my $encoded_request (
        $encoded_produce_request,
        $encoded_fetch_request,
        $encoded_offset_request,
        $encoded_metadata_request,
    ) {
    $io->send( $encoded_request );
    my $encoded_response = ${ $io->receive( 4 ) };  # response length
    ok !$io->last_errorcode, 'No errorcode';
    $encoded_response .= ${ $io->receive( unpack( 'l>', $encoded_response ) ) };
    ok !$io->last_errorcode, 'No errorcode';
    ok( defined( _STRING( $encoded_response ) ), 'response received' );
}

#-- APIKEY_OFFSET

#-- APIKEY_METADATA

# POSTCONDITIONS ---------------------------------------------------------------

Kafka::MockIO::restore();
