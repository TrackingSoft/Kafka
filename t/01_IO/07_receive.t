#!/usr/bin/perl -w

use 5.008003;
use strict;
use warnings;

use bytes;
use lib 'lib';

use Test::More tests => 10;

use Kafka::IO;

use Kafka::Mock;


my %requests = (
    0   =>
         '00000006'                             # REQUEST_LENGTH
        .'0000'                                 # REQUEST_TYPE
        .'00000000',                            # body
    );

my %responses = (
    0   =>
        '00000004'                              # RESPONSE_LENGTH
        .'ffffff0a',                            # body
    );

my $server;

$server = Kafka::Mock->new(
    requests    => \%requests,
    responses   => \%responses,
    timeout     => 0.1,
    );
isa_ok( $server, 'Kafka::Mock');

my $port = $server->port;
ok $port, "server port = $port";

my $io;
my $ret;

$io = Kafka::IO->new(
    host        => "localhost",
    port        => $port,
    timeout     => 1.0,
    RaiseError  => 0
    );
isa_ok( $io, 'Kafka::IO');

# arguments
foreach my $len ( ( undef, "", [], $io, -1 ) )
{
    $io->receive( $len );
    ok( defined( $io->last_error ), "last_error = ".$io->last_error." (".( $len || "" ).")" );
}

my $msg = pack( "H*", $requests{0} );
is( $ret = $io->send( $msg ), bytes::length( $msg ), "sent bytes = $ret" );
my $len = bytes::length( pack( "H*", $responses{0} ) );
is( bytes::length( ${ $ret = $io->receive( $len ) } ), $len, "receive = ".( $ret ? unpack( "H*", $ret ) : "" ) );

$io->close;
$server->close;
