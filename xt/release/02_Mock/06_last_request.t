#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use bytes;
use IO::Socket::INET;
use lib 'lib';

use Test::More tests => 19;

use Kafka::Mock;

my %requests = (
    0   =>
         '00000006'                             # REQUEST_LENGTH
        .'0000'                                 # REQUEST_TYPE
        .'00000000',                            # body
    1   =>
         '00000006'                             # REQUEST_LENGTH
        .'0001'                                 # REQUEST_TYPE
        .'11111111',                            # body
    2   =>
         '00000006'                             # REQUEST_LENGTH
        .'0002'                                 # REQUEST_TYPE
        .'22222222',                            # body
    3   =>
         '00000006'                             # REQUEST_LENGTH
        .'0003'                                 # REQUEST_TYPE
        .'33333333',                            # body
    4   =>
         '00000006'                             # REQUEST_LENGTH
        .'0004'                                 # REQUEST_TYPE
        .'44444444',                            # body
    );

my %responses = (
    0   =>
        '00000004'                              # RESPONSE_LENGTH
        .'ffffff0a',                            # body
    1   =>
        '00000004'                              # RESPONSE_LENGTH
        .'eeeeee0a',                            # body
    2   =>
        '00000004'                              # RESPONSE_LENGTH
        .'dddddd0a',                            # body
    3   =>
        '00000004'                              # RESPONSE_LENGTH
        .'cccccc0a',                            # body
    4   =>
        '00000004'                              # RESPONSE_LENGTH
        .'bbbbbb0a',                            # body
    );

my $server = Kafka::Mock->new(
    requests    => \%requests,
    responses   => \%responses,
    timeout     => 0.1,
    );
isa_ok( $server, 'Kafka::Mock');

my $port = $server->port;
ok $port, "server port = $port";

my $sock = IO::Socket::INET->new(
    PeerPort => $port,
    PeerAddr => '127.0.0.1',
    Proto    => 'tcp'
    ) or die "Cannot open client socket: $!";

foreach ( ( 0...( scalar( keys %requests ) - 1 ) ) )
{
    print {$sock} pack( "H*", $requests{ $_ } );
    my $res = <$sock>;
    is unpack( "H*", $res ), $responses{ $_ }, "response corresponds to the request";
    is $server->last_request, $requests{ $_ }, "received a valid request ".$server->last_request;
}

foreach ( ( -1.5, '00', '11111111', '000000000006', '0000000a0006' ) )
{
    print {$sock} pack( "H*", $_ );
    my $res = <$sock>;
    is unpack( "H*", $res ), "0a", "invalid query (".$server->last_request.")";
}

print {$sock} pack( "H*", $requests{0} );
my $res = <$sock>;
is $server->last_request, $requests{0}, "a valid request  (After an invalid query)";
is unpack( "H*", $res ), $responses{0}, "a valid response (After an invalid query)";

$server->close;
