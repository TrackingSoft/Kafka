#!/usr/bin/perl -w

use 5.008;
use strict;
use warnings;

use IO::Socket::INET;
use Time::HiRes qw ( sleep );
use lib 'lib';

use Test::More tests => 17;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing Kafka::Mock::delay" if $@;
}

use Kafka::Mock;

# in a format for pack( 'H*', ... )
my %requests = (
    0   =>                                      # PRODUCE       Request     - "no compression" now
        # Request Header
         '0000005f'                             # REQUEST_LENGTH
        .'0000'                                 # REQUEST_TYPE
        .'0004'                                 # TOPIC_LENGTH
        .'74657374'                             # TOPIC ("test")
        .'00000000'                             # PARTITION
        # PRODUCE Request
        .'0000004f'                             # MESSAGES_LENGTH
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'d94a22be'                             # CHECKSUM
        .'546865206669727374206d657373616765'   # PAYLOAD ("The first message")
        # MESSAGE
        .'00000017'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'a3810845'                             # CHECKSUM
        .'546865207365636f6e64206d657373616765' # PAYLOAD ("The second message")
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'58611780'                             # CHECKSUM
        .'546865207468697264206d657373616765',  # PAYLOAD ("The third message")

    1   =>                                      # FETCH         Request
        # Request Header
         '00000018'                             # REQUEST_LENGTH
        .'0001'                                 # REQUEST_TYPE
        .'0004'                                 # TOPIC_LENGTH
        .'74657374'                             # TOPIC ("test")
        .'00000000'                             # PARTITION
        # FETCH Request
        .'0000000000000000'                     # OFFSET
        .'00100000',                            # MAX_SIZE (1MB)

    2   => '',                                  # MULTIFETCH    Request     - it is not realized yet
    3   => '',                                  # MULTIPRODUCE  Request     - it is not realized yet

    4   =>                                      # OFFSETS       Request
        # Request Header
         '00000018'                             # REQUEST_LENGTH
        .'0004'                                 # REQUEST_TYPE
        .'0004'                                 # TOPIC_LENGTH
        .'74657374'                             # TOPIC ("test")
        .'00000000'                             # PARTITION
        # OFFSETS Request
        .'fffffffffffffffe'                     # TIME (-2 : earliest)
        .'00000064',                            # MAX NUMBER of OFFSETS (100)
    );

# in a format for pack( 'H*', ... )
my %responses = (
    0   => '',                                  # PRODUCE       Response    - None

    1   =>                                      # FETCH         Response    - "no compression" now
        # Response Header
         '00000051'                             # RESPONSE_LENGTH
        .'0000'                                 # ERROR_CODE
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'d94a22be'                             # CHECKSUM
        .'546865206669727374206d657373616765'   # PAYLOAD ("The first message")
        # MESSAGE
        .'00000017'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'a3810845'                             # CHECKSUM
        .'546865207365636f6e64206d657373616765' # PAYLOAD ("The second message")
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'58611780'                             # CHECKSUM
        .'546865207468697264206d657373616765',  # PAYLOAD ("The third message")

    2   => '',                                  # MULTIFETCH    Response    - None
    3   => '',                                  # MULTIPRODUCE  Request     - None

    4   =>                                      # OFFSETS       Response
        # Response Header
         '0000000e'                             # RESPONSE_LENGTH
        .'0000'                                 # ERROR_CODE
        # OFFSETS Response
        .'00000001'                             # NUMBER of OFFSETS
        .'0000000000000000'                     # OFFSET
    );

my $server;

# read by <...> line should end with '0a'
foreach ( ( 0...( scalar( keys %requests ) - 1 ) ) )
{
    substr( $responses{ $_ }, -2, 2 ) = '0a' if $responses{ $_ };
}

my $timeout = 0.1;

$server = Kafka::Mock->new(
    requests    => \%requests,
    responses   => \%responses,
    timeout     => $timeout,
    );
isa_ok( $server, 'Kafka::Mock');

my $port = $server->port;
ok $port, "server port = $port";

SKIP:
{
    my $sock = IO::Socket::INET->new(
        PeerPort => $port,
        PeerAddr => '127.0.0.1',
        Proto    => 'tcp'
        );
    skip( "because Cannot open client socket: $!", 1 ) unless $sock;

    ok defined( $sock ), "open client socket";

    $server->delay( "response", 10, 0.1 );
    $server->delay( "request",  12, 0.2 );
    is( $server->last_request( "note" ), "delay response: 10 0.1 delay request: 12 0.2", "set the delay" );

    $server->clear();
    $server->delay( "request",  10, 0.001 );
    $server->delay( "request",  12, 0.002 );
    $server->delay( "response", 4,  0.001 );
    $server->delay( "response", 14, 0.002 );
    $server->delay( "response", 16, 0.003 );

    my ( $res, $ret );
    foreach ( ( 0...( scalar( keys %requests ) - 1 ) ) )
    {
        next unless $requests{ $_ };
        print {$sock} pack( "H*", $requests{ $_ } );
        my $delay = sleep( $timeout );
        $res = <$sock> if $responses{ $_ };
        is $delay >= $timeout, 1, "server processing time $delay secs";
    
        $ret = $server->last_request( "sleep" );
        like $ret = $ret, qr/receive: \d+ \d+/, "delays in receiving the request ($_ => $ret)";
        is $server->last_request, $requests{ $_ }, "received a valid request";
    
        if ( $responses{ $_ } )
        {
            is unpack( "H*", $res ), $responses{ $_ }, "response corresponds to the request";
            like $ret, qr/send: \d+ \d+/, "delays in the transmission response ($_ => $ret)";
        }
    }
}

$server->close;
