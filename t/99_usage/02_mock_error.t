#!/usr/bin/perl -w

use 5.10.0;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 49;

# Usage - Basic functionalities to include a simple Producer and Consumer
# You need to have access to your Kafka instance and be able to connect through TCP

use Kafka qw(
    DEFAULT_TIMEOUT
    TIMESTAMP_EARLIEST
    DEFAULT_MAX_OFFSETS
    DEFAULT_MAX_SIZE
    );
use Kafka::Protocol qw(
    REQUESTTYPE_FETCH
    REQUESTTYPE_OFFSETS
    );
use Kafka::IO;
use Kafka::Consumer;
use Kafka::Mock;

# If the reader closes the connection, though, the writer will get a SIGPIPE when it next tries to write there.
$SIG{PIPE} = sub { die };

#-- Mock server

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

# Control responses ------------------------------------------------------------

my %errors = (
    offsets => [
        #-- bad ERROR_CODE
        # Response Header
         '0000000e'                             # RESPONSE_LENGTH
        .'ffff'                                 # ERROR_CODE - bad
        # OFFSETS Response
        .'00000001'                             # NUMBER of OFFSETS
        .'0000000000000000',                    # OFFSET
        ],
    fetch   => [
#-- error in ERROR_CODE: for the test it must be the first error
        # Response Header
         '00000051'                             # RESPONSE_LENGTH
        .'ffff'                                 # ERROR_CODE - with error
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

        #-- control response (not valid CHECKSUM)
        # Response Header
         '00000051'                             # RESPONSE_LENGTH
        .'0000'                                 # ERROR_CODE
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'00000000'                             # CHECKSUM - not valid
        .'546865206669727374206d657373616765'   # PAYLOAD ("The first message")
        # MESSAGE
        .'00000017'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'00000000'                             # CHECKSUM - not valid
        .'546865207365636f6e64206d657373616765' # PAYLOAD ("The second message")
        # MESSAGE
        .'00000016'                             # LENGTH
        .'00'                                   # MAGIC
        .''                                     # COMPRESSION
        .'00000000'                             # CHECKSUM - not valid
        .'546865207468697264206d657373616765',  # PAYLOAD ("The third message")

        #-- bad MAGIC
        # Response Header
         '00000054'                             # RESPONSE_LENGTH
        .'0000'                                 # ERROR_CODE
        # MESSAGE
        .'00000017'                             # LENGTH
        .'01'                                   # MAGIC - bad
        .'00'                                   # COMPRESSION - no compression
        .'d94a22be'                             # CHECKSUM
        .'546865206669727374206d657373616765'   # PAYLOAD ("The first message")
        # MESSAGE
        .'00000018'                             # LENGTH
        .'01'                                   # MAGIC - bad
        .'00'                                   # COMPRESSION - no compression
        .'a3810845'                             # CHECKSUM
        .'546865207365636f6e64206d657373616765' # PAYLOAD ("The second message")
        # MESSAGE
        .'00000017'                             # LENGTH
        .'01'                                   # MAGIC -bad
        .'00'                                   # COMPRESSION - no compression
        .'58611780'                             # CHECKSUM
        .'546865207468697264206d657373616765',  # PAYLOAD ("The third message")

        #-- not valid CHECKSUM & bad MAGIC
        # Response Header
         '00000054'                             # RESPONSE_LENGTH
        .'0000'                                 # ERROR_CODE
        # MESSAGE
        .'00000017'                             # LENGTH
        .'01'                                   # MAGIC - bad
        .'00'                                   # COMPRESSION - no compression
        .'00000000'                             # CHECKSUM - not valid
        .'546865206669727374206d657373616765'   # PAYLOAD ("The first message")
        # MESSAGE
        .'00000018'                             # LENGTH
        .'01'                                   # MAGIC - bad
        .'00'                                   # COMPRESSION - no compression
        .'00000000'                             # CHECKSUM - not valid
        .'546865207365636f6e64206d657373616765' # PAYLOAD ("The second message")
        # MESSAGE
        .'00000017'                             # LENGTH
        .'01'                                   # MAGIC - bad
        .'00'                                   # COMPRESSION - no compression
        .'00000000'                             # CHECKSUM - not valid
        .'546865207468697264206d657373616765',  # PAYLOAD ("The third message")
        ]
    );

#-------------------------------------------------------------------------------

foreach my $type ( ( "offsets", "fetch" ) )
{
    my $not_first;
    foreach my $bad_response ( @{$errors{ $type }} )
    {
        my $backup = $responses{ $type eq "offsets" ? REQUESTTYPE_OFFSETS : REQUESTTYPE_FETCH };
        $responses{ $type eq "offsets" ? REQUESTTYPE_OFFSETS : REQUESTTYPE_FETCH } = $bad_response;
        error_test( $type, $not_first );
        $responses{ $type eq "offsets" ? REQUESTTYPE_OFFSETS : REQUESTTYPE_FETCH } = $backup;
        ++$not_first;
    }
}

sub error_test {
    my $type        = shift;
    my $not_first   = shift;

    my $server = Kafka::Mock->new(
        requests    => \%requests,
        responses   => \%responses,
        timeout     => 0.1,                     # Optional, secs float
        );
    isa_ok( $server, 'Kafka::Mock');
    my $pid = $server->last_request( "pid" );

#-- IO
    my $io;

    unless ( $io = Kafka::IO->new(
        host        => "localhost",                 # for work with the mock server use "localhost"
        port        => $server->port,
        timeout     => DEFAULT_TIMEOUT,             # Optional, default = DEFAULT_TIMEOUT
        RaiseError  => 0                            # Optional, default = 0
        ) )
    {
        fail "(".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error();
        exit 1;
    }
    isa_ok( $io, 'Kafka::IO');

#-- Consumer
    unless ( $io = Kafka::IO->new(
        host        => "localhost",
        port        => $server->port,
        timeout     => DEFAULT_TIMEOUT,             # Optional, default = DEFAULT_TIMEOUT
        RaiseError  => 0                            # Optional, default = 0
        ) )
    {
        fail "(".Kafka::IO::last_errorcode().") ".Kafka::IO::last_error();
        exit 1;
    }

    my $consumer;

    $consumer = Kafka::Consumer->new(
        IO          => $io,
        RaiseError  => 0                            # Optional, default = 0
        );
    unless ( $consumer )
    {
        fail "(".Kafka::Consumer::last_errorcode().") ".Kafka::Consumer::last_error();
        exit 1;
    }
    isa_ok( $consumer, 'Kafka::Consumer');

#-- Get a list of valid offsets (up max_number) before the given time.
    my $offsets = $consumer->offsets(
        "test",                                     # topic
        0,                                          # partition
        TIMESTAMP_EARLIEST,                         # time
        DEFAULT_MAX_OFFSETS                         # max_number
        );
    if( $offsets )
    {
        $type eq "offsets" ?
            fail( "received offsets" )
            : pass( "received offsets" );
        foreach my $offset ( @$offsets )
        {
            note "Received offset: $offset";
        }
    }
    if ( !$offsets or $consumer->last_error )
    {
        $type eq "offsets" ?
            pass( "(".$consumer->last_errorcode.") ".$consumer->last_error )
            : fail( "(".$consumer->last_errorcode.") ".$consumer->last_error );
    }

#-- Consuming messages one by one
    my $messages = $consumer->fetch(
        "test",                                     # topic
        0,                                          # partition
        0,                                          # offset
        DEFAULT_MAX_SIZE,                           # Maximum size of MESSAGE(s) to receive 1MB
        );
    if ( $messages )
    {
        ( $type eq "fetch" and !$not_first ) ?
            fail( "received messages" )
            : pass( "received messages" );
        foreach my $m ( @$messages )
        {
            if( $m->valid )
            {
                note "Payload    : ", $m->payload;
                note "offset     : ", $m->offset;
                note "next_offset: ", $m->next_offset;
            }
            else
            {
                ( $type eq "fetch" and $not_first ) ?
                    pass( "Error: ".$m->error )
                    : fail( "Error: ".$m->error );
            }
        }
    }
    if ( !$messages or $consumer->last_error )
    {
        ( $type eq "fetch" and !$not_first ) ?
            pass( "(".$consumer->last_errorcode.") ".$consumer->last_error )
            : fail( "(".$consumer->last_errorcode.") ".$consumer->last_error );
    }

# Closes the consumer and cleans up
    $consumer->close;
    ok( scalar( keys %$consumer ) == 0, "the consumer object is an empty" );

# to kill the mock server process
    $server->close;
    ok( scalar( keys %$server ) == 0, "the server object is an empty" );
    is( kill( 0, $pid ), 0, "the server process isn't alive" );
}