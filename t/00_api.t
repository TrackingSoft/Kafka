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

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Kafka qw (
    $BITS64
);

#-- setting up facilities ------------------------------------------------------

our %ordinary;

BEGIN {

    unless ( $BITS64 ) {
        our ( $constants_Kafka_Int64, $functions_Kafka_Int64 );
        $ordinary{ 'Kafka::Int64' } = [ $constants_Kafka_Int64, $functions_Kafka_Int64 ];

        # Kafka::Int64

        $constants_Kafka_Int64 = [ qw(
        ) ];

        $functions_Kafka_Int64 = [ qw(
            intsum
            packq
            unpackq
        ) ];

        use_ok 'Kafka::Int64',
            @$constants_Kafka_Int64,
            @$functions_Kafka_Int64,
        ;
    }
}


#-- declarations ---------------------------------------------------------------

#-- Global data ----------------------------------------------------------------

our (
    $constants_Kafka,               $functions_Kafka,
    $constants_Kafka_Internals,     $functions_Kafka_Internals,
    $constants_Kafka_TestInternals, $functions_Kafka_TestInternals,
    $constants_Kafka_Protocol,      $functions_Kafka_Protocol,
    $constants_Kafka_MockProtocol,  $functions_Kafka_MockProtocol,

    $ours_Kafka_IO,                 $methods_Kafka_IO,
    $ours_Kafka_MockIO,             $methods_Kafka_MockIO,
    $ours_Kafka_Connection,         $methods_Kafka_Connection,
    $ours_Kafka_Message,            $methods_Kafka_Message,
    $ours_Kafka_Consumer,           $methods_Kafka_Consumer,
    $ours_Kafka_Producer,           $methods_Kafka_Producer,
    $ours_Kafka_Cluster,            $methods_Kafka_Cluster,
);

$ordinary{ 'Kafka' }                = [ $constants_Kafka,               $functions_Kafka ];
$ordinary{ 'Kafka::Internals' }     = [ $constants_Kafka_Internals,     $functions_Kafka_Internals ];
$ordinary{ 'Kafka::TestInternals' } = [ $constants_Kafka_TestInternals, $functions_Kafka_TestInternals ];
$ordinary{ 'Kafka::Protocol' }      = [ $constants_Kafka_Protocol,      $functions_Kafka_Protocol ];
$ordinary{ 'Kafka::MockProtokol' }  = [ $constants_Kafka_MockProtocol,  $functions_Kafka_MockProtocol ];

my %OO = (
    'Kafka::IO'             => [ $ours_Kafka_IO,                    $methods_Kafka_IO ],
    'Kafka::MockIO'         => [ $ours_Kafka_MockIO,                $methods_Kafka_MockIO ],
    'Kafka::Connection'     => [ $ours_Kafka_Connection,            $methods_Kafka_Connection ],
    'Kafka::Message'        => [ $ours_Kafka_Message,               $methods_Kafka_Message ],
    'Kafka::Consumer'       => [ $ours_Kafka_Consumer,              $methods_Kafka_Consumer ],
    'Kafka::Producer'       => [ $ours_Kafka_Producer,              $methods_Kafka_Producer ],
    'Kafka::Cluster'        => [ $ours_Kafka_Cluster,               $methods_Kafka_Cluster ],
);

# INSTRUCTIONS -----------------------------------------------------------------

#-- verify load the module

# Kafka

BEGIN {
    $constants_Kafka = [ qw(
        $BITS64
        $BLOCK_UNTIL_IS_COMMITTED
        $DEFAULT_MAX_BYTES
        $DEFAULT_MAX_NUMBER_OF_OFFSETS
        $DEFAULT_MAX_WAIT_TIME
        %ERROR
        $ERROR_BROKER_NOT_AVAILABLE
        $ERROR_CANNOT_BIND
        $ERROR_CANNOT_GET_METADATA
        $ERROR_CANNOT_RECV
        $ERROR_CANNOT_SEND
        $ERROR_COMPRESSED_PAYLOAD
        $ERROR_LEADER_NOT_FOUND
        $ERROR_INVALID_MESSAGE
        $ERROR_INVALID_MESSAGE_SIZE
        $ERROR_LEADER_NOT_AVAILABLE
        $ERROR_MESSAGE_SIZE_TOO_LARGE
        $ERROR_MISMATCH_ARGUMENT
        $ERROR_MISMATCH_CORRELATIONID
        $ERROR_NO_ERROR
        $ERROR_NO_KNOWN_BROKERS
        $ERROR_NOT_BINARY_STRING
        $ERROR_NOT_LEADER_FOR_PARTITION
        $ERROR_OFFSET_METADATA_TOO_LARGE_CODE
        $ERROR_OFFSET_OUT_OF_RANGE
        $ERROR_PARTITION_DOES_NOT_MATCH
        $ERROR_REPLICA_NOT_AVAILABLE
        $ERROR_REQUEST_OR_RESPONSE
        $ERROR_REQUEST_TIMED_OUT
        $ERROR_STALE_CONTROLLER_EPOCH_CODE
        $ERROR_TOPIC_DOES_NOT_MATCH
        $ERROR_UNKNOWN
        $ERROR_UNKNOWN_APIKEY
        $ERROR_UNKNOWN_TOPIC_OR_PARTITION
        $KAFKA_SERVER_PORT
        $MIN_BYTES_RESPOND_HAS_DATA
        $MIN_BYTES_RESPOND_IMMEDIATELY
        $NOT_SEND_ANY_RESPONSE
        $RECEIVE_EARLIEST_OFFSETS
        $RECEIVE_LATEST_OFFSET
        $REQUEST_TIMEOUT
        $RETRY_BACKOFF
        $SEND_MAX_RETRIES
        $WAIT_WRITTEN_TO_LOCAL_LOG
    ) ];

    $functions_Kafka = [ qw(
    ) ];

    use_ok 'Kafka',
        @$constants_Kafka,
        @$functions_Kafka,
    ;
}

# Kafka::Internals

BEGIN {
    $constants_Kafka_Internals = [ qw(
        $APIKEY_PRODUCE
        $APIKEY_FETCH
        $APIKEY_OFFSET
        $APIKEY_METADATA
        $MAX_SOCKET_REQUEST_BYTES
        $PRODUCER_ANY_OFFSET
    ) ];

    $functions_Kafka_Internals = [ qw(
        _get_CorrelationId
        _isbig
    ) ];

    use_ok 'Kafka::Internals',
        @$constants_Kafka_Internals,
        @$functions_Kafka_Internals,
    ;
}

# Kafka::TestInternals

BEGIN {
    $constants_Kafka_TestInternals = [ qw(
        @not_array
        @not_array0
        @not_empty_string
        @not_hash
        @not_is_like_server_list
        @not_isint
        @not_nonnegint
        @not_number
        @not_posint
        @not_posnumber
        @not_right_object
        @not_string
        @not_string_array
        @not_topics_array
        $topic
    ) ];

    $functions_Kafka_TestInternals = [ qw(
        _is_suitable_int
    ) ];

    use_ok 'Kafka::TestInternals',
        @$constants_Kafka_TestInternals,
        @$functions_Kafka_TestInternals,
    ;
}

# Kafka::Protocol

BEGIN {
    $constants_Kafka_Protocol = [ qw(
        $APIVERSION
        $BAD_OFFSET
        $COMPRESSION_NONE
        $CONSUMERS_REPLICAID
        $NULL_BYTES_LENGTH
        $_int64_template
    ) ];

    $functions_Kafka_Protocol = [ qw(
        decode_fetch_response
        decode_metadata_response
        decode_offset_response
        decode_produce_response
        encode_fetch_request
        encode_metadata_request
        encode_offset_request
        encode_produce_request
        _decode_MessageSet_template
        _decode_MessageSet_array
        _encode_MessageSet_array
        _encode_string
        _pack64
        _unpack64
    ) ];

    use_ok 'Kafka::Protocol',
        @$constants_Kafka_Protocol,
        @$functions_Kafka_Protocol,
    ;
}

# Kafka::MockProtocol

BEGIN {
    $constants_Kafka_MockProtocol = [ qw(
    ) ];

    $functions_Kafka_MockProtocol = [ qw(
        decode_fetch_request
        decode_metadata_request
        decode_offset_request
        decode_produce_request
        encode_fetch_response
        encode_metadata_response
        encode_offset_response
        encode_produce_response
    ) ];

    use_ok 'Kafka::MockProtocol',
        @$constants_Kafka_MockProtocol,
        @$functions_Kafka_MockProtocol,
    ;
}

# Kafka::IO

BEGIN {
    $ours_Kafka_IO = [ qw (
        DEBUG
        _hdr
    ) ];

    $methods_Kafka_IO = [ qw(
        close
        is_alive
        new
        receive
        send
    ) ];

    use_ok 'Kafka::IO';
}

# Kafka::MockIO

BEGIN {
    $ours_Kafka_MockIO = [ qw(
        PARTITION
    ) ];

    $methods_Kafka_MockIO = [ qw(
        add_special_case
        close
        del_special_case
        is_alive
        new
        override
        receive
        restore
        send
        special_cases
    ) ];

    use_ok 'Kafka::MockIO',
        @$ours_Kafka_MockIO,
        @$methods_Kafka_MockIO,
    ;
}

# Kafka::Connection

BEGIN {
    $ours_Kafka_Connection = [ qw (
    ) ];

    $methods_Kafka_Connection = [ qw(
        close
        close_connection
        get_known_servers
        is_server_alive
        is_server_known
        new
        receive_response_to_request
    ) ];

    use_ok 'Kafka::Connection';
}

# Kafka::Message

BEGIN {
    $ours_Kafka_Message = [ qw (
        _standard_fields
    ) ];

    $methods_Kafka_Message = [ qw(
        Attributes
        error
        HighwaterMarkOffset
        key
        MagicByte
        next_offset
        payload
        offset
        valid
    ) ];

    use_ok 'Kafka::Message';
}

# Kafka::Consumer

BEGIN {
    $ours_Kafka_Consumer = [ qw (
    ) ];

    $methods_Kafka_Consumer = [ qw(
        fetch
        new
        offsets
    ) ];

    use_ok 'Kafka::Consumer';
}

# Kafka::Producer

BEGIN {
    $ours_Kafka_Producer = [ qw (
    ) ];

    $methods_Kafka_Producer = [ qw(
        new
        send
    ) ];

    use_ok 'Kafka::Producer';
}

# Kafka::Cluster

BEGIN {
    $ours_Kafka_Cluster = [ qw (
        DEFAULT_TOPIC
        START_PORT
    ) ];

    $methods_Kafka_Cluster = [ qw(
        base_dir
        close
        init
        is_run_in_base_dir
        log_dir
        new
        node_id
        request
        servers
        start
        stop
        zookeeper_port
    ) ];

    use_ok 'Kafka::Cluster';
}

#-- Verify that the simple module has the necessary API

foreach my $module ( keys %ordinary ) {
    # verify import the constants
    my $value;
    ok( defined( $value = eval( "$_" ) ), "import OK: $_ = $value" ) for @{ $ordinary{ $module }->[0] };    ## no critic

    # verify import of functions
    can_ok( __PACKAGE__, $_ ) for @{ $ordinary{ $module }->[1] };
}

#-- Verify that the OO module has the necessary API

foreach my $module ( keys %OO ) {
    # verify import the our variables
    foreach my $name ( @{ $OO{ $module }->[0] } ) {
        my $var_name = "\$${module}::$name";
        ok( eval( "exists \$${module}::{$name}" ), "import OK: $var_name exists" ); ## no critic
    }

    # verify availability of methods
    can_ok( $module, $_ ) for @{ $OO{ $module }->[1] };
}

# POSTCONDITIONS ---------------------------------------------------------------
