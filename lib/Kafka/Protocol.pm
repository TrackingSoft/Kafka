package Kafka::Protocol;

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

use Exporter qw(
    import
);
our @EXPORT_OK = qw(
    decode_fetch_response
    decode_metadata_response
    decode_offset_response
    decode_produce_response
    encode_fetch_request
    encode_metadata_request
    encode_offset_request
    encode_produce_request
    last_error
    last_errorcode
    _protocol_error
    _decode_MessageSet_template
    _decode_MessageSet_array
    _encode_MessageSet_array
    _encode_string
    _is_hex_stream_correct
    _pack64
    _unpack64
    _verify_string
    $APIVERSION
    $BAD_OFFSET
    $COMPRESSION_CODEC_MASK
    $COMPRESSION_EXISTS
    $COMPRESSION_GZIP
    $COMPRESSION_NONE
    $COMPRESSION_NOT_EXIST
    $COMPRESSION_SNAPPY
    $CONSUMER_HAVE_NO_NODE_ID
    $CONSUMERS_REPLICAID
    $NULL_BYTES_LENGTH
    $_int64_template
);

our $VERSION = '0.8001';

#-- load the modules -----------------------------------------------------------

use Carp;
use Const::Fast;
use Params::Util qw(
    _ARRAY
    _ARRAY0
    _HASH
    _POSINT
    _SCALAR
    _STRING
);
use Scalar::Util qw(
    dualvar
);
use Scalar::Util::Numeric qw(
    isint
);
use String::CRC32;

use Kafka qw(
    $BITS64
    $BLOCK_UNTIL_IS_COMMITED
    $DEFAULT_MAX_WAIT_TIME
    %ERROR
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NO_ERROR
    $ERROR_NOT_BINARY_STRING
    $ERROR_REQUEST_OR_RESPONSE
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_EARLIEST_OFFSETS
    $RECEIVE_LATEST_OFFSET
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_METADATA
    $APIKEY_OFFSET
    $APIKEY_PRODUCE
    $PRODUCER_ANY_OFFSET
    _is_suitable_int
);

#-- declarations ---------------------------------------------------------------

=for Protocol Information

A Guide To The Kafka Protocol 0.8:
https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

-- Protocol Primitive Types
int8, int16, int32, int64
    Signed integers
    stored in big endian order.
bytes, string
    consist of a signed integer
    giving a length N
    followed by N bytes of content.
    A length of -1 indicates null.
    string uses an int16 for its size,
    and bytes uses an int32.
Arrays
    These will always be encoded as an int32 size containing the length N
    followed by N repetitions of the structure
    which can itself be made up of other primitive types.

-- N.B.
- The response will always match the paired request
- One structure common to both the produce and fetch requests is the message set format.
- MessageSets are not preceded by an int32 like other array elements in the protocol.
- A message set is also the unit of compression in Kafka,
    and we allow messages to recursively contain compressed message sets.

-- Protocol Fields
ApiKey => int16                 That identifies the API being invoked
ApiVersion => int16             This is a numeric version number for this api.
                                Currently the supported version for all APIs is 0.
Attributes => int8              Metadata attributes about the message.
                                In particular the last 3 bits contain the compression codec used for the message.
ClientId => string              This is a user supplied identifier for the client application.
CorrelationId => int32          This is a user-supplied integer.
                                It will be passed back in the response by the server, unmodified.
                                It is useful for matching request and response between the client and server.
Crc => int32                    The CRC32 of the remainder of the message bytes.
ErrorCode => int16              The error from this partition, if any.
                                Errors are given on a per-partition basis
                                    because a given partition may be unavailable or maintained on a different host,
                                    while others may have successfully accepted the produce request.
FetchOffset => int64            The offset to begin this fetch from.
HighwaterMarkOffset => int64    The offset at the end of the log for this partition.
                                This can be used by the client to determine how many messages behind the end of the log they are.
                                - 0.8 documents: Replication design
                                The high watermark (HW) is the offset of the last committed message.
                                Each log is periodically synced to disks.
                                Data before the flushed offset is guaranteed to be persisted on disks.
                                As we will see, the flush offset can be before or after HW.
                                - 0.7 documents: Wire protocol
                                If the last segment file for the partition is not empty and was modified earlier than TIME,
                                        it will return both the first offset for that segment and the high water mark.
                                The high water mark is not the offset of the last message,
                                        but rather the offset that the next message sent to the partition will be written to.
Host => string                  The brokers hostname
Isr => [ReplicaId]              The set subset of the replicas that are "caught up" to the leader - a set of in-sync replicas (ISR)
Key => bytes                    An optional message key
                                The key can be null.
Leader => int32                 The node id for the kafka broker currently acting as leader for this partition.
                                If no leader exists because we are in the middle of a leader election this id will be -1.
MagicByte => int8               A version id used to allow backwards compatible evolution of the message binary format.
                                0 = COMPRESSION attribute byte does not exist (v0.6 and below)
                                1 = COMPRESSION attribute byte exists (v0.7 and above)
                                (? 0 means that COMPRESSION is None)
MaxBytes => int32               The maximum bytes to include in the message set for this partition.
MaxNumberOfOffsets => int32     Kafka here is return up to 'MaxNumberOfOffsets' of offsets
MaxWaitTime => int32            The maximum amount of time (ms)
                                    to block waiting
                                    if insufficient data is available at the time the request is issued.
MessageSetSize => int32         The size in bytes of the message set for this partition
MessageSize => int32            The size of the subsequent request or response message in bytes
MinBytes => int32               The minimum number of bytes of messages that must be available to give a response.
                                If the client sets this to 0 the server will always respond immediately.
                                If this is set to 1,
                                    the server will respond as soon
                                    as at least one partition
                                    has at least 1 byte of data
                                    or the specified timeout occurs.
                                By setting higher values
                                    in combination with the timeout
                                    for reading only large chunks of data
NodeId => int32                 The id of the broker.
                                This must be set to a unique integer for each broker.
Offset => int64                 The offset used in kafka as the log sequence number.
                                When the producer is sending messages it doesn't actually know the offset
                                    and can fill in any value here it likes.
Partition => int32              The id of the partition the fetch is for
                                    or the partition that data is being published to
                                    or the partition this response entry corresponds to.
Port => int32                   The brokers port
ReplicaId => int32              Indicates the node id of the replica initiating this request.
                                Normal client consumers should always specify this as -1 as they have no node id.
Replicas => [ReplicaId]         The set of alive nodes that currently acts as slaves for the leader for this partition.
RequiredAcks => int16           Indicates how many acknowledgements the servers should receive
                                    before responding to the request.
                                If it is 0 the server does not send any response.
                                If it is 1, the server will wait the data is written to the local log before sending a response.
                                If it is -1 the server will block until the message is committed by all in sync replicas before sending a response.
                                For any number > 1 the server will block waiting for this number of acknowledgements to occur
                                (but the server will never wait for more acknowledgements than there are in-sync replicas).
Size => int32                   The size of the subsequent request or response message in bytes
Time => int64                   Used to ask for all messages before a certain time (ms).
                                There are two special values.
                                Specify -1 to receive the latest offset (this will only ever return one offset).
                                Specify -2 to receive the earliest available offsets.
Timeout => int32                This provides a maximum time (ms) the server can await the receipt
                                    of the number of acknowledgements in RequiredAcks.
                                The timeout is not an exact limit on the request time for a few reasons:
                                (1) it does not include network latency,
                                (2) the timer begins at the beginning of the processing of this request
                                    so if many requests are queued due to server overload
                                    that wait time will not be included,
                                (3) we will not terminate a local write
                                    so if the local write time exceeds this timeout it will not be respected.
                                To get a hard timeout of this type the client should use the socket timeout.
TopicName => string             The name of the topic.
Value => bytes                  The actual message contents
                                Kafka supports recursive messages in which case this may itself contain a message set.
                                The message can be null.

=cut

our $_int64_template;                           # Used to unpack a 64 bit number
if ( !$BITS64 ) {
    eval                                        ## no critic
    q{
        use Kafka::Int64;
        1;
    } or die "Cannot load Kafka::Int64 : $@";

    $_int64_template = q{a8};
}
else {
    $_int64_template = q{q>};
}

const our $APIVERSION                   => 0;       # RTFM: Currently the supported version for all APIs is 0

# MagicByte
const our $COMPRESSION_NOT_EXIST        => 0;
const our $COMPRESSION_EXISTS           => 1;

# Attributes
const our $COMPRESSION_CODEC_MASK       => 0b111;
#-- Codec numbers:
const our $COMPRESSION_NONE             => 0;
const our $COMPRESSION_GZIP             => 1;       # Not used now
const our $COMPRESSION_SNAPPY           => 2;       # Not used now

const our $CONSUMER_HAVE_NO_NODE_ID     => -1;

const our $CONSUMERS_REPLICAID          => -1;      # RTFM: Normal client consumers should always specify this as -1 as they have no node id

const our $NULL_BYTES_LENGTH            => -1;

const our $BAD_OFFSET                   => -1;

my $_package_error;

my ( $_Request_header_template,             $_Request_header_length ) = (
    q{
        l>                  # Size
        s>                  # 2 ApiKey
        s>                  # 2 ApiVersion
        l>                  # 4 CorrelationId
        s>                  # 2 ClientId length
    },
    10                      # 'Size' is not included in the calculation of length
);
my ( $_ProduceRequest_header_template,      $_ProduceRequest_header_length ) = (
    q{
        s>                  # 2 RequiredAcks
        l>                  # 4 Timeout
        l>                  # 4 topics array size
    },
    10
);
my ( $_MessageSet_template,                 $_MessageSet_length ) = (
    q{
        a8                  # 8 Offset
        l>                  # 4 MessageSize
    },
    12
);
my ( $_FetchRequest_header_template,        $_FetchRequest_header_length ) = (
    q{
        l>                  # 4 ReplicaId
        l>                  # 4 MaxWaitTime
        l>                  # 4 MinBytes
        l>                  # 4 topics array size
    },
    16
);
my ( $_FetchRequest_body_template,          $_FetchRequest_body_length ) = (
    q{
        l>                  # 4 Partition
        a8                  # 8 FetchOffset
        l>                  # 4 MaxBytes
    },
    16
);
my ( $_OffsetRequest_header_template,       $_OffsetRequest_header_length ) = (
    q{
        l>                  # 4 ReplicaId
        l>                  # 4 topics array size
    },
    8
);
my ( $_OffsetRequest_body_template,         $_OffsetRequest_body_length ) = (
    q{
        l>                  # 4 Partition
        a8                  # 8 Time
        l>                  # 4 MaxNumberOfOffsets
    },
    16
);
my ( $_FetchResponse_header_template,       $_FetchResponse_header_length ) = (
    q{
        x[l]                # Size (skip)
        l>                  # 4 CorrelationId
        l>                  # 4 topics array size
    },
    8
);
my ( $_Message_template,                    $_Message_length ) = (
    qq{
        $_int64_template    # 8 Offset
        l>                  # MessageSize
        l>                  # Crc
        c                   # MagicByte
        c                   # Attributes
        l>                  # Key length
    },
    8                       # Only Offset length
);
my ( $_FetchResponse_topic_body_template,   $_FetchResponse_topic_body_length )= (
    qq{
        s>/a                # TopicName
        l>                  # partitions array size
        l>                  # 4 Partition
        s>                  # 2 ErrorCode
        $_int64_template    # 8 HighwaterMarkOffset
    },
    14                      # without TopicName and partitions array size
);
my $_Key_or_Value_template = q{
    X[l]
    l>/a                    # Key or Value
};

#-- public functions -----------------------------------------------------------

# PRODUCE Request --------------------------------------------------------------

sub encode_produce_request {
    my ( $Produce_Request ) = @_;

    _HASH( $Produce_Request )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my (
        $template,
        $request_length,
        @data,
    );

    _encode_request_header( \@data, $APIKEY_PRODUCE, $Produce_Request, \$template, \$request_length )
        or return;
                                                                            # Size
                                                                            # ApiKey
                                                                            # ApiVersion
                                                                            # CorrelationId
                                                                            # ClientId

    ( defined( $Produce_Request->{RequiredAcks} ) && isint( $Produce_Request->{RequiredAcks} ) )    # RequiredAcks
        ? push( @data, $Produce_Request->{RequiredAcks} )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'RequiredAcks' );
    ( defined( $Produce_Request->{Timeout} ) && isint( $Produce_Request->{Timeout} ) )  # Timeout
        ? push( @data, $Produce_Request->{Timeout} )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Timeout' );
    my $topics_array = $Produce_Request->{topics};
    _ARRAY( $topics_array )                                                 # topics array size
        ? push( @data, scalar( @$topics_array ) )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );
    $template       .= $_ProduceRequest_header_template;
    $request_length += $_ProduceRequest_header_length;

    foreach my $topic ( @$topics_array ) {
        _HASH( $topic )
            or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );
        $template       .= qq{    s>      # 2 string length\n};             # string length
        $request_length += 2;
        _encode_string( $topic->{TopicName}, \$request_length, \@data, \$template, 'TopicName' )    # TopicName
            or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'TopicName ('.last_error().')' );

        my $partitions_array = $topic->{partitions};
        _ARRAY( $partitions_array )
            ? push( @data, scalar( @$partitions_array ) )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'partitions' );
        $template       .= qq{        l>      # 4 partitions array size\n}; # partitions array size
        $request_length += 4;
        foreach my $partition ( @$partitions_array ) {
            _HASH( $partition )
                or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'partitions' );
            ( defined( $partition->{Partition} ) && isint( $partition->{Partition} ) )
                ? push( @data, $partition->{Partition} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Partition' );
            $template .= qq{        l>      # 4 Partition\n};               # Partition
            $request_length += 4;

            _ARRAY( $partition->{MessageSet} )
                or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'partitions' );
            _encode_MessageSet_array( $partition->{MessageSet}, \$request_length, \@data, \$template )
                or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'MessageSet'.( last_error() ? ' ('.last_error().')' : q{} ) );
        }
    }

    return pack( $template, $request_length, @data );
}

# PRODUCE Response -------------------------------------------------------------

my $_decode_produce_response_template = qq{
    x[l]                    # Size (skip)
    l>                      # CorrelationId

    l>                      # topics array size
    X[l]
    l>/(                    # topics array
        s>/a                    # TopicName

        l>                      # partitions array size
        X[l]
        l>/(                    # partitions array
            l>                      # Partition
            s>                      # ErrorCode
            $_int64_template        # Offset
        )
    )
};

sub decode_produce_response {
    my ( $hex_stream_ref ) = @_;

    _is_hex_stream_correct( $hex_stream_ref )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data = unpack( $_decode_produce_response_template, $$hex_stream_ref );

    my ( $i, $Produce_Response ) = ( 0, {} );

    $Produce_Response->{CorrelationId}              =  $data[ $i++ ];   # CorrelationId

    my $topics_array = $Produce_Response->{topics}  =  [];
    my $topics_array_size                           =  $data[ $i++ ];   # topics array size
    while ( $topics_array_size-- ) {
        my $topic = {
            TopicName                               => $data[ $i++ ],
        };

        my $partitions_array = $topic->{partitions} =  [];
        my $partitions_array_size                   =  $data[ $i++ ];   # partitions array size
        while ( $partitions_array_size-- ) {
            my $partition = {
                Partition                           => $data[ $i++ ],   # Partition
                ErrorCode                           => $data[ $i++ ],   # ErrorCode
                Offset                   => _unpack64( $data[ $i++ ] ), # Offset
            };

            push( @$partitions_array, $partition );
        }

        push( @$topics_array, $topic );
    }

    return $Produce_Response;
}

# FETCH Request ----------------------------------------------------------------

sub encode_fetch_request {
    my ( $Fetch_Request ) = @_;

    _HASH( $Fetch_Request )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my (
        $template,
        $request_length,
        @data,
    );


    _encode_request_header( \@data, $APIKEY_FETCH, $Fetch_Request, \$template, \$request_length )
        or return;
                                                                            # Size
                                                                            # ApiKey
                                                                            # ApiVersion
                                                                            # CorrelationId
                                                                            # ClientId

    push( @data, $CONSUMERS_REPLICAID );                                    # ReplicaId
    ( defined( $Fetch_Request->{MaxWaitTime} ) && isint( $Fetch_Request->{MaxWaitTime} ) )   # MaxWaitTime
        ? push( @data, $Fetch_Request->{MaxWaitTime} )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'MaxWaitTime' );
    ( defined( $Fetch_Request->{MinBytes} ) && isint( $Fetch_Request->{MinBytes} ) ) # MinBytes
        ? push( @data, $Fetch_Request->{MinBytes} )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'MinBytes' );
    my $topics_array = $Fetch_Request->{topics};
    _ARRAY( $topics_array )                                                # topics array size
        ? push( @data, scalar( @$topics_array ) )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );
    $template       .= $_FetchRequest_header_template;
    $request_length += $_FetchRequest_header_length;

    foreach my $topic ( @$topics_array ) {
        _HASH( $topic )
            or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );
        $template       .= qq{    s>      # 2 string length\n};             # string length
        $request_length += 2;
        _encode_string( $topic->{TopicName}, \$request_length, \@data, \$template, 'TopicName' )    # TopicName
            or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'TopicName ('.last_error().')' );

        my $partitions_array = $topic->{partitions};
        _ARRAY( $partitions_array )
            ? push( @data, scalar( @$partitions_array ) )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'partitions' );
        $template .= q{l>};                                                 # partitions array size
        $request_length += 4;
        foreach my $partition ( @$partitions_array ) {
            _HASH( $partition )
                or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'partitions' );
            ( defined( $partition->{Partition} ) && isint( $partition->{Partition} ) )  # Partition
                ? push( @data, $partition->{Partition} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Partition' );
            _is_suitable_int( $partition->{FetchOffset} )                   # FetchOffset
                ? push( @data, _pack64( $partition->{FetchOffset} ) )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'FetchOffset' );
            ( defined( $partition->{MaxBytes} ) && isint( $partition->{MaxBytes} ) )    # MaxBytes
                ? push( @data, $partition->{MaxBytes} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'MaxBytes' );
            $template       .= $_FetchRequest_body_template;
            $request_length += $_FetchRequest_body_length;
        }
    }

    return pack( $template, $request_length, @data );
}

# FETCH Response ---------------------------------------------------------------

sub decode_fetch_response {
    my ( $hex_stream_ref ) = @_;

    _is_hex_stream_correct( $hex_stream_ref )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

# RTFM: As an optimization the server is allowed to return a partial message at the end of the message set.
# Clients should handle this case.
# NOTE: look inside _decode_MessageSet_template and _decode_MessageSet_array

    my @data = unpack( _decode_fetch_response_template( $hex_stream_ref ), $$hex_stream_ref );

    my ( $i, $Fetch_Response ) = ( 0, {} );

    $Fetch_Response->{CorrelationId}                        =  $data[ $i++ ];   # CorrelationId

    my $topics_array = $Fetch_Response->{topics}            =  [];
    my $topics_array_size                                   =  $data[ $i++ ];   # topics array size
    while ( $topics_array_size-- ) {
        my $topic = {
            TopicName                                       => $data[ $i++ ],
        };

        my $partitions_array = $topic->{partitions}         =  [];
        my $partitions_array_size                           =  $data[ $i++ ];   # partitions array size
        while ( $partitions_array_size-- ) {
            my $partition = {
                Partition                                   => $data[ $i++ ],   # Partition
                ErrorCode                                   => $data[ $i++ ],   # ErrorCode
                HighwaterMarkOffset              => _unpack64( $data[ $i++ ] ), # HighwaterMarkOffset
            };

            my $MessageSetSize                              =  $data[ $i++ ];   # MessageSetSize
            my $MessageSet_array = $partition->{MessageSet} =  [];

            _decode_MessageSet_array( $MessageSetSize, \@data, \$i, $MessageSet_array );

            push( @$partitions_array, $partition );
        }

        push( @$topics_array, $topic );
    }

    return $Fetch_Response;
}

# OFFSET Request ---------------------------------------------------------------

sub encode_offset_request {
    my ( $Offset_Request ) = @_;

    _HASH( $Offset_Request )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my (
        $template,
        $request_length,
        @data,
    );

    _encode_request_header( \@data, $APIKEY_OFFSET, $Offset_Request, \$template, \$request_length )
        or return;
                                                                            # Size
                                                                            # ApiKey
                                                                            # ApiVersion
                                                                            # CorrelationId
                                                                            # ClientId

    push( @data, $CONSUMERS_REPLICAID );                                    # ReplicaId
    my $topics_array = $Offset_Request->{topics};
    _ARRAY( $topics_array )                                                 # topics array size
        ? push( @data, scalar( @$topics_array ) )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );
    $template       .= $_OffsetRequest_header_template;
    $request_length += $_OffsetRequest_header_length;

    foreach my $topic ( @$topics_array ) {
        _HASH( $topic )
            or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );
        $template       .= qq{    s>      # 2 string length\n};             # string length
        $request_length += 2;
        _encode_string( $topic->{TopicName}, \$request_length, \@data, \$template, 'TopicName' )    # TopicName
            or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'TopicName ('.last_error().')' );

        my $partitions_array = $topic->{partitions};
        _ARRAY( $partitions_array )
            ? push( @data, scalar( @$partitions_array ) )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'partitions' );
        $template .= q{l>};                                                 # partitions array size
        $request_length += 4;   # [l] partitions array size
        foreach my $partition ( @$partitions_array ) {
            _HASH( $partition )
                or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'partitions' );
            ( defined( $partition->{Partition} ) && isint( $partition->{Partition} ) )  # Partition
                ? push( @data, $partition->{Partition} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Partition' );
            _is_suitable_int( $partition->{Time} )                          # Time
                ? push( @data, _pack64( $partition->{Time} ) )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Time' );
            ( defined( $partition->{MaxNumberOfOffsets} ) && isint( $partition->{MaxNumberOfOffsets} ) )    # MaxNumberOfOffsets
                ? push( @data, $partition->{MaxNumberOfOffsets} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'MaxNumberOfOffsets' );
            $template       .= $_OffsetRequest_body_template;
            $request_length += $_OffsetRequest_body_length;
        }
    }

    return pack( $template, $request_length, @data );
}

# OFFSET Response --------------------------------------------------------------

my $_decode_offset_response_template = qq{
    x[l]                    # Size (skip)
    l>                      # CorrelationId

    l>                      # topics array size
    X[l]
    l>/(                    # topics array
        s>/a                    # TopicName

        l>                      # PartitionOffsets array size
        X[l]
        l>/(                    # PartitionOffsets array
            l>                      # Partition
            s>                      # ErrorCode

            l>                      # Offset array size
            X[l]
            l>/(                    # Offset array
                $_int64_template        # Offset
            )
        )
    )
};

sub decode_offset_response {
    my ( $hex_stream_ref ) = @_;

    _is_hex_stream_correct( $hex_stream_ref )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data = unpack( $_decode_offset_response_template, $$hex_stream_ref );

    my ( $i, $Offset_Response ) = ( 0, {} );

    $Offset_Response->{CorrelationId}                           =  $data[ $i++ ];   # CorrelationId

    my $topics_array = $Offset_Response->{topics}               =  [];
    my $topics_array_size                                       =  $data[ $i++ ];   # topics array size
    while ( $topics_array_size-- ) {
        my $topic = {
            TopicName                                           => $data[ $i++ ],
        };

        my $PartitionOffsets_array = $topic->{PartitionOffsets} =  [];
        my $PartitionOffsets_array_size                         =  $data[ $i++ ];   # PartitionOffsets array size
        while ( $PartitionOffsets_array_size-- ) {
            my $PartitionOffset = {
                Partition                                       => $data[ $i++ ],   # Partition
                ErrorCode                                       => $data[ $i++ ],   # ErrorCode
            };

            my $Offset_array = $PartitionOffset->{Offset}       =  [];
            my $Offset_array_size                               =  $data[ $i++ ];   # Offset array size
            while ( $Offset_array_size-- ) {
                push( @$Offset_array,                   _unpack64( $data[ $i++ ] ) );   # Offset
            }

            push( @$PartitionOffsets_array, $PartitionOffset );
        }

        push( @$topics_array, $topic );
    }

    return $Offset_Response;
}

# METADATA Request -------------------------------------------------------------

sub encode_metadata_request {
    my ( $Metadata_Request ) = @_;

    _HASH( $Metadata_Request )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my (
        $template,
        $request_length,
        @data,
    );

    _encode_request_header( \@data, $APIKEY_METADATA, $Metadata_Request, \$template, \$request_length )
        or return;
                                                                            # Size
                                                                            # ApiKey
                                                                            # ApiVersion
                                                                            # CorrelationId
                                                                            # ClientId

    my $topics_array = $Metadata_Request->{topics};
    _ARRAY( $topics_array )                                                 # topics array size
        ? push( @data, scalar( @$topics_array ) )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );
    $template .= q{l>};
    $request_length += 4;

    foreach my $topic ( @$topics_array ) {
        $template       .= qq{    s>      # 2 string length\n};             # string length
        $request_length += 2;
        _encode_string( $topic, \$request_length, \@data, \$template, 'TopicName' )  # TopicName
            or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'TopicName ('.last_error().')' );
    }

    return pack( $template, $request_length, @data );
}

# METADATA Response ------------------------------------------------------------

my $_decode_metadata_response_template = q{
    x[l]                    # Size (skip)
    l>                      # CorrelationId

    l>                      # Broker array size
    X[l]
    l>/(                    # Broker array
        l>                      # NodeId
        s>/a                    # Host
        l>                      # Port
    )

    l>                      # TopicMetadata array size
    X[l]
    l>/(                    # TopicMetadata array
        s>                      # ErrorCode
        s>/a                    # TopicName

        l>                      # PartitionMetadata array size
        X[l]
        l>/(                    # PartitionMetadata array
            s>                      # ErrorCode
            l>                      # Partition
            l>                      # Leader

            l>                      # Replicas array size
            X[l]
            l>/(                    # Replicas array
                l>                      # ReplicaId
            )

            l>                      # Isr array size
            X[l]
            l>/(                    # Isr array
                l>                      # ReplicaId
            )
        )
    )
};

sub decode_metadata_response {
    my ( $hex_stream_ref ) = @_;

    _is_hex_stream_correct( $hex_stream_ref )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data = unpack( $_decode_metadata_response_template, $$hex_stream_ref );

    my ( $i, $Metadata_Response ) = ( 0, {} );

    $Metadata_Response->{CorrelationId}                           =  $data[ $i++ ];   # CorrelationId

    my $Broker_array = $Metadata_Response->{Broker}               =  [];
    my $Broker_array_size                                         =  $data[ $i++ ];   # Broker array size
    while ( $Broker_array_size-- ) {
        push( @$Broker_array, {
            NodeId                                                => $data[ $i++ ],   # NodeId
            Host                                                  => $data[ $i++ ],   # Host
            Port                                                  => $data[ $i++ ],   # Port
            }
        );
    }

    my $TopicMetadata_array = $Metadata_Response->{TopicMetadata} =  [];
    my $TopicMetadata_array_size                                  =  $data[ $i++ ];   # TopicMetadata array size
    while ( $TopicMetadata_array_size-- ) {
        my $TopicMetadata = {
            ErrorCode                                             => $data[ $i++ ],   # ErrorCode
            TopicName                                             => $data[ $i++ ],   # TopicName
        };

        my $PartitionMetadata_array = $TopicMetadata->{PartitionMetadata} =  [];
        my $PartitionMetadata_array_size                          =  $data[ $i++ ];   # PartitionMetadata array size
        while ( $PartitionMetadata_array_size-- ) {
            my $PartitionMetadata = {
                ErrorCode                                         => $data[ $i++ ],   # ErrorCode
                Partition                                         => $data[ $i++ ],   # Partition
                Leader                                            => $data[ $i++ ],   # Leader
            };

            my $Replicas_array = $PartitionMetadata->{Replicas}   =  [];
            my $Replicas_array_size                               =  $data[ $i++ ];   # Replicas array size
            while ( $Replicas_array_size-- ) {
                push( @$Replicas_array,                              $data[ $i++ ] ); # ReplicaId
            }

            my $Isr_array = $PartitionMetadata->{Isr}             =  [];
            my $Isr_array_size                                    =  $data[ $i++ ];   # Isr array size
            while ( $Isr_array_size-- ) {
                push( @$Isr_array,                                   $data[ $i++ ] ); # ReplicaId
            }

            push( @$PartitionMetadata_array, $PartitionMetadata );
        }

        push( @$TopicMetadata_array, $TopicMetadata );
    }

    return $Metadata_Response;
}

sub last_error {
    return $_package_error.q{};
}

sub last_errorcode {
    return $_package_error + 0;
}

#-- private functions ----------------------------------------------------------

sub _encode_request_header {
    my ( $data_array_ref, $api_key, $request_ref, $template_ref, $request_length_ref ) = @_;

    push @$data_array_ref, (
                                                                            # Size
        $api_key,                                                           # ApiKey
        $APIVERSION,                                                        # ApiVersion
    );
    ( defined( $request_ref->{CorrelationId} ) && isint( $request_ref->{CorrelationId} ) )  # CorrelationId
        ? push( @$data_array_ref, $request_ref->{CorrelationId} )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'CorrelationId' );
    $$template_ref          = $_Request_header_template;
    $$request_length_ref    = $_Request_header_length;
    _encode_string( $request_ref->{ClientId}, $request_length_ref, $data_array_ref, $template_ref, 'ClientId' ) # ClientId
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ClientId ('.last_error().')' );

    return 1;
}

sub _decode_fetch_response_template {
    my ( $hex_stream_ref ) = @_;

    my (
        $template,
        $stream_offset,
        $ClientId_length,
        $topics_array_size,
        $TopicName_length,
        $partitions_array_size,
    );

    $template       = $_FetchResponse_header_template;
    $stream_offset  = $_FetchResponse_header_length;    # bytes before topics array size
                                                                                # [l] Size
                                                                                # [l] CorrelationId
    $topics_array_size = unpack( qq{
        x$stream_offset
        l>                              # topics array size
        }, $$hex_stream_ref
    );
    $stream_offset += 4;                # bytes before TopicName length
                                                                                # [l] topics array size

    while ( $topics_array_size-- ) {
        $TopicName_length = unpack( qq{
            x$stream_offset
            s>                          # TopicName length
            }, $$hex_stream_ref
        );
        $stream_offset +=               # bytes before partitions array size
              2                                                                 # [s] TopicName length
            + $TopicName_length                                                 # TopicName
            ;
        $partitions_array_size = unpack( qq{
            x$stream_offset
            l>                          # partitions array size
            }, $$hex_stream_ref
        );
        $stream_offset += 4;            # bytes before Partition
                                                                                # [l] partitions array size

        $template       .= $_FetchResponse_topic_body_template;
        $stream_offset  += $_FetchResponse_topic_body_length;   # (without TopicName and partitions array size)
                                        # bytes before MessageSetSize
                                                                                # TopicName
                                                                                # [l] # partitions array size
                                                                                # [l] Partition
                                                                                # [s] ErrorCode
                                                                                # [q] HighwaterMarkOffset

        _decode_MessageSet_template( $hex_stream_ref, $stream_offset, \$template );
    }

    return $template;
}

sub _decode_MessageSet_array {
    my ( $MessageSetSize, $data_array_ref, $i_ref, $MessageSet_array_ref ) = @_;

    my $data_array_size = scalar @$data_array_ref;

# NOTE: not all messages can be returned
    while ( $MessageSetSize && $$i_ref < $data_array_size ) {

        my $Message = {
            Offset                   => _unpack64( $data_array_ref->[ $$i_ref++ ] ),    # Offset
        };

        my $MessageSize                         =  $data_array_ref->[ $$i_ref++ ];      # MessageSize
# NOTE: The CRC is the CRC32 of the remainder of the message bytes.
# This is used to check the integrity of the message on the broker and consumer:
# MagicByte + Attributes + Key length + Key + Value length + Value
        my $Crc                                 =  $data_array_ref->[ $$i_ref++ ];      # Crc
# WARNING: The current version of the module does not support the following:
# A message set is also the unit of compression in Kafka,
# and we allow messages to recursively contain compressed message sets to allow batch compression.
        $Message->{MagicByte}                   =  $data_array_ref->[ $$i_ref++ ];      # MagicByte
        $Message->{Attributes}                  =  $data_array_ref->[ $$i_ref++ ];      # Attributes

        my $Key_length                          =  $data_array_ref->[ $$i_ref++ ];      # Key length
        $Message->{Key}   = $Key_length   == $NULL_BYTES_LENGTH ? q{} : $data_array_ref->[ $$i_ref++ ];      # Key
        my $Value_length                        =  $data_array_ref->[ $$i_ref++ ];      # Value length
        $Message->{Value} = $Value_length == $NULL_BYTES_LENGTH ? q{} : $data_array_ref->[ $$i_ref++ ];      # Value

        push( @$MessageSet_array_ref, $Message );

        $MessageSetSize -= 12
                                    # [q] Offset
                                    # [l] MessageSize
            + $MessageSize          # Message
            ;
    }
}

sub _encode_MessageSet_array {
    my ( $MessageSet_array_ref, $length_ref, $data_array_ref, $template_ref ) = @_;

#    _ARRAY0( $MessageSet_array_ref )
#        or return;

    my $MessageSetSize = 0;
    my $MessageSize;
    foreach my $MessageSet ( @$MessageSet_array_ref ) {
        _HASH( $MessageSet )
            or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'MessageSet' );
        $MessageSize = _get_MessageSize( $MessageSet )
            or return;
        $MessageSetSize +=
              12                # [q] Offset
                                # [l] MessageSize
            + $MessageSize      # MessageSize
            ;
    }
    push( @$data_array_ref, $MessageSetSize );
    $$template_ref .= q{        l>      # 4 MessageSetSize};                    # MessageSetSize
    $$length_ref    += 4;

    foreach my $MessageSet ( @$MessageSet_array_ref ) {
        _is_suitable_int( $MessageSet->{Offset} )                               # Offset (It may be $PRODUCER_ANY_OFFSET)
            ? push( @$data_array_ref, _pack64( $MessageSet->{Offset} ) )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Offset' );
        $MessageSize = _get_MessageSize( $MessageSet )
            or return;
        push( @$data_array_ref, $MessageSize );                                 # MessageSize
        $$template_ref  .= $_MessageSet_template;
        $$length_ref    += $_MessageSet_length;

        my $message_body = _get_encoded_Message_body( $MessageSet )
            or return;
        push( @$data_array_ref, crc32( $message_body ), $message_body );
        $$template_ref .= q{
            l>  # 4 Crc
            a}.(
                $MessageSize    # Message
                - 4                                                             # Crc
                                # Message body:
                                                                                # MagicByte
                                                                                # Attributes
                                                                                # Key length
                                                                                # Key
                                                                                # Value length
                                                                                # Value
            )
            .qq{ # Message\n};
        $$length_ref += $MessageSize;   # Message
    }

    return 1;
}

sub _get_MessageSize {
    my ( $MessageSet ) = @_;

    my $MessageSize = 10;
                                                # [l] Crc
                                                # [c] MagicByte
                                                # [c] Attributes
                                                # [l] Key length
    if ( defined( my $Key = $MessageSet->{Key} ) ) {
        _verify_string( $Key, 'Key' )
            or return;
        $MessageSize += length $Key;            # Key
    }
    $MessageSize += 4;                          # [l] Value length
    if ( defined( my $Value = $MessageSet->{Value} ) ) {
        _verify_string( $Value, 'Value' )
            or return;
        $MessageSize += length $Value;          # Value
    }

    return $MessageSize;
}

sub _get_encoded_Message_body {
    my ( $MessageSet ) = @_;

    my ( $key_length, $value_length );

    my $Key = $MessageSet->{Key};
    if ( !defined( $Key ) || $Key eq q{} ) {
        $key_length = 0;
    }
    else {
        _verify_string( $Key, 'Key' )
            or return;
        $key_length = length $Key;
    }
    my $Value = $MessageSet->{Value};
    if ( !defined( $Value ) || $Value eq q{} ) {
        $value_length = 0;
    }
    else {
        _verify_string( $Value, 'Value' )
            or return;
        $value_length = length $MessageSet->{Value};
    }

    return pack( q{
            c                                                                   # MagicByte
            c                                                                   # Attributes
            l>                                                                  # Key length
            }.( $key_length     ? qq{a$key_length}   : q{} )                     # Key
            .q{l>}                                                              # Value length
            .( $value_length   ? qq{a$value_length} : q{} )                     # Value
        ,
        $COMPRESSION_NOT_EXIST,
        $COMPRESSION_NONE,      # RTFM: last 3 bits contain the compression codec
                                # The other bits are not described in the documentation
        $key_length     ? ( $key_length,    $Key )    : ( -1 ),
        $value_length   ? ( $value_length,  $Value )  : ( -1 ),
    );
}

sub _decode_MessageSet_template {
    my ( $hex_stream_ref, $stream_offset, $template_ref ) = @_;

    my (
        $MessageSetSize,
        $MessageSize,
        $Key_length,
        $Value_length,
    );

    my $hex_stream_length = length $$hex_stream_ref;

    $MessageSetSize = unpack( qq{
        x$stream_offset
        l>                          # MessageSetSize
        }, $$hex_stream_ref
    );
    $$template_ref .= q{
            l>                      # MessageSetSize
        };
    $stream_offset += 4;            # bytes before Offset
                                                                                # [l] MessageSetSize

    CREATE_TEMPLATE:
    while ( $MessageSetSize ) {
# Not the full MessageSet
        last CREATE_TEMPLATE if $MessageSetSize < 22;
                # [q] Offset
                # [l] MessageSize
                # [l] Crc
                # [c] MagicByte
                # [c] Attributes
                # [l] Key length
                # [l] Value length

        my $local_template = q{};
        MESSAGE_SET:
        {
            $local_template .= $_Message_template;
            $stream_offset  += $_Message_length;    # (Only Offset length)
                # [q] Offset
                # [l] MessageSize
                # [l] Crc
                # [c] MagicByte
                # [c] Attributes
                # [l] Key length
                                        # bytes before MessageSize
                                                                                # [q] Offset
            $MessageSize = unpack( qq{
                x$stream_offset
                l>                      # MessageSize
                }, $$hex_stream_ref
            );

            $stream_offset += 10;       # bytes before Crc
                                                                                # [l] MessageSize
                                        # bytes before Key length
                                                                                # [l] Crc
                                                                                # [c] MagicByte
                                                                                # [c] Attributes
            $Key_length = unpack( qq{
                x$stream_offset
                l>                      # Key length
                }, $$hex_stream_ref
            );

            $stream_offset += 4;        # bytes before Key or Value length
                                                                                # [l] Key length
            $stream_offset += $Key_length   # bytes before Key
                if $Key_length != $NULL_BYTES_LENGTH;                           # Key
            if ( $hex_stream_length >= $stream_offset + 4 ) {   # + [l] Value length
                $local_template .= $_Key_or_Value_template
                    if $Key_length != $NULL_BYTES_LENGTH;
            }
            else {
# Not the full MessageSet
                $local_template = q{};
                last MESSAGE_SET;
            }

            $local_template .= q{
                l>                      # Value length
            };
            $Value_length = unpack( qq{
                x$stream_offset
                l>                      # Value length
                }, $$hex_stream_ref
            );
            $stream_offset +=           # bytes before Value or next Message
                  4                                                             # [l] Value length
                ;
            $stream_offset += $Value_length # bytes before next Message
                if $Value_length != $NULL_BYTES_LENGTH;                         # Value
            if ( $hex_stream_length >= $stream_offset ) {
                $local_template .= $_Key_or_Value_template
                    if $Value_length != $NULL_BYTES_LENGTH;
            }
            else {
# Not the full MessageSet
                $local_template = q{};
                last MESSAGE_SET;
            }
        }

        if ( $local_template ) {
            $$template_ref .= $local_template;
            $MessageSetSize -= 12
                                        # [q] Offset
                                        # [l] MessageSize
                + $MessageSize          # Message
                ;
        }
        else {
            last CREATE_TEMPLATE;
        }
    }
}

sub _encode_string {
    my ( $string, $length_ref, $data_array_ref, $template_ref, $description ) = @_;

    if ( !defined( $string ) || $string eq q{} ) {
        push( @$data_array_ref, 0 );
    }
    else {
        _verify_string( $string, $description )
            or return;
        my $string_length = length $string;
        push( @$data_array_ref, $string_length, $string );
        $$template_ref  .= "        a*      # string\n";
        $$length_ref    += $string_length;
    }

    return 1;
}

sub _unpack64 {
    my ( $value ) = @_;

    return( $BITS64 ? $value : Kafka::Int64::unpackq( $value ) );
}

sub _pack64 {
    my ( $value ) = @_;

    return $BITS64 ? pack( q{q>}, $value ) : Kafka::Int64::packq( $value )
}

sub _is_hex_stream_correct {
    my ( $hex_stream_ref ) = @_;

    return _SCALAR( $hex_stream_ref ) && _STRING( $$hex_stream_ref );
}

sub _protocol_error {
    my ( $error_code, $description ) = @_;

    $_package_error = dualvar $error_code, $ERROR{ $error_code }.( $description ? ': '.$description : q{} );
    return;
}

sub _verify_string {
    my ( $string, $description ) = @_;

    return 1
        if defined( $string ) && $string eq q{};
    defined( _STRING( $string ) )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT, $description // () );
    utf8::is_utf8( $string )
        and return _protocol_error( $ERROR_NOT_BINARY_STRING );

    return 1;
}

1;

__END__
