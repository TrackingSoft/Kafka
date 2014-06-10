package Kafka::Protocol;

=head1 NAME

Kafka::Protocol - Functions to process messages in the Apache Kafka protocol.

=head1 VERSION

This documentation refers to C<Kafka::Protocol> version 0.8008_1 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.8008_1';

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
    _decode_MessageSet_template
    _decode_MessageSet_array
    _encode_MessageSet_array
    _encode_string
    _pack64
    _unpack64
    _verify_string
    $APIVERSION
    $BAD_OFFSET
    $COMPRESSION_CODEC_MASK
    $CONSUMERS_REPLICAID
    $NULL_BYTES_LENGTH
    $_int64_template
);

#-- load the modules -----------------------------------------------------------

use Compress::Snappy;
use Const::Fast;
use IO::Compress::Gzip qw(
    gzip
    $GzipError
);
use IO::Uncompress::Gunzip qw(
    gunzip
    $GunzipError
);
use Params::Util qw(
    _ARRAY
    _HASH
    _SCALAR
    _STRING
);
use Scalar::Util qw(
    dualvar
);
use String::CRC32;

use Kafka qw(
    $BITS64
    $BLOCK_UNTIL_IS_COMMITTED
    $COMPRESSION_GZIP
    $COMPRESSION_NONE
    $COMPRESSION_SNAPPY
    $DEFAULT_MAX_WAIT_TIME
    %ERROR
    $ERROR_COMPRESSION
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NOT_BINARY_STRING
    $ERROR_REQUEST_OR_RESPONSE
    $NOT_SEND_ANY_RESPONSE
    $RECEIVE_EARLIEST_OFFSETS
    $RECEIVE_LATEST_OFFSET
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Exceptions;
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_METADATA
    $APIKEY_OFFSET
    $APIKEY_PRODUCE
    $PRODUCER_ANY_OFFSET
);

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Data::Compare;
    use Kafka qw(
        $COMPRESSION_NONE
        $ERROR_NO_ERROR
        $REQUEST_TIMEOUT
        $WAIT_WRITTEN_TO_LOCAL_LOG
    );
    use Kafka::Internals qw(
        $PRODUCER_ANY_OFFSET
    );
    use Kafka::Protocol qw(
        decode_produce_response
        encode_produce_request
    );

    # a encoded produce request hex stream
    my $encoded = pack( q{H*}, '00000049000000000000000400000001000005dc0000000100076d79746f7069630000000100000000000000200000000000000000000000148dc795a20000ffffffff0000000648656c6c6f21' );

    # a decoded produce request
    my $decoded = {
        CorrelationId                       => 4,
        ClientId                            => q{},
        RequiredAcks                        => $WAIT_WRITTEN_TO_LOCAL_LOG,
        Timeout                             => $REQUEST_TIMEOUT * 100,  # ms
        topics                              => [
            {
                TopicName                   => 'mytopic',
                partitions                  => [
                    {
                        Partition           => 0,
                        MessageSet              => [
                            {
                                Offset          => $PRODUCER_ANY_OFFSET,
                                MagicByte       => 0,
                                Attributes      => $COMPRESSION_NONE,
                                Key             => q{},
                                Value           => 'Hello!',
                            },
                        ],
                    },
                ],
            },
        ],
    };

    my $encoded_request = encode_produce_request( $decoded );
    say 'encoded correctly' if $encoded_request eq $encoded;

    # a encoded produce response hex stream
    $encoded = pack( q{H*}, '00000023000000040000000100076d79746f706963000000010000000000000000000000000000' );

    # a decoded produce response
    $decoded = {
        CorrelationId                           => 4,
        topics                                  => [
            {
                TopicName                       => 'mytopic',
                partitions                      => [
                    {
                        Partition               => 0,
                        ErrorCode               => $ERROR_NO_ERROR,
                        Offset                  => 0,
                    },
                ],
            },
        ],
    };

    my $decoded_response = decode_produce_response( \$encoded );
    say 'decoded correctly' if Compare( $decoded_response, $decoded );

    # more examples, see t/*_decode_encode.t

=head1 DESCRIPTION

This module is not a user module.

In order to achieve better performance,
functions of this module do not perform arguments validation.

The main features of the C<Kafka::Protocol> module are:

=over 3

=item *

Supports parsing the Apache Kafka protocol.

=item *

Supports Apache Kafka Requests and Responses (PRODUCE and FETCH).
Within this package we currently support
access to PRODUCE, FETCH, OFFSET, METADATA Requests and Responses.

=item *

Support for working with 64 bit elements of the Kafka protocol on 32 bit systems.

=back

=cut

# A Guide To The Kafka Protocol 0.8:
# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
#
# -- Protocol Primitive Types
# int8, int16, int32, int64
#     Signed integers
#     stored in big endian order.
# bytes, string
#     consist of a signed integer
#     giving a length N
#     followed by N bytes of content.
#     A length of -1 indicates null.
#     string uses an int16 for its size,
#     and bytes uses an int32.
# Arrays
#     These will always be encoded as an int32 size containing the length N
#     followed by N repetitions of the structure
#     which can itself be made up of other primitive types.
#
# -- N.B.
# - The response will always match the paired request
# - One structure common to both the produce and fetch requests is the message set format.
# - MessageSets are not preceded by an int32 like other array elements in the protocol.
# - A message set is also the unit of compression in Kafka,
#     and we allow messages to recursively contain compressed message sets.
#
# -- Protocol Fields
# ApiKey => int16                 That identifies the API being invoked
# ApiVersion => int16             This is a numeric version number for this api.
#                                Currently the supported version for all APIs is 0.
# Attributes => int8              Metadata attributes about the message.
#                                 The lowest 2 bits contain the compression codec used for the message.
# ClientId => string              This is a user supplied identifier for the client application.
# CorrelationId => int32          This is a user-supplied integer.
#                                 It will be passed back in the response by the server, unmodified.
#                                 It is useful for matching request and response between the client and server.
# Crc => int32                    The CRC32 of the remainder of the message bytes.
# ErrorCode => int16              The error from this partition, if any.
#                                 Errors are given on a per-partition basis
#                                     because a given partition may be unavailable or maintained on a different host,
#                                     while others may have successfully accepted the produce request.
# FetchOffset => int64            The offset to begin this fetch from.
# HighwaterMarkOffset => int64    The offset at the end of the log for this partition.
#                                 This can be used by the client to determine how many messages behind the end of the log they are.
#                                 - 0.8 documents: Replication design
#                                 The high watermark is the offset of the last committed message.
#                                 Each log is periodically synced to disks.
#                                 Data before the flushed offset is guaranteed to be persisted on disks.
#                                 As we will see, the flush offset can be before or after high watermark.
#                                 - 0.7 documents: Wire protocol
#                                 If the last segment file for the partition is not empty and was modified earlier than TIME,
#                                         it will return both the first offset for that segment and the high water mark.
#                                 The high water mark is not the offset of the last message,
#                                         but rather the offset that the next message sent to the partition will be written to.
# Host => string                  The brokers hostname
# Isr => [ReplicaId]              The set subset of the replicas that are "caught up" to the leader - a set of in-sync replicas (ISR)
# Key => bytes                    An optional message key
#                                 The key can be null.
# Leader => int32                 The node id for the kafka broker currently acting as leader for this partition.
#                                 If no leader exists because we are in the middle of a leader election this id will be -1.
# MagicByte => int8               A version id used to allow backwards compatible evolution of the message binary format.
# MaxBytes => int32               The maximum bytes to include in the message set for this partition.
# MaxNumberOfOffsets => int32     Kafka here is return up to 'MaxNumberOfOffsets' of offsets
# MaxWaitTime => int32            The maximum amount of time (ms)
#                                     to block waiting
#                                     if insufficient data is available at the time the request is issued.
# MessageSetSize => int32         The size in bytes of the message set for this partition
# MessageSize => int32            The size of the subsequent request or response message in bytes
# MinBytes => int32               The minimum number of bytes of messages that must be available to give a response.
#                                 If the client sets this to 0 the server will always respond immediately.
#                                 If this is set to 1,
#                                     the server will respond as soon
#                                     as at least one partition
#                                     has at least 1 byte of data
#                                     or the specified timeout occurs.
#                                 By setting higher values
#                                     in combination with the timeout
#                                     for reading only large chunks of data
# NodeId => int32                 The id of the broker.
#                                 This must be set to a unique integer for each broker.
# Offset => int64                 The offset used in kafka as the log sequence number.
#                                 When the producer is sending messages it doesn't actually know the offset
#                                     and can fill in any value here it likes.
# Partition => int32              The id of the partition the fetch is for
#                                     or the partition that data is being published to
#                                     or the partition this response entry corresponds to.
# Port => int32                   The brokers port
# ReplicaId => int32              Indicates the node id of the replica initiating this request.
#                                 Normal client consumers should always specify this as -1 as they have no node id.
# Replicas => [ReplicaId]         The set of alive nodes that currently acts as slaves for the leader for this partition.
# RequiredAcks => int16           Indicates how many acknowledgements the servers should receive
#                                     before responding to the request.
#                                 If it is 0 the server does not send any response.
#                                 If it is 1, the server will wait the data is written to the local log before sending a response.
#                                 If it is -1 the server will block until the message is committed by all in sync replicas before sending a response.
#                                 For any number > 1 the server will block waiting for this number of acknowledgements to occur
#                                 (but the server will never wait for more acknowledgements than there are in-sync replicas).
# Size => int32                   The size of the subsequent request or response message in bytes
# Time => int64                   Used to ask for all messages before a certain time (ms).
#                                 There are two special values.
#                                 Specify -1 to receive the latest offset (this will only ever return one offset).
#                                 Specify -2 to receive the earliest available offsets.
# Timeout => int32                This provides a maximum time (ms) the server can await the receipt
#                                     of the number of acknowledgements in RequiredAcks.
#                                 The timeout is not an exact limit on the request time for a few reasons:
#                                 (1) it does not include network latency,
#                                 (2) the timer begins at the beginning of the processing of this request
#                                     so if many requests are queued due to server overload
#                                     that wait time will not be included,
#                                 (3) we will not terminate a local write
#                                     so if the local write time exceeds this timeout it will not be respected.
#                                 To get a hard timeout of this type the client should use the socket timeout.
# TopicName => string             The name of the topic.
# Value => bytes                  The actual message contents
#                                 Kafka supports recursive messages in which case this may itself contain a message set.
#                                 The message can be null.

our $_int64_template;                           # Used to unpack a 64 bit value
if ( $BITS64 ) {
    $_int64_template    = q{q>};
    # unpack a big-endian signed quad (64-bit) value on 64 bit systems.
    *_unpack64          = sub { $_[0] };
    # pack a big-endian signed quad (64-bit) value on 64 bit systems.
    *_pack64            = sub { pack( q{q>}, $_[0] ) };
} else {
    eval q{ require Kafka::Int64; }                 ## no critic
        or die "Cannot load Kafka::Int64 : $@";

    $_int64_template    = q{a[8]};
    # unpack a big-endian signed quad (64-bit) value on 32 bit systems.
    *_unpack64          = \&Kafka::Int64::unpackq;
    # pack a big-endian signed quad (64-bit) value on 32 bit systems.
    *_pack64            = \&Kafka::Int64::packq;
}

=head2 EXPORT

The following constants are available for export

=cut

=head3 C<$APIVERSION>

According to Apache Kafka documentation: 'This is a numeric version number for this api.
Currently the supported version for all APIs is 0 .'

=cut
const our $APIVERSION                   => 0;

# Attributes

# According to Apache Kafka documentation:
# Attributes - Metadata attributes about the message.
# The lowest 2 bits contain the compression codec used for the message.
const our $COMPRESSION_CODEC_MASK       => 0b11;

=head3 C<$CONSUMERS_REPLICAID>

According to Apache Kafka documentation: 'ReplicaId - Normal client consumers should always specify this as -1 as they have no node id.'

=cut
const our $CONSUMERS_REPLICAID          => -1;

=head3 C<$NULL_BYTES_LENGTH>

According to Apache Kafka documentation: 'Protocol Primitive Types: ... bytes, string - A length of -1 indicates null.'

=cut
const our $NULL_BYTES_LENGTH            => -1;

=head3 C<$BAD_OFFSET>

According to Apache Kafka documentation: 'Offset - When the producer is sending messages it doesn't actually know the offset
and can fill in any value here it likes.'

=cut
const our $BAD_OFFSET                   => -1;

my ( $_Request_header_template,             $_Request_header_length ) = (
    q{l>s>s>l>s>},          # Size
                            # 2 ApiKey
                            # 2 ApiVersion
                            # 4 CorrelationId
                            # 2 ClientId length
    10                      # 'Size' is not included in the calculation of length
);
my ( $_ProduceRequest_header_template,      $_ProduceRequest_header_length ) = (
    q{s>l>l>},              # 2 RequiredAcks
                            # 4 Timeout
                            # 4 topics array size
    10
);
my ( $_MessageSet_template,                 $_MessageSet_length ) = (
    q{a[8]l>},              #    a8                  # 8 Offset
                            # 4 MessageSize
    12
);
my ( $_FetchRequest_header_template,        $_FetchRequest_header_length ) = (
    q{l>l>l>l>},
                            # 4 ReplicaId
                            # 4 MaxWaitTime
                            # 4 MinBytes
                            # 4 topics array size
    16
);
my ( $_FetchRequest_body_template,          $_FetchRequest_body_length ) = (
    q{l>a[8]l>},            # 4 Partition
                            # 8 FetchOffset
                            # 4 MaxBytes
    16
);
my ( $_OffsetRequest_header_template,       $_OffsetRequest_header_length ) = (
    q{l>l>},                # 4 ReplicaId
                            # 4 topics array size
    8
);
my ( $_OffsetRequest_body_template,         $_OffsetRequest_body_length ) = (
    q{l>a[8]l>},            # 4 Partition
                            # 8 Time
                            # 4 MaxNumberOfOffsets
    16
);
my ( $_FetchResponse_header_template,       $_FetchResponse_header_length ) = (
    q{x[l]l>l>},            # Size (skip)
                            # 4 CorrelationId
                            # 4 topics array size
    8
);
my ( $_Message_template,                    $_Message_length ) = (
    qq(${_int64_template}l>l>ccl>),
                            # 8 Offset
                            # MessageSize
                            # Crc
                            # MagicByte
                            # Attributes
                            # Key length
    8                       # Only Offset length
);
my ( $_FetchResponse_topic_body_template,   $_FetchResponse_topic_body_length )= (
    qq(s>/al>l>s>${_int64_template}),
                            # TopicName
                            # partitions array size
                            # 4 Partition
                            # 2 ErrorCode
                            # 8 HighwaterMarkOffset
    14                      # without TopicName and partitions array size
);
my $_Key_or_Value_template = q{X[l]l>/a};   # Key or Value

#-- public functions -----------------------------------------------------------

=head2 FUNCTIONS

The following functions are available for C<Kafka::MockProtocol> module.

=cut

# PRODUCE Request --------------------------------------------------------------

=head3 C<encode_produce_request( $Produce_Request, $compression_codec )>

Encodes the argument and returns a reference to the encoded binary string
representing a Request buffer.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$Produce_Request>

C<$Produce_Request> is a reference to the hash representing
the structure of the PRODUCE Request (examples see C<t/*_decode_encode.t>).

=item C<$compression_codec>

Optional.

C<$compression_codec> sets the required type of C<$messages> compression,
if the compression is desirable.

Supported codecs:
L<$COMPRESSION_NONE|Kafka/$COMPRESSION_NONE>,
L<$COMPRESSION_GZIP|Kafka/$COMPRESSION_GZIP>,
L<$COMPRESSION_SNAPPY|Kafka/$COMPRESSION_SNAPPY>.

=back

=cut
sub encode_produce_request {
    my ( $Produce_Request, $compression_codec ) = @_;

    my @data;
    my $request = {
                                                # template    => '...',
                                                # len         => ...,
        data        => \@data,
    };

    _encode_request_header( $request, $APIKEY_PRODUCE, $Produce_Request );
                                                                            # Size
                                                                            # ApiKey
                                                                            # ApiVersion
                                                                            # CorrelationId
                                                                            # ClientId

    my $topics_array = $Produce_Request->{topics};
    push( @data,
        $Produce_Request->{RequiredAcks},                                   # RequiredAcks
        $Produce_Request->{Timeout},                                        # Timeout
        scalar( @$topics_array ),                                           # topics array size
    );
    $request->{template}    .= $_ProduceRequest_header_template;
    $request->{len}         += $_ProduceRequest_header_length;

    foreach my $topic ( @$topics_array ) {
        $request->{template}    .= q{s>};                                   # string length
        $request->{len}         += 2;
        _encode_string( $request, $topic->{TopicName} );                    # TopicName

        my $partitions_array = $topic->{partitions};
        push( @data, scalar( @$partitions_array ) );
        $request->{template}    .= q{l>};                                   # partitions array size
        $request->{len}         += 4;
        foreach my $partition ( @$partitions_array ) {
            push( @data, $partition->{Partition} );
            $request->{template}    .= q{l>};                               # Partition
            $request->{len}         += 4;

            _encode_MessageSet_array( $request, $partition->{MessageSet}, $compression_codec );
        }
    }

    return pack( $request->{template}, $request->{len}, @data );
}

# PRODUCE Response -------------------------------------------------------------

my $_decode_produce_response_template = qq{x[l]l>l>X[l]l>/(s>/al>X[l]l>/(l>s>${_int64_template}))};
                                        # x[l]                    # Size (skip)
                                        # l>                      # CorrelationId

                                        # l>                      # topics array size
                                        # X[l]
                                        # l>/(                    # topics array
                                        #     s>/a                    # TopicName

                                        #     l>                      # partitions array size
                                        #     X[l]
                                        #     l>/(                    # partitions array
                                        #         l>                      # Partition
                                        #         s>                      # ErrorCode
                                        #         $_int64_template        # Offset
                                        #     )
                                        # )

=head3 C<decode_produce_response( $bin_stream_ref )>

Decodes the argument and returns a reference to the hash representing
the structure of the PRODUCE Response (examples see C<t/*_decode_encode.t>).

This function take argument. The following argument is currently recognized:

=over 3

=item C<$bin_stream_ref>

C<$bin_stream_ref> is a reference to the encoded Response buffer. The buffer
must be a non-empty binary string.

=back

=cut
sub decode_produce_response {
    my ( $bin_stream_ref ) = @_;

    my @data = unpack( $_decode_produce_response_template, $$bin_stream_ref );

    my $i = 0;
    my $Produce_Response = {};

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

=head3 C<encode_fetch_request( $Fetch_Request )>

Encodes the argument and returns a reference to the encoded binary string
representing a Request buffer.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$Fetch_Request>

C<$Fetch_Request> is a reference to the hash representing
the structure of the FETCH Request (examples see C<t/*_decode_encode.t>).

=back

=cut
sub encode_fetch_request {
    my ( $Fetch_Request ) = @_;

    my @data;
    my $request = {
                                                # template    => '...',
                                                # len         => ...,
        data        => \@data,
    };

    _encode_request_header( $request, $APIKEY_FETCH, $Fetch_Request );
                                                                            # Size
                                                                            # ApiKey
                                                                            # ApiVersion
                                                                            # CorrelationId
                                                                            # ClientId

    push( @data, $CONSUMERS_REPLICAID );                                    # ReplicaId
    my $topics_array = $Fetch_Request->{topics};
    push( @data,
        $Fetch_Request->{MaxWaitTime},                                      # MaxWaitTime
        $Fetch_Request->{MinBytes},                                         # MinBytes
        scalar( @$topics_array ),                                           # topics array size
    );
    $request->{template}    .= $_FetchRequest_header_template;
    $request->{len}         += $_FetchRequest_header_length;

    foreach my $topic ( @$topics_array ) {
        $request->{template}    .= q{s>};                                   # string length
        $request->{len}         += 2;
        _encode_string( $request, $topic->{TopicName} );                    # TopicName

        my $partitions_array = $topic->{partitions};
        push( @data, scalar( @$partitions_array ) );
        $request->{template}    .= q{l>};                                   # partitions array size
        $request->{len}         += 4;
        foreach my $partition ( @$partitions_array ) {
            push( @data,
                $partition->{Partition},                                    # Partition
                _pack64( $partition->{FetchOffset} ),                       # FetchOffset
                $partition->{MaxBytes},                                     # MaxBytes
            );
            $request->{template}    .= $_FetchRequest_body_template;
            $request->{len}         += $_FetchRequest_body_length;
        }
    }

    return pack( $request->{template}, $request->{len}, @data );
}

# FETCH Response ---------------------------------------------------------------

=head3 C<decode_fetch_response( $bin_stream_ref )>

Decodes the argument and returns a reference to the hash representing
the structure of the FETCH Response (examples see C<t/*_decode_encode.t>).

This function take argument. The following argument is currently recognized:

=over 3

=item C<$bin_stream_ref>

C<$bin_stream_ref> is a reference to the encoded Response buffer. The buffer
must be a non-empty binary string.

=back

=cut
sub decode_fetch_response {
    my ( $bin_stream_ref ) = @_;

# According to Apache Kafka documentation:
# As an optimization the server is allowed to return a partial message at the end of the message set.
# Clients should handle this case.
# NOTE: look inside _decode_MessageSet_template and _decode_MessageSet_array

    my @data;
    my $response = {
                                                # template      => '...',
                                                # stream_offset => ...,
        data        => \@data,
        bin_stream  => $bin_stream_ref,
    };

    _decode_fetch_response_template( $response );
    @data = unpack( $response->{template}, $$bin_stream_ref );

    my $i = 0;
    my $Fetch_Response = {};

    $Fetch_Response->{CorrelationId}                        =  $data[ $i++ ];   # CorrelationId

    my $topics_array = $Fetch_Response->{topics}            =  [];
    my $topics_array_size                                   =  $data[ $i++ ];   # topics array size
    while ( $topics_array_size-- ) {
        my $topic = {
            TopicName                                       => $ data[ $i++ ],  # TopicName
        };

        my $partitions_array = $topic->{partitions}         =  [];
        my $partitions_array_size                           =  $data[ $i++ ];   # partitions array size
        my ( $MessageSetSize, $MessageSet_array );
        while ( $partitions_array_size-- ) {
            my $partition = {
                Partition                                   => $data[ $i++ ],   # Partition
                ErrorCode                                   => $data[ $i++ ],   # ErrorCode
                HighwaterMarkOffset              => _unpack64( $data[ $i++ ] ), # HighwaterMarkOffset
            };

            $MessageSetSize                                 =  $data[ $i++ ];   # MessageSetSize
            $MessageSet_array = $partition->{MessageSet}    =  [];

            _decode_MessageSet_array( $response, $MessageSetSize, \$i, $MessageSet_array );

            push( @$partitions_array, $partition );
        }

        push( @$topics_array, $topic );
    }

    return $Fetch_Response;
}

# OFFSET Request ---------------------------------------------------------------

=head3 C<encode_offset_request( $Offset_Request )>

Encodes the argument and returns a reference to the encoded binary string
representing a Request buffer.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$Offset_Request>

C<$Offset_Request> is a reference to the hash representing
the structure of the OFFSET Request (examples see C<t/*_decode_encode.t>).

=back

=cut
sub encode_offset_request {
    my ( $Offset_Request ) = @_;

    my @data;
    my $request = {
                                                # template    => '...',
                                                # len         => ...,
        data        => \@data,
    };

    _encode_request_header( $request, $APIKEY_OFFSET, $Offset_Request );
                                                                            # Size
                                                                            # ApiKey
                                                                            # ApiVersion
                                                                            # CorrelationId
                                                                            # ClientId

    my $topics_array = $Offset_Request->{topics};
    push( @data,
        $CONSUMERS_REPLICAID,                                               # ReplicaId
        scalar( @$topics_array ),                                           # topics array size
    );
    $request->{template}    .= $_OffsetRequest_header_template;
    $request->{len}         += $_OffsetRequest_header_length;

    foreach my $topic ( @$topics_array ) {
        $request->{template}    .= q{s>};                                   # string length
        $request->{len}          += 2;
        _encode_string( $request, $topic->{TopicName} );                    # TopicName

        my $partitions_array = $topic->{partitions};
        push( @data, scalar( @$partitions_array ) );
        $request->{template}    .= q{l>};                                   # partitions array size
        $request->{len}         += 4;   # [l] partitions array size
        foreach my $partition ( @$partitions_array ) {
            push( @data,
                $partition->{Partition},                                    # Partition
                _pack64( $partition->{Time} ),                              # Time
                $partition->{MaxNumberOfOffsets},                           # MaxNumberOfOffsets
            );
            $request->{template}    .= $_OffsetRequest_body_template;
            $request->{len}         += $_OffsetRequest_body_length;
        }
    }

    return pack( $request->{template}, $request->{len}, @data );
}

# OFFSET Response --------------------------------------------------------------

my $_decode_offset_response_template = qq{x[l]l>l>X[l]l>/(s>/al>X[l]l>/(l>s>l>X[l]l>/(${_int64_template})))};
                                        # x[l]                    # Size (skip)
                                        # l>                      # CorrelationId

                                        # l>                      # topics array size
                                        # X[l]
                                        # l>/(                    # topics array
                                        #     s>/a                    # TopicName

                                        #     l>                      # PartitionOffsets array size
                                        #     X[l]
                                        #     l>/(                    # PartitionOffsets array
                                        #         l>                      # Partition
                                        #         s>                      # ErrorCode

                                        #         l>                      # Offset array size
                                        #         X[l]
                                        #         l>/(                    # Offset array
                                        #             $_int64_template        # Offset
                                        #         )
                                        #     )
                                        # )

=head3 C<decode_offset_response( $bin_stream_ref )>

Decodes the argument and returns a reference to the hash representing
the structure of the OFFSET Response (examples see C<t/*_decode_encode.t>).

This function take argument. The following argument is currently recognized:

=over 3

=item C<$bin_stream_ref>

C<$bin_stream_ref> is a reference to the encoded Response buffer. The buffer
must be a non-empty binary string.

=back

=cut
sub decode_offset_response {
    my ( $bin_stream_ref ) = @_;

    my @data = unpack( $_decode_offset_response_template, $$bin_stream_ref );

    my $i = 0;
    my $Offset_Response = {};

    $Offset_Response->{CorrelationId}                           =  $data[ $i++ ];   # CorrelationId

    my $topics_array = $Offset_Response->{topics}               =  [];
    my $topics_array_size                                       =  $data[ $i++ ];   # topics array size
    while ( $topics_array_size-- ) {
        my $topic = {
            TopicName                                           => $data[ $i++ ],   # TopicName
        };

        my $PartitionOffsets_array = $topic->{PartitionOffsets} =  [];
        my $PartitionOffsets_array_size                         =  $data[ $i++ ];   # PartitionOffsets array size
        my ( $PartitionOffset, $Offset_array, $Offset_array_size );
        while ( $PartitionOffsets_array_size-- ) {
            $PartitionOffset = {
                Partition                                       => $data[ $i++ ],   # Partition
                ErrorCode                                       => $data[ $i++ ],   # ErrorCode
            };

            $Offset_array = $PartitionOffset->{Offset}          =  [];
            $Offset_array_size                                  =  $data[ $i++ ];   # Offset array size
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

=head3 C<encode_metadata_request( $Metadata_Request )>

Encodes the argument and returns a reference to the encoded binary string
representing a Request buffer.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$Metadata_Request>

C<$Metadata_Request> is a reference to the hash representing
the structure of the METADATA Request (examples see C<t/*_decode_encode.t>).

=back

=cut
sub encode_metadata_request {
    my ( $Metadata_Request ) = @_;

    my @data;
    my $request = {
                                                # template    => '...',
                                                # len         => ...,
        data        => \@data,
    };

    _encode_request_header( $request, $APIKEY_METADATA, $Metadata_Request );
                                                                            # Size
                                                                            # ApiKey
                                                                            # ApiVersion
                                                                            # CorrelationId
                                                                            # ClientId

    my $topics_array = $Metadata_Request->{topics};
    push( @data, scalar( @$topics_array ) );                                # topics array size
    $request->{template}    .= q{l>};
    $request->{len}         += 4;

    foreach my $topic ( @$topics_array ) {
        $request->{template}    .= q{s>};                                   # string length
        $request->{len}         += 2;
        _encode_string( $request, $topic );                                 # TopicName
    }

    return pack( $request->{template}, $request->{len}, @data );
}

# METADATA Response ------------------------------------------------------------

my $_decode_metadata_response_template = q{x[l]l>l>X[l]l>/(l>s>/al>)l>X[l]l>/(s>s>/al>X[l]l>/(s>l>l>l>X[l]l>/(l>)l>X[l]l>/(l>)))};
                                        # x[l]                    # Size (skip)
                                        # l>                      # CorrelationId

                                        # l>                      # Broker array size
                                        # X[l]
                                        # l>/(                    # Broker array
                                        #     l>                      # NodeId
                                        #     s>/a                    # Host
                                        #     l>                      # Port
                                        # )

                                        # l>                      # TopicMetadata array size
                                        # X[l]
                                        # l>/(                    # TopicMetadata array
                                        #     s>                      # ErrorCode
                                        #     s>/a                    # TopicName

                                        #     l>                      # PartitionMetadata array size
                                        #     X[l]
                                        #     l>/(                    # PartitionMetadata array
                                        #         s>                      # ErrorCode
                                        #         l>                      # Partition
                                        #         l>                      # Leader

                                        #         l>                      # Replicas array size
                                        #         X[l]
                                        #         l>/(                    # Replicas array
                                        #             l>                      # ReplicaId
                                        #         )

                                        #         l>                      # Isr array size
                                        #         X[l]
                                        #         l>/(                    # Isr array
                                        #             l>                      # ReplicaId
                                        #         )
                                        #     )
                                        # )

=head3 C<decode_metadata_response( $bin_stream_ref )>

Decodes the argument and returns a reference to the hash representing
the structure of the METADATA Response (examples see C<t/*_decode_encode.t>).

This function take argument. The following argument is currently recognized:

=over 3

=item C<$bin_stream_ref>

C<$bin_stream_ref> is a reference to the encoded Response buffer. The buffer
must be a non-empty binary string.

=back

=cut
sub decode_metadata_response {
    my ( $bin_stream_ref ) = @_;

    my @data = unpack( $_decode_metadata_response_template, $$bin_stream_ref );

    my $i = 0;
    my $Metadata_Response = {};

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

#-- private functions ----------------------------------------------------------

# Generates a template to encrypt the request header
sub _encode_request_header {
    my ( $request, $api_key, $request_ref ) = @_;

    @{ $request->{data} } = (
                                                                            # Size
        $api_key,                                                           # ApiKey
        $APIVERSION,                                                        # ApiVersion
        $request_ref->{CorrelationId},                                      # CorrelationId
    );
    $request->{template}    = $_Request_header_template;
    $request->{len}         = $_Request_header_length;
    _encode_string( $request, $request_ref->{ClientId} );                   # ClientId
}

# Generates a template to decrypt the fetch response body
sub _decode_fetch_response_template {
    my ( $response ) = @_;

    $response->{template}       = $_FetchResponse_header_template;
    $response->{stream_offset}  = $_FetchResponse_header_length;    # bytes before topics array size
                                                                                # [l] Size
                                                                                # [l] CorrelationId
    my $topics_array_size = unpack(
         q{x[}.$response->{stream_offset}
        .q{]l>},                            # topics array size
        ${ $response->{bin_stream} }
    );
    $response->{stream_offset} += 4;        # bytes before TopicName length
                                                                                # [l] topics array size

    my ( $TopicName_length, $partitions_array_size );
    while ( $topics_array_size-- ) {
        $TopicName_length = unpack(
             q{x[}.$response->{stream_offset}
            .q{]s>},                        # TopicName length
            ${ $response->{bin_stream} }
        );
        $response->{stream_offset} +=       # bytes before partitions array size
              2                                                                 # [s] TopicName length
            + $TopicName_length                                                 # TopicName
            ;
        $partitions_array_size = unpack(
             q{x[}.$response->{stream_offset}
            .q{]l>},                        # partitions array size
            ${ $response->{bin_stream} }
        );
        $response->{stream_offset} += 4;    # bytes before Partition
                                                                                # [l] partitions array size

        $response->{template}       .= $_FetchResponse_topic_body_template;
        $response->{stream_offset}  += $_FetchResponse_topic_body_length;   # (without TopicName and partitions array size)
                                            # bytes before MessageSetSize
                                                                                # TopicName
                                                                                # [l] # partitions array size
                                                                                # [l] Partition
                                                                                # [s] ErrorCode
                                                                                # [q] HighwaterMarkOffset

        _decode_MessageSet_template( $response );
    }
}

# Decrypts MessageSet
sub _decode_MessageSet_array {
    my ( $response, $MessageSetSize, $i_ref, $MessageSet_array_ref ) = @_;

    my $data = $response->{data};
    my $data_array_size = scalar @{ $data };

# NOTE: not all messages can be returned
    my ( $Message, $MessageSize, $Crc, $Key_length, $Value_length );
    while ( $MessageSetSize && $$i_ref < $data_array_size ) {

        $Message = {
            Offset                                        => _unpack64( $data->[ $$i_ref++ ] ), # Offset
        };

        $MessageSize                                                 =  $data->[ $$i_ref++ ];   # MessageSize
# NOTE: The CRC is the CRC32 of the remainder of the message bytes.
# This is used to check the integrity of the message on the broker and consumer:
# MagicByte + Attributes + Key length + Key + Value length + Value
        $Crc                                                         =  $data->[ $$i_ref++ ];   # Crc
# WARNING: The current version of the module does not support the following:
# A message set is also the unit of compression in Kafka,
# and we allow messages to recursively contain compressed message sets to allow batch compression.
        $Message->{MagicByte}                                        =  $data->[ $$i_ref++ ];   # MagicByte
        $Message->{Attributes}                                       =  $data->[ $$i_ref++ ];   # Attributes

        $Key_length                                                  =  $data->[ $$i_ref++ ];   # Key length
        $Message->{Key}   = $Key_length   == $NULL_BYTES_LENGTH ? q{} : $data->[ $$i_ref++ ];   # Key
        $Value_length                                                =  $data->[ $$i_ref++ ];   # Value length
        $Message->{Value} = $Value_length == $NULL_BYTES_LENGTH ? q{} : $data->[ $$i_ref++ ];   # Value

        if ( my $compression_codec = $Message->{Attributes} & $COMPRESSION_CODEC_MASK ) {
            my $decompressed;
            if ( $compression_codec == $COMPRESSION_GZIP ) {
                gunzip( \$Message->{Value} => \$decompressed )
                    or _error( $ERROR_COMPRESSION, "gunzip failed: $GunzipError" );
            } elsif ( $compression_codec == $COMPRESSION_SNAPPY ) {
                my ( $header, $x_version, $x_compatversion, undef ) = unpack( q{a[8]L>L>L>}, $Message->{Value} );   # undef - $x_length

                # Special thanks to Colin Blower
                if ( $header eq "\x82SNAPPY\x00" ) {
                    # Found a xerial header.... nonstandard snappy compression header, remove the header
                    if ( $x_compatversion == 1 && $x_version == 1 ) {
                        $Message->{Value} = substr( $Message->{Value}, 20 );    # 20 = q{a[8]L>L>L>}
                    } else {
                        #warn("V $x_version and comp $x_compatversion");
                        _error( $ERROR_COMPRESSION, "Snappy compression with incompatible xerial header version found (x_version = $x_version, x_compatversion = $x_compatversion)" );
                    }
                }

                $decompressed = Compress::Snappy::decompress( $Message->{Value} )
                    // _error( $ERROR_COMPRESSION, 'Unable to decompress snappy compressed data' );
            } else {
                _error( $ERROR_COMPRESSION, "Unknown compression codec $compression_codec" );
            }
            my @data;
            my $Value_length = length $decompressed;
            my $resp = {
                data            => \@data,
                bin_stream      => \$decompressed,
                stream_offset   => 0,
            };
            _decode_MessageSet_sized_template( $Value_length, $resp );
            @data = unpack( $resp->{template}, ${ $resp->{bin_stream} } );
            my $i = 0;  # i_ref
            my $size = length( $decompressed );
            _decode_MessageSet_array(
                $resp,
                $size,  # message set size
                \$i,    # i_ref
                $MessageSet_array_ref,
            );
        } else {
            push( @$MessageSet_array_ref, $Message );
        }

        $MessageSetSize -= 12
                                    # [q] Offset
                                    # [l] MessageSize
            + $MessageSize          # Message
            ;
    }
}

# Generates a template to encrypt MessageSet
sub _encode_MessageSet_array {
    my ( $request, $MessageSet_array_ref, $compression_codec ) = @_;

    my ( $MessageSize, $Key, $Value, $key_length, $value_length, $message_body, $message_set );

    if ( $compression_codec ) {
        foreach my $MessageSet ( @$MessageSet_array_ref ) {
            $key_length   = length( $Key    = $MessageSet->{Key} );
            $value_length = length( $Value  = $MessageSet->{Value} );

            $message_body = pack(
                    q{ccl>}                                         # MagicByte
                                                                    # Attributes
                                                                    # Key length
                    .( $key_length   ? qq{a[$key_length]}   : q{} ) # Key
                    .q{l>}                                          # Value length
                    .( $value_length ? qq{a[$value_length]} : q{} ) # Value
                ,
                0,
                $COMPRESSION_NONE,  # According to Apache Kafka documentation:
                                    # The lowest 2 bits contain the compression codec used for the message.
                                    # The other bits should be set to 0.
                $key_length     ? ( $key_length,    $Key )    : ( $NULL_BYTES_LENGTH ),
                $value_length   ? ( $value_length,  $Value )  : ( $NULL_BYTES_LENGTH ),
            );

            $message_set .= pack( qq(x[8]l>l>),     # 8 Offset ($PRODUCER_ANY_OFFSET)
                length( $message_body ) + 4,        # [l] MessageSize ( $message_body + Crc )
                crc32( $message_body )              # [l] Crc
            ).$message_body;
        }

        $MessageSet_array_ref = [
            {
                Offset  => $PRODUCER_ANY_OFFSET,
                Key     => $Key,
            }
        ];

        # Compression
        if ( $compression_codec == $COMPRESSION_GZIP ) {
            $MessageSet_array_ref->[0]->{Value} = q{};
            gzip( \$message_set => \$MessageSet_array_ref->[0]->{Value} )
                or _error( $ERROR_COMPRESSION, "gzip failed: $GzipError" );
        } elsif ( $compression_codec == $COMPRESSION_SNAPPY ) {
            $MessageSet_array_ref->[0]->{Value} = Compress::Snappy::compress( $message_set )
                // _error( $ERROR_COMPRESSION, 'Unable to compress snappy data' );
        } else {
             _error( $ERROR_COMPRESSION, "Unknown compression codec $compression_codec" );
        }
    }

    my $data = $request->{data};
    my $MessageSetSize = 0;
    my %sizes;
    foreach my $MessageSet ( @$MessageSet_array_ref ) {
        $MessageSetSize +=
              12                                                            # [q] Offset
                                                                            # [l] MessageSize
            + ( $sizes{ $MessageSet } =                                     # MessageSize
                  10                                                        # [l] Crc
                                                                            # [c] MagicByte
                                                                            # [c] Attributes
                                                                            # [l] Key length
                + length( $MessageSet->{Key}    //= q{} )                   # Key
                + 4                                                         # [l] Value length
                + length( $MessageSet->{Value}  //= q{} )                   # Value
            )   # MessageSize
            ;
    }
    push( @$data, $MessageSetSize );
    $request->{template}    .= q{l>};                                       # MessageSetSize
    $request->{len}         += 4;

    foreach my $MessageSet ( @$MessageSet_array_ref ) {
        push( @$data,
            _pack64( $MessageSet->{Offset} ),                               # Offset (It may be $PRODUCER_ANY_OFFSET)
            ( $MessageSize = $sizes{ $MessageSet } ),                       # MessageSize
        );
        $request->{template}    .= $_MessageSet_template;
        $request->{len}         += $_MessageSet_length;

        $key_length   = length( $Key    = $MessageSet->{Key} );
        $value_length = length( $Value  = $MessageSet->{Value} );

        $message_body = pack(
                q{ccl>}                                         # MagicByte
                                                                # Attributes
                                                                # Key length
                .( $key_length   ? qq{a[$key_length]}   : q{} ) # Key
                .q{l>}                                          # Value length
                .( $value_length ? qq{a[$value_length]} : q{} ) # Value
            ,
            0,
            $compression_codec // $COMPRESSION_NONE,    # According to Apache Kafka documentation:
                                # The lowest 2 bits contain the compression codec used for the message.
                                # The other bits should be set to 0.
            $key_length     ? ( $key_length,    $Key )    : ( $NULL_BYTES_LENGTH ),
            $value_length   ? ( $value_length,  $Value )  : ( $NULL_BYTES_LENGTH ),
        );

        push( @$data, crc32( $message_body ), $message_body );
        # Message
        $request->{template} .= q{l>a[}                                         # Crc
            .( $MessageSize - 4 )   # 4 Crc
            .qq{]};
        # Message body:
                                                                                # MagicByte
                                                                                # Attributes
                                                                                # Key length
                                                                                # Key
                                                                                # Value length
                                                                                # Value
        $request->{len} += $MessageSize;    # Message
    }
}

# Generates a template to decrypt MessageSet
sub _decode_MessageSet_template {
    my ( $response ) = @_;

    my $MessageSetSize = unpack(
         q{x[}.$response->{stream_offset}
        .q{]l>},                            # MessageSetSize
        ${ $response->{bin_stream} }
    );
    $response->{template} .= q{l>};         # MessageSetSize
    $response->{stream_offset} += 4;        # bytes before Offset

    return _decode_MessageSet_sized_template($MessageSetSize, $response);
}

sub _decode_MessageSet_sized_template {
    my ( $MessageSetSize, $response ) = @_;

    my $bin_stream_length = length ${ $response->{bin_stream} };

    my ( $local_template, $MessageSize, $Key_length, $Value_length );
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

        $local_template = q{};
        MESSAGE_SET:
        {
            $local_template .= $_Message_template;
            $response->{stream_offset} += $_Message_length; # (Only Offset length)
                # [q] Offset
                # [l] MessageSize
                # [l] Crc
                # [c] MagicByte
                # [c] Attributes
                # [l] Key length
                                                # bytes before MessageSize
                                                                                # [q] Offset
            $MessageSize = unpack(
                 q{x[}.$response->{stream_offset}
                .q{]l>},                        # MessageSize
                ${ $response->{bin_stream} }
            );

            $response->{stream_offset} += 10;   # bytes before Crc
                                                                                # [l] MessageSize
                                                # bytes before Key length
                                                                                # [l] Crc
                                                                                # [c] MagicByte
                                                                                # [c] Attributes
            $Key_length = unpack(
                 q{x[}.$response->{stream_offset}
                .q{]l>},                        # Key length
                ${ $response->{bin_stream} }
            );

            $response->{stream_offset} += 4;    # bytes before Key or Value length
                                                                                # [l] Key length
            $response->{stream_offset} += $Key_length   # bytes before Key
                if $Key_length != $NULL_BYTES_LENGTH;                           # Key
            if ( $bin_stream_length >= $response->{stream_offset} + 4 ) {   # + [l] Value length
                $local_template .= $_Key_or_Value_template
                    if $Key_length != $NULL_BYTES_LENGTH;
            }
            else {
# Not the full MessageSet
                $local_template = q{};
                last MESSAGE_SET;
            }

            $local_template .= q{l>};           # Value length
            $Value_length = unpack(
                 q{x[}.$response->{stream_offset}
                .q{]l>},                        # Value length
                ${ $response->{bin_stream} }
            );
            $response->{stream_offset} +=       # bytes before Value or next Message
                  4                                                             # [l] Value length
                ;
            $response->{stream_offset} += $Value_length # bytes before next Message
                if $Value_length != $NULL_BYTES_LENGTH;                         # Value
            if ( $bin_stream_length >= $response->{stream_offset} ) {
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
            $response->{template} .= $local_template;
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

# Generates a template to encrypt string
sub _encode_string {
    my ( $request, $string ) = @_;

    if ( $string eq q{} ) {
        push( @{ $request->{data} }, 0 );
    }
    else {
        my $string_length = length $string;
        push( @{ $request->{data} }, $string_length, $string );
        $request->{template}    .= q{a*};   # string;
        $request->{len}         += $string_length;
    }
}

# Handler for errors
sub _error {
    Kafka::Exception::Protocol->throw( throw_args( @_ ) );
}

1;

__END__

=head1 DIAGNOSTICS

In order to achieve better performance, functions of this module do not perform
arguments validation.

=head1 SEE ALSO

The basic operation of the Kafka package modules:

L<Kafka|Kafka> - constants and messages used by the Kafka package modules.

L<Kafka::Connection|Kafka::Connection> - interface to connect to a Kafka cluster.

L<Kafka::Producer|Kafka::Producer> - interface for producing client.

L<Kafka::Consumer|Kafka::Consumer> - interface for consuming client.

L<Kafka::Message|Kafka::Message> - interface to access Kafka message
properties.

L<Kafka::Int64|Kafka::Int64> - functions to work with 64 bit elements of the
protocol on 32 bit systems.

L<Kafka::Protocol|Kafka::Protocol> - functions to process messages in the
Apache Kafka's Protocol.

L<Kafka::IO|Kafka::IO> - low-level interface for communication with Kafka server.

L<Kafka::Exceptions|Kafka::Exceptions> - module designated to handle Kafka exceptions.

L<Kafka::Internals|Kafka::Internals> - internal constants and functions used
by several package modules.

A wealth of detail about the Apache Kafka and the Kafka Protocol:

Main page at L<http://kafka.apache.org/>

Kafka Protocol at L<https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol>

=head1 SOURCE CODE

Kafka package is hosted on GitHub:
L<https://github.com/TrackingSoft/Kafka>

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Sergiy Zuban

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2013 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
