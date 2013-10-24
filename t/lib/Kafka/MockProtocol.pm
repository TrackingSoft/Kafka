package Kafka::MockProtocol;

=head1 NAME

Kafka::MockProtocol - functions to simulate the messages formation
by the kafka server (in the Apache Kafka's Protocol).

=head1 VERSION

This documentation refers to C<Kafka::MockProtocol> version 0.800_6 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.800_6';

use Exporter qw(
    import
);
our @EXPORT_OK = qw(
    decode_fetch_request
    decode_metadata_request
    decode_offset_request
    decode_produce_request
    encode_fetch_response
    encode_metadata_response
    encode_offset_response
    encode_produce_response
);

#-- load the modules -----------------------------------------------------------

use Params::Util qw(
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

use Kafka qw(
    %ERROR
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NO_ERROR
    $ERROR_NOT_BINARY_STRING
    $ERROR_REQUEST_OR_RESPONSE
);
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_METADATA
    $APIKEY_OFFSET
    $APIKEY_PRODUCE
);
use Kafka::Protocol qw(
    $APIVERSION
    $COMPRESSION_NONE
    $CONSUMERS_REPLICAID
    $NULL_BYTES_LENGTH
    $_int64_template
    _encode_MessageSet_array
    _encode_string
    _decode_MessageSet_array
    _decode_MessageSet_template
    _pack64
    _unpack64
);
use Kafka::TestInternals qw(
    _is_suitable_int
);

#-- declarations ---------------------------------------------------------------

=head1 DESCRIPTION

This module is not a user module.

The main features of the C<Kafka::MockProtocol> module are:

=over 3

=item *

Supports parsing the Apache Kafka protocol to emulate the operations performed
by the kafka server.

=item *

Supports Apache Kafka Requests and Responses (PRODUCE, FETCH, OFFSET, METADATA).

=item *

Support for working with 64 bit elements of the Kafka protocol on 32 bit systems.

=back

=cut

my ( $_Response_header_template,                            $_Response_header_length ) = (
    q{l>l>l>},      # Size
                    # 4 CorrelationId
                    # 4 topics array size (Broker array size)
    8               # 'Size' is not included in the calculation of length
);
my ( $_ProduceResponse_body_template,                       $_ProduceResponse_body_length ) = (
    q{l>s>a[8]},    # 4 Partition
                    # 2 ErrorCode
                    # 8 Offset
    14
);
my ( $_FetchResponse_body_template,                         $_FetchResponse_body_length ) = (
    q{l>s>a[8]},    # 4 Partition
                    # 2 ErrorCode
                    # 8 HighwaterMarkOffset
    14
);
my ( $_OffsetResponse_body_template,                        $_OffsetResponse_body_length ) = (
    q{l>s>l>},      # 4 Partition
                    # 2 ErrorCode
                    # 4 Offset array size
    10
);
my ( $_MetadataResponse_Broker_body_template,               $_MetadataResponse_Broker_body_length ) = (
    q{l>s>a*l>},    # 4 NodeId
                    # 2 Host length
                    # Host
                    # 4 Port
    10          # without real Host length
);
my ( $_MetadataResponse_PartitionMetadata_body_template,    $_MetadataResponse_PartitionMetadata_body_length ) = (
    q{s>l>l>l>},    # 2 ErrorCode
                    # 4 Partition
                    # 4 Leader
                    # 4 Replicas array size
    14
);
my ( $_ProduceRequest_header_template,                      $_ProduceRequest_header_length ) = (
    q{x[l]s>s>l>s>/as>l>l>},
                    # 4 Size (skip)
                    # 2 ApiKey
                    # 2 ApiVersion
                    # 4 CorrelationId
                    # ClientId
                    # RequiredAcks
                    # Timeout
                    # topics array size
    12          # bytes before ClientId length
);
my $_ProduceRequest_topic_body_template = q{s>/al>l>};
                    # TopicName
                    # partitions array size
                    # Partition

my $_package_error;

#-- public functions -----------------------------------------------------------

=head2 FUNCTIONS

The following functions are available for C<Kafka::MockProtocol> module.

=cut

# PRODUCE Request --------------------------------------------------------------

=head3 C<decode_produce_request( $bin_stream_ref )>

Decodes the argument and returns a reference to the hash representing
the structure of the PRODUCE Request (examples see C<t/??_decode_encode.t>).

This function take argument. The following argument is currently recognized:

=over 3

=item C<$bin_stream_ref>

C<$bin_stream_ref> is a reference to the encoded Request buffer. The buffer
must be a non-empty binary string.

=back

=cut
sub decode_produce_request {
    my ( $bin_stream_ref ) = @_;

    _is_bin_stream_correct( $bin_stream_ref )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data;
    my $request = {
                                                # template      => '...',
                                                # stream_offset => ...,
        bin_stream  => $bin_stream_ref,
        data        => \@data,
    };

    _decode_produce_request_template( $request );
    @data = unpack( $request->{template}, $$bin_stream_ref );

    my ( $i, $Produce_Request ) = ( 0, {} );

    $APIKEY_PRODUCE                                          == $data[ $i++ ]       # ApiKey
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ApiKey' );
    $APIVERSION                                              == $data[ $i++ ]       # ApiVersion
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ApiVersion' );
    $Produce_Request = {
        CorrelationId                                        => $data[ $i++ ],      # CorrelationId
        ClientId                                             => $data[ $i++ ],      # ClientId
        RequiredAcks                                         => $data[ $i++ ],      # RequiredAcks
        Timeout                                              => $data[ $i++ ],      # Timeout
    };

    my $topics_array = $Produce_Request->{topics}            =  [];
    my $topics_array_size                                    =  $data[ $i++ ];      # topics array size
    while ( $topics_array_size-- ) {
        my $topic = {
            TopicName                                        => $data[ $i++ ],      # TopicName length
        };

        my $partitions_array = $topic->{partitions}          =  [];
        my $partitions_array_size                            =  $data[ $i++ ];      # partitions array size
        while ( $partitions_array_size-- ) {
            my $partition = {
                Partition                                    => $data[ $i++ ],      # Partition
            };

            my $MessageSetSize                               =  $data[ $i++ ];      # MessageSetSize
            my $MessageSet_array = $partition->{MessageSet}  =  [];

            _decode_MessageSet_array( $request, $MessageSetSize, \$i, $MessageSet_array );

            push( @$partitions_array, $partition );
        }

        push( @$topics_array, $topic );
    }

    return $Produce_Request;
}

# PRODUCE Response -------------------------------------------------------------

=head3 C<encode_produce_response( $Produce_Response )>

Encodes the argument and returns a reference to the encoded binary string
representing a Response buffer.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$Produce_Response>

C<$Produce_Response> is a reference to the hash representing
the structure of the PRODUCE Response (examples see C<t/??_decode_encode.t>).

=back

=cut
sub encode_produce_response {
    my ( $Produce_Response ) = @_;

    _HASH( $Produce_Response )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data = ();
    my $response = {
                                                # template    => '...',
                                                # len         => ...,
        data        => \@data,
    };

    ( defined( $Produce_Response->{CorrelationId} ) && isint( $Produce_Response->{CorrelationId} ) )    # CorrelationId
        ? push( @data, $Produce_Response->{CorrelationId} )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'CorrelationId' );
    $response->{template}   = $_Response_header_template;
    $response->{len}        = $_Response_header_length;

    my $topics_array = $Produce_Response->{topics};
    _ARRAY0( $topics_array )                                                # topics array size
        ? push( @data, scalar( @$topics_array ) )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );

    foreach my $topic ( @$topics_array ) {
        $response->{template}   .= q{s>};                                   # string length
        $response->{len}        += 2;
        _encode_string( $response, $topic->{TopicName} );                   # TopicName

        my $partitions_array = $topic->{partitions};
        _ARRAY0( $partitions_array )
            ? push( @data, scalar( @$partitions_array ) )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'partitions' );
        $response->{template}   .= q{l>};                                   # partitions array size
        $response->{len}        += 4;   # [l] partitions array size

        foreach my $partition ( @$partitions_array ) {
            ( defined( $partition->{Partition} ) && isint( $partition->{Partition} ) )  # Partition
                ? push( @data, $partition->{Partition} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Partition' );
            ( defined( $partition->{ErrorCode} ) && isint( $partition->{ErrorCode} ) )  # ErrorCode
                ? push( @data, $partition->{ErrorCode} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ErrorCode' );
            _is_suitable_int( $partition->{Offset} )
                ? push( @data, _pack64( $partition->{Offset} ) )            # Offset
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Offset' );
            $response->{template}   .= $_ProduceResponse_body_template;
            $response->{len}        += $_ProduceResponse_body_length;
        }
    }

    return pack( $response->{template}, $response->{len}, @data );
}

# FETCH Request ----------------------------------------------------------------

my $_decode_fetch_request_template = qq{x[l]s>s>l>s>/al>l>l>l>X[l]l>/(s>/al>X[l]l>/(l>${_int64_template}l>))};
                                        # x[l]                    # Size (skip)
                                        # s>                      # ApiKey
                                        # s>                      # ApiVersion
                                        # l>                      # CorrelationId
                                        # s>/a                    # ClientId
                                        # l>                      # ReplicaId
                                        # l>                      # MaxWaitTime
                                        # l>                      # MinBytes

                                        # l>                      # topics array size
                                        # X[l]
                                        # l>/(                    # topics array
                                        #     s>/a                    # TopicName

                                        #     l>                      # partitions array size
                                        #     X[l]
                                        #     l>/(                    # partitions array
                                        #         l>                      # Partition
                                        #         $_int64_template        # FetchOffset
                                        #         l>                      # MaxBytes
                                        #     )
                                        # )


=head3 C<decode_fetch_request( $bin_stream_ref )>

Decodes the argument and returns a reference to the hash representing
the structure of the FETCH Request (examples see C<t/??_decode_encode.t>).

This function take argument. The following argument is currently recognized:

=over 3

=item C<$bin_stream_ref>

C<$bin_stream_ref> is a reference to the encoded Request buffer. The buffer
must be a non-empty binary string.

=back

=cut
sub decode_fetch_request {
    my ( $bin_stream_ref ) = @_;

    _is_bin_stream_correct( $bin_stream_ref )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data = unpack( $_decode_fetch_request_template, $$bin_stream_ref );

    my ( $i, $Fetch_Request ) = ( 0, {} );

    $APIKEY_FETCH                                            == $data[ $i++ ]       # ApiKey
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ApiKey' );
    $APIVERSION                                              == $data[ $i++ ]       # ApiVersion
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ApiVersion' );
    $Fetch_Request->{CorrelationId}                          =  $data[ $i++ ];      # CorrelationId
    $Fetch_Request->{ClientId}                               =  $data[ $i++ ];      # ClientId

    $CONSUMERS_REPLICAID                                     == $data[ $i++ ]       # ReplicaId
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ReplicaId' );
    $Fetch_Request->{MaxWaitTime}                            =  $data[ $i++ ];      # MaxWaitTime
    $Fetch_Request->{MinBytes}                               =  $data[ $i++ ];      # MinBytes

    my $topics_array = $Fetch_Request->{topics}              =  [];
    my $topics_array_size                                    =  $data[ $i++ ];      # topics array size
    while ( $topics_array_size-- ) {
        my $topic = {
            TopicName                                        => $data[ $i++ ],      # TopicName
        };

        my $partitions_array = $topic->{partitions}          =  [];
        my $partitions_array_size                            =  $data[ $i++ ];      # partitions array size
        while ( $partitions_array_size-- ) {
            my $partition = {
                Partition                                    => $data[ $i++ ],      # Partition
                FetchOffset                       => _unpack64( $data[ $i++ ] ),    # FetchOffset
                MaxBytes                                     => $data[ $i++ ],      # MaxBytes
            };

            push( @$partitions_array, $partition );
        }

        push( @$topics_array, $topic );
    }

    return $Fetch_Request;
}

# FETCH Response ---------------------------------------------------------------

=head3 C<encode_fetch_response( $Fetch_Response )>

Encodes the argument and returns a reference to the encoded binary string
representing a Response buffer.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$Fetch_Response>

C<$Fetch_Response> is a reference to the hash representing
the structure of the FETCH Response (examples see C<t/??_decode_encode.t>).

=back

=cut
sub encode_fetch_response {
    my ( $Fetch_Response ) = @_;

    _HASH( $Fetch_Response )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data = ();
    my $response = {
                                                # template    => '...',
                                                # len         => ...,
        data        => \@data,
    };

    ( defined( $Fetch_Response->{CorrelationId} ) && isint( $Fetch_Response->{CorrelationId} ) )    # CorrelationId
        ? push( @data, $Fetch_Response->{CorrelationId} )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'CorrelationId' );
    $response->{template}   = $_Response_header_template;
    $response->{len}        = $_Response_header_length;

    my $topics_array = $Fetch_Response->{topics} // [];
    _ARRAY0( $topics_array )
        ? push( @data, scalar( @$topics_array ) )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );

    foreach my $topic ( @$topics_array ) {
        $response->{template}   .= q{s>};                                   # string length
        $response->{len}        += 2;
        _encode_string( $response, $topic->{TopicName} );                   # TopicName

        my $partitions_array = $topic->{partitions} // [];
        _ARRAY0( $partitions_array )
            ? push( @data, scalar( @$partitions_array ) )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'partitions' );
        $response->{template}   .= q{l>};                                   # partitions array size
        $response->{len}        += 4;       # [l] partitions array size

        foreach my $partition ( @$partitions_array ) {
            ( defined( $partition->{Partition} ) && isint( $partition->{Partition} ) )  # Partition
                ? push( @data, $partition->{Partition} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Partition' );
            ( defined( $partition->{ErrorCode} ) && isint( $partition->{ErrorCode} ) )  # ErrorCode
                ? push( @data, $partition->{ErrorCode} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ErrorCode' );
            _is_suitable_int( $partition->{HighwaterMarkOffset} )           # HighwaterMarkOffset
                ? push( @data, _pack64( $partition->{HighwaterMarkOffset} ) )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'HighwaterMarkOffset' );
            $response->{template}   .= $_FetchResponse_body_template;
            $response->{len}        += $_FetchResponse_body_length;

            _encode_MessageSet_array( $response, $partition->{MessageSet} );
        }
    }

    return pack( $response->{template}, $response->{len}, @data );
}

# OFFSET Request ---------------------------------------------------------------

my $_decode_offset_request_template = qq{x[l]s>s>l>s>/al>l>X[l]l>/(s>/al>X[l]l>/(l>${_int64_template}l>))};
                                        # x[l]                    # Size (skip)
                                        # s>                      # ApiKey
                                        # s>                      # ApiVersion
                                        # l>                      # CorrelationId
                                        # s>/a                    # ClientId
                                        # l>                      # ReplicaId

                                        # l>                      # topics array size
                                        # X[l]
                                        # l>/(                    # topics array
                                        #     s>/a                    # TopicName

                                        #     l>                      # partitions array size
                                        #     X[l]
                                        #     l>/(                    # partitions array
                                        #         l>                      # Partition
                                        #         $_int64_template        # Time
                                        #         l>                      # MaxNumberOfOffsets
                                        #     )
                                        # )


=head3 C<decode_offset_request( $bin_stream_ref )>

Decodes the argument and returns a reference to the hash representing
the structure of the OFFSET Request (examples see C<t/??_decode_encode.t>).

This function take argument. The following argument is currently recognized:

=over 3

=item C<$bin_stream_ref>

C<$bin_stream_ref> is a reference to the encoded Request buffer. The buffer
must be a non-empty binary string.

=back

=cut
sub decode_offset_request {
    my ( $bin_stream_ref ) = @_;

    _is_bin_stream_correct( $bin_stream_ref )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data = unpack( $_decode_offset_request_template, $$bin_stream_ref );

    my ( $i, $Offset_Request ) = ( 0, {} );

    $APIKEY_OFFSET                                           == $data[ $i++ ]       # ApiKey
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ApiKey' );
    $APIVERSION                                              == $data[ $i++ ]       # ApiVersion
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ApiVersion' );
    $Offset_Request->{CorrelationId}                         =  $data[ $i++ ];      # CorrelationId
    $Offset_Request->{ClientId}                              =  $data[ $i++ ];      # ClientId

    $CONSUMERS_REPLICAID                                     == $data[ $i++ ]       # ReplicaId
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ReplicaId' );

    my $topics_array = $Offset_Request->{topics}             =  [];
    my $topics_array_size                                    =  $data[ $i++ ];      # topics array size
    while ( $topics_array_size-- ) {
        my $topic = {
            TopicName                                        => $data[ $i++ ],      # TopicName
        };

        my $partitions_array = $topic->{partitions}          =  [];
        my $partitions_array_size                            =  $data[ $i++ ];      # partitions array size
        while ( $partitions_array_size-- ) {
            my $partition = {
                Partition                                    => $data[ $i++ ],      # Partition
                Time                              => _unpack64( $data[ $i++ ] ),    # Time
                MaxNumberOfOffsets                           => $data[ $i++ ],      # MaxNumberOfOffsets
            };

            push @$partitions_array, $partition;
        }

        push @$topics_array, $topic;
    }

    return $Offset_Request;
}

# OFFSET Response --------------------------------------------------------------

=head3 C<encode_offset_response( $Produce_Response )>

Encodes the argument and returns a reference to the encoded binary string
representing a Response buffer.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$Offset_Response>

C<$Offset_Response> is a reference to the hash representing
the structure of the OFFSET Response (examples see C<t/??_decode_encode.t>).

=back

=cut
sub encode_offset_response {
    my ( $Offset_Response ) = @_;

    _HASH( $Offset_Response )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data;
    my $response = {
                                                # template    => '...',
                                                # len         => ...,
        data        => \@data,
    };

    ( defined( $Offset_Response->{CorrelationId} ) && isint( $Offset_Response->{CorrelationId} ) )  # CorrelationId
        ? push( @data, $Offset_Response->{CorrelationId} )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'CorrelationId' );
    $response->{template}   = $_Response_header_template;
    $response->{len}        = $_Response_header_length;

    my $topics_array = $Offset_Response->{topics} // [];
    _ARRAY0( $topics_array )                                                # topics array size
        ? push( @data, scalar( @$topics_array ) )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'topics' );

    foreach my $topic ( @$topics_array ) {
        $response->{template}   .= q{s>};                                   # string length
        $response->{len}        += 2;
        _encode_string( $response, $topic->{TopicName} );                   # TopicName

        my $PartitionOffsets_array = $topic->{PartitionOffsets} // [];
        _ARRAY0( $PartitionOffsets_array )
            ? push( @data, scalar( @$PartitionOffsets_array ) )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'PartitionOffsets' );
        $response->{template}   .= q{l>};                                   # PartitionOffsets array size
        $response->{len}        += 4;   # [l] PartitionOffsets array size

        foreach my $PartitionOffsets ( @$PartitionOffsets_array ) {
            ( defined( $PartitionOffsets->{Partition} ) && isint( $PartitionOffsets->{Partition} ) )    # Partition
                ? push( @data, $PartitionOffsets->{Partition} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Partition' );
            ( defined( $PartitionOffsets->{ErrorCode} ) && isint( $PartitionOffsets->{ErrorCode} ) )    # ErrorCode
                ? push( @data, $PartitionOffsets->{ErrorCode} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ErrorCode' );
            my $Offset_array = $PartitionOffsets->{Offset} // [];
            _ARRAY0( $Offset_array )                                        # Offset array size
                ? push( @data, scalar( @$Offset_array ) )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Offset array' );
            $response->{template}   .= $_OffsetResponse_body_template;
            $response->{len}        += $_OffsetResponse_body_length;

            foreach my $Offset ( @$Offset_array ) {
                _is_suitable_int( $Offset )                                 # Offset
                    ? push( @data, _pack64( $Offset ) )
                    : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Offset' );
                $response->{template}   .= q{a[8]};
                $response->{len}        += 8;
            }
        }
    }

    return pack( $response->{template}, $response->{len}, @data );
}

# METADATA Request -------------------------------------------------------------

my $_decode_metadata_request_template = q{x[l]s>s>l>s>/al>X[l]l>/(s>/a)};
                                        # x[l]                    # Size (skip)
                                        # s>                      # ApiKey
                                        # s>                      # ApiVersion
                                        # l>                      # CorrelationId
                                        # s>/a                    # ClientId

                                        # l>                      # topics array size
                                        # X[l]
                                        # l>/(                    # topics array
                                        #     s>/a                    # TopicName
                                        # )

=head3 C<decode_metadata_request( $bin_stream_ref )>

Decodes the argument and returns a reference to the hash representing
the structure of the METADATA Request (examples see C<t/??_decode_encode.t>).

This function take argument. The following argument is currently recognized:

=over 3

=item C<$bin_stream_ref>

C<$bin_stream_ref> is a reference to the encoded Request buffer. The buffer
must be a non-empty binary string.

=back

=cut
sub decode_metadata_request {
    my ( $bin_stream_ref ) = @_;

    _is_bin_stream_correct( $bin_stream_ref )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data = unpack( $_decode_metadata_request_template, $$bin_stream_ref );

    my ( $i, $Metadata_Request ) = ( 0, {} );

    $APIKEY_METADATA                                     == $data[ $i++ ]   # ApiKey
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ApiKey' );
    $APIVERSION                                          == $data[ $i++ ]   # ApiVersion
        or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ApiVersion' );
    $Metadata_Request->{CorrelationId}                   =  $data[ $i++ ];  # CorrelationId
    $Metadata_Request->{ClientId}                        =  $data[ $i++ ];  # ClientId

    my $topics_array = $Metadata_Request->{topics}       =  [];
    my $topics_array_size                                =  $data[ $i++ ];  # topics array size
    while ( $topics_array_size-- ) {
        push( @$topics_array,                               $data[ $i++ ] );    # TopicName
    }

    return $Metadata_Request;
}

# METADATA Response ------------------------------------------------------------

=head3 C<encode_metadata_response( $Metadata_Response )>

Encodes the argument and returns a reference to the encoded binary string
representing a Response buffer.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$Metadata_Response>

C<$Metadata_Response> is a reference to the hash representing
the structure of the METADATA Response (examples see C<t/??_decode_encode.t>).

=back

=cut
sub encode_metadata_response {
    my ( $Metadata_Response ) = @_;

    _HASH( $Metadata_Response )
        or return _protocol_error( $ERROR_MISMATCH_ARGUMENT );

    my @data;
    my $response = {
                                                # template    => '...',
                                                # len         => ...,
        data        => \@data,
    };

    ( defined( $Metadata_Response->{CorrelationId} ) && isint( $Metadata_Response->{CorrelationId} ) )  # CorrelationId
        ? push( @data, $Metadata_Response->{CorrelationId} )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'CorrelationId' );
    $response->{template}   = $_Response_header_template;
    $response->{len}        = $_Response_header_length;

    my $Broker_array = $Metadata_Response->{Broker} // [];
    _ARRAY0( $Broker_array )                                                # Broker array size
        ? push( @data, scalar( @$Broker_array ) )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Broker' );

    foreach my $Broker ( @$Broker_array ) {
        ( defined( $Broker->{NodeId} ) && isint( $Broker->{NodeId} ) )      # NodeId
            ? push( @data, $Broker->{NodeId} )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'NodeId' );
        defined( my $Host = $Broker->{Host} )
            or return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Host' );
        _verify_string( $Host, 'Host' )
            or return;
        my $Host_length = length $Broker->{Host};
        _STRING( $Broker->{Host} )                                          # Host
            ? push( @data, $Host_length, $Broker->{Host} )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Host' );
        $response->{len} += $Host_length;
        _POSINT( $Broker->{Port} )                                          # Port
            ? push( @data, $Broker->{Port} )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Port' );
        $response->{template}   .= $_MetadataResponse_Broker_body_template;
        $response->{len}        += $_MetadataResponse_Broker_body_length;
    }

    my $TopicMetadata_array = $Metadata_Response->{TopicMetadata} // [];

    _ARRAY0( $TopicMetadata_array )                                         # TopicMetadata array size
        ? push( @data, scalar( @$TopicMetadata_array ) )
        : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'TopicMetadata' );
    $response->{template}   .= q{l>};
    $response->{len}        += 4;

    foreach my $TopicMetadata ( @$TopicMetadata_array ) {
        ( defined( $TopicMetadata->{ErrorCode} ) && isint( $TopicMetadata->{ErrorCode} ) )  # ErrorCode
            ? push( @data, $TopicMetadata->{ErrorCode} )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ErrorCode' );
        $response->{template} .= q{s>s>};
                    # 2 ErrorCode
                    # 2 string length
        $response->{len} += 4;
        _encode_string( $response, $TopicMetadata->{TopicName} );           # TopicName

        my $PartitionMetadata_array = $TopicMetadata->{PartitionMetadata} // [];
        _ARRAY0( $PartitionMetadata_array )
            ? push( @data, scalar( @$PartitionMetadata_array ) )
            : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'PartitionMetadata' );
        $response->{template}   .= q{l>};                                   # PartitionMetadata array size
        $response->{len}        += 4;

        foreach my $PartitionMetadata ( @$PartitionMetadata_array ) {
            ( defined( $PartitionMetadata->{ErrorCode} ) && isint( $PartitionMetadata->{ErrorCode} ) )  # ErrorCode
                ? push( @data, $PartitionMetadata->{ErrorCode} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'ErrorCode' );
            ( defined( $PartitionMetadata->{Partition} ) && isint( $PartitionMetadata->{Partition} ) )  # Partition
                ? push( @data, $PartitionMetadata->{Partition} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Partition' );
            ( defined( $PartitionMetadata->{Leader} ) && isint( $PartitionMetadata->{Leader} ) )    # Leader
                ? push( @data, $PartitionMetadata->{Leader} )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Leader' );
            my $Replicas_array = $PartitionMetadata->{Replicas} // [];
            _ARRAY0( $Replicas_array )                                      # Replicas array size
                ? push( @data, scalar( @$Replicas_array ) )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Replicas' );
            $response->{template}   .= $_MetadataResponse_PartitionMetadata_body_template;
            $response->{len}        += $_MetadataResponse_PartitionMetadata_body_length;

            foreach my $Replica ( @$Replicas_array ) {
                ( defined( $Replica ) && isint( $Replica ) )                 # ReplicaId
                    ? push( @data, $Replica )
                    : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Replicas ReplicaId' );
                $response->{template}   .= q{l>};
                $response->{len}        += 4;
            }

            my $Isr_array = $PartitionMetadata->{Isr} // [];
            _ARRAY0( $Isr_array )                                           # Isr array size
                ? push( @data, scalar( @$Isr_array ) )
                : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Isr' );
            $response->{template}   .= q{l>};
            $response->{len}        += 4;

            foreach my $Isr ( @$Isr_array ) {
                ( defined( $Isr ) && isint( $Isr ) )                        # ReplicaId
                    ? push( @data, $Isr )
                    : return _protocol_error( $ERROR_REQUEST_OR_RESPONSE, 'Isr ReplicaId' );
                $response->{template}   .= q{l>};
                $response->{len}        += 4;
            }
        }
    }

    return pack( $response->{template}, $response->{len}, @data );
}

=head3 C<Kafka::MockProtocol::last_errorcode()>

This method returns an error code that specifies the
description in the C<%Kafka::ERROR> hash. Analysing this information
can be done to determine the cause of the error.

=cut
sub last_errorcode {
    return ( $_package_error // 0 ) + 0;
}

=head3 C<Kafka::MockProtocol::last_error()>

This method returns an error message that contains information about the
encountered failure. Messages returned from this method may contain
additional details and do not coincide completely with the C<%Kafka::ERROR> hash.

=cut
sub last_error {
    return ( $_package_error // q{} ).q{};
}

#-- private functions ----------------------------------------------------------

# Generates a template to decrypt the request body
sub _decode_produce_request_template {
    my ( $request ) = @_;

    my (
        $ClientId_length,
        $topics_array_size,
        $TopicName_length,
        $partitions_array_size,
    );

    $request->{template}        = $_ProduceRequest_header_template;
    $request->{stream_offset}   = $_ProduceRequest_header_length;   # bytes before ClientId length
                                                                                # [l] Size
                                                                                # [s] ApiKey
                                                                                # [s] ApiVersion
                                                                                # [l] CorrelationId

    $ClientId_length = unpack(
         q{x[}.$request->{stream_offset}
        .q{]s>},                        # ClientId length
        ${ $request->{bin_stream} }
    );

    $request->{stream_offset} += 8      # bytes before RequiredAcks
                                                                                # [s] ClientId length
        + $ClientId_length                                                      # ClientId
                                        # bytes before topics array size
                                                                                # [s] RequiredAcks
                                                                                # [l] Timeout
        ;
    $topics_array_size = unpack(
         q{x[}.$request->{stream_offset}
        .q{]l>},                        # topics array size
        ${ $request->{bin_stream} }
    );

    $request->{stream_offset} += 4;     # bytes before TopicName length
                                                                                # [l] topics array size

    while ( $topics_array_size-- ) {
        $request->{template} .= $_ProduceRequest_topic_body_template;
                                        # TopicName
                                        # partitions array size
                                        # Partition

        $TopicName_length = unpack(
             q{x[}.$request->{stream_offset}
            .q{]s>},                    # TopicName length
            ${ $request->{bin_stream} }
        );
        $request->{stream_offset} +=    # bytes before partitions array size
              2                                                                 # [s] TopicName length
            + $TopicName_length                                                 # TopicName
            ;

        $partitions_array_size = unpack(
             q{x[}.$request->{stream_offset}
            .q{]l>},                    # partitions array size
            ${ $request->{bin_stream} }
        );

        $request->{stream_offset} += 8; # bytes before Partition
                                                                                # [l] partitions array size
                                        # bytes before MessageSetSize
                                                                                # [l] Partition

        _decode_MessageSet_template( $request );
    }
}

# Verifies that the argument is a reference to a string
sub _is_bin_stream_correct {
    my ( $bin_stream_ref ) = @_;

    return _SCALAR( $bin_stream_ref ) && _STRING( $$bin_stream_ref );
}

# Verifies that the first argument is the string does not contain Unicode data
sub _verify_string {
    my ( $string, $description ) = @_;

    return 1
        if $string eq q{};
    _STRING( $string )
        // return _protocol_error( $ERROR_MISMATCH_ARGUMENT, $description );
    utf8::is_utf8( $string )
        and return _protocol_error( $ERROR_NOT_BINARY_STRING, $description );

    return 1;
}

# Sets the value of an internal attribute describing the error:
#   $error_code     - error code
#   $description    - additional description of the error
sub _protocol_error {
    my ( $error_code, $description ) = @_;

    $_package_error = dualvar $error_code, $ERROR{ $error_code }.( $description ? ': '.$description : q{} );
    return;
}

1;

__END__

=head1 DIAGNOSTICS

Error will set the non-zero value returned by L</"Kafka::MockProtocol::last_errorcode()"> function.

Error messages are presented in the hash L<%Kafka::ERROR|%Kafka::ERROR>.
You may experience the following errors:

=over 3

=item C<$Kafka::ERROR_MISMATCH_ARGUMENT>

This means that you didn't give the right argument to some of
L<functions|/FUNCTIONS>.

=item C<$Kafka::ERROR_REQUEST_OR_RESPONSE>

This means that one of the fields of the request or response contains invalid data.

=item C<$Kafka::ERROR_NOT_BINARY_STRING>

This means that one of the string fields of response does not contain a string
or contains a string with Unicode data.

=back

For more error description, always look at the message
from the C<Kafka::MockProtocol::last_error()> function.

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

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

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
