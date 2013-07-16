package Kafka::MockIO;

# Kafka::IO imitation

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# PRECONDITIONS ----------------------------------------------------------------

our $VERSION = '0.8001';

#-- load the modules -----------------------------------------------------------

use Carp;
use Const::Fast;
use List::Util qw(
    min
);
use Params::Util qw(
    _HASH
    _NONNEGINT
    _NUMBER
    _POSINT
    _STRING
);
use Sub::Install;

use Kafka qw(
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NO_ERROR
    $ERROR_NOT_BINARY_STRING
    $KAFKA_SERVER_PORT
    $RECEIVE_LATEST_OFFSET
    $RECEIVE_EARLIEST_OFFSETS
    $REQUEST_TIMEOUT
);
use Kafka::Internals qw(
    $APIKEY_PRODUCE
    $APIKEY_FETCH
    $APIKEY_OFFSET
    $APIKEY_METADATA
    $MAX_SOCKET_REQUEST_BYTES
);
use Kafka::IO;
use Kafka::Protocol qw(
    $COMPRESSION_NONE
    $COMPRESSION_NOT_EXIST
);
use Kafka::MockProtocol qw(
    decode_fetch_request
    decode_metadata_request
    decode_offset_request
    decode_produce_request
    encode_fetch_response
    encode_metadata_response
    encode_offset_response
    encode_produce_response
);

#-- declarations ---------------------------------------------------------------

const my $MOCKED_PACKAGE    => 'Kafka::IO';

# Use Kafka::MockIO only with the following information:
const our $PARTITION        => 0;

#-- Global data ----------------------------------------------------------------

my %_reinstall = (
    new                         => [ \&Kafka::IO::new,      \&new ],
    send                        => [ \&Kafka::IO::send,     \&send ],
    receive                     => [ \&Kafka::IO::receive,  \&receive ],
    close                       => [ \&Kafka::IO::close,    \&close ],
    is_alive                    => [ \&Kafka::IO::is_alive, \&is_alive ],
    _decoded_topic_partition    => [ sub {},                \&_decoded_topic_partition ],
    _verify_string              => [ sub {},                \&_verify_string ],
    add_special_case            => [ sub {},                \&add_special_case ],
    del_special_case            => [ sub {},                \&del_special_case ],
    special_cases               => [ sub {},                \&special_cases ],
);

my ( $ApiKey, $encoded_response );

my %received_data;                                          # (
                                                            #   topic   => {
                                                            #       partition   => [
                                                            #           [ Key, Value ],
                                                            #           ...,
                                                            #       ],
                                                            #       ...,
                                                            #   }
                                                            #   ...,
                                                            # )

my %_special_cases;                                         # encoded_request => encoded_response, ...

my $decoded_produce_response = {
    CorrelationId                           => 0,           # for example
    topics                                  => [
        {
            TopicName                       => 'mytopic',   # for example
            partitions                      => [
                {
                    Partition               => $PARTITION,
                    ErrorCode               => 0,
                    Offset                  => 0,
                },
            ],
        },
    ],
};

my $decoded_fetch_response = {
    CorrelationId                           => 0,           # for example
    topics                                  => [
        {
            TopicName                       => 'mytopic',   # for example
            partitions                      => [
                {
                    Partition               => $PARTITION,
                    ErrorCode               => 0,
                    HighwaterMarkOffset     => 2,
                    MessageSet              => [
                        #{
                        #    Offset          => 0,
                        #    MagicByte       => 0,
                        #    Attributes      => 0,
                        #    Key             => q{},
                        #    Value           => 'Hello!',
                        #},
                        #{
                        #    Offset          => 1,
                        #    MagicByte       => 0,
                        #    Attributes      => 0,
                        #    Key             => q{},
                        #    Value           => 'Hello, World!',
                        #},
                    ],
                },
            ],
        },
    ],
};

my $decoded_offset_response = {
    CorrelationId                       => 0,           # for example
    topics                              => [
        {
            TopicName                   => 'mytopic',   # for example
            PartitionOffsets            => [
                {
                    Partition           => $PARTITION,
                    ErrorCode           => 0,
                    Offset              => [
#                                           0,
                    ],
                },
            ],
        },
    ],
};

my $decoded_metadata_response = {
    CorrelationId                       => 0,           # for example
    Broker                              => [
        {
            NodeId                      => 2,
            Host                        => 'myhost',
            Port                        => 9097,
        },
        {
            NodeId                      => 0,
            Host                        => 'myhost',
            Port                        => 9095,
        },
        {
            NodeId                      => 1,
            Host                        => 'myhost',
            Port                        => 9096,
        },
    ],
    TopicMetadata                       => [
        {
            ErrorCode                   => 0,
            TopicName                   => 'mytopic',   # for example
            PartitionMetadata           => [
                {
                    ErrorCode           => 0,
                    Partition           => $PARTITION,
                    Leader              => 2,
                    Replicas            => [    # of ReplicaId
                                           2,
                                           0,
                                           1,
                    ],
                    Isr                 => [    # of ReplicaId
                                           2,
                                           0,
                                           1,
                    ],
                },
            ],
        },
    ],
};

#-- public functions -----------------------------------------------------------

sub override {
    foreach my $method ( keys %_reinstall ) {
        Sub::Install::reinstall_sub( {
            code    => $_reinstall{ $method }->[1],
            into    => $MOCKED_PACKAGE,
            as      => $method,
        } );
    }
}

sub undefine {
    foreach my $method ( keys %_reinstall ) {
        Sub::Install::reinstall_sub( {
            code    => $_reinstall{ $method }->[0],
            into    => $MOCKED_PACKAGE,
            as      => $method,
        } );
    }
}

sub add_special_case {
    my ( $cases ) = @_;

    _HASH( $cases ) or confess 'requires a hash request-response';
    foreach my $encoded_request ( keys %{ $cases } ) {
        _STRING( $cases->{ $encoded_request } ) or confess 'hash must contain encoded responses';
    }

    foreach my $encoded_request ( keys %{ $cases } ) {
        $_special_cases{ $encoded_request } = $cases->{ $encoded_request };
    }
}

sub del_special_case {
    my ( $encoded_request ) = @_;

    delete $_special_cases{ $encoded_request };
}

sub special_cases {
    return \%_special_cases;
}

#-- private functions ----------------------------------------------------------

#-- constructor ----------------------------------------------------------------

sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        host        => q{},
        port        => $KAFKA_SERVER_PORT,
        timeout     => $REQUEST_TIMEOUT,
    }, $class;

    while ( @args ) {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    Kafka::IO::_error( $self, $ERROR_NO_ERROR );

    if    ( !_STRING( $self->{host} ) )                                 { Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, 'Kafka::IO->new - host' ); }
    elsif ( !_POSINT( $self->{port} ) )                                 { Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, 'Kafka::IO::->new - port' ); }
    elsif ( !( _NUMBER( $self->{timeout} ) && $self->{timeout} > 0 ) )  { Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, 'Kafka::IO::->new - timeout' ); }
    else  {
        # nothing to do
    }

    return $self;
}

#-- public attributes ----------------------------------------------------------

#-- public methods -------------------------------------------------------------

sub send {
    my ( $self, $message ) = @_;

    my $description = 'Kafka::IO->send';
    $self->_verify_string( $message, $description )
        or return;
    ( my $len = length( $message .= q{} ) ) <= $MAX_SOCKET_REQUEST_BYTES
        or $self->_error( $ERROR_MISMATCH_ARGUMENT, $description );

    Kafka::IO::_debug_msg( $self, 'Request to', 'green', $message ) if $Kafka::IO::DEBUG;

    if ( exists $_special_cases{ $message } ) {
        $encoded_response = $_special_cases{ $message };
        return length( $message );
    }

    $ApiKey = unpack( '
        x[l]                # Size
        s>                  # ApiKey
        ', $message );

    # Set up the response

    if ( $ApiKey == $APIKEY_PRODUCE ) {
        my $decoded_produce_request = decode_produce_request( \$message )
            or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        my ( $topic, $partition ) = $self->_decoded_topic_partition( $decoded_produce_request, $decoded_produce_response );
        $partition // return;

        if ( !exists( $received_data{ $topic }->{ $partition } ) ) {
            $received_data{ $topic }->{ $partition } = [];
        }
        my $data = $received_data{ $topic }->{ $partition };

        $decoded_produce_response->{topics}->[0]->{partitions}->[0]->{Offset} = scalar @{ $data };
        foreach my $Message ( @{ $decoded_produce_request->{topics}->[0]->{partitions}->[0]->{MessageSet} } ) {
            foreach my $key_name ( 'Key', 'Value' ) {
                if ( defined( my $value = $Message->{ $key_name } ) ) {
                    $self->_verify_string( $value, "$description ($key_name)" )
                        or return;
                }
            }
            push @{ $data }, [ $Message->{Key} // q{}, $Message->{Value} // q{} ];
        }

        $encoded_response = encode_produce_response( $decoded_produce_response )
            or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
    }

    elsif ( $ApiKey == $APIKEY_FETCH ) {
        my $decoded_fetch_request = decode_fetch_request( \$message )
            or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        my ( $topic, $partition ) = $self->_decoded_topic_partition( $decoded_fetch_request, $decoded_fetch_response );
        $partition // return;

        my $partition_data = $decoded_fetch_request->{topics}->[0]->{partitions}->[0];
        my $FetchOffset = $partition_data->{FetchOffset};
        my $MaxBytes    = $partition_data->{MaxBytes};

        $partition_data = $decoded_fetch_response->{topics}->[0]->{partitions}->[0];
        my $messages = $partition_data->{MessageSet} = [];
        my $data = $received_data{ $topic }->{ $partition };
        my $HighwaterMarkOffset = $partition_data->{HighwaterMarkOffset} = scalar @{ $data };
        my $full_message_set_size = 0;
        for ( my $i = $FetchOffset; $i < $HighwaterMarkOffset; ++$i ) {
            my $Key     = $data->[ $i ]->[0];
            my $Value   = $data->[ $i ]->[1];
            my $message_set_size +=
                  8                         # [q]   Offset
                + 4                         # [l]   MessageSize
                + 4                         # [l]   Crc
                + 1                         # [c]   MagicByte
                + 1                         # [c]   Attributes
                + 4                         # [l]   Key length
                + length( $Key )            # Key
                + 4                         # [l]   Value length
                + length( $Value )          # Value
                ;
            if ( $full_message_set_size + $message_set_size <= $MaxBytes ) {
                push @{ $messages }, {
                    Offset      => $i,
                    MagicByte   => $COMPRESSION_NOT_EXIST,
                    Attributes  => $COMPRESSION_NONE,
                    Key         => $Key,
                    Value       => $Value,
                };
            }
            else {
# NOTE: not all messages can be returned
                last;
            }
            $full_message_set_size += $message_set_size;
        }

        $self->_verify_string( $topic, "$description (TopicName)" )
            or return;
        my $max_response_length = $MaxBytes
            + 4                         # [l]   Size
            + 4                         # [l]   CorrelationId
            + 4                         # [l]   topics array size
            + 2                         # [s]   TopicName length
            + length( $topic )          # TopicName
            + 4                         # [l]   partitions array size
            + 4                         # [l]   Partition
            + 2                         # [s]   ErrorCode
            + 8                         # [q]   HighwaterMarkOffset
            + 4                         # [l]   MessageSetSize
            ;
        $encoded_response = encode_fetch_response( $decoded_fetch_response )
            or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        $encoded_response = substr( $encoded_response, 0, $max_response_length );
    }

    elsif ( $ApiKey == $APIKEY_OFFSET ) {
        my $decoded_offset_request = decode_offset_request( \$message )
            or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        my ( $topic, $partition ) = $self->_decoded_topic_partition( $decoded_offset_request, $decoded_offset_response );
        $partition // return;

        my $partition_data = $decoded_offset_request->{topics}->[0]->{partitions}->[0];
        my $MaxNumberOfOffsets = $partition_data->{MaxNumberOfOffsets};
        my $Time = $partition_data->{Time};

        my $offsets = $decoded_offset_response->{topics}->[0]->{PartitionOffsets}->[0]->{Offset} = [];
        if ( $Time == $RECEIVE_LATEST_OFFSET ) {
            push( @{ $offsets }, ( exists( $received_data{ $topic }->{ $partition } )
                ? scalar( @{ $received_data{ $topic }->{ $partition } } )
                : () ),
                0 );
        }
        elsif ( $Time == $RECEIVE_EARLIEST_OFFSETS ) {
            push @{ $offsets }, 0;
        }
        else {
            if ( exists( $received_data{ $topic }->{ $partition } ) ) {
                my $max_offset = min $MaxNumberOfOffsets, $#{ $received_data{ $topic }->{ $partition } };
# NOTE:
# - always return starting at offset 0
# - not verified in practice, the order in which the kafka server returns the offsets
                push @$offsets, ( 0..$max_offset );
            }
        }

        $encoded_response = encode_offset_response( $decoded_offset_response )
            or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
    }

    elsif ( $ApiKey == $APIKEY_METADATA ) {
        my $decoded_metadata_request = decode_metadata_request( \$message )
            or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        my $TopicName = $decoded_metadata_request->{topics}->[0];
        $self->_verify_string( $TopicName, "$description (TopicName)" )
            or return;
        $decoded_metadata_response->{TopicMetadata}->[0]->{TopicName} = $TopicName;
        $decoded_metadata_response->{CorrelationId} = $decoded_metadata_request->{CorrelationId};

        $encoded_response = encode_metadata_response( $decoded_metadata_response )
            or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
    }

    return $len;
}

sub receive {
    my ( $self, $length ) = @_;

    _POSINT( $length )
        or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, 'Kafka::IO->receive' );

    my $message = substr( $encoded_response, 0, $length, q{} );

    Kafka::IO::_debug_msg( $self, 'Response from', 'yellow', $message )
        if $Kafka::IO::DEBUG;

    return \$message;
}

sub close {
    my ( $self ) = @_;

    delete $self->{$_} foreach keys %$self;
    return;
}

sub is_alive {
#    my ( $self ) = @_;

    return 1;
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

sub _decoded_topic_partition {
    my ( $self, $decoded_request, $decoded_response ) = @_;

    my $topic_data = $decoded_request->{topics}->[0];
    my $topic = $topic_data->{TopicName};

    my $partition = $topic_data->{partitions}->[0]->{Partition};
    $partition == $PARTITION
        or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, "Use Kafka::MockIO only with partition = $PARTITION" );

    $topic_data = $decoded_response->{topics}->[0];
    $topic_data->{TopicName} = $topic;
    if ( $ApiKey == $APIKEY_OFFSET ) {
        $topic_data->{PartitionOffsets}->[0]->{Partition} = $partition;
    }
    else {
        $topic_data->{partitions}->[0]->{Partition} = $partition;
    }

    $decoded_response->{CorrelationId} = $decoded_request->{CorrelationId};

    return $topic, $partition;
}

sub _verify_string {
    my ( $self, $string, $description ) = @_;

    ( _STRING( $string ) or $string eq q{} )
        or return Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
    !utf8::is_utf8( $string )
        or return Kafka::IO::_error( $self, $ERROR_NOT_BINARY_STRING, $description );

    return 1;
}

#-- Closes and cleans up -------------------------------------------------------

1;

__END__
