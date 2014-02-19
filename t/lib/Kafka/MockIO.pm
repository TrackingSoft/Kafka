package Kafka::MockIO;

=head1 NAME

Kafka::MockIO - object interface to simulate communications with the Apache Kafka
server via socket.

=head1 VERSION

This documentation refers to C<Kafka::MockIO> version 0.8006 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.8006';

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
use Scalar::Util qw(
    blessed
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

=head1 DESCRIPTION

This module is not a user module.

The main features of the C<Kafka::MockIO> class are:

=over 3

=item *

Emulates an object oriented model of communication (L<Kafka::IO|Kafka::IO> class).

=item *

Simplistically emulates interaction with kafka server.

=back

Examples see C<t/*_mock_io.t>.

=cut

const my $MOCKED_PACKAGE            => 'Kafka::IO';

=head2 EXPORT

Use Kafka::MockIO only with the following information.

The following constants are available for export

=cut

=head3 C<$TOPIC>

Topic name.

=cut
const our $TOPIC                    => 'mytopic';

=head3 C<$PARTITION>

0 - Partition number.

=cut
const our $PARTITION                => 0;

=head3 C<$KAFKA_MOCK_SERVER_PORT>

C<$KAFKA_MOCK_SERVER_PORT> is the default Apache Kafka server port
that can be imported from the L<Kafka|Kafka> module and = 9092.

=cut
const our $KAFKA_MOCK_SERVER_PORT   => $KAFKA_SERVER_PORT;

=head3 C<$KAFKA_MOCK_HOSTNAME>

'localhost' - C<$KAFKA_MOCK_HOSTNAME> is the default local host name.

=cut
const our $KAFKA_MOCK_HOSTNAME      => 'localhost';

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

our %_received_data;                                        # (
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
            TopicName                       => $TOPIC,      # for example
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
            TopicName                       => $TOPIC,      # for example
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
            TopicName                   => $TOPIC,      # for example
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
            Host                        => $KAFKA_MOCK_HOSTNAME,
            Port                        => $KAFKA_MOCK_SERVER_PORT + 2,
        },
        {
            NodeId                      => 0,
            Host                        => $KAFKA_MOCK_HOSTNAME,
            Port                        => $KAFKA_MOCK_SERVER_PORT,
        },
        {
            NodeId                      => 1,
            Host                        => $KAFKA_MOCK_HOSTNAME,
            Port                        => $KAFKA_MOCK_SERVER_PORT + 1,
        },
    ],
    TopicMetadata                       => [
        {
            ErrorCode                   => 0,
            TopicName                   => $TOPIC,      # for example
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

=head2 FUNCTIONS

The following methods are defined for the C<Kafka::MockIO> class:

=cut

=head3 C<override>

Override the C<Kafka::IO> class methods.

=cut
sub override {
    foreach my $method ( keys %_reinstall ) {
        Sub::Install::reinstall_sub( {
            code    => $_reinstall{ $method }->[1],
            into    => $MOCKED_PACKAGE,
            as      => $method,
        } );
    }
}

=head3 C<restore>

Restore the C<Kafka::IO> class methods.

=cut
sub restore {
    foreach my $method ( keys %_reinstall ) {
        Sub::Install::reinstall_sub( {
            code    => $_reinstall{ $method }->[0],
            into    => $MOCKED_PACKAGE,
            as      => $method,
        } );
    }
}

=head3 C<add_special_case( $cases )>

Adds special cases for use in the simulation of interaction with kafka server.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$cases>

C<$cases> is a reference to the hash representing
the special cases.

The keys of the hash should be binary-encoded query string to kafka server.
The values of each key must be encoded binary string of the expected response.

=back

=cut
sub add_special_case {
    my ( $cases ) = @_;

    blessed( $cases )
        and confess 'Do not use a class method as a method of the object';
    _HASH( $cases )
        or confess 'requires a hash request-response';
    foreach my $encoded_request ( keys %{ $cases } ) {
        _STRING( $cases->{ $encoded_request } )
            or confess 'hash must contain encoded responses';
    }

    foreach my $encoded_request ( keys %{ $cases } ) {
        $_special_cases{ $encoded_request } = $cases->{ $encoded_request };
    }
}

=head3 C<del_special_case( $encoded_request )>

Removes the special case.

This function take argument. The following argument is currently recognized:

=over 3

=item C<$encoded_request>

Binary string of the encoded request.

=back

=cut
sub del_special_case {
    my ( $encoded_request ) = @_;

    blessed( $encoded_request )
        and confess 'Do not use a class method as a method of the object';

    delete $_special_cases{ $encoded_request };
}

=head3 C<special_cases>

Returns a reference to a hash of special cases.

The keys of the hash are binary strings encoded requests to kafka server.
The value of each key is encoded binary string of the expected response.

=cut
sub special_cases {
    return \%_special_cases;
}

#-- private functions ----------------------------------------------------------

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Constructor emulation (C<Kafka::IO->new>).

=cut
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

    if    ( !( defined( $self->{host} ) && defined( _STRING( $self->{host} ) ) && !utf8::is_utf8( $self->{host} ) ) )   { Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, 'Kafka::IO->new - host' ); }
    elsif ( !_POSINT( $self->{port} ) )                                 { Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, 'Kafka::IO->new - port' ); }
    elsif ( !( _NUMBER( $self->{timeout} ) && $self->{timeout} > 0 ) )  { Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, 'Kafka::IO->new - timeout' ); }
    else  {
        $self->{socket} = 'fake true value';
    }

    return $self;
}

#-- public attributes ----------------------------------------------------------

#-- public methods -------------------------------------------------------------

=head2 METHODS

The following methods are defined for the C<Kafka::MockIO> class:

=cut

=head3 C<send>

Method emulation (C<Kafka::IO::send>).

=cut
sub send {
    my ( $self, $message ) = @_;

    my $description = 'Kafka::IO->send';
    defined( _STRING( $message ) )
        or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
    utf8::is_utf8( $message )
        and Kafka::IO::_error( $self, $ERROR_NOT_BINARY_STRING, $description );
    ( my $len = length( $message .= q{} ) ) <= $MAX_SOCKET_REQUEST_BYTES
        or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );

    Kafka::IO::_debug_msg( $self, $message, 'Request to', 'green' )
        if Kafka::IO->debug_level == 1;

    if ( exists $_special_cases{ $message } ) {
        $encoded_response = $_special_cases{ $message };
        return length( $message );
    }

    $ApiKey = unpack( q{
        x[l]                # Size
        s>                  # ApiKey
    }, $message );

    # Set up the response

    if ( $ApiKey == $APIKEY_PRODUCE ) {
        my $decoded_produce_request = decode_produce_request( \$message )
            or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        my ( $topic, $partition ) = $self->_decoded_topic_partition( $decoded_produce_request, $decoded_produce_response );
        $partition // return;

        if ( !exists( $_received_data{ $topic }->{ $partition } ) ) {
            $_received_data{ $topic }->{ $partition } = [];
        }
        my $data = $_received_data{ $topic }->{ $partition };

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
            or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
    }

    elsif ( $ApiKey == $APIKEY_FETCH ) {
        my $decoded_fetch_request = decode_fetch_request( \$message )
            or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        my ( $topic, $partition ) = $self->_decoded_topic_partition( $decoded_fetch_request, $decoded_fetch_response );
        $partition // return;

        my $partition_data = $decoded_fetch_request->{topics}->[0]->{partitions}->[0];
        my $FetchOffset = $partition_data->{FetchOffset};
        my $MaxBytes    = $partition_data->{MaxBytes};

        $partition_data = $decoded_fetch_response->{topics}->[0]->{partitions}->[0];
        my $messages = $partition_data->{MessageSet} = [];
        my $data = $_received_data{ $topic }->{ $partition } // [];
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
                    MagicByte   => 0,
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
            or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        $encoded_response = substr( $encoded_response, 0, $max_response_length );
    }

    elsif ( $ApiKey == $APIKEY_OFFSET ) {
        my $decoded_offset_request = decode_offset_request( \$message )
            or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        my ( $topic, $partition ) = $self->_decoded_topic_partition( $decoded_offset_request, $decoded_offset_response );
        $partition // return;

        my $partition_data = $decoded_offset_request->{topics}->[0]->{partitions}->[0];
        my $MaxNumberOfOffsets = $partition_data->{MaxNumberOfOffsets};
        my $Time = $partition_data->{Time};

        my $offsets = $decoded_offset_response->{topics}->[0]->{PartitionOffsets}->[0]->{Offset} = [];
        if ( $Time == $RECEIVE_LATEST_OFFSET ) {
            push( @{ $offsets }, ( exists( $_received_data{ $topic }->{ $partition } )
                ? scalar( @{ $_received_data{ $topic }->{ $partition } } )
                : () ),
                0 );
        }
        elsif ( $Time == $RECEIVE_EARLIEST_OFFSETS ) {
            push @{ $offsets }, 0;
        }
        else {
            if ( exists( $_received_data{ $topic }->{ $partition } ) ) {
                my $max_offset = min $MaxNumberOfOffsets, $#{ $_received_data{ $topic }->{ $partition } };
# NOTE:
# - always return starting at offset 0
# - not verified in practice, the order in which the kafka server returns the offsets
                push @$offsets, ( 0..$max_offset );
            }
        }

        $encoded_response = encode_offset_response( $decoded_offset_response )
            or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
    }

    elsif ( $ApiKey == $APIKEY_METADATA ) {
        my $decoded_metadata_request = decode_metadata_request( \$message )
            or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
        my $TopicName = $decoded_metadata_request->{topics}->[0];
        $self->_verify_string( $TopicName, "$description (TopicName)" )
            or return;
        $decoded_metadata_response->{TopicMetadata}->[0]->{TopicName} = $TopicName;
        $decoded_metadata_response->{CorrelationId} = $decoded_metadata_request->{CorrelationId};

        $encoded_response = encode_metadata_response( $decoded_metadata_response )
            or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
    }

    return $len;
}

=head3 C<receive>

Method emulation (C<Kafka::IO::receive>).

=cut
sub receive {
    my ( $self, $length ) = @_;

    _POSINT( $length )
        or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, 'Kafka::IO->receive' );

    my $message = substr( $encoded_response, 0, $length, q{} );

    Kafka::IO::_debug_msg( $self, $message, 'Response from', 'yellow' )
        if Kafka::IO->debug_level == 1;

    return \$message;
}

=head3 C<close>

Method emulation (C<Kafka::IO::close>).

=cut
sub close {
    my ( $self ) = @_;

    delete $self->{$_} foreach keys %$self;
    return;
}

=head3 C<is_alive>

Method emulation (C<Kafka::IO::is_alive>).

=cut
sub is_alive {
    my ( $self ) = @_;

    return !!$self->{socket};
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

# Determines the type of request, the topic and partition
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

# Verifies that the first argument is the string does not contain Unicode data
sub _verify_string {
    my ( $self, $string, $description ) = @_;

    return 1
        if defined( $string ) && $string eq q{};
    defined( _STRING( $string ) )
        or Kafka::IO::_error( $self, $ERROR_MISMATCH_ARGUMENT, $description );
    utf8::is_utf8( $string )
        and Kafka::IO::_error( $self, $ERROR_NOT_BINARY_STRING, $description );

    return 1;
}

#-- Closes and cleans up -------------------------------------------------------

1;

__END__

=head1 DIAGNOSTICS

Error diagnosis emulated methods corresponds to the work of class L<Kafka::IO|Kafka::IO/"DIAGNOSTICS">.

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
