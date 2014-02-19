package Kafka::Consumer;

=head1 NAME

Kafka::Consumer - Perl interface for Kafka consumer client.

=head1 VERSION

This documentation refers to C<Kafka::Consumer> version 0.8006 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.8006';

#-- load the modules -----------------------------------------------------------

use Carp;
use Params::Util qw(
    _INSTANCE
    _NONNEGINT
    _POSINT
    _STRING
);
use Scalar::Util::Numeric qw(
    isint
);
use Try::Tiny;

use Kafka qw(
    $BITS64
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    %ERROR
    $ERROR_COMPRESSED_PAYLOAD
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_PARTITION_DOES_NOT_MATCH
    $ERROR_TOPIC_DOES_NOT_MATCH
    $MESSAGE_SIZE_OVERHEAD
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $RECEIVE_EARLIEST_OFFSETS
);
use Kafka::Exceptions;
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_OFFSET
    $MAX_INT32
    _get_CorrelationId
    _isbig
);
use Kafka::Connection;
use Kafka::Message;

if ( !$BITS64 ) { eval 'use Kafka::Int64; 1;' or die "Cannot load Kafka::Int64 : $@"; } ## no critic

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka qw(
        $DEFAULT_MAX_BYTES
        $DEFAULT_MAX_NUMBER_OF_OFFSETS
        $RECEIVE_EARLIEST_OFFSETS
    );
    use Kafka::Connection;
    use Kafka::Consumer;

    my ( $connection, $consumer );
    try {

        #-- Connection
        $connection = Kafka::Connection->new( host => 'localhost' );

        #-- Consumer
        $consumer = Kafka::Consumer->new( Connection  => $connection );

        # Get a list of valid offsets up max_number before the given time
        my $offsets = $consumer->offsets(
            'mytopic',                      # topic
            0,                              # partition
            $RECEIVE_EARLIEST_OFFSETS,      # time
            $DEFAULT_MAX_NUMBER_OF_OFFSETS  # max_number
        );

        if( @$offsets ) {
            say "Received offset: $_" foreach @$offsets;
        } else {
            warn "Error: Offsets are not received\n";
        }

        # Consuming messages
        my $messages = $consumer->fetch(
            'mytopic',                      # topic
            0,                              # partition
            0,                              # offset
            $DEFAULT_MAX_BYTES              # Maximum size of MESSAGE(s) to receive
        );

        if ( $messages ) {
            foreach my $message ( @$messages ) {
                if( $message->valid ) {
                    say 'payload    : ', $message->payload;
                    say 'key        : ', $message->key;
                    say 'offset     : ', $message->offset;
                    say 'next_offset: ', $message->next_offset;
                } else {
                    say 'error      : ', $message->error;
                }
            }
        }

    } catch {
        if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $_->code, ') ',  $_->message, "\n";
            exit;
        } else {
            die $_;
        }
    };

    # Closes the consumer and cleans up
    undef $consumer;
    undef $connection;

=head1 DESCRIPTION

Kafka consumer API is implemented by C<Kafka::Consumer> class.

The main features of the C<Kafka::Consumer> class are:

=over 3

=item *

Provides an object-oriented API for consuming messages.

=item *

Provides Kafka FETCH and OFFSETS requests (FETCH does not support compression codec).

=item *

Supports parsing the Apache Kafka 0.8 Wire Format protocol.

=item *

Works with 64-bit elements of the Kafka Wire Format protocol
on 32 bit systems.

=back

The Kafka consumer response returns ARRAY references for C<offsets> and
C<fetch> methods.

Array returned by C<offsets> contains offset integers.

Array returned by C<fetch> contains objects of L<Kafka::Message|Kafka::Message> class.

=cut

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Creates a new consumer client object. Returns the created C<Kafka::Consumer> object.

C<new()> takes arguments in key-value pairs. The following arguments are recognized:

=over 3

=item C<Connection =E<gt> $connection>

C<$connection> is the L<Kafka::Connection|Kafka::Connection> object responsible for communication with
the Apache Kafka cluster.

=item C<CorrelationId =E<gt> $correlation_id>

Optional, default is C<undef>.

C<Correlation> is a user-supplied integer.
It will be passed back in the response by the server, unmodified.
The C<$correlation_id> should be an integer number.

C<CorrelationId> will be auto-assigned (random negative number) if it was not provided on
creation of L<Kafka::Producer|Kafka::Producer> object.

An exception is thrown if C<CorrelationId> sent with request does not match C<CorrelationId> received in response.

=item C<ClientId =E<gt> $client_id>

This is a user supplied identifier (string) for the client application.

C<ClientId> will be auto-assigned if not passed in when creating L<Kafka::Producer|Kafka::Producer> object.

=item C<MaxWaitTime =E<gt> $max_time>

The maximum amount of time (ms) to wait when no sufficient data is available at the time the
request was issued.

Optional, default is C<$DEFAULT_MAX_WAIT_TIME>.

C<$DEFAULT_MAX_WAIT_TIME> is the default time that can be imported from the
L<Kafka|Kafka> module.

The C<$max_time> must be a positive integer.

=item C<MinBytes =E<gt> $min_bytes>

The minimum number of bytes of messages that must be available to give a response.
If the client sets this to C<$MIN_BYTES_RESPOND_IMMEDIATELY> the server will always respond
immediately. If it is set to C<$MIN_BYTES_RESPOND_HAS_DATA>, the server will respond as soon
as at least one partition has at least 1 byte of data or the specified timeout occurs.
Setting higher values in combination with the bigger timeouts allows reading larger chunks of data.

Optional, int32 signed integer, default is C<$MIN_BYTES_RESPOND_IMMEDIATELY>.

C<$MIN_BYTES_RESPOND_IMMEDIATELY>, C<$MIN_BYTES_RESPOND_HAS_DATA> are the defaults that
can be imported from the L<Kafka|Kafka> module.

The C<$min_bytes> must be a non-negative int32 signed integer.

=item C<MaxBytes =E<gt> $max_bytes>

The maximum bytes to include in the message set for this partition.

Optional, int32 signed integer, default = C<$DEFAULT_MAX_BYTES> (1_000_000).

The C<$max_bytes> must be more than C<$MESSAGE_SIZE_OVERHEAD>
(size of protocol overhead - data added by Kafka wire protocol to each message).

C<$DEFAULT_MAX_BYTES>, C<$MESSAGE_SIZE_OVERHEAD>
are the defaults that can be imported from the L<Kafka|Kafka> module.

=item C<MaxNumberOfOffsets =E<gt> $max_number>

Limit the number of offsets returned by Kafka.

That is a non-negative integer.

Optional, int32 signed integer, default = C<$DEFAULT_MAX_NUMBER_OF_OFFSETS> (100).

C<$DEFAULT_MAX_NUMBER_OF_OFFSETS>
is the default that can be imported from the L<Kafka|Kafka> module.

=back

=cut
sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        Connection          => undef,
        CorrelationId       => undef,
        ClientId            => undef,
        MaxWaitTime         => $DEFAULT_MAX_WAIT_TIME,
        MinBytes            => $MIN_BYTES_RESPOND_IMMEDIATELY,
        MaxBytes            => $DEFAULT_MAX_BYTES,
        MaxNumberOfOffsets  => $DEFAULT_MAX_NUMBER_OF_OFFSETS,
    }, $class;

    while ( @args )
    {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    $self->{CorrelationId}  //= _get_CorrelationId;
    $self->{ClientId}       //= 'consumer';

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Connection' )
        unless _INSTANCE( $self->{Connection}, 'Kafka::Connection' );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'CorrelationId' )
        unless defined( $self->{CorrelationId} ) && isint( $self->{CorrelationId} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'ClientId' )
        unless ( $self->{ClientId} eq q{} || defined( _STRING( $self->{ClientId} ) ) ) && !utf8::is_utf8( $self->{ClientId} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxWaitTime ('.( $self->{MaxWaitTime} // '<undef>' ).')' )
        unless defined( $self->{MaxWaitTime} ) && isint( $self->{MaxWaitTime} ) && $self->{MaxWaitTime} > 0 && $self->{MaxWaitTime} <= $MAX_INT32;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MinBytes ('.( $self->{MinBytes} // '<undef>' ).')' )
        unless ( _isbig( $self->{MinBytes} ) ? ( $self->{MinBytes} >= 0 ) : defined( _NONNEGINT( $self->{MinBytes} ) ) ) && $self->{MinBytes} <= $MAX_INT32;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxBytes ('.( $self->{MaxBytes} // '<undef>' ).')' )
        unless ( _isbig( $self->{MaxBytes} ) ? ( $self->{MaxBytes} > 0 ) : _POSINT( $self->{MaxBytes} ) ) && $self->{MaxBytes} >= $MESSAGE_SIZE_OVERHEAD && $self->{MaxBytes} <= $MAX_INT32;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxNumberOfOffsets ('.( $self->{MaxNumberOfOffsets} // '<undef>' ).')' )
        unless defined( _POSINT( $self->{MaxNumberOfOffsets} ) ) && $self->{MaxNumberOfOffsets} <= $MAX_INT32;

    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are defined for the C<Kafka::Consumer> class:

=cut

#-- public methods -------------------------------------------------------------

=head3 C<fetch( $topic, $partition, $offset, $max_size )>

Get a list of messages to consume one by one up to C<$max_size> bytes.

Returns the reference to array of the L<Kafka::Message|Kafka::Message> objects.

C<fetch()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer.

=item C<$offset>

Offset in topic and partition to start from (64-bit integer).

The argument must be a non-negative integer. The argument may be a
L<Math::BigInt|Math::BigInt> integer on 32-bit system.

=item C<$max_size>

C<$max_size> is the maximum size of the messages set to return. The argument
must be a positive int32 signed integer.

The maximum size of a request limited by C<MAX_SOCKET_REQUEST_BYTES> that
can be imported from L<Kafka|Kafka> module.

=back

=cut
sub fetch {
    my ( $self, $topic, $partition, $offset, $max_size ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$topic' )
        unless defined( $topic ) && ( $topic eq q{} || defined( _STRING( $topic ) ) ) && !utf8::is_utf8( $topic );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$partition' )
        unless defined( $partition ) && isint( $partition );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$offset' )
        unless defined( $offset ) && ( ( _isbig( $offset ) && $offset >= 0 ) || defined( _NONNEGINT( $offset ) ) );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, "\$max_size ($max_size)" )
        unless ( !defined( $max_size ) || ( ( _isbig( $max_size ) || _POSINT( $max_size ) ) && $max_size >= $MESSAGE_SIZE_OVERHEAD && $max_size <= $MAX_INT32 ) );

    my $request = {
        ApiKey                              => $APIKEY_FETCH,
        CorrelationId                       => $self->{CorrelationId},
        ClientId                            => $self->{ClientId},
        MaxWaitTime                         => $self->{MaxWaitTime},
        MinBytes                            => $self->{MinBytes},
        topics                              => [
            {
                TopicName                   => $topic,
                partitions                  => [
                    {
                        Partition           => $partition,
                        FetchOffset         => $offset,
                        MaxBytes            => $max_size // $self->{MaxBytes},
                    },
                ],
            },
        ],
    };

    my $response = $self->{Connection}->receive_response_to_request( $request );

    my $messages = [];
    foreach my $received_topic ( @{ $response->{topics} } ) {
        $received_topic->{TopicName} eq $topic
            or $self->_error( $ERROR_TOPIC_DOES_NOT_MATCH, "'$topic' ne '".$received_topic->{TopicName}."'" );
        foreach my $received_partition ( @{ $received_topic->{partitions} } ) {
            $received_partition->{Partition} == $partition
                or $self->_error( $ERROR_PARTITION_DOES_NOT_MATCH, "$partition != ".$received_partition->{Partition} );
            my $HighwaterMarkOffset = $received_partition->{HighwaterMarkOffset};
            foreach my $Message ( @{ $received_partition->{MessageSet} } ) {
                my $offset = $Message->{Offset};
                my $next_offset;
                if ( $BITS64 ) {
                    $next_offset += $offset + 1;
                }
                else {
                    $offset = Kafka::Int64::intsum( $offset, 0 );
                    $next_offset = Kafka::Int64::intsum( $offset, 1 );
                }
                my $message_error = $Message->{Attributes} ? $ERROR{ $ERROR_COMPRESSED_PAYLOAD } : q{};

                push( @$messages, Kafka::Message->new( {
                        Attributes          => $Message->{Attributes},
                        MagicByte           => $Message->{MagicByte},
                        key                 => $Message->{Key},
                        payload             => $Message->{Value},
                        offset              => $offset,
                        next_offset         => $next_offset,
                        error               => $message_error,
                        valid               => !$message_error,
                        HighwaterMarkOffset => $HighwaterMarkOffset,
                    } )
                );
            }
        }
    }

    return $messages;
}

=head3 C<offsets( $topic, $partition, $time, $max_number )>

Get a list of valid offsets up to C<$max_number> before the given time.

Returns reference to array of the offset integers
(L<Math::BigInt|Math::BigInt> integers on 32 bit system).

C<offsets()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer.

=item C<$time>

Get offsets before the given time (in milliseconds since UNIX Epoch).

The argument must be a positive number.

The argument may be a L<Math::BigInt|Math::BigInt> integer on 32 bit
system.

The special values C<$RECEIVE_LATEST_OFFSET> (-1), C<$RECEIVE_EARLIEST_OFFSETS> (-2) are allowed.

C<$RECEIVE_LATEST_OFFSET>, C<$RECEIVE_EARLIEST_OFFSETS>
are the defaults that can be imported from the L<Kafka|Kafka> module.

=item C<$max_number>

C<$max_number> is the maximum number of offsets to retrieve.

Optional. The argument must be a positive int32 signed integer.

=back

=cut
sub offsets {
    my ( $self, $topic, $partition, $time, $max_number ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$topic' )
        unless defined( $topic) && ( $topic eq q{} || defined( _STRING( $topic ) ) ) && !utf8::is_utf8( $topic );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$partition' )
        unless defined( $partition ) && isint( $partition );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$time' )
        unless defined( $time ) && ( _isbig( $time ) || isint( $time ) ) && $time >= $RECEIVE_EARLIEST_OFFSETS;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, "\$max_number ($max_number)" )
        unless !defined( $max_number ) || ( _POSINT( $max_number ) && $max_number <= $MAX_INT32 );

    my $request = {
        ApiKey                              => $APIKEY_OFFSET,
        CorrelationId                       => $self->{CorrelationId},
        ClientId                            => $self->{ClientId},
        topics                              => [
            {
                TopicName                   => $topic,
                partitions                  => [
                    {
                        Partition           => $partition,
                        Time                => $time,
                        MaxNumberOfOffsets  => $max_number // $self->{MaxNumberOfOffsets},
                    },
                ],
            },
        ],
    };

    my $response = $self->{Connection}->receive_response_to_request( $request );

    my $offsets = [];
    foreach my $received_topic ( @{ $response->{topics} } ) {
        foreach my $partition_offsets ( @{ $received_topic->{PartitionOffsets} } ) {
            push @$offsets, @{ $partition_offsets->{Offset} };
        }
    }

    return $offsets;
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

# Handler for errors
sub _error {
    my $self = shift;

    Kafka::Exception::Consumer->throw( throw_args( @_ ) );
}

#-- Closes and cleans up -------------------------------------------------------

1;

__END__

=head1 DIAGNOSTICS

When error is detected, an exception, represented by object of C<Kafka::Exception::Consumer> class,
is thrown (see L<Kafka::Exceptions|Kafka::Exceptions>).

L<code|Kafka::Exceptions/code> and a more descriptive L<message|Kafka::Exceptions/message> provide
information about thrown exception. Consult documentation of the L<Kafka::Exceptions|Kafka::Exceptions>
for the list of all available methods.

Authors suggest using of L<Try::Tiny|Try::Tiny>'s C<try> and C<catch> to handle exceptions while
working with L<Kafka|Kafka> package.

=over 3

=item C<Invalid argument>

Invalid argument passed to a C<new> L<constructor|/CONSTRUCTOR> or other L<method|/METHODS>.

=item C<Can't send>

Request cannot be sent.

=item C<Can't recv>

Response cannot be received.

=item C<Can't bind>

TCP connection can't be established on the given host and port.

=item C<Can't get metadata>

Failed to obtain metadata from Kafka servers.

=item C<Leader not found>

Missing information about server-leader in metadata.

=item C<Mismatch CorrelationId>

C<CorrelationId> of response doesn't match one in request.

=item C<There are no known brokers>

Resulting metadata has no information about cluster brokers.

=item C<Can't get metadata>

Received metadata has incorrect internal structure.

=back

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
