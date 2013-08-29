package Kafka::Consumer;

=head1 NAME

Kafka::Consumer - Perl interface for 'consumer' client.

=head1 VERSION

This documentation refers to C<Kafka::Consumer> version 0.800_1 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.800_1';

#-- load the modules -----------------------------------------------------------

use Params::Util qw(
    _INSTANCE
    _NONNEGINT
    _POSINT
    _STRING
);
use Scalar::Util::Numeric qw(
    isbig
    isint
);

use Kafka qw(
    $BITS64
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    %ERROR
    $ERROR_COMPRESSED_PAYLOAD
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NO_ERROR
    $ERROR_PARTITION_DOES_NOT_MATCH
    $ERROR_TOPIC_DOES_NOT_MATCH
    $MESSAGE_SIZE_OVERHEAD
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $RECEIVE_EARLIEST_OFFSETS
);
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_OFFSET
    $DEFAULT_RAISE_ERROR
    _get_CorrelationId
    last_error
    last_errorcode
    RaiseError
    _fulfill_request
    _error
    _connection_error
    _set_error
);
use Kafka::Connection;
use Kafka::Message;

if ( !$BITS64 ) { eval 'use Kafka::Int64; 1;' or die "Cannot load Kafka::Int64 : $@"; } ## no critic

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Kafka qw(
        $DEFAULT_MAX_BYTES
        $DEFAULT_MAX_NUMBER_OF_OFFSETS
        $RECEIVE_EARLIEST_OFFSETS
    );
    use Kafka::Connection;
    use Kafka::Consumer;

    #-- Connection
    my $connect = Kafka::Connection->new( host => 'localhost' );

    #-- Consumer
    my $consumer = Kafka::Consumer->new( Connection  => $connect );

    # Get a list of valid offsets up max_number before the given time
    my $offsets = $consumer->offsets(
        'mytopic',                      # topic
        0,                              # partition
        $RECEIVE_EARLIEST_OFFSETS,      # time
        $DEFAULT_MAX_NUMBER_OF_OFFSETS  # max_number
    );
    if( $offsets ) {
        say "Received offset: $_" foreach @$offsets;
    }
    say STDERR 'Error: (', $consumer->last_errorcode, ') ', $consumer->last_error
        if !$offsets || $consumer->last_errorcode;

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
            }
            else {
                say 'error      : ', $message->error;
            }
        }
    }

    # Closes the consumer and cleans up
    undef $consumer;

=head1 DESCRIPTION

Kafka consumer API is implemented by C<Kafka::Consumer> class.

The main features of the C<Kafka::Consumer> class are:

=over 3

=item *

Provides an object oriented model of communication.

=item *

Supports parsing the Apache Kafka 0.8 Wire Format protocol.

=item *

Supports Apache Kafka Requests and Responses (FETCH with
no compression codec attribute now). Within this module we currently support
access to FETCH, OFFSETS Requests and Responses.

=item *

Support for working with 64 bit elements of the Kafka Wire Format protocol
on 32 bit systems.

=back

The Kafka consumer response has an ARRAY reference type for C<offsets> and
C<fetch> methods.
For the C<offsets> response array has the offset integers.

For the C<fetch> response the array has the class name
L<Kafka::Message|Kafka::Message> elements.

=cut

our $_package_error;

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Creates new consumer client object. Returns the created C<Kafka::Consumer>
object.

An error will cause the program to halt or the constructor will return the
C<Kafka::Consumer> object without halt, depending on the value of the C<RaiseError> attribute.

You can use the methods of the C<Kafka::Consumer> class - L</last_errorcode>
and L</last_error> for information about the error.

C<new()> takes arguments in key-value pairs.
The following arguments are currently recognized:

=over 3

=item C<Connection =E<gt> $connect>

C<$connect> is the L<Kafka::Connection|Kafka::Connection> object that allow you to communicate to
the Apache Kafka cluster.

=item C<RaiseError =E<gt> $mode>

Optional, default is 0.

An error will cause the program to halt if L</RaiseError> is true: C<confess>
if the argument is not valid or C<die> in the other error case
(this can always be trapped with C<eval>).

You should always check for errors, when not establishing the C<RaiseError>
mode to true.

=item C<CorrelationId =E<gt> $correlation_id>

Optional, default is C<undef>.

C<Correlation> is a user-supplied integer.
It will be passed back in the response by the server, unmodified.
The C<$correlation_id> should be an integer number.

Error is thrown if C<CorrelationId> of request will not match C<CorrelationId> in response.

C<CorrelationId> will be auto-assigned (random negative number) if it was not provided during
creation of C<Kafka::Producer> object.

=item C<ClientId =E<gt> $client_id>

This is a user supplied identifier (string) for the client application.

C<ClientId> will be auto-assigned if not passed in when creating C<Kafka::Producer> object.

=item C<MaxWaitTime =E<gt> $max_time>

The maximum amount of time (ms) to wait when no sufficient data is available at the time the
request was issued.

Optional, default is C<$DEFAULT_MAX_WAIT_TIME>.

C<$DEFAULT_MAX_WAIT_TIME> is the default time that can be imported from the
L<Kafka|Kafka> module.

The C<$max_time> must be a positive integer.

=item C<MinBytes =E<gt> $min_bytes>

RTFM: The minimum number of bytes of messages that must be available to give a response.
If the client sets this to C<$MIN_BYTES_RESPOND_IMMEDIATELY> the server will always respond
immediately. If it is set to C<$MIN_BYTES_RESPOND_HAS_DATA>, the server will respond as soon
as at least one partition has at least 1 byte of data or the specified timeout occurs.
Setting higher values in combination with the bigger timeouts results in reading larger chunks of data.

Optional, default is C<$MIN_BYTES_RESPOND_IMMEDIATELY>.

C<$MIN_BYTES_RESPOND_IMMEDIATELY>, C<$MIN_BYTES_RESPOND_HAS_DATA> are the defaults that
can be imported from the L<Kafka|Kafka> module.

The C<$min_bytes> must be a non-negative integer.

=item C<MaxBytes =E<gt> $max_bytes>

The maximum bytes to include in the message set for this partition.

Optional, default = C<$DEFAULT_MAX_BYTES> (1_000_000).

The C<$max_bytes> must be more than C<$MESSAGE_SIZE_OVERHEAD>
(size of protocol overhead - data added by protocol for each message).

C<$DEFAULT_MAX_BYTES>, C<$MESSAGE_SIZE_OVERHEAD>
are the defaults that can be imported from the L<Kafka|Kafka> module.

=item C<MaxNumberOfOffsets =E<gt> $max_number>

Kafka is return up to C<$max_number> of offsets.

That is a non-negative integer.

Optional, default = C<$DEFAULT_MAX_NUMBER_OF_OFFSETS> (100).

C<$DEFAULT_MAX_NUMBER_OF_OFFSETS>
is the default that can be imported from the L<Kafka|Kafka> module.

=back

=cut
sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        Connection          => undef,
        RaiseError          => $DEFAULT_RAISE_ERROR,
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

    if ( !defined _NONNEGINT( $self->RaiseError ) ) {
        $self->{RaiseError} = $DEFAULT_RAISE_ERROR;
        $self->_error( $ERROR_MISMATCH_ARGUMENT, 'RaiseError' );
    }
    elsif ( !_INSTANCE( $self->{Connection}, 'Kafka::Connection' ) )                    { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Connection' ); }
    elsif ( !( defined( $self->{CorrelationId} ) && isint( $self->{CorrelationId} ) ) ) { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'CorrelationId' ); }
    elsif ( !( $self->{ClientId} eq q{} || defined( _STRING( $self->{ClientId} ) ) && !utf8::is_utf8( $self->{ClientId} ) ) )   { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'ClientId' ); }
    elsif ( !( defined( $self->{MaxWaitTime} ) && isint( $self->{MaxWaitTime} ) && $self->{MaxWaitTime} > 0 ) ) { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxWaitTime' ); }
    elsif ( !defined( _NONNEGINT( $self->{MinBytes} ) ) )                               { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MinBytes' ); }
    elsif ( !( _POSINT( $self->{MaxBytes} ) && $self->{MaxBytes} >= $MESSAGE_SIZE_OVERHEAD ) )      { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxBytes' ); }
    elsif ( !defined( _POSINT( $self->{MaxNumberOfOffsets} ) ) )                        { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxNumberOfOffsets' ); }
    else {
        $self->_error( $ERROR_NO_ERROR )
            if $self->last_error;
    }

    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are defined for the C<Kafka::Consumer> class:

=over 3

=item *

The arguments below B<offset>, B<max_size> or B<time>, B<max_number> are
the additional information that might be encoded parameters of the messages
we want to access.

=back

The following methods are defined for the C<Kafka::Consumer> class:

=cut

#-- public methods -------------------------------------------------------------

=head3 C<fetch( $topic, $partition, $offset, $max_size )>

Get a list of messages to consume one by one up C<$max_size> bytes.

Returns the reference to array of the  L<Kafka::Message|Kafka::Message> class
name elements.
If there's an error,
returns the reference to empty array if the C<RaiseError> is not true.

C<fetch()> takes arguments. The following arguments are currently recognized:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer.

=item C<$offset>

Offset in topic and partition to start from (64 bits).

Optional. The argument must be a non-negative integer. The argument may be a
L<Math::BigInt|Math::BigInt> integer on 32 bit system.

=item C<$max_size>

C<$max_size> is the maximum size of the message set to return. The argument
must be a positive integer.
The maximum size of a request limited by C<MAX_SOCKET_REQUEST_BYTES> that
can be imported from L<Kafka|Kafka> module.

=back

=cut
sub fetch {
    my ( $self, $topic, $partition, $offset, $max_size ) = @_;

    if    ( !( $topic eq q{} || defined( _STRING( $topic ) ) && !utf8::is_utf8( $topic ) ) )    { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$topic' ); }
    elsif ( !( defined( $partition ) && isint( $partition ) ) )                                 { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$partition' ); }
    elsif ( !( defined( $offset ) && ( isbig( $offset ) && $offset >= 0 ) || defined( _NONNEGINT( $offset ) ) ) )   { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$offset' ); }
    elsif ( defined( $max_size ) && !( _POSINT( $max_size ) && $max_size >= $MESSAGE_SIZE_OVERHEAD ) )  { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$max_size' ); }

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

    $self->_error( $ERROR_NO_ERROR )
        if $self->last_error;

    my $response = $self->_fulfill_request( $request )
        or return;

    my $messages = [];
    foreach my $received_topic ( @{ $response->{topics} } ) {
        $received_topic->{TopicName} eq $topic
            or return $self->_error( $ERROR_TOPIC_DOES_NOT_MATCH, "'$topic' ne '".$received_topic->{TopicName}."'" );
        foreach my $received_partition ( @{ $received_topic->{partitions} } ) {
            $received_partition->{Partition} == $partition
                or return $self->_error( $ERROR_PARTITION_DOES_NOT_MATCH, "$partition != ".$received_partition->{Partition} );
            my $HighwaterMarkOffset = $received_partition->{HighwaterMarkOffset};
            foreach my $Message ( @{ $received_partition->{MessageSet} } ) {
                my ( $offset, $next_offset, $message_error );
                if ( $BITS64 ) {
                    $offset = $Message->{Offset};
                    $next_offset += $offset + 1;
                }
                else {
                    $offset = Kafka::Int64::intsum( $offset, 0 );
                    $next_offset = Kafka::Int64::intsum( $offset, 1 );
                }
                $message_error = $Message->{Attributes} ? $ERROR{ $ERROR_COMPRESSED_PAYLOAD } : q{};

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

Get a list of valid offsets up C<$max_number> before the given time.

Returns reference to the offsets response array of the offset integers
(L<Math::BigInt|Math::BigInt> integers on 32 bit system).
If there's an error,
returns the reference to empty array if the C<RaiseError> is not true.

C<offsets()> takes arguments. The following arguments are currently recognized:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer.

=item C<$time>

Used to ask for all messages before a certain time (ms).
C<$time> is the timestamp of the offsets before this time - milliseconds since
UNIX Epoch.

The argument must be a positive number.
The argument may be a L<Math::BigInt|Math::BigInt> integer on 32 bit
system.

The special values C<$RECEIVE_LATEST_OFFSET> (-1), C<$RECEIVE_EARLIEST_OFFSETS> (-2) are allowed.

C<$RECEIVE_LATEST_OFFSET>, C<$RECEIVE_EARLIEST_OFFSETS>
are the defaults that can be imported from the L<Kafka|Kafka> module.

=item C<$max_number>

C<$max_number> is the maximum number of offsets to retrieve.

Optional. The argument must be a positive integer.

=back

=cut
sub offsets {
    my ( $self, $topic, $partition, $time, $max_number ) = @_;

    if    ( !( $topic eq q{} || defined( _STRING( $topic ) ) && !utf8::is_utf8( $topic ) ) )    { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$topic' ); }
    elsif ( !( defined( $partition ) && isint( $partition ) ) )                                 { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$partition' ); }
    elsif ( !( defined( $time ) && ( isbig( $time ) || isint( $time ) ) && $time >= $RECEIVE_EARLIEST_OFFSETS ) )   { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$time' ); }
    elsif ( defined( $max_number ) && !_POSINT( $max_number ) )                                 { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$max_number' ); }

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

    my $response = $self->_fulfill_request( $request )
        or return;

    my $offsets = [];
    foreach my $received_topic ( @{ $response->{topics} } ) {
        foreach my $partition_offsets ( @{ $received_topic->{PartitionOffsets} } ) {
            push @$offsets, @{ $partition_offsets->{Offset} };
        }
    }

    $self->_error( $ERROR_NO_ERROR )
        if $self->last_error;

    return $offsets;
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

#-- Closes and cleans up -------------------------------------------------------

1;

__END__

=head3 C<RaiseError>

This method returns current value showing how errors are handled within Kafka module.
If set to true, die() is dispatched when error during communication is detected.

C<last_errorcode> and C<last_error> are diagnostic methods and can be used to get detailed
error codes and messages for various cases: when server or the resource is not available,
access to the resource was denied, etc.

=head3 C<last_errorcode>

Returns code of the last error.

=head3 C<last_error>

Returns an error message that contains information about the encountered failure.

=head1 DIAGNOSTICS

Review documentation of the L</RaiseError> method for additional information about possible errors.

It's advised to always check L</last_errorcode> and more descriptive L</last_error> when
L</RaiseError> is not set.

=over 3

=item C<Invalid argument>

Invalid argument passed to a C<new> L<constructor|/CONSTRUCTOR> or other L<method|/METHODS>.

=item C<Can't send>

Message can't be sent using L<Kafka::IO|Kafka::IO> object socket.

=item C<Can't recv>

Message can't be received using L<Kafka::IO|Kafka::IO> object socket.

=item C<Can't bind>

TCP connection can't be established on the given host and port.

=item C<Can't get metadata>

Failed to obtain metadata from kafka servers

=item C<Leader not found>

Missing information about server-leader in metadata.

=item C<Mismatch CorrelationId>

C<CorrelationId> of response doesn't match one in request.

=item C<There are no known brokers>

Resulting metadata has no information about cluster brokers.

=item C<Can't get metadata>

Received metadata has incorrect internal structure.

=back

For more detailed error explanation call L</last_error> method of C<Kafka::Consumer> object.

=head1 SEE ALSO

The basic operation of the Kafka package modules:

L<Kafka|Kafka> - constants and messages used by the Kafka package modules.

L<Kafka::Connection|Kafka::Connection> - interface to connect to a Kafka cluster.

L<Kafka::Producer|Kafka::Producer> - interface for producing client.

L<Kafka::Consumer|Kafka::Consumer> - interface for consuming client.

L<Kafka::Message|Kafka::Message> - interface to access Kafka message properties.

L<Kafka::Int64|Kafka::Int64> - functions to work with 64 bit elements of the
protocol on 32 bit systems.

L<Kafka::Protocol|Kafka::Protocol> - functions to process messages in the
Apache Kafka's Protocol.

L<Kafka::IO|Kafka::IO> - low level interface for communication with Kafka server.

L<Kafka::Internals|Kafka::Internals> - Internal constants and functions used
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

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
