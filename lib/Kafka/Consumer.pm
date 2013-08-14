package Kafka::Consumer;

# Basic functionalities to include a simple Consumer

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

our $_package_error;

#-- constructor ----------------------------------------------------------------

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
    elsif ( !( defined( $self->{MaxWaitTime} ) && isint( $self->{MaxWaitTime} ) ) )     { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxWaitTime' ); }
    elsif ( !( defined( $self->{MinBytes} ) && isint( $self->{MinBytes} ) ) )           { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MinBytes' ); }
    elsif ( !( _POSINT( $self->{MaxBytes} ) && $self->{MaxBytes} >= $MESSAGE_SIZE_OVERHEAD ) )      { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxBytes' ); }
    elsif ( !( defined( $self->{MaxNumberOfOffsets} ) && isint( $self->{MaxNumberOfOffsets} ) ) )   { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'MaxNumberOfOffsets' ); }
    else {
        $self->_error( $ERROR_NO_ERROR )
            if $self->last_error;
    }

    return $self;
}

#-- public attributes ----------------------------------------------------------

#-- public methods -------------------------------------------------------------

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
                $message_error = $Message->{MagicByte} ? $ERROR{ $ERROR_COMPRESSED_PAYLOAD } : q{};

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

# do not close the $self->{Connection} connections as they can be used by other instances of the class Kafka::Connection

1;

__END__

=head1 NAME

Kafka::Consumer - object interface to the consumer client

=head1 VERSION

This documentation refers to C<Kafka::Consumer> version 0.800_1

=head1 SYNOPSIS

    use 5.010;
    use strict;

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
                say 'key        : ', $message->key;
                say 'payload    : ', $message->payload;
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
access to FETCH Request, OFFSETS Request, FETCH Response, OFFSETS Response.

=item *

Support for working with 64 bit elements of the Kafka Wire Format protocol
on 32 bit systems.

=back

The Kafka consumer response has an ARRAY reference type for C<offsets>, and
C<fetch> methods.
For the C<offsets> response array has the offset integers, in descending order.

For the C<fetch> response the array has the class name
L<Kafka::Message|Kafka::Message> elements.

=head2 CONSTRUCTOR

=head3 C<new>

Creates new consumer client object. Returns the created C<Kafka::Consumer>
object.

An error will cause the program to halt or the constructor will return the
undefined value, depending on the value of the C<RaiseError>
attribute. You can use the methods of the C<Kafka::Consumer> class
L</last_errorcode> and L</last_error> for the information about the error.

C<new()> takes arguments in key-value pairs.
The following arguments are currently recognized:

=over 3

=item C<IO =E<gt> $io>

C<$io> is the L<Kafka::IO|Kafka::IO> object that allow you to communicate to
the Apache Kafka server without using the Apache ZooKeeper service.

=item C<RaiseError =E<gt> $mode>

Optional, default = 0 .

An error will cause the program to halt if C<RaiseError>
is true: C<confess> if the argument is not valid or C<die> in the other
error case (this can always be trapped with C<eval>).

It must be a non-negative integer. That is, a positive integer, or zero.

You should always check for errors, when not establishing the C<RaiseError>
mode to true.

=back

=head2 METHODS

=over 3

=item *

The arguments below B<offset>, B<max_size> or B<time>, B<max_number> are
the additional information that might be encoded parameters of the messages
we want to access.

=back

The following methods are defined for the C<Kafka::Consumer> class:

=head3 C<offsets( $topic, $partition, $time, $max_number )>

Get a list of valid offsets up C<$max_number> before the given time.

Returns the offsets response array of the offset integers, in descending order
(L<Math::BigInt|Math::BigInt> integers on 32 bit system). If there's an error,
returns the undefined value if the C<RaiseError> is not true.

C<offsets()> takes arguments. The following arguments are currently recognized:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer (of any length).
That is, a positive integer, or zero.

=item C<$time>

C<$time> is the timestamp of the offsets before this time - milliseconds since
UNIX Epoch.

The argument be a positive number. That is, it is defined and Perl thinks it's
a number. The argument may be a L<Math::BigInt|Math::BigInt> integer on 32 bit
system.

The special values -1 (latest), -2 (earliest) are allowed.

=item C<$max_number>

C<$max_number> is the maximum number of offsets to retrieve. The argument must
be a positive integer (of any length).

=back

=head3 C<fetch( $topic, $partition, $offset, $max_size )>

Get a list of messages to consume one by one up C<$max_size> bytes.

Returns the reference to array of the  L<Kafka::Message|Kafka::Message> class
name elements.
If there's an error, returns the undefined value if the C<RaiseError> is
not true.

C<fetch()> takes arguments. The following arguments are currently recognized:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer (of any length).
That is, a positive integer, or zero.

=item C<$offset>

Offset in topic and partition to start from (64 bits).

The argument must be a non-negative integer (of any length).
That is, a positive integer, or zero. The argument may be a
L<Math::BigInt|Math::BigInt> integer on 32 bit system.

=item C<$max_size>

C<$max_number> is the maximum size of the message set to return. The argument
be a positive integer (of any length).
The maximum size of a request limited by C<MAX_SOCKET_REQUEST_BYTES> that
can be imported from L<Kafka|Kafka> module.

=back

=head3 C<close>

The method to close the C<Kafka::Consumer> object and clean up.

=head3 C<last_errorcode>

This method returns an error code that specifies the position of the
description in the C<@Kafka::ERROR> array.  Analysing this information
can be done to determine the cause of the error.

The server or the resource might not be available, access to the resource
might be denied or other things might have failed for some reason.

=head3 C<last_error>

This method returns an error message that contains information about the
encountered failure.  Messages returned from this method may contain
additional details and do not comply with the C<Kafka::ERROR> array.

=head1 DIAGNOSTICS

Look at the C<RaiseError> description for additional information on
error handling.

The methods for the possible error to analyse: L</last_errorcode> and
more descriptive L</last_error>.

=over 3

=item C<Invalid argument>

This means that you didn't give the right argument to a C<new>
L<constructor|/CONSTRUCTOR> or to other L<method|/METHODS>.

=item C<Nothing to receive>

This means that there are no messages matching your request.

=item C<Response contains an error in 'ERROR_CODE'>

This means that the response to a request contains an error code in the box
ERROR_CODE. The error description is available through the method
L</last_error>.

=item C<Can't send>

This means that the request can't be sent on a C<Kafka::IO> object socket.

=item C<Can't recv>

This means that the response can't be received on a C<Kafka::IO>
IO object socket.

=item IO errors

Look at L<Kafka::IO|Kafka::IO> L<DIAGNOSTICS|Kafka::IO/"DIAGNOSTICS"> section
to obtain information about IO errors.

=back

For more error description, always look at the message from the L</last_error>
method or from the C<Kafka::Consumer::last_error> class method.

=head1 SEE ALSO

The basic operation of the Kafka package modules:

L<Kafka|Kafka> - constants and messages used by the Kafka package modules

L<Kafka::IO|Kafka::IO> - object interface to socket communications with
the Apache Kafka server

L<Kafka::Producer|Kafka::Producer> - object interface to the producer client

L<Kafka::Consumer|Kafka::Consumer> - object interface to the consumer client

L<Kafka::Message|Kafka::Message> - object interface to the Kafka message
properties

L<Kafka::Protocol|Kafka::Protocol> - functions to process messages in the
Apache Kafka's wire format

L<Kafka::Int64|Kafka::Int64> - functions to work with 64 bit elements of the
protocol on 32 bit systems

L<Kafka::Mock|Kafka::Mock> - object interface to the TCP mock server for testing

A wealth of detail about the Apache Kafka and Wire Format:

Main page at L<http://incubator.apache.org/kafka/>

Wire Format at L<http://cwiki.apache.org/confluence/display/KAFKA/Wire+Format/>

Writing a Driver for Kafka at
L<http://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka>

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
