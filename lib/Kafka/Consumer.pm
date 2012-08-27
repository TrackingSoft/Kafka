package Kafka::Consumer;

use 5.010;
use strict;
use warnings;

# Basic functionalities to include a simple Consumer

our $VERSION = '0.02';

use Carp;
use Params::Util qw( _INSTANCE _STRING _NONNEGINT _POSINT _NUMBER );

use Kafka qw(
    ERROR_MISMATCH_ARGUMENT
    ERROR_CANNOT_SEND
    ERROR_CANNOT_RECV
    ERROR_NOTHING_RECEIVE
    ERROR_IN_ERRORCODE
    BITS64
    );
use Kafka::Protocol qw(
    REQUESTTYPE_FETCH
    REQUESTTYPE_OFFSETS
    fetch_request
    fetch_response
    offsets_request
    offsets_response
    );
use Kafka::Message;

if ( !BITS64 ) { eval 'use Kafka::Int64; 1;' or die "Cannot load Kafka::Int64 : $@"; }

our $_last_error;
our $_last_errorcode;

sub new {
    my $class   = shift;
    my $self = {
        IO              => undef,
        RaiseError      => 0,
        };

    my @args = @_;
    while ( @args )
    {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    bless( $self, $class );

    $@ = "";
    unless ( defined( _NONNEGINT( $self->{RaiseError} ) ) )
    {
        $self->{RaiseError} = 0;
        return $self->_error( ERROR_MISMATCH_ARGUMENT );
    }
    $self->{last_error} = $self->{last_errorcode} = $_last_error = $_last_errorcode = undef;
    _INSTANCE( $self->{IO}, 'Kafka::IO' ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );

    $_last_error        = $_last_errorcode          = undef;
    $self->{last_error} = $self->{last_errorcode}   = undef;

    return $self;
}

sub last_error {
    my $self = shift;

    return $self->{last_error} if defined $self;
    return $_last_error;
}

sub last_errorcode {
    my $self = shift;

    return $self->{last_errorcode} if defined $self;
    return $_last_errorcode;
}

sub _error {
    my $self        = shift;
    my $error_code  = shift;
    my $description = shift;

    $self->{last_errorcode} = $_last_errorcode  = $error_code;
    $self->{last_error}     = $_last_error      = $description || $Kafka::ERROR[ $error_code ];
    confess $self->{last_error} if $self->{RaiseError} and $self->{last_errorcode} == ERROR_MISMATCH_ARGUMENT;
    die $self->{last_error} if $self->{RaiseError} or ( $self->{IO} and ( ref( $self->{IO} eq "Kafka::IO" ) and $self->{IO}->RaiseError ) );
    return;
}

sub _receive {
    my $self            = shift;
    my $request_type    = shift;

    my $response = {};
    my $message = $self->{IO}->receive( 4 );
    return $self->_error( $self->{IO}->last_errorcode, $self->{IO}->last_error )
        unless ( $message and defined $$message );
    my $tail = $self->{IO}->receive( unpack( "N", $$message ) );
    return $self->_error( $self->{IO}->last_errorcode, $self->{IO}->last_error )
        unless ( $tail and defined $$tail );
    $$message .= $$tail;

    my $decoded;
    if ( $request_type == REQUESTTYPE_FETCH )
    {
        $decoded = fetch_response( $message );
# WARNING: remember the error code of the last received packet
        unless ( $response->{error_code} = $decoded->{header}->{error_code} )
        {
            $response->{messages} = [] unless defined $response->{messages};
            push @{$response->{messages}}, @{$decoded->{messages}};
        }
    }
    elsif ( $request_type == REQUESTTYPE_OFFSETS )
    {
        $decoded = offsets_response( $message );
# WARNING: remember the error code of the last received packet
        unless ( $response->{error_code} = $decoded->{header}->{error_code} )
        {
            $response->{offsets} = [] unless defined $response->{offsets};
            push @{$response->{offsets}}, @{$decoded->{offsets}};
# WARNING: remember the error code of the last received packet
            $response->{error} = $decoded->{error};
        }
    }
    return $response;
}

sub fetch {
    my $self        = shift;
    my $topic       = _STRING( shift ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );
    my $partition   = shift;
    my $offset      = shift;
    my $max_size    = _POSINT( shift ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );

    return $self->_error( ERROR_MISMATCH_ARGUMENT ) unless defined( _NONNEGINT( $partition ) );
    ( ref( $offset ) eq "Math::BigInt" and $offset >= 0 ) or defined( _NONNEGINT( $offset ) ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );

    $_last_error        = $_last_errorcode          = undef;
    $self->{last_error} = $self->{last_errorcode}   = undef;

    my $sent;
    eval { $sent = $self->{IO}->send( fetch_request( $topic, $partition, $offset, $max_size ) ) };
    return $self->_error( $self->{IO}->last_errorcode, $self->{IO}->last_error )
        unless ( defined $sent );

    my $decoded = {};
    eval { $decoded = $self->_receive( REQUESTTYPE_FETCH ) };
    return $self->_error( $self->{last_errorcode}, $self->{last_error} )
        if ( $self->{last_error} );

    if ( defined $decoded->{messages} )
    {
        my $response = [];
        my $next_offset = $offset;
        foreach my $message ( @{$decoded->{messages}} )
        {
            # To find the offset of the next message,
            # take the offset of this message (that you made in the request),
            # and add LENGTH + 4 bytes (length of this message + 4 byte header to represent the length of this message).
            if ( BITS64 )
            {
                $message->{offset} = $next_offset;
                $next_offset += $message->{length} + 4;
            }
            else
            {
                $message->{offset} = Kafka::Int64::intsum( $next_offset, 0 );
                $next_offset = Kafka::Int64::intsum( $next_offset, $message->{length} + 4 );
            }
            $message->{next_offset} = $next_offset;
            push @$response, Kafka::Message->new( $message )
        }
        return $response;
    }
    elsif ( $decoded->{error_code} )
    {
        return $self->_error( ERROR_IN_ERRORCODE, $Kafka::ERROR[ERROR_IN_ERRORCODE].": ".( $Kafka::ERROR_CODE{ $decoded->{error_code} } || $Kafka::ERROR_CODE{ -1 } ) );
    }
    else
    {
        return $self->_error( ERROR_NOTHING_RECEIVE );
    }
}

sub offsets {
    my $self        = shift;
    my $topic       = _STRING( shift ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );
    my $partition   = shift;
    my $time        = shift;
    my $max_number  = _POSINT( shift ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );

    return $self->_error( ERROR_MISMATCH_ARGUMENT ) unless defined( _NONNEGINT( $partition ) );
    ( ref( $time ) eq "Math::BigInt" ) or defined( _NUMBER( $time ) ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );
    $time = int( $time );
    return $self->_error( ERROR_MISMATCH_ARGUMENT ) if $time < -2;

    $_last_error        = $_last_errorcode          = undef;
    $self->{last_error} = $self->{last_errorcode}   = undef;

    my $sent;
    eval { $sent = $self->{IO}->send( offsets_request( $topic, $partition, $time, $max_number ) ) };
    return $self->_error( $self->{IO}->last_errorcode, $self->{IO}->last_error )
        unless ( defined $sent );

    my $decoded = {};
    eval { $decoded = $self->_receive( REQUESTTYPE_OFFSETS ) };
    return $self->_error( $self->{last_errorcode}, $self->{last_error} )
        if ( $self->{last_error} );

    if ( defined $decoded->{offsets} )
    {
        my $response = [];
        push @$response, @{$decoded->{offsets}};
        return $response;
    }
    elsif ( $decoded->{error_code} )
    {
        return $self->_error( ERROR_IN_ERRORCODE, $Kafka::ERROR[ERROR_IN_ERRORCODE].": ".( $Kafka::ERROR_CODE{ $decoded->{error_code} } || $Kafka::ERROR_CODE{ -1 } ) );
    }
    else
    {
        return $self->_error( ERROR_NOTHING_RECEIVE );
    }
}

sub close {
    my $self = shift;

    $self->{IO}->close if ref( $self->{IO} ) eq "Kafka::IO";
    delete $self->{$_} foreach keys %$self;
}

sub DESTROY {
    my $self = shift;

    $self->close;
}

1;

__END__

=head1 NAME

Kafka::Consumer - object interface to the consumer client

=head1 VERSION

This documentation refers to C<Kafka::Consumer> version 0.02

=head1 SYNOPSIS

Setting up:

    #-- IO
    use Kafka qw( KAFKA_SERVER_PORT DEFAULT_TIMEOUT );
    use Kafka::IO;
    
    my $io;
    
    $io = Kafka::IO->new(
        host        => "localhost",
        port        => KAFKA_SERVER_PORT,
        timeout     => DEFAULT_TIMEOUT, # Optional,
                                        # default = DEFAULT_TIMEOUT
        RaiseError  => 0                # Optional, default = 0
        );

Consumer:

    #-- Consumer
    use Kafka::Consumer;
    
    my $consumer = Kafka::Consumer->new(
        IO          => $io,
        RaiseError  => 0    # Optional, default = 0
        );
    
    # Get a list of valid offsets up max_number before the given time
    my $offsets = $consumer->offsets(
        "test",             # topic
        0,                  # partition
        TIMESTAMP_EARLIEST, # time
        DEFAULT_MAX_OFFSETS # max_number
        );
    if( $offsets )
    {
        foreach my $offset ( @$offsets )
        {
            print "Received offset: $offset\n";
        }
    }
    if ( !$offsets or $consumer->last_error )
    {
        print STDERR
            "(", $consumer->last_errorcode, ") ",
            $consumer->last_error, "\n";
    }
    
    # Consuming messages
    my $messages = $consumer->fetch(
        "test",             # topic
        0,                  # partition
        0,                  # offset
        DEFAULT_MAX_SIZE    # Maximum size of MESSAGE(s) to receive
        );
    if ( $messages )
    {
        foreach my $message ( @$messages )
        {
            if( $message->valid )
            {
                print "payload    : ", $message->payload,       "\n";
                print "offset     : ", $message->offset,        "\n";
                print "next_offset: ", $message->next_offset,   "\n";
            }
            else
            {
                print "error      : ", $message->error,         "\n";
            }
        }
    }
    
    # Closes the consumer and cleans up
    $consumer->close;

Use only one C<Kafka::Consumer> object at the same time.

=head1 DESCRIPTION

Kafka consumer API is implemented by C<Kafka::Consumer> class.

The main features of the C<Kafka::Consumer> class are:

=over 3

=item *

Provides an object oriented model of communication.

=item *

Supports parsing the Apache Kafka Wire Format protocol.

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

For the C<fetch> response array has the class name
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
the additional information that might encode parameters of the messages
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
error handeling.

The methods for the possible error to analyse: L</last_errorcode> and
more descriptive L</last_error>.

=over 3

=item C<Mismatch argument>

This means that you didn't give the right argument to a C<new>
L<constructor|/CONSTRUCTOR> or to other L<method|/METHODS>.

=item C<Nothing to receive>

This means that there are no messages matching your request.

=item C<Response contains an error in 'ERROR_CODE'>

This means that the response to a request contains an error code in the box
ERROR_CODE. The error description is available through the method
L</last_error>.

=item C<Can't send>

This means that the request can't be sent on a C<Kafka::IO> IO object socket.

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

Copyright (C) 2012 by TrackingSoft LLC.
All rights reserved.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut