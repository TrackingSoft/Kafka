package Kafka::Producer;

use 5.010;
use strict;
use warnings;

# Basic functionalities to include a simple Producer

our $VERSION = '0.08';

use Carp;
use Params::Util qw( _INSTANCE _STRING _NONNEGINT _ARRAY0 );

use Kafka qw(
    ERROR_MISMATCH_ARGUMENT
    ERROR_CANNOT_SEND
    );
use Kafka::Protocol qw( produce_request );

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

    $self->{last_errorcode} = $_last_errorcode  = $error_code;
    $self->{last_error}     = $_last_error      = $Kafka::ERROR[$self->{last_errorcode}];
    confess $self->{last_error} if $self->{RaiseError} and $self->{last_errorcode} == ERROR_MISMATCH_ARGUMENT;
    die $self->{last_error} if $self->{RaiseError} or ( $self->{IO} and ( ref( $self->{IO} eq "Kafka::IO" ) and $self->{IO}->RaiseError ) );
    return;
}

sub send {
    my $self        = shift;
    my $topic       = _STRING( shift ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );
    my $partition   = shift;
    my $messages    = shift;

    return $self->_error( ERROR_MISMATCH_ARGUMENT ) unless defined( _NONNEGINT( $partition ) );

    (
        _STRING( $messages ) or
        _ARRAY0( $messages )
    ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );

    $_last_error        = $_last_errorcode          = undef;
    $self->{last_error} = $self->{last_errorcode}   = undef;
    my $sent;
    eval { $sent = $self->{IO}->send( produce_request( $topic, $partition, $messages ) ) };
    unless ( defined( $sent ) )
    {
        if ( $self->{IO}->last_errorcode )
        {
            $_last_errorcode    = $self->{IO}->last_errorcode;
            $_last_error        = $self->{IO}->last_error;
        }
        else
        {
            $_last_errorcode    = Kafka::Protocol::last_errorcode;
            $_last_error        = Kafka::Protocol::last_error;
        }
        $self->{last_errorcode} = $_last_errorcode;
        $self->{last_error}     = $_last_error;
        die $@ if $self->{RaiseError} or $self->{IO}->RaiseError;
        return;
    }

    return 1;
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

Kafka::Producer - object interface to the producer client

=head1 VERSION

This documentation refers to C<Kafka::Producer> version 0.08

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

Producer:

    #-- Producer
    use Kafka::Producer;
    
    my $producer = Kafka::Producer->new(
        IO          => $io,
        RaiseError  => 0    # Optional, default = 0
        );
    
    # Sending a single message
    $producer->send(
        "test",             # topic
        0,                  # partition
        "Single message"    # message
        );
    
    unless ( $producer )
    {
        die "(",
            Kafka::Producer::last_errorcode(), .") ",
            Kafka::Producer::last_error(), "\n";
    }
    
    # Sending a series of messages
    $producer->send(
        "test",             # topic
        0,                  # partition
        [                   # messages
            "The first message",
            "The second message",
            "The third message",
        ]
        );
    
    # Closes the producer and cleans up
    $producer->close;

Use only one C<Kafka::Producer> object at the same time.

=head1 DESCRIPTION

Kafka producer API is implemented by C<Kafka::Producer> class.

The main features of the C<Kafka::Producer> class are:

=over 3

=item *

Provides an object oriented model of communication.

=item *

Supports Apache Kafka PRODUCE Requests (with no compression codec attribute
now).

=back

=head2 CONSTRUCTOR

=head3 C<new>

Creates new producer client object. Returns the created C<Kafka::Producer>
object.

An error will cause the program to halt or the constructor will return the
undefined value, depending on the value of the C<RaiseError>
attribute.

You can use the methods of the C<Kafka::Producer> class - L</last_errorcode>
and L</last_error> for information about the error.

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

The following methods are defined for the C<Kafka::Producer> class:

=head3 C<send( $topic, $partition, $messages )>

Sends a messages (coded according to the Apache Kafka Wire Format protocol)
on a C<Kafka::IO> object socket.

Returns 1 if the message is successfully sent. If there's an error, returns
the undefined value if the C<RaiseError> is not true.

C<send()> takes arguments. The following arguments are currently recognized:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer (of any length).
That is, a positive integer, or zero.

=item C<$messages>

The C<$messages> is an arbitrary amount of data (a simple data string or
a reference to an array of the data strings).

=back

=head3 C<close>

The method to close the C<Kafka::Producer> object and clean up.

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

=item IO errors

Look at L<Kafka::IO|Kafka::IO> L<DIAGNOSTICS|Kafka::IO/"DIAGNOSTICS"> section
to obtain information about IO errors.

=back

For more error description, always look at the message from the L</last_error>
method or from the C<Kafka::Producer::last_error> class method.

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