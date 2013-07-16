package Kafka::Producer;

# Basic functionalities to include a simple Producer

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# PRECONDITIONS ----------------------------------------------------------------

our $VERSION = '0.8001';

#-- load the modules -----------------------------------------------------------

use Carp;
use Params::Util qw(
    _ARRAY0
    _INSTANCE
    _NONNEGINT
    _STRING
);
use Scalar::Util::Numeric qw(
    isint
);

use Kafka qw(
    %ERROR
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NO_ERROR
    $REQUEST_TIMEOUT
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Internals qw(
    $APIKEY_PRODUCE
    $PRODUCER_ANY_OFFSET
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

#-- declarations ---------------------------------------------------------------

our $_package_error;

#-- constructor ----------------------------------------------------------------

sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        Connection      => undef,
        RaiseError      => 0,
        CorrelationId   => undef,
        ClientId        => 'producer',
        RequiredAcks    => $WAIT_WRITTEN_TO_LOCAL_LOG,
        Timeout         => $REQUEST_TIMEOUT * 1000, # This provides a maximum time (ms) the server can await the receipt of the number of acknowledgements in RequiredAcks
    }, $class;

    while ( @args ) {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    $self->_error( $ERROR_NO_ERROR );
    $self->{CorrelationId} //= _get_CorrelationId;

    if    ( !defined _NONNEGINT( $self->RaiseError ) )                          { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'RaiseError' ); }
    elsif ( !_INSTANCE( $self->{Connection}, 'Kafka::Connection' ) )            { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Connection' ); }
    elsif ( !isint( $self->{CorrelationId} ) )                                  { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'CorrelationId' ); }
    elsif ( !( $self->{ClientId} eq q{} || _STRING( $self->{ClientId} ) ) )     { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'ClientId' ); }
    elsif ( !isint( $self->{RequiredAcks} ) )                                   { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'RequiredAcks' ); }
    elsif ( !_NONNEGINT( $self->{Timeout} ) )                                   { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Timeout' ); }
    else {
        # nothing to do
    }

    return $self;
}

#-- public attributes ----------------------------------------------------------

#-- public methods -------------------------------------------------------------

sub send {
    my ( $self, $topic, $partition, $messages, $key ) = @_;

    $key //= q{};

    if    ( !( $topic eq q{} || _STRING( $topic ) ) )           { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$topic' ); }
    elsif ( !isint( $partition ) )                              { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$partition' ); }
    elsif ( !( _STRING( $messages ) || _ARRAY0( $messages ) ) ) { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$messages' ); }
    elsif ( !( $key eq q{} || _STRING( $key ) ) )               { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$key' ); }

    $messages = [ $messages ] unless ref( $messages );

    my $MessageSet = [];
    my $request = {
        ApiKey                              => $APIKEY_PRODUCE,
        CorrelationId                       => $self->{CorrelationId},
        ClientId                            => $self->{ClientId},
        RequiredAcks                        => $self->{RequiredAcks},
        Timeout                             => $self->{Timeout},
        topics                              => [
            {
                TopicName                   => $topic,
                partitions                  => [
                    {
                        Partition           => $partition,
                        MessageSet          => $MessageSet,
                    },
                ],
            },
        ],
    };

    foreach my $message ( @$messages ) {
        push @$MessageSet, {
            Offset  => $PRODUCER_ANY_OFFSET,
            Key     => $key,
            Value   => $message,
        };
    }

    $self->_error( $ERROR_NO_ERROR );

    if ( my $response = $self->_fulfill_request( $request ) ) {
        return $response;
    }

    return;
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

#-- Closes and cleans up -------------------------------------------------------

# do not close the $self->{Connection} connections as they can be used by other instances of the class Kafka::Connection

1;

__END__

=head1 NAME

Kafka::Producer - object interface to the producer client

=head1 VERSION

This documentation refers to C<Kafka::Producer> version 0.12

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

=item C<Invalid argument>

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

Copyright (C) 2012-2013 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
