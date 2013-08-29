package Kafka::Producer;

=head1 NAME

Kafka::Producer -  interface to the 'producer' client.

=head1 VERSION

This documentation refers to C<Kafka::Producer> version 0.800_1 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.800_1';

#-- load the modules -----------------------------------------------------------

use Params::Util qw(
    _ARRAY0
    _INSTANCE
    _NONNEGINT
    _NUMBER
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
    $DEFAULT_RAISE_ERROR
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

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    #-- Connection
    use Kafka::Connection;

    my $connect = Kafka::Connection->new( host => 'localhost' );

    #-- Producer
    use Kafka::Producer;

    my $producer = Kafka::Producer->new( Connection => $connect );

    # Sending a single message
    my $response = $producer->send(
        'mytopic',          # topic
        0,                  # partition
        'Single message'    # message
    );

    die 'Error: ('.$producer->last_errorcode.') '.$producer->last_error."\n"
        unless $response;

    # Sending a series of messages
    $response = $producer->send(
        'mytopic',          # topic
        0,                  # partition
        [                   # messages
            'The first message',
            'The second message',
            'The third message',
        ]
    );

    # Closes the producer and cleans up
    undef $producer;
    undef $connect;

=head1 DESCRIPTION

Kafka producer API is implemented by C<Kafka::Producer> class.

The main features of the C<Kafka::Producer> class are:

=over 3

=item *

Provides an object oriented model of communication.

=item *

Supports Apache Kafka 0.8 PRODUCE Requests (with no compression codec attribute
now).

=back

=cut

our $_package_error;

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Creates new producer client object. Returns the created C<Kafka::Producer>
object.

An error will cause the program to halt or the constructor will return the
C<Kafka::Producer> object without halt, depending on the value of the C<RaiseError> attribute.

You can use the methods of the C<Kafka::Producer> class - L</last_errorcode>
and L</last_error> for information about the error.

C<new()> takes arguments in key-value pairs.
The following arguments are currently recognized:

=over 3

=item C<Connection =E<gt> $connect>

C<$connect> is the L<Kafka::Connection|Kafka::Connection> object that allow you to communicate to
the Apache Kafka cluster.

=item C<RaiseError =E<gt> $mode>

Optional, default = 0 .

An error will cause the program to halt if L</RaiseError> is true: C<confess>
if the argument is not valid or C<die> in the other error case
(this can always be trapped with C<eval>).

You should always check for errors, when not establishing the C<RaiseError>
mode to true.

=item C<CorrelationId =E<gt> $correlation_id>

Optional, default = C<undef> .

C<Correlation> is a user-supplied integer.
It will be passed back in the response by the server, unmodified.
The C<$correlation_id> should be an integer number.

The program will cause an error if a C<CorrelationId> in request
does not match the C<CorrelationId> received in response.

If C<CorrelationId> is not passed to constructor, its value will be assigned automatically
(random negative integer).

=item C<ClientId =E<gt> $client_id>

This is a user supplied identifier (string) for the client application.

If C<ClientId> is not passed to constructor, its value will be automatically assigned
(to string C<'producer'>).

=item C<RequiredAcks =E<gt> $acks>

The C<$acks> should be an integer number.

RTFM: Indicates how many acknowledgements the servers should receive before responding to the request.

If it is C<$NOT_SEND_ANY_RESPONSE> the server does not send any response.

If it is C<$WAIT_WRITTEN_TO_LOCAL_LOG>,
the server will wait the data is written to the local log before sending a response.

If it is C<$BLOCK_UNTIL_IS_COMMITTED>
the server will block until the message is committed by all in sync replicas before sending a response.

For any number > 1 the server will block waiting for this number of acknowledgements to occur.

C<$NOT_SEND_ANY_RESPONSE>, C<$WAIT_WRITTEN_TO_LOCAL_LOG>, C<$BLOCK_UNTIL_IS_COMMITTED>
can be imported from the L<Kafka|Kafka> module.

=item C<Timeout =E<gt> $timeout>

This provides a maximum time the server can await the receipt
of the number of acknowledgements in RequiredAcks.

The C<$timeout> in secs, could be any integer or floating-point type.

Optional, default = C<$REQUEST_TIMEOUT>.

C<$REQUEST_TIMEOUT> is the default timeout that can be imported from the
L<Kafka|Kafka> module.

=back

=cut
sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        Connection      => undef,
        RaiseError      => $DEFAULT_RAISE_ERROR,
        CorrelationId   => undef,
        ClientId        => undef,
        RequiredAcks    => $WAIT_WRITTEN_TO_LOCAL_LOG,
        Timeout         => $REQUEST_TIMEOUT,
    }, $class;

    while ( @args ) {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    $self->{CorrelationId}  //= _get_CorrelationId;
    $self->{ClientId}       //= 'producer';

    if    ( !defined _NONNEGINT( $self->RaiseError ) ) {
        $self->{RaiseError} = $DEFAULT_RAISE_ERROR;
        $self->_error( $ERROR_MISMATCH_ARGUMENT, 'RaiseError' );
    }
    elsif ( !_INSTANCE( $self->{Connection}, 'Kafka::Connection' ) )                    { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Connection' ); }
    elsif ( !isint( $self->{CorrelationId} ) )                                          { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'CorrelationId' ); }
    elsif ( !( $self->{ClientId} eq q{} || defined( _STRING( $self->{ClientId} ) ) && !utf8::is_utf8( $self->{ClientId} ) ) )   { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'ClientId' ); }
    elsif ( !( defined( $self->{RequiredAcks} ) && isint( $self->{RequiredAcks} ) ) )   { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'RequiredAcks' ); }
    elsif ( !_NUMBER( $self->{Timeout} ) )                                              { $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Timeout' ); }
    else {
        $self->_error( $ERROR_NO_ERROR )
            if $self->last_error;
    }

    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are defined for the C<Kafka::Producer> class:

=cut

#-- public methods -------------------------------------------------------------

=head3 C<send( $topic, $partition, $messages )>

Sends a messages on a L<Kafka::Connection|Kafka::Connection> object.

Returns a non-blank value (a reference to a hash describing the answer)
if the message is successfully sent.
If there's an error, returns the undefined value if the C<RaiseError> is not true.

C<send()> takes arguments. The following arguments are currently recognized:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer.

=item C<$messages>

The C<$messages> is an arbitrary amount of data (a simple data string or
a reference to an array of the data strings).

=item C<$key>

The C<$key> is an optional message key, must be a string.
C<$key> may used in the producer for partitioning with each message,
so the consumer knows the partitioning key.

=back

=cut
sub send {
    my ( $self, $topic, $partition, $messages, $key ) = @_;

    $key //= q{};

    if    ( !( $topic eq q{} || defined( _STRING( $topic ) ) && !utf8::is_utf8( $topic ) ) )    { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$topic' ); }
    elsif ( !( defined( $partition ) && isint( $partition ) ) )             { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$partition' ); }
# Checking the encoding of strings is performed in the functions of module Kafka::Protocol
    elsif ( !( defined( _STRING( $messages ) ) || _ARRAY0( $messages ) ) )  { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$messages' ); }
    elsif ( !( $key eq q{} || ( defined( _STRING( $key ) ) && !utf8::is_utf8( $key ) ) ) )      { return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$key' ); }

    $messages = [ $messages ] unless ref( $messages );
    foreach my $message ( @$messages ) {
        ( $message eq q{} || ( defined( _STRING( $message ) ) && !utf8::is_utf8( $message ) ) )
            or return $self->_error( $ERROR_MISMATCH_ARGUMENT, '$messages' );
    }

    my $MessageSet = [];
    my $request = {
        ApiKey                              => $APIKEY_PRODUCE,
        CorrelationId                       => $self->{CorrelationId},
        ClientId                            => $self->{ClientId},
        RequiredAcks                        => $self->{RequiredAcks},
        Timeout                             => $self->{Timeout} * 1000,
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

    $self->_error( $ERROR_NO_ERROR )
        if $self->last_error;

    if ( my $response = $self->_fulfill_request( $request ) ) {
        return $response;
    }

    return;
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

Consult documentation of the L</RaiseError> method for additional information about possible errors.

It's advised to always check L</last_errorcode> and more descriptive L</last_error> when
L</RaiseError> is not set.

=over 3

=item C<Invalid argument>

This means that you didn't give the right argument to a C<new>
L<constructor|/CONSTRUCTOR> or to other L<method|/METHODS>.

=item C<Can't send>

This means that the message can't be sent on a L<Kafka::IO|Kafka::IO> object socket.

=item C<Can't recv>

This means that the message can't be received on a L<Kafka::IO|Kafka::IO>
object socket.

=item C<Can't bind>

This means that the socket TCP connection can't be established on on given host
and port.

=item C<Can't get metadata>

This means that the IO error present,
errors found in the structure of the reply or the reply contains a non-zero error codes.

=item C<Description leader not found>

This means that information about the server-leader in the resulting metadata is missing.

=item C<Mismatch CorrelationId>

This means that do not match C<CorrelationId> request and response.

=item C<There are no known brokers>

This means that information about brokers in the cluster obtained metadata is missing.

=item C<Can't get metadata>

This means that metadata obtained incorrect internal structure.

=back

For more error description, always look at the message from L</last_error>
method from C<Kafka::Producer> object.

If the reply does not contain zero error codes,
the error description can also be seen in the information of the method L</ last_error>.

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

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
