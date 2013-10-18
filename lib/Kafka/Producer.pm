package Kafka::Producer;

=head1 NAME

Kafka::Producer - Perl interface for Kafka producer client.

=head1 VERSION

This documentation refers to C<Kafka::Producer> version 0.800_5 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.800_5';

#-- load the modules -----------------------------------------------------------

use Carp;
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
use Try::Tiny;

use Kafka qw(
    %ERROR
    $ERROR_MISMATCH_ARGUMENT
    $REQUEST_TIMEOUT
    $WAIT_WRITTEN_TO_LOCAL_LOG
);
use Kafka::Exceptions;
use Kafka::Internals qw(
    $APIKEY_PRODUCE
    $PRODUCER_ANY_OFFSET
    _get_CorrelationId
);
use Kafka::Connection;

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka::Connection;
    use Kafka::Producer;

    my ( $connection, $producer );
    try {

        #-- Connection
        $connection = Kafka::Connection->new( host => 'localhost' );

        #-- Producer
        $producer = Kafka::Producer->new( Connection => $connection );

        # Sending a single message
        my $response = $producer->send(
            'mytopic',          # topic
            0,                  # partition
            'Single message'    # message
        );

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

    } catch {
        if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $_->code, ') ',  $_->message, "\n";
            exit;
        } else {
            die $_;
        }
    };

    # Closes the producer and cleans up
    undef $producer;
    undef $connection;

=head1 DESCRIPTION

Kafka producer API is implemented by C<Kafka::Producer> class.

The main features of the C<Kafka::Producer> class are:

=over 3

=item *

Provides object-oriented API for producing messages.

=item *

Provides Kafka PRODUCE requests (with no support for compression codec).

=back

=cut

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Creates new producer client object.

C<new()> takes arguments in key-value pairs. The following arguments are currently recognized:

=over 3

=item C<Connection =E<gt> $connection>

C<$connection> is the L<Kafka::Connection|Kafka::Connection> object responsible for communication with
the Apache Kafka cluster.

=item C<CorrelationId =E<gt> $correlation_id>

Optional, default = C<undef> .

C<Correlation> is a user-supplied integer.
It will be passed back in the response by the server, unmodified.
The C<$correlation_id> should be an integer number.

If C<CorrelationId> is not passed to constructor, its value will be assigned automatically
(random negative integer).

An exception is thrown if C<CorrelationId> sent with request does not match C<CorrelationId> received in response.

=item C<ClientId =E<gt> $client_id>

This is a user supplied identifier (string) for the client application.

If C<ClientId> is not passed to constructor, its value will be automatically assigned
(to string C<'producer'>).

=item C<RequiredAcks =E<gt> $acks>

The C<$acks> should be an integer number.

Indicates how many acknowledgements the servers should receive before responding to the request.

If it is C<$NOT_SEND_ANY_RESPONSE> the server does not send any response.

If it is C<$WAIT_WRITTEN_TO_LOCAL_LOG>,
the server will wait until the data is written to the local log before sending a response.

If it is C<$BLOCK_UNTIL_IS_COMMITTED>
the server will block until the message is committed by all in sync replicas before sending a response.

For any number > 1 the server will block waiting for this number of acknowledgements to occur.

C<$NOT_SEND_ANY_RESPONSE>, C<$WAIT_WRITTEN_TO_LOCAL_LOG>, C<$BLOCK_UNTIL_IS_COMMITTED>
can be imported from the L<Kafka|Kafka> module.

=item C<Timeout =E<gt> $timeout>

This provides a maximum time the server can await the receipt
of the number of acknowledgements in RequiredAcks.

The C<$timeout> in seconds, could be any integer or floating-point type.

Optional, default = C<$REQUEST_TIMEOUT>.

C<$REQUEST_TIMEOUT> is the default timeout that can be imported from the
L<Kafka|Kafka> module.

=back

=cut
sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        Connection      => undef,
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

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Connection' )
        unless _INSTANCE( $self->{Connection}, 'Kafka::Connection' );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'CorrelationId' )
        unless isint( $self->{CorrelationId} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'ClientId' )
        unless ( $self->{ClientId} eq q{} || defined( _STRING( $self->{ClientId} ) ) ) && !utf8::is_utf8( $self->{ClientId} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'RequiredAcks' )
        unless defined( $self->{RequiredAcks} ) && isint( $self->{RequiredAcks} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Timeout' )
        unless _NUMBER( $self->{Timeout} );

    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are defined for the C<Kafka::Producer> class:

=cut

#-- public methods -------------------------------------------------------------

=head3 C<send( $topic, $partition, $messages )>

Sends a messages on a L<Kafka::Connection|Kafka::Connection> object.

Returns a non-blank value (a reference to a hash with server response description)
if the message is successfully sent.

C<send()> takes the following arguments:

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

    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$topic' )
        unless defined( $topic ) && ( $topic eq q{} || defined( _STRING( $topic ) ) ) && !utf8::is_utf8( $topic );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$partition' )
        unless defined( $partition ) && isint( $partition );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$messages' )
        unless defined( _STRING( $messages ) ) || _ARRAY0( $messages );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '$key' )
        unless ( $key eq q{} || defined( _STRING( $key ) ) ) && !utf8::is_utf8( $key );

    $messages = [ $messages ] unless ref( $messages );
    foreach my $message ( @$messages ) {
        $self->_error( $ERROR_MISMATCH_ARGUMENT, 'message' )
            unless defined( $message ) && ( $message eq q{} || ( defined( _STRING( $message ) ) && !utf8::is_utf8( $message ) ) );
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

    return $self->{Connection}->receive_response_to_request( $request );
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

# Handler for errors
sub _error {
    my $self = shift;

    Kafka::Exception::Producer->throw( throw_args( @_ ) );
}

#-- Closes and cleans up -------------------------------------------------------

1;

__END__

=head1 DIAGNOSTICS

When error is detected, an exception, represented by object of C<Kafka::Exception::Producer> class,
is thrown (see L<Kafka::Exceptions|Kafka::Exceptions>).

L<code|Kafka::Exceptions/code> and a more descriptive L<message|Kafka::Exceptions/message> provide
information about thrown exception. Consult documentation of the L<Kafka::Exceptions|Kafka::Exceptions>
for the list of all available methods.

Authors suggest using of L<Try::Tiny|Try::Tiny>'s C<try> and C<catch> to handle exceptions while
working with Kafka module.

=over 3

=item C<Invalid argument>

Invalid arguments were provided to a C<new>
L<constructor|/CONSTRUCTOR> or to other L<method|/METHODS>.

=item C<Can't send>

Request cannot be sent.

=item C<Can't recv>

Response cannot be received.

=item C<Can't bind>

TCP connection can't be established on a given host and port.

=item C<Can't get metadata>

IO error is present, errors found in the structure of the reply or the reply contains a non-zero error codes.

=item C<Description leader not found>

Information about the server-leader is missing in metadata.

=item C<Mismatch CorrelationId>

C<CorrelationId> of response doesn't match one in request.

=item C<There are no known brokers>

Information about brokers in the cluster is missing.

=item C<Can't get metadata>

Obtained metadata is incorrect or failed to obtain metadata.

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
