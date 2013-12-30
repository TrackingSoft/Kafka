package Kafka::Message;

=head1 NAME

Kafka::Message - Interface to the Kafka message properties.

=head1 VERSION

This documentation refers to C<Kafka::Message> version 0.8005 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.8005';

#-- load the modules -----------------------------------------------------------

#-- declarations ---------------------------------------------------------------

our @_standard_fields = qw(
    Attributes
    error
    HighwaterMarkOffset
    key
    MagicByte
    next_offset
    payload
    offset
    valid
);

#-- constructor ----------------------------------------------------------------

sub new {
    my ( $class, $self ) = @_;

    bless $self, $class;

    return $self;
}

#-- public attributes ----------------------------------------------------------

{
    no strict 'refs';   ## no critic

    # getters
    foreach my $method ( @_standard_fields )
    {
        *{ __PACKAGE__.'::'.$method } = sub {
            my ( $self ) = @_;
            return $self->{ $method };
        };
    }
}

#-- public methods -------------------------------------------------------------

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

#-- Closes and cleans up -------------------------------------------------------

1;

__END__

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Kafka qw(
        $DEFAULT_MAX_BYTES
    );
    use Kafka::Connection;
    use Kafka::Consumer;

    #-- Connection
    my $connection = Kafka::Connection->new( host => 'localhost' );

    #-- Consumer
    my $consumer = Kafka::Consumer->new( Connection  => $connection );

    # The Kafka consumer response has an ARRAY reference type.
    # For the fetch response array has the class name Kafka::Message elements.

    # Consuming messages
    my $messages = $consumer->fetch(
        'mytopic',          # topic
        0,                  # partition
        0,                  # offset
        $DEFAULT_MAX_BYTES  # Maximum size of MESSAGE(s) to receive
    );
    if ( $messages ) {
        foreach my $message ( @$messages ) {
            if( $message->valid ) {
                say 'key        : ', $message->key;
                say 'payload    : ', $message->payload;
                say 'offset     : ', $message->offset;
                say 'next_offset: ', $message->next_offset;
            } else {
                say 'error      : ', $message->error;
            }
        }
    }

    # Closes and cleans up
    undef $consumer;
    undef $connection;

=head1 DESCRIPTION

This module is not intended to be used by the end user.

L<Kafka::Message|Kafka::Message> class implements API for L<Kafka|Kafka> message.

C<fetch> method of the L<Consumer|Kafka::Consumer> client returns reference to an array of objects of this class.

The main features of the C<Kafka::Message> class are:

=over 3

=item *

Represents Apache Kafka Message structure (no support for compression codec attribute now). Description of the structure
is available at L<https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets>

=back

=head2 CONSTRUCTOR

=head3 C<new ( \%arg )>

Creates a new C<Kafka::Message> object.
C<new()> takes an argument - HASH reference with the message attributes corresponding to
L<accessors|/METHODS>.

=head2 METHODS

=head3 C<payload>

A simple message received from the Apache Kafka server.

=head3 C<key>

The key is an optional message key that was used for partition assignment.
The key can be an empty string.

=head3 C<valid>

Boolean value: indicates whether received message is valid or not.

=head3 C<error>

A description why message is invalid (currently happens only when message is compressed).

=head3 C<offset>

The offset of the message in the Apache Kafka server.

=head3 C<next_offset>

The offset of the next message in the Apache Kafka server.

=head3 C<Attributes>

This holds metadata attributes about the message.
The last 3 bits contain the compression codec used for the message.
The other bits are currently unused.

=head3 C<HighwaterMarkOffset>

The offset at the end of the log for this partition.
This can be used by the client to determine how many messages behind the end of the log they are.

=head3 C<MagicByte>

This is version id used to allow backwards compatible evolution of the message binary format.

=head1 DIAGNOSTICS

In order to achieve better performance, constructor of this module does not perform validation of
arguments.

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
