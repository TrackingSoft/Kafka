package Kafka::Internals;

=head1 NAME

Kafka::Internals - Constants and functions used internally.

=head1 VERSION

This documentation refers to C<Kafka::Internals> version 0.800_1 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.800_1';

use Exporter qw(
    import
);

our @EXPORT_OK = qw(
    $APIKEY_PRODUCE
    $APIKEY_FETCH
    $APIKEY_OFFSET
    $APIKEY_METADATA
    $DEFAULT_RAISE_ERROR
    $MAX_SOCKET_REQUEST_BYTES
    $PRODUCER_ANY_OFFSET
    last_error
    last_errorcode
    RaiseError
    _connection_error
    _error
    _fulfill_request
    _get_CorrelationId
    _set_error
);

#-- load the modules -----------------------------------------------------------

use Carp;
use Const::Fast;
use Scalar::Util qw(
    dualvar
);

use Kafka qw(
    %ERROR
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NO_ERROR
);

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Kafka::Internals qw(
        $MAX_SOCKET_REQUEST_BYTES
    );

    my $bin_stream_size = $MAX_SOCKET_REQUEST_BYTES;

=head1 DESCRIPTION

This module is not a user module.

In order to achieve better performance, functions of this module do
not perform arguments validation.

=head2 EXPORT

The following constants are available for export

=cut

=head3 C<$DEFAULT_RAISE_ERROR>

Default value for C<RaiseError>, meaning to not drop exception.

=cut
const our $DEFAULT_RAISE_ERROR                  => 0;

#-- Api Keys

=head3 C<$APIKEY_PRODUCE>

The numeric code that the ApiKey in the request take for the C<ProduceRequest> request type.

=cut
const our $APIKEY_PRODUCE                       => 0;

=head3 C<$APIKEY_FETCH>

The numeric code that the ApiKey in the request take for the C<FetchRequest> request type.

=cut
const our $APIKEY_FETCH                         => 1;

=head3 C<$APIKEY_OFFSET>

The numeric code that the ApiKey in the request take for the C<OffsetRequest> request type.

=cut
const our $APIKEY_OFFSET                        => 2;

=head3 C<$APIKEY_METADATA>

The numeric code that the ApiKey in the request take for the C<MetadataRequest> request type.

=cut
const our $APIKEY_METADATA                      => 3;
# The numeric code that the ApiKey in the request take for the C<LeaderAndIsrRequest> request type.
const our $APIKEY_LEADERANDISR                  => 4;   # Not used now
# The numeric code that the ApiKey in the request take for the C<StopReplicaRequest> request type.
const our $APIKEY_STOPREPLICA                   => 5;   # Not used now
# The numeric code that the ApiKey in the request take for the C<OffsetCommitRequest> request type.
const our $APIKEY_OFFSETCOMMIT                  => 6;   # Not used now
# The numeric code that the ApiKey in the request take for the C<OffsetFetchRequest> request type.
const our $APIKEY_OFFSETFETCH                   => 7;   # Not used now

# Important configuration properties

=head3 C<$MAX_SOCKET_REQUEST_BYTES>

The maximum number of bytes in a socket request.

The maximum size of a request that the socket server will accept.
Default limit (as configured in server.properties) is 104857600.

=cut
const our $MAX_SOCKET_REQUEST_BYTES             => 100 * 1024 * 1024;

=head3 C<$PRODUCER_ANY_OFFSET>

RTFM: When the producer is sending messages it doesn't actually know the offset and can fill in any
value here it likes.

=cut
const our $PRODUCER_ANY_OFFSET                  => 0;

=head3 C<$MAX_CORRELATIONID>

Largest positive integer on 32-bit machines

=cut
const my  $MAX_CORRELATIONID                    => 2**31 - 1;

#-- public functions -----------------------------------------------------------

=head3 FUNCTIONS

The following functions are available for C<Kafka::Internals> module.

=cut

#-- private functions ----------------------------------------------------------

# Used to generate a CorrelationId.
sub _get_CorrelationId {
    return( -int( rand( $MAX_CORRELATIONID ) ) );
}

#-- public attributes ----------------------------------------------------------

=head3 C<last_errorcode>

This method returns an error code that specifies the description in the
C<%Kafka::ERROR> hash. Analysing this information can be done to determine the
cause of the error.

=cut
sub last_errorcode {
    my $class = ref( $_[0] ) || $_[0];

    no strict 'refs';   ## no critic
    return( ( ${ $class.'::_package_error' } // 0 ) + 0 );
}

=head3 C<last_error>

This method returns an error message that contains information about the
encountered failure. Messages may contain additional details and do not
coincide completely with the C<%Kafka::ERROR> hash.

=cut
sub last_error {
    my $class = ref( $_[0] ) || $_[0];

    no strict 'refs';   ## no critic
    return( ( ${ $class.'::_package_error' } // q{} ).q{} );
}

=head3 C<RaiseError>

This method returns a value of the mode responding to the error.

C<RaiseError> leads set an internal attribute describing the error,
when an error is detected if C<RaiseError> set to false,
or to die automatically if C<RaiseError> set to true
(this can always be trapped with C<eval>).

Code and a description of the error can be obtained by calling
the methods L</last_errorcode> and L</last_error> respectively.

You should always check for errors, when L</RaiseError> is not true.

=cut
sub RaiseError {
    my ( $self ) = @_;

    return $self->{RaiseError};
}

#-- public methods -------------------------------------------------------------

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

# process a request, handle responce
sub _fulfill_request {
    my ( $self, $request ) = @_;

    my $connection = $self->{Connection};
    local $@;
    if ( my $response = eval { $connection->receive_response_to_request( $request ) } ) {
        return $response;
    }
    else {
        $self->_connection_error;
        return;
    }
}

# Handler for errors not tied with network communication
# Dies if RaiseError set to true.
sub _error {
    my ( $self, $error_code, $description ) = @_;

    $self->_set_error( $error_code, $ERROR{ $error_code }.( $description ? ': '.$description : q{} ) );

    confess( $self->last_error )
        if $self->last_errorcode == $ERROR_MISMATCH_ARGUMENT;

    return unless $self->RaiseError;

    if ( $self->last_errorcode == $ERROR_NO_ERROR ) { return; }
    else                                            { die $self->last_error; }
}

#
# Handles connection errors (Kafka::IO).
# Dies if RaiseError set to true.
sub _connection_error {
    my ( $self ) = @_;

    my $connection  = $self->{Connection};
    my $errorcode   = $connection->last_errorcode;

    return if $errorcode == $ERROR_NO_ERROR;

    my $error       = $connection->last_error;
    $self->_set_error( $errorcode, $error );

    return if !$self->RaiseError && !$connection->RaiseError;

    if    ( $errorcode == $ERROR_MISMATCH_ARGUMENT )    { confess $error; }
    else                                                { die $error; }
}

# Sets internal variable describing error
sub _set_error {
    my ( $self, $error_code, $description ) = @_;

    no strict 'refs';   ## no critic
    ${ ref( $self ).'::_package_error' } = dualvar $error_code, $description;
}

1;

__END__

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

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
