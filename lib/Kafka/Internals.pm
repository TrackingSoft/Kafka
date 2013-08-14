package Kafka::Internals;

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

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
    _get_CorrelationId
    last_error
    last_errorcode
    RaiseError
    _fulfill_request
    _error
    _connection_error
    _set_error
);

our $VERSION = '0.800_1';

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

const our $DEFAULT_RAISE_ERROR                  => 0;

#-- Api Keys
const our $APIKEY_PRODUCE                       => 0;
const our $APIKEY_FETCH                         => 1;
const our $APIKEY_OFFSET                        => 2;
const our $APIKEY_METADATA                      => 3;
const our $APIKEY_LEADERANDISR                  => 4;   # Not used now
const our $APIKEY_STOPREPLICA                   => 5;   # Not used now
const our $APIKEY_OFFSETCOMMIT                  => 6;   # Not used now
const our $APIKEY_OFFSETFETCH                   => 7;   # Not used now

# Important configuration properties
const our $MAX_SOCKET_REQUEST_BYTES             => 100 * 1024 * 1024;   # The maximum number of bytes in a socket request
const our $PRODUCER_ANY_OFFSET                  => 0;                   # RTFM: When the producer is sending messages it doesn't actually know the offset and can fill in any value here it likes.

const my  $MAX_CORRELATIONID                    => 2**31 - 1;           # Largest positive integer on 32-bit machines

#-- public functions -----------------------------------------------------------

#-- private functions ----------------------------------------------------------

sub _get_CorrelationId {
    return int( rand( $MAX_CORRELATIONID ) );
}

#-- public attributes ----------------------------------------------------------

sub last_error {
    my $class = ref( $_[0] ) || $_[0];

    no strict 'refs';   ## no critic
    return( ( ${ $class.'::_package_error' } // q{} ).q{} );
}

sub last_errorcode {
    my $class = ref( $_[0] ) || $_[0];

    no strict 'refs';   ## no critic
    return( ( ${ $class.'::_package_error' } // 0 ) + 0 );
}

sub RaiseError {
    my ( $self ) = @_;

    return $self->{RaiseError};
}

#-- public methods -------------------------------------------------------------

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

sub _fulfill_request {
    my ( $self, $request ) = @_;

    my $connection = $self->{Connection};
    local $@;
    if ( my $response = eval { $connection->receive_response_to_request( $request ) } ) {
        return $response;
    }
    if ( Kafka::Protocol::last_errorcode() ) {
        return $self->_error( Kafka::Protocol::last_errorcode(), Kafka::Protocol::last_error() );
    }
    else {
        return $self->_connection_error;
    }
}

sub _error {
    my ( $self, $error_code, $description ) = @_;

    $self->_set_error( $error_code, $ERROR{ $error_code }.( $description ? ': '.$description : q{} ) );

    confess( $self->last_error )
        if $self->last_errorcode == $ERROR_MISMATCH_ARGUMENT;

    return unless $self->RaiseError;

    if ( $self->last_errorcode == $ERROR_NO_ERROR ) { return; }
    else                                            { die $self->last_error; }
}

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

sub _set_error {
    my ( $self, $error_code, $description ) = @_;

    no strict 'refs';   ## no critic
    ${ ref( $self ).'::_package_error' } = dualvar $error_code, $description;
}

1;

__END__

=head1 NAME

Kafka::Internals - blah-blah-blah

=head1 VERSION

This documentation refers to C<Kafka::Internals> version 0.800_1

=head1 SYNOPSIS

    use 5.010;
    use strict;

    use Kafka::Internals;

    my $hex_stream_size = $Kafka::Internals::MAX_SOCKET_REQUEST_BYTES;

=head1 DESCRIPTION

blah-blah-blah

=head2 EXPORT

blah-blah-blah

=head2 GLOBAL VARIABLES

=over

=item C<@Kafka::ERROR>

Contain the descriptions for possible error codes returned by
C<last_errorcode> methods and functions of the package modules.

=item C<%Kafka::ERROR_CODE>

blah-blah-blah

=back

=head1 DEPENDENCIES

blah-blah-blah

=head2 FUNCTIONS

blah-blah-blah

=head3 C<last_errorcode>

blah-blah-blah

=head3 C<last_error>

blah-blah-blah

=head3 C<RaiseError>

blah-blah-blah

=head1 BUGS AND LIMITATIONS

blah-blah-blah

=head1 MORE DOCUMENTATION

All modules contain detailed information on the interfaces they provide.

=head1 SEE ALSO

blah-blah-blah

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
