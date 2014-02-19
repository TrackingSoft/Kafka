package Kafka::Int64;

=head1 NAME

Kafka::Int64 - Functions to work with 64 bit elements of the
protocol on 32 bit systems.

=head1 VERSION

This documentation refers to C<Kafka::Int64> version 0.8006 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use bigint; # this allows integers of practially any size at the cost of significant performance drop

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.8006';

use Exporter qw(
    import
);

our @EXPORT_OK = qw(
    intsum
    packq
    unpackq
);

#-- load the modules -----------------------------------------------------------

use Carp;

use Kafka qw(
    %ERROR
    $ERROR_MISMATCH_ARGUMENT
);
use Kafka::Exceptions;

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka qw(
        $BITS64
    );

    try {

        # Apache Kafka Protocol: FetchOffset, Time

        my $offset = 123;

        my $encoded = $BITS64
            ? pack( 'q>', $offset )
            : Kafka::Int64::packq( $offset );

        my $response = chr( 0 ) x 8;

        $offset = $BITS64
            ? unpack( 'q>', substr( $response, 0, 8 ) )
            : Kafka::Int64::unpackq( substr( $response, 0, 8 ) );

        my $next_offset = $BITS64
            ? $offset + 1
            : Kafka::Int64::intsum( $offset, 1 );

    } catch {
        if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $_->code, ') ',  $_->message, "\n";
            exit;
        } else {
            die $_;
        }
    };

=head1 DESCRIPTION

This module is not intended to be used by end user.

In order to achieve better performance, functions of this module do not perform
validation of arguments.

Transparent L<BigInteger|bigint> support on 32-bit platforms where native
integer type is limited to 32 bits and slow bigint must be used instead.
Use L<functions|/FUNCTIONS> from this module in such case.

The main features of the C<Kafka::Int64> module are:

=over 3

=item *

Support for working with 64 bit elements of the Kafka protocol
on 32 bit systems.

=back

=cut

#-- public functions -----------------------------------------------------------

=head2 FUNCTIONS

The following functions are available for the C<Kafka::Int64> module.

=cut

=head3 C<intsum( $bint, $int )>

Adds two numbers to emulate bigint adding 64-bit integers in 32-bit systems.

The both arguments must be a number. That is, it is defined and Perl thinks
it's a number. Arguments may be a L<Math::BigInt|Math::BigInt>
integers.

Returns the value as a L<Math::BigInt|Math::BigInt> integer.

=cut
sub intsum {
    my ( $frst, $scnd ) = @_;

    my $ret = $frst + $scnd + 0;    # bigint coercion
    Kafka::Exception::Int64->throw( throw_args( $ERROR_MISMATCH_ARGUMENT, 'intsum' ) )
        if $ret->is_nan();

    return $ret;
}

=head3 C<packq( $bint )>

Emulates C<pack( q{qE<gt>}, $bint )> to 32-bit systems - assumes decimal string
or integer input.

An argument must be a positive number. That is, it is defined and Perl thinks
it's a number. The argument may be a L<Math::BigInt|Math::BigInt> integer.

The special values -1, -2 are allowed
(C<$Kafka::RECEIVE_LATEST_OFFSET>, C<$Kafka::RECEIVE_EARLIEST_OFFSETS>).

Returns the value as a packed binary string.

=cut
sub packq {
    my ( $n ) = @_;

    if      ( $n == -1 )    { return pack q{C8}, ( 255 ) x 8; }
    elsif   ( $n == -2 )    { return pack q{C8}, ( 255 ) x 7, 254; }
    elsif   ( $n < 0 )      { Kafka::Exception::Int64->throw( throw_args( $ERROR_MISMATCH_ARGUMENT, 'packq' ) ); }

    return pack q{H16}, substr( '00000000000000000000000000000000'.substr( ( $n + 0 )->as_hex(), 2 ), -16 );
}

=head3 C<unpackq( $bstr )>

Emulates C<unpack( q{qE<gt>}, $bstr )> to 32-bit systems - assumes binary input.

The argument must be a binary string of 8 bytes length.

Returns the value as a L<Math::BigInt|Math::BigInt> integer.

=cut
sub unpackq {
    my ( $s ) = @_;

    my $ret = Math::BigInt->from_hex( '0x'.unpack( q{H16}, $s ) );
    $ret = -1 if $ret == 18446744073709551615;
    $ret = -2 if $ret == 18446744073709551614;

    return $ret;
}

#-- private functions ----------------------------------------------------------

1;

__END__

=head1 DIAGNOSTICS

When error is detected, an exception, represented by object of C<Kafka::Exception::Producer> class,
is thrown (see L<Kafka::Exceptions|Kafka::Exceptions>).

Any error L<functions|/FUNCTIONS> is FATAL.
FATAL errors will cause the program to halt (C<confess>), since the
problem is so severe that it would be dangerous to continue.

L<code|Kafka::Exceptions/code> and a more descriptive L<message|Kafka::Exceptions/message> provide
information about thrown exception. Consult documentation of the L<Kafka::Exceptions|Kafka::Exceptions>
for the list of all available methods.

Authors suggest using of L<Try::Tiny|Try::Tiny>'s C<try> and C<catch> to handle exceptions while
working with L<Kafka|Kafka> package.

=over 3

=item C<Invalid argument>

This means that you didn't give the right argument to some of the
L<functions|/FUNCTIONS>.

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
