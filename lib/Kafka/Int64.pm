package Kafka::Int64;

use 5.010;
use strict;
use warnings;

# Transparent BigInteger support on 32-bit platforms where native integer type is
# limited to 32 bits and slow bigint must be used instead. Use subs from this module
# in such case.

our $VERSION = '0.10';

use bytes;
use bigint; # this allows integers of practially any size at the cost of significant performance drop
use Carp;
use Params::Util qw( _STRING _NUMBER );

use Kafka qw(
    ERROR_MISMATCH_ARGUMENT
    );

sub intsum {
    my $frst = shift;
    my $scnd = shift;

    ( ( ref( $frst ) eq "Math::BigInt" or defined( _NUMBER( $frst ) ) ) and defined( _NUMBER( $scnd ) ) )
        or confess $Kafka::ERROR[ERROR_MISMATCH_ARGUMENT];

    my $ret = ( $frst + 0 ) + $scnd;
    confess $Kafka::ERROR[ERROR_MISMATCH_ARGUMENT] if $ret->is_nan();

    return $ret;
}

sub packq {
    my $n = shift;

    ( ref( $n ) eq "Math::BigInt" or defined( _NUMBER( $n ) ) ) or confess $Kafka::ERROR[ERROR_MISMATCH_ARGUMENT];

    if      ( $n == -1 )    { return pack 'C8', ( 255 ) x 8; }
    elsif   ( $n == -2 )    { return pack 'C8', ( 255 ) x 7, 254; }
    elsif   ( $n < 0 )      { confess $Kafka::ERROR[ERROR_MISMATCH_ARGUMENT]; }

    return pack 'H16', substr( '00000000000000000000000000000000'.substr( ( $n + 0 )->as_hex(), 2 ), -16 );
}

sub unpackq {
    my $s = _STRING( shift ) or confess $Kafka::ERROR[ERROR_MISMATCH_ARGUMENT];

    confess $Kafka::ERROR[ERROR_MISMATCH_ARGUMENT] if bytes::length( $s ) != 8;

    return Math::BigInt->from_hex( "0x".unpack( "H16", $s ) );
}

1;

__END__

=head1 NAME

Kafka::Int64 - functions to work with 64 bit elements of 
the Apache Kafka Wire Format protocol on 32 bit systems

=head1 VERSION

This documentation refers to C<Kafka::Int64> version 0.10

=head1 SYNOPSIS

    use Kafka qw( BITS64 );
    
    # Apache Kafka Wire Format: OFFSET, TIME
    
    $encoded = BITS64 ?
        pack( "q>", $offset + 0 )
        : Kafka::Int64::packq( $offset + 0 );
    
    $offset = BITS64 ?
        unpack( "q>", substr( $response, 0, 8 ) )
        : Kafka::Int64::unpackq( substr( $response, 0, 8 ) );
    
    if ( BITS64 )
    {
        $message->{offset} = $next_offset;
        $next_offset += $message->{length} + 4;
    }
    else
    {
        $message->{offset} = Kafka::Int64::intsum( $next_offset, 0 );
        $next_offset = Kafka::Int64::intsum(
            $next_offset,
            $message->{length} + 4
            );
    }

=head1 DESCRIPTION

Transparent L<BigInteger|bigint> support on 32-bit platforms where native
integer type is limited to 32 bits and slow bigint must be used instead.
Use L<functions|/FUNCTIONS> from this module in such case.

The main features of the C<Kafka::Int64> module are:

=over 3

=item *

Support for working with 64 bit elements of the Kafka Wire Format protocol
on 32 bit systems.

=back

=head2 FUNCTIONS

The following functions are available for the C<Kafka::Int64> module.

=head3 C<intsum( $bint, $int )>

Adds two numbers to emulate bigint adding 64-bit integers in 32-bit systems.

The both arguments must be a number. That is, it is defined and Perl thinks
it's a number. The first argument may be a L<Math::BigInt|Math::BigInt>
integer.

Returns the value as a L<Math::BigInt|Math::BigInt> integer, or error will
cause the program to halt (C<confess>) if the argument is not a valid number.

=head3 C<packq( $bint )>

Emulates C<pack( "qE<gt>", $bint )> to 32-bit systems - assumes decimal string
or integer input.

An argument must be a positive number. That is, it is defined and Perl thinks
it's a number. The argument may be a L<Math::BigInt|Math::BigInt> integer.

The special values -1, -2 are allowed.

Returns the value as a packed binary string, or error will cause the program
to halt (C<confess>) if the argument is not a valid number.

=head3 C<unpackq( $bstr )>

Emulates C<unpack( "qE<gt>", $bstr )> to 32-bit systems - assumes binary input.

The argument must be a binary string of 8 bytes length.

Returns the value as a L<Math::BigInt|Math::BigInt> integer, or error will
cause the program to halt (C<confess>) if the argument is not a valid binary
string.

=head1 DIAGNOSTICS

C<Kafka::Int64> is not a user module and any L<functions|/FUNCTIONS> error
is FATAL.
FATAL errors will cause the program to halt (C<confess>), since the
problem is so severe that it would be dangerous to continue. (This can
always be trapped with C<eval>. Under the circumstances, dying is the best
thing to do).

=over 3

=item C<Mismatch argument>

This means that you didn't give the right argument to some of the
L<functions|/FUNCTIONS>.

=back

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
All rights reserved.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut