package Kafka::TestInternals;

=head1 NAME

Kafka::TestInternals - Constants and functions used in the tests.

=head1 VERSION

This documentation refers to C<Kafka::TestInternals> version 0.8008 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.8008';

use Exporter qw(
    import
);

our @EXPORT_OK = qw(
    _is_suitable_int
    @not_array
    @not_array0
    @not_empty_string
    @not_hash
    @not_is_like_server_list
    @not_isint
    @not_nonnegint
    @not_number
    @not_posint
    @not_posnumber
    @not_right_object
    @not_string
    @not_string_array
    @not_topics_array
    $topic
);

#-- load the modules -----------------------------------------------------------

use Const::Fast;
use Scalar::Util::Numeric qw(
    isbig
    isint
);

use Kafka::Cluster qw(
    $DEFAULT_TOPIC
);
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_PRODUCE
    $APIKEY_OFFSET
);

#-- declarations ---------------------------------------------------------------

=head1 DESCRIPTION

This module is not a user module.

In order to achieve better performance,
functions of this module do not perform arguments validation.

=head2 EXPORT

The following constants are available for export

=cut

# options for testing arguments:
#    "\x{20ac}",
#    undef,
#    0,
#    0.5,
#    1,
#    -1,
#    -3,
#    q{},
#    '0',
#    '0.5',
#    '1',
#    9999999999999999,
#    \1,
#    [],
#    {},
#    [ 'something' ],
#    { foo => 'bar' },
#    bless( {}, 'FakeName' ),
#   'simple string',

=head3 C<$topic>

Name topic used in the tests.

=cut
const our $topic    => $DEFAULT_TOPIC;

=head3 C<@not_right_object>

The values do not correspond to the object type you want.

=cut
const our @not_right_object => (
    "\x{20ac}",
    undef,
    0,
    0.5,
    1,
    -1,
    -3,
    q{},
    '0',
    '0.5',
    '1',
    9999999999999999,
    \1,
    [],
    {},
    [ 'something' ],
    { foo => 'bar' },
    bless( {}, 'FakeName' ),
   'simple string',
);

=head3 C<@not_nonnegint>

The values do not correspond to not negative integers.

=cut
const our @not_nonnegint => (
    "\x{20ac}",
    undef,
    0.5,
    -1,
    -3,
    q{},
    '0.5',
    \'scalar',
    [],
    {},
    [ 'something' ],
    { foo => 'bar' },
    bless( {}, 'FakeName' ),
);

=head3 C<@not_empty_string>

The values do not correspond to a non-empty string.

=cut
const our @not_empty_string => (
    "\x{20ac}",
    \1,
    [],
    {},
    [ 'something' ],
    { foo => 'bar' },
    \'string',
    bless( {}, 'FakeName' ),
);

=head3 C<@not_string>

The values do not correspond to any string.

=cut
const our @not_string => (
    @not_empty_string,
    undef,
    q{},
);

=head3 C<@not_posint>

The values do not correspond to a positive integers.

=cut
const our @not_posint => (
    "\x{20ac}",
    undef,
    0,
    0.5,
    -1,
    -3,
    q{},
    '0',
    '0.5',
    \'scalar',
    [],
    {},
    [ 'something' ],
    { foo => 'bar' },
    bless( {}, 'FakeName' ),
);

=head3 C<@not_number>

The values do not correspond to any number.

=cut
const our @not_number => (
    "\x{20ac}",
    undef,
    q{},
    \'scalar',
    [],
    {},
    [ 'something' ],
    { foo => 'bar' },
    bless( {}, 'FakeName' ),
    'incorrect number,'
);

=head3 C<@not_posnumber>

The values do not correspond to a positive number.

=cut
const our @not_posnumber => (
    @not_number,
    0,
    -1,
);

=head3 C<@not_isint>

The values do not correspond to any integers.

=cut
const our @not_isint => (
    "\x{20ac}",
#    undef,
    0.5,
    q{},
    '0.5',
    \'scalar',
    [],
    {},
    [ 'something' ],
    { foo => 'bar' },
    bless( {}, 'FakeName' ),
);

=head3 C<@not_array0>

The values do not correspond to a raw and unblessed ARRAY reference.

=cut
const our @not_array0 => (
    "\x{20ac}",
    undef,
    0,
    0.5,
    1,
    -1,
    -3,
    q{},
    '0',
    '0.5',
    '1',
    9999999999999999,
    \'scalar',
    {},
    { foo => 'bar' },
    bless( {}, 'FakeName' ),
);

=head3 C<@not_array0>

The values do not correspond to a raw and unblessed ARRAY reference containing at least one element of any kind.

=cut
const our @not_array => (
    @not_array0,
    [],
);

=head3 C<@not_topics_array>

The values do not correspond to a 'topics' ARRAY reference.
For 'topics' ARRAY examples see C<t/*_decode_encode.t>.

=cut
our @not_topics_array;
foreach my $bad_value (
        @not_array0,
        [],
        'scalar',
    ) {
    push @not_topics_array,
        {
            ApiKey  => $APIKEY_PRODUCE,
            topics  => $bad_value,
        },
        {
            ApiKey  => $APIKEY_PRODUCE,
            topics  => [ $bad_value ],
        },
        {
            ApiKey  => $APIKEY_PRODUCE,
            topics  => [
                {
                    TopicName   => $topic,
                    partitions  => $bad_value,
                },
            ],
        },
        {
            ApiKey  => $APIKEY_PRODUCE,
            topics  => [
                {
                    TopicName   => $topic,
                    partitions  => [ $bad_value ],
                },
            ],
        },
    ;
}

=head3 C<@not_is_like_server_list>

The values do not correspond to a reference to an array of server names.

=cut
const our @not_is_like_server_list => (
    [ "\x{20ac}" ],
    [ undef ],
    [ 0 ],
    [ 0.5 ],
    [ 1 ],
    [ -1 ],
    [ -3 ],
    [ q{} ],
    [ '0' ],
    [ '0.5' ],
    [ '1' ],
    [ 9999999999999999 ],
    [ \'scalar' ],
    [ [] ],
    [ {} ],
    [ [ 'something' ] ],
    [ { foo => 'bar' } ],
    [ bless( {}, 'FakeName' ) ],
    [ 'string' ],
);

=head3 C<@not_string_array>

The values do not correspond to a reference to an array of any strings.

=cut
const our @not_string_array => (
    [ "\x{20ac}" ],
    [ undef ],
    [ \1 ],
    [ [] ],
    [ {} ],
    [ [ 'something' ] ],
    [ { foo => 'bar' } ],
    [ bless( {}, 'FakeName' ) ],
);

=head3 C<@not_hash>

The values do not correspond to a raw and unblessed HASH reference with at least one entry.

=cut
const our @not_hash => (
    "\x{20ac}",
    undef,
    0,
    0.5,
    1,
    -1,
    -3,
    q{},
    '0',
    '0.5',
    '1',
    9999999999999999,
#    \'scalar',
    [],
    {},
    [ 'something' ],
    bless( {}, 'FakeName' ),
   'simple string',
);

#-- public functions -----------------------------------------------------------

#-- private functions ----------------------------------------------------------

# Verifying whether the argument is a simple int or a bigint
sub _is_suitable_int {
    my ( $n ) = @_;

    $n // return;
    return isint( $n ) || isbig( $n );
}

#-- public attributes ----------------------------------------------------------

#-- public methods -------------------------------------------------------------

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

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

L<Kafka::IO|Kafka::IO> - low-level interface for communication with Kafka server.

L<Kafka::Exceptions|Kafka::Exceptions> - module designated to handle Kafka exceptions.

L<Kafka::Internals|Kafka::Internals> - internal constants and functions used
by several package modules.

A wealth of detail about the Apache Kafka and the Kafka Protocol:

Main page at L<http://kafka.apache.org/>

Kafka Protocol at L<https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol>

=head1 SOURCE CODE

Kafka package is hosted on GitHub:
L<https://github.com/TrackingSoft/Kafka>

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
