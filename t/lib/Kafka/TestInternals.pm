package Kafka::TestInternals;

#TODO: title - the name, purpose, and a warning that this one is for internal use only

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

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

our $VERSION = '0.800_1';

#-- load the modules -----------------------------------------------------------

use Const::Fast;
use Scalar::Util::Numeric qw(
    isbig
    isint
);

use Kafka::Cluster;
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_PRODUCE
    $APIKEY_OFFSET
);

#-- declarations ---------------------------------------------------------------

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

const our $topic        => $Kafka::Cluster::DEFAULT_TOPIC;

our @not_right_object = (
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

our @not_nonnegint = (
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

our @not_empty_string = (
    "\x{20ac}",
    \1,
    [],
    {},
    [ 'something' ],
    { foo => 'bar' },
    \'string',
    bless( {}, 'FakeName' ),
);

our @not_string = (
    @not_empty_string,
    undef,
    q{},
);

our @not_posint = (
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

our @not_number = (
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

our @not_posnumber = (
    @not_number,
    0,
    -1,
);

our @not_isint = (
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

our @not_array0 = (
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

our @not_array = (
    @not_array0,
    [],
);

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

our @not_is_like_server_list = (
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

our @not_string_array = (
    [ "\x{20ac}" ],
    [ undef ],
    [ \1 ],
    [ [] ],
    [ {} ],
    [ [ 'something' ] ],
    [ { foo => 'bar' } ],
    [ bless( {}, 'FakeName' ) ],
);

our @not_hash = (
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

sub _is_suitable_int {
    my ( $n ) = @_;

    $n // return;
    return isint( $n ) || isbig( $n );
}

#-- private functions ----------------------------------------------------------

#-- public attributes ----------------------------------------------------------

#-- public methods -------------------------------------------------------------

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

1;

__END__
