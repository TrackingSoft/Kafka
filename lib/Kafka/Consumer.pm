package Kafka::Consumer;

=head1 NAME

Kafka::Consumer - Perl interface for Kafka consumer client.

=head1 VERSION

This documentation refers to C<Kafka::Consumer> version 1.05 .

=cut

use 5.010;
use strict;
use warnings;

our $VERSION = '1.05';

use Carp;
use Params::Util qw(
    _INSTANCE
    _NONNEGINT
    _NUMBER
    _POSINT
    _STRING
);
use Scalar::Util::Numeric qw(
    isint
);

use Kafka qw(
    $BITS64
    $DEFAULT_MAX_BYTES
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_WAIT_TIME
    %ERROR
    $ERROR_CANNOT_GET_METADATA
    $ERROR_METADATA_ATTRIBUTES
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_PARTITION_DOES_NOT_MATCH
    $ERROR_TOPIC_DOES_NOT_MATCH
    $MESSAGE_SIZE_OVERHEAD
    $MIN_BYTES_RESPOND_IMMEDIATELY
    $RECEIVE_LATEST_OFFSETS
    $RECEIVE_EARLIEST_OFFSET
);
use Kafka::Exceptions;
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_OFFSET
    $APIKEY_OFFSETCOMMIT
    $APIKEY_OFFSETFETCH
    $MAX_INT32
    _get_CorrelationId
    _isbig
    format_message
);
use Kafka::Connection;
use Kafka::Message;

if ( !$BITS64 ) { eval 'use Kafka::Int64; 1;' or die "Cannot load Kafka::Int64 : $@"; } ## no critic

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka qw(
        $DEFAULT_MAX_BYTES
        $DEFAULT_MAX_NUMBER_OF_OFFSETS
        $RECEIVE_EARLIEST_OFFSET
    );
    use Kafka::Connection;
    use Kafka::Consumer;

    my ( $connection, $consumer );
    try {

        #-- Connection
        $connection = Kafka::Connection->new( host => 'localhost' );

        #-- Consumer
        $consumer = Kafka::Consumer->new( Connection  => $connection );

        # Get a valid offset before the given time
        my $offsets = $consumer->offset_before_time(
            'mytopic',                      # topic
            0,                              # partition
            (time()-3600) * 1000,           # time
        );

        if ( @$offsets ) {
            say "Received offset: $_" foreach @$offsets;
        } else {
            warn "Error: Offsets are not received\n";
        }

        # Consuming messages
        my $messages = $consumer->fetch(
            'mytopic',                      # topic
            0,                              # partition
            0,                              # offset
            $DEFAULT_MAX_BYTES              # Maximum size of MESSAGE(s) to receive
        );

        if ( $messages ) {
            foreach my $message ( @$messages ) {
                if ( $message->valid ) {
                    say 'payload    : ', $message->payload;
                    say 'key        : ', $message->key;
                    say 'offset     : ', $message->offset;
                    say 'next_offset: ', $message->next_offset;
                } else {
                    say 'error      : ', $message->error;
                }
            }
        }

    } catch {
        my $error = $_;
        if ( blessed( $error ) && $error->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $error->code, ') ',  $error->message, "\n";
            exit;
        } else {
            die $error;
        }
    };

    # Closes the consumer and cleans up
    undef $consumer;
    $connection->close;
    undef $connection;

=head1 DESCRIPTION

Kafka consumer API is implemented by C<Kafka::Consumer> class.

The main features of the C<Kafka::Consumer> class are:

=over 3

=item *

Provides an object-oriented API for consuming messages.

=item *

Provides Kafka FETCH and OFFSETS requests.

=item *

Supports parsing the Apache Kafka 0.9+ Wire Format protocol.

=item *

Works with 64-bit elements of the Kafka Wire Format protocol
on 32 bit systems.

=back

The Kafka consumer response returns ARRAY references for C<offsets> and
C<fetch> methods.

Array returned by C<offsets> contains offset integers.

Array returned by C<fetch> contains objects of L<Kafka::Message|Kafka::Message> class.

=cut

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Creates a new consumer client object. Returns the created C<Kafka::Consumer> object.

C<new()> takes arguments in key-value pairs. The following arguments are recognized:

=over 3

=item C<Connection =E<gt> $connection>

C<$connection> is the L<Kafka::Connection|Kafka::Connection> object responsible for communication with
the Apache Kafka cluster.

=item C<ClientId =E<gt> $client_id>

This is a user supplied identifier (string) for the client application.

C<ClientId> will be auto-assigned if not passed in when creating L<Kafka::Producer|Kafka::Producer> object.

=item C<MaxWaitTime =E<gt> $max_time>

The maximum amount of time (seconds, may be fractional) to wait when no sufficient data is available at the time the
request was issued.

Optional, default is C<$DEFAULT_MAX_WAIT_TIME>.

C<$DEFAULT_MAX_WAIT_TIME> is the default time that can be imported from the
L<Kafka|Kafka> module.

The C<$max_time> must be a positive number.

=item C<MinBytes =E<gt> $min_bytes>

The minimum number of bytes of messages that must be available to give a response.
If the client sets this to C<$MIN_BYTES_RESPOND_IMMEDIATELY> the server will always respond
immediately. If it is set to C<$MIN_BYTES_RESPOND_HAS_DATA>, the server will respond as soon
as at least one partition has at least 1 byte of data or the specified timeout occurs.
Setting higher values in combination with the bigger timeouts allows reading larger chunks of data.

Optional, int32 signed integer, default is C<$MIN_BYTES_RESPOND_IMMEDIATELY>.

C<$MIN_BYTES_RESPOND_IMMEDIATELY>, C<$MIN_BYTES_RESPOND_HAS_DATA> are the defaults that
can be imported from the L<Kafka|Kafka> module.

The C<$min_bytes> must be a non-negative int32 signed integer.

=item C<MaxBytes =E<gt> $max_bytes>

The maximum bytes to include in the message set for this partition.

Optional, int32 signed integer, default = C<$DEFAULT_MAX_BYTES> (1_000_000).

The C<$max_bytes> must be more than C<$MESSAGE_SIZE_OVERHEAD>
(size of protocol overhead - data added by Kafka wire protocol to each message).

C<$DEFAULT_MAX_BYTES>, C<$MESSAGE_SIZE_OVERHEAD>
are the defaults that can be imported from the L<Kafka|Kafka> module.

=item C<MaxNumberOfOffsets =E<gt> $max_number>

Limit the number of offsets returned by Kafka.

That is a non-negative integer.

Optional, int32 signed integer, default = C<$DEFAULT_MAX_NUMBER_OF_OFFSETS> (100).

C<$DEFAULT_MAX_NUMBER_OF_OFFSETS>
is the default that can be imported from the L<Kafka|Kafka> module.

=back

=cut
sub new {
    my ( $class, %p ) = @_;

    my $self = bless {
        Connection          => undef,
        ClientId            => undef,
        MaxWaitTime         => $DEFAULT_MAX_WAIT_TIME,
        MinBytes            => $MIN_BYTES_RESPOND_IMMEDIATELY,
        MaxBytes            => $DEFAULT_MAX_BYTES,
        MaxNumberOfOffsets  => $DEFAULT_MAX_NUMBER_OF_OFFSETS,
    }, $class;

    exists $p{$_} and $self->{$_} = $p{$_} foreach keys %$self;

    $self->{ClientId}       //= 'consumer';

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'Connection' )
        unless _INSTANCE( $self->{Connection}, 'Kafka::Connection' );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'ClientId' )
        unless ( $self->{ClientId} eq q{} || defined( _STRING( $self->{ClientId} ) ) ) && !utf8::is_utf8( $self->{ClientId} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'MaxWaitTime (%s)', $self->{MaxWaitTime} ) )
        unless defined( $self->{MaxWaitTime} ) && defined _NUMBER( $self->{MaxWaitTime} ) && int( $self->{MaxWaitTime} * 1000 ) >= 1 && int( $self->{MaxWaitTime} * 1000 ) <= $MAX_INT32;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'MinBytes (%s)', $self->{MinBytes} ) )
        unless ( _isbig( $self->{MinBytes} ) ? ( $self->{MinBytes} >= 0 ) : defined( _NONNEGINT( $self->{MinBytes} ) ) ) && $self->{MinBytes} <= $MAX_INT32;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'MaxBytes (%s)', $self->{MaxBytes} ) )
        unless ( _isbig( $self->{MaxBytes} ) ? ( $self->{MaxBytes} > 0 ) : _POSINT( $self->{MaxBytes} ) ) && $self->{MaxBytes} >= $MESSAGE_SIZE_OVERHEAD && $self->{MaxBytes} <= $MAX_INT32;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'MaxNumberOfOffsets (%s)', $self->{MaxNumberOfOffsets} ) )
        unless defined( _POSINT( $self->{MaxNumberOfOffsets} ) ) && $self->{MaxNumberOfOffsets} <= $MAX_INT32;

    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are defined for the C<Kafka::Consumer> class:

=cut

#-- public methods -------------------------------------------------------------

=head3 C<fetch( $topic, $partition, $start_offset, $max_size )>

Get a list of messages to consume one by one up to C<$max_size> bytes.

Returns the reference to array of the L<Kafka::Message|Kafka::Message> objects.

C<fetch()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer.

=item C<$start_offset>

Offset in topic and partition to start from (64-bit integer).

The argument must be a non-negative integer. The argument may be a
L<Math::BigInt|Math::BigInt> integer on 32-bit system.

=item C<$max_size>

C<$max_size> is the maximum size of the messages set to return. The argument
must be a positive int32 signed integer.

The maximum size of a request limited by C<MAX_SOCKET_REQUEST_BYTES> that
can be imported from L<Kafka|Kafka> module.

=back

=cut
sub fetch {
    my ( $self, $topic, $partition, $start_offset, $max_size, $_return_all, $api_version ) = @_;
    # Special argument: $_return_all - return redundant messages sent out of a compressed package posts

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'topic' )
        unless defined( $topic ) && ( $topic eq q{} || defined( _STRING( $topic ) ) ) && !utf8::is_utf8( $topic );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'partition' )
        unless defined( $partition ) && isint( $partition ) && $partition >= 0;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'offset' )
        unless defined( $start_offset ) && ( ( _isbig( $start_offset ) && $start_offset >= 0 ) || defined( _NONNEGINT( $start_offset ) ) );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'max_size (%s)', $max_size ) )
        unless ( !defined( $max_size ) || ( ( _isbig( $max_size ) || _POSINT( $max_size ) ) && $max_size >= $MESSAGE_SIZE_OVERHEAD && $max_size <= $MAX_INT32 ) );

    my $request = {
        ApiKey                              => $APIKEY_FETCH,
        ApiVersion                          => $api_version,
        CorrelationId                       => _get_CorrelationId(),
        ClientId                            => $self->{ClientId},
        MaxWaitTime                         => int( $self->{MaxWaitTime} * 1000 ),
        MinBytes                            => $self->{MinBytes},
        MaxBytes                            => $max_size // $self->{MaxBytes},
        topics                              => [
            {
                TopicName                   => $topic,
                partitions                  => [
                    {
                        Partition           => $partition,
                        FetchOffset         => $start_offset,
                        MaxBytes            => $max_size // $self->{MaxBytes},
                    },
                ],
            },
        ],
    };

    my $response = $self->{Connection}->receive_response_to_request( $request, undef, $self->{MaxWaitTime} );

    my $messages = [];
    foreach my $received_topic ( @{ $response->{topics} } ) {
        $received_topic->{TopicName} eq $topic
            or $self->_error( $ERROR_TOPIC_DOES_NOT_MATCH, format_message( "'%s' ne '%s'", $topic, $received_topic->{TopicName} ) );
        foreach my $received_partition ( @{ $received_topic->{partitions} } ) {
            $received_partition->{Partition} == $partition
                or $self->_error( $ERROR_PARTITION_DOES_NOT_MATCH, format_message( '%s != %s', $partition, $received_partition->{Partition} ) );
            my $HighwaterMarkOffset = $received_partition->{HighwaterMarkOffset};
            foreach my $Message ( @{ $received_partition->{MessageSet} } ) {
                my $offset = $Message->{Offset};
                my $next_offset;
                if ( $BITS64 ) {
                    $next_offset += $offset + 1;
                } else {
                    $offset = Kafka::Int64::intsum( $offset, 0 );
                    $next_offset = Kafka::Int64::intsum( $offset, 1 );
                }

                # skip previous messages of a compressed package posts
                next if $offset < $start_offset && !$_return_all;

                my $message_error = q{};
                # According to Apache Kafka documentation:
                # This byte holds metadata attributes about the message. The
                # lowest 3 bits contain the compression codec used for the
                # message. The fourth lowest bit represents the timestamp type.
                # 0 stands for CreateTime and 1 stands for LogAppendTime. The
                # producer should always set this bit to 0. (since 0.10.0).
                # All other bits should be set to 0.
                my $attributes = $Message->{Attributes};
                # check that attributes is valid
                $attributes & 0b11110000
                  and $message_error = $ERROR{ $ERROR_METADATA_ATTRIBUTES };

                if (my $compression_codec = $attributes & 0b00000111) {
                    unless (   $compression_codec == 1 # GZIP
                            || $compression_codec == 2 # Snappy
                           ) {
                        $message_error = $ERROR{ $ERROR_METADATA_ATTRIBUTES };
                    }
                }

                push( @$messages, Kafka::Message->new( {
                        Attributes          => $Message->{Attributes},
                        Timestamp           => $Message->{Timestamp},
                        MagicByte           => $Message->{MagicByte},
                        key                 => $Message->{Key},
                        payload             => $Message->{Value},
                        offset              => $offset,
                        next_offset         => $next_offset,
                        error               => $message_error,
                        valid               => !$message_error,
                        HighwaterMarkOffset => $HighwaterMarkOffset,
                    } )
                );
            }
        }
    }

    return $messages;
}

=head3 C<offset_at_time( $topic, $partition, $time )>

Returns an offset, given a topic, partition and time.

The returned offset is the earliest offset whose timestamp is greater than or
equal to the given timestamp. The return value is a HashRef, containing
C<timestamp> and C<offset> keys.

B<WARNING>: this method requires Kafka 0.10.0, and messages with timestamps.
Check the configuration of the brokers or topic, specifically
C<message.timestamp.type>, and set it either to C<LogAppentTime> to have Kafka
automatically set messages timestamps based on the broker clock, or
C<CreateTime>, in which case the client populating your topic has to set the
timestamps when producing messages. .

C<offset_at_time()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topics> must be a normal non-false strings of non-zero length.

=item C<$partition>

The C<$partitions> must be a non-negative integers.

=item C<$time>

Get offsets before the given time (in milliseconds since UNIX Epoch).

The argument must be a positive number.

The argument may be a L<Math::BigInt|Math::BigInt> integer on 32 bit
system.

=back

=cut

sub offset_at_time {
    my ( $self, $topic, $partition, $time ) = @_;

    # we don't accept special values for $time, we want a real timestamp
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'time' )
        unless defined( $time ) && ( _isbig( $time ) || isint( $time ) ) && $time >= 0;

    # no max_number, api version = 1
    return $self->_query_offsets($topic, $partition, $time, undef, 1)->[0];
}

=head3 C<offset_before_time( $topic, $partition, $time )>

Returns an offset, given a topic, partition and time.

The returned offset is an offset whose timestamp is guaranteed to be earlier
than the given timestamp. The return value is a Number

This method works with all version of Kafka, and doesn't require messages with
timestamps.

C<offset_before_time()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topics> must be a normal non-false strings of non-zero length.

=item C<$partition>

The C<$partitions> must be a non-negative integers.

=item C<$time>

Get offsets before the given time (in milliseconds since UNIX Epoch).

The argument must be a positive number.

The argument may be a L<Math::BigInt|Math::BigInt> integer on 32 bit
system.

=back

=cut

sub offset_before_time {
    my ( $self, $topic, $partition, $time ) = @_;

    # we don't accept special values for $time, we want a real timestamp
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'time' )
        unless defined( $time ) && ( _isbig( $time ) || isint( $time ) ) && $time >= 0;
    # $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'max_number (%s)', $max_number ) )
    #     unless !defined( $max_number ) || ( _POSINT( $max_number ) && $max_number <= $MAX_INT32 );

    # max_number = 1, api version = 0
    return $self->_query_offsets($topic, $partition, $time, 1, 0)->[0];
}

=head3 C<offset_earliest( $topic, $partition )>

Returns the earliest offset for a given topic and partition

C<offset_earliest()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topics> must be a normal non-false strings of non-zero length.

=item C<$partition>

The C<$partitions> must be a non-negative integers.

=back

=cut

sub offset_earliest {
    my ( $self, $topic, $partition ) = @_;

    # max_number = 1, api version = 0
    return $self->_query_offsets($topic, $partition, $RECEIVE_EARLIEST_OFFSET, 1, 0)->[0];
}

=head3 C<offset_latest( $topic, $partition )>

Returns the latest offset for a given topic and partition

C<offset_latest()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topics> must be a normal non-false strings of non-zero length.

=item C<$partition>

The C<$partitions> must be a non-negative integers.

=back

=cut

sub offset_latest {
    my ( $self, $topic, $partition ) = @_;

    # max_number = 1, api version = 0
    return $self->_query_offsets($topic, $partition, $RECEIVE_LATEST_OFFSETS, 1, 0)->[0];
}

=head3 C<offsets( $topic, $partition, $time, $max_number )>

B<WARNING>: This method is DEPRECATED, please use one of C<offset_at_time()>, C<offset_before_time()>, C<offset_earliest()>, C<offset_latest()>. It is kept for backward compatibility.

Returns an ArrayRef of offsets

C<offsets()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topics> must be a normal non-false strings of non-zero length.

=item C<$partition>

The C<$partitions> must be a non-negative integers.

=item C<$time>

Get offsets before the given time (in milliseconds since UNIX Epoch). It must
be a positive number. It may be a L<Math::BigInt|Math::BigInt> integer on 32
bit system.

The special values C<$RECEIVE_LATEST_OFFSETS> (-1), C<$RECEIVE_EARLIEST_OFFSET>
(-2) are allowed. They can be imported from the L<Kafka|Kafka> module.

=item C<$max_number>

Maximum number of offsets to be returned

=back

=cut

sub offsets {
    my ( $self, $topic, $partition, $time, $max_number ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'time' )
        unless defined( $time ) && ( _isbig( $time ) || isint( $time ) ) && $time >= $RECEIVE_EARLIEST_OFFSET;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, format_message( 'max_number (%s)', $max_number ) )
        unless !defined( $max_number ) || ( _POSINT( $max_number ) && $max_number <= $MAX_INT32 );

    return $self->_query_offsets($topic, $partition, $time, $max_number, 0);
}

sub _query_offsets {
    my ( $self, $topic, $partition, $time, $max_number, $api_version ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'topic' )
        unless defined( $topic) && ( $topic eq q{} || defined( _STRING( $topic ) ) ) && !utf8::is_utf8( $topic );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'partition' )
        unless defined( $partition ) && isint( $partition ) && $partition >= 0;

    my $is_v1 = $api_version == 1;

    my $request = {
        ApiKey                              => $APIKEY_OFFSET,
        ApiVersion                          => $api_version,
        CorrelationId                       => _get_CorrelationId(),
        ClientId                            => $self->{ClientId},
        topics                              => [
            {
                TopicName                   => $topic,
                partitions                  => [
                    {
                        Partition           => $partition,
                        Time                => $time,
                        MaxNumberOfOffsets  => $max_number // $self->{MaxNumberOfOffsets},
                    },
                ],
            },
        ],
    };

    my $response = $self->{Connection}->receive_response_to_request( $request );

    my $offsets = [];
    # because we accepted only one topic and partition, we are sure that the
    # response is all about this single topic and partition, so we can merge
    # the offsets.
    if ($is_v1) {
        foreach my $received_topic ( @{ $response->{topics} } ) {
            foreach my $partition_offsets ( @{ $received_topic->{PartitionOffsets} } ) {
                push @$offsets, { timestamp => $partition_offsets->{Timestamp},
                                  offset    => $partition_offsets->{Offset} };
            }
        }
    } else {
        foreach my $received_topic ( @{ $response->{topics} } ) {
            foreach my $partition_offsets ( @{ $received_topic->{PartitionOffsets} } ) {
                push @$offsets, @{ $partition_offsets->{Offset} };
            }
        }
    }

    return $offsets;
}

=head3 C<commit_offsets( $topic, $partition, $offset, $group )>

Commit offsets using the offset commit/fetch API.

Returns a non-blank value (a reference to a hash with server response description)
if the message is successfully sent.

C<commit_offsets()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer.

=item C<$offset>

Offset in topic and partition to commit.

The argument must be a positive number.

The argument may be a L<Math::BigInt|Math::BigInt> integer on 32 bit
system.

=item C<$group>

The name of the consumer group

The argument must be a normal non-false string of non-zero length.

=back

=cut
sub commit_offsets {
    my ( $self, $topic, $partition, $offset, $group ) = @_;


    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'topic' )
        unless defined( $topic ) && ( $topic eq q{} || defined( _STRING( $topic ) ) ) && !utf8::is_utf8( $topic );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'partition' )
        unless defined( $partition ) && isint( $partition ) && $partition >= 0;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'offset' )
        unless defined( $offset ) && ( ( _isbig( $offset ) && $offset >= 0 ) || defined( _NONNEGINT( $offset ) ) );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'group' )
        unless defined( $group ) && ( $group eq q{} || defined( _STRING( $group ) ) ) && !utf8::is_utf8( $group );

    my $request = {
        __send_to__               => 'group_coordinator',
        ApiKey                    => $APIKEY_OFFSETCOMMIT,
        CorrelationId             => _get_CorrelationId(),
        ClientId                  => $self->{ClientId},
        GroupId                   => $group,
        topics  => [
            {
                TopicName         => $topic,
                partitions        => [
                    {
                        Partition => $partition,
                        Offset    => $offset,
                        Metadata  => '',
                    },
                ],
            },
        ],
    };

    return $self->{Connection}->receive_response_to_request( $request );
}

=head3 C<fetch_offsets( $topic, $partition, $group )>

Fetch Committed offsets using the offset commit/fetch API.

Returns a non-blank value (a reference to a hash with server response description)
if the message is successfully sent.

C<fetch_offsets()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer.

=item C<$group>

The name of the consumer group

The argument must be a normal non-false string of non-zero length.

=back

=cut
sub fetch_offsets {
    my ( $self, $topic, $partition, $group ) = @_;


    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'topic' )
        unless defined( $topic ) && ( $topic eq q{} || defined( _STRING( $topic ) ) ) && !utf8::is_utf8( $topic );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'partition' )
        unless defined( $partition ) && isint( $partition ) && $partition >= 0;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'group' )
        unless defined( $group ) && ( $group eq q{} || defined( _STRING( $group ) ) ) && !utf8::is_utf8( $group );

    my $request = {
        __send_to__               => 'group_coordinator',
        ApiKey                    => $APIKEY_OFFSETFETCH,
        CorrelationId             => _get_CorrelationId(),
        ClientId                  => $self->{ClientId},
        GroupId                   => $group,
        topics  => [
            {
                TopicName         => $topic,
                partitions        => [
                    {
                        Partition => $partition,
                    },
                ],
            },
        ],
    };

    return $self->{Connection}->receive_response_to_request( $request );
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

# Handler for errors
sub _error {
    my $self = shift;

    Kafka::Exception::Consumer->throw( throw_args( @_ ) );

    return;
}



1;

__END__

=head1 DIAGNOSTICS

When error is detected, an exception, represented by object of C<Kafka::Exception::Consumer> class,
is thrown (see L<Kafka::Exceptions|Kafka::Exceptions>).

L<code|Kafka::Exceptions/code> and a more descriptive L<message|Kafka::Exceptions/message> provide
information about thrown exception. Consult documentation of the L<Kafka::Exceptions|Kafka::Exceptions>
for the list of all available methods.

Authors suggest using of L<Try::Tiny|Try::Tiny>'s C<try> and C<catch> to handle exceptions while
working with L<Kafka|Kafka> package.

=over 3

=item C<Invalid argument>

Invalid argument passed to a C<new> L<constructor|/CONSTRUCTOR> or other L<method|/METHODS>.

=item C<Cannot send>

Request cannot be sent.

=item C<Cannot receive>

Response cannot be received.

=item C<Cannott bind>

TCP connection can't be established on the given host and port.

=item C<Cannot get metadata>

Failed to obtain metadata from Kafka servers.

=item C<Leader not found>

Missing information about server-leader in metadata.

=item C<Mismatch CorrelationId>

C<CorrelationId> of response doesn't match one in request.

=item C<There are no known brokers>

Resulting metadata has no information about cluster brokers.

=item C<Cannot get metadata>

Received metadata has incorrect internal structure.

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

=head1 SOURCE CODE

Kafka package is hosted on GitHub:
L<https://github.com/TrackingSoft/Kafka>

=head1 AUTHOR

Sergey Gladkov

Please use GitHub project link above to report problems or contact authors.

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Sergiy Zuban

Vlad Marchenko

Damien Krotkine

Greg Franklin

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2017 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
