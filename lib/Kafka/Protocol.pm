package Kafka::Protocol;

use 5.010;
use strict;
use warnings;

use Exporter qw( import );
our @EXPORT_OK  = qw(
    REQUESTTYPE_PRODUCE
    REQUESTTYPE_FETCH
    REQUESTTYPE_MULTIFETCH
    REQUESTTYPE_MULTIPRODUCE
    REQUESTTYPE_OFFSETS
    produce_request
    fetch_request
    offsets_request
    fetch_response
    offsets_response
    );

our $VERSION = '0.07';

use bytes;
use Carp;
use Digest::CRC     qw( crc32 );
use Params::Util    qw( _STRING _NONNEGINT _POSINT _NUMBER _ARRAY0 _SCALAR );

use Kafka qw(
    ERROR_INVALID_MESSAGE_CODE
    ERROR_MISMATCH_ARGUMENT
    ERROR_CHECKSUM_ERROR
    ERROR_COMPRESSED_PAYLOAD
    ERROR_NUMBER_OF_OFFSETS
    BITS64
    );

if ( !BITS64 ) { eval 'use Kafka::Int64; 1;' or die "Cannot load Kafka::Int64 : $@"; }  ## no critic

use constant {
    DEBUG                               => 0,

    REQUESTTYPE_PRODUCE                 => 0,
    REQUESTTYPE_FETCH                   => 1,
    REQUESTTYPE_MULTIFETCH              => 2,   # Not used now
    REQUESTTYPE_MULTIPRODUCE            => 3,   # Not used now
    REQUESTTYPE_OFFSETS                 => 4,

    MAGICVALUE_NOCOMPRESSION            => 0,
    MAGICVALUE_COMPRESSION              => 1,   # Not used now
    COMPRESSION_NO_COMPRESSION          => 0,
    COMPRESSION_GZIP                    => 1,   # Not used now
    COMPRESSION_SNAPPY                  => 2,   # Not used now
};

our $_last_error;
our $_last_errorcode;

my $position;

sub last_error {
    return $_last_error;
}

sub last_errorcode {
    return $_last_errorcode;
}

sub _error {
    my $error_code  = shift;

    $_last_errorcode  = $error_code;
    $_last_error      = $Kafka::ERROR[$_last_errorcode];
    confess $_last_error;
}

# Wire Format: https://cwiki.apache.org/confluence/display/KAFKA/Wire+Format
################################################################################
# Requests Wire Format

# Request Header ---------------------------------------------------------------

sub _request_header_encode {
    my $request_length  = shift;
    my $request_type    = shift;
    my $topic           = shift;
    my $partition       = shift;

    my$ encoded =
        pack( "
            N                                   # REQUEST_LENGTH
            n                                   # REQUEST_TYPE
            n                                   # TOPIC_LENGTH
            ",
            $request_length + 2+ 2 + bytes::length( $topic ) + 4,
            $request_type,
            bytes::length( $topic ),
            )
        .$topic
        .pack( "
            N                                   # PARTITION
            ",
            $partition,
            );

    if ( DEBUG )
    {
        print STDERR "Request header:\n"
            ."REQUEST_LENGTH    = ".($request_length + 2+ 2 + bytes::length( $topic ) + 4)."\n"
            ."REQUEST_TYPE      = $request_type\n"
            ."TOPIC_LENGTH      = ".bytes::length( $topic )."\n"
            ."TOPIC             = $topic\n"
            ."PARTITION         = $partition\n";
    }

    return $encoded;
}

# PRODUCE Request --------------------------------------------------------------

# LIMITATION: For all messages use the same properties:
#   magic:          Magic Value
#   compression:    (only for magic = MAGICVALUE_COMPRESSION)
# $messages may be one of:
#   simple SCALAR
#   reference to an array of scalars

sub produce_request {
    my $topic           = _STRING( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );
    my $partition       = shift;
    my $messages        = shift;

    return _error( ERROR_MISMATCH_ARGUMENT ) unless defined( _NONNEGINT( $partition ) );

    (
        _STRING( $messages ) or
        _ARRAY0( $messages )
    ) or return _error( ERROR_MISMATCH_ARGUMENT );

    $messages = [ $messages ] if ( !ref( $messages ) );

    my $encoded = _messages_encode( $messages );

    $encoded = pack( "
        N                                       # MESSAGES_LENGTH
        ",
        bytes::length( $encoded ),
        ).$encoded;

    if ( DEBUG )
    {
        print STDERR "Produce request:\n"
            ."MESSAGES_LENGTH    = ".bytes::length( $encoded )."\n";
    }

    $encoded = _request_header_encode(
            bytes::length( $encoded ),
            REQUESTTYPE_PRODUCE,
            $topic,
            $partition
            ).$encoded;

    return $encoded;
}

# MESSAGE ----------------------------------------------------------------------

sub _messages_encode {
    my $messages    = shift;
# for future versions
    my $magic       = shift || MAGICVALUE_NOCOMPRESSION;
    my $compression = shift || COMPRESSION_NO_COMPRESSION;

    my $encoded = "";
    foreach my $message ( @$messages )
    {
        return _error( ERROR_INVALID_MESSAGE_CODE ) if ref( $message );

        $encoded .=
            pack( "
                N                               # LENGTH
                C                               # MAGIC
                ",
                bytes::length( $message ) + 5 + ( $magic ? 1 : 0 ),
                $magic,
                )
            .( $magic ?
                pack( "
                    C                           # COMPRESSION
                    ",
                    $compression,
                    )
                : "" )
            .pack( "
                N                               # CHECKSUM
                ",
                crc32( $message ),
                )
            .$message;
    }

    if ( DEBUG )
    {
        my $tmp = $encoded;
        _messages_decode( \$tmp );
    }

    return $encoded;
}

sub _messages_decode {
    my $encoded_messages = shift;               # requires a reference

    my $decoded_messages = [];
    my $len = bytes::length( $$encoded_messages );

    # 10 = lenfth( LENGTH + MAGIC + CHECKSUM + ( COMPRESSION or 1 byte of PAYLOAD ) )
    while ( $len - $position >= 10 )
    {
# will unpack exception if the message structure disrupted
        my $message = {};

        (
            $message->{length},
        ) = unpack( "x${position}
            N                                    # LENGTH
            ", $$encoded_messages );
        $position += 4;

        last if ( ( $len - $position ) < $message->{length} );

        (
            $message->{magic},
        ) = unpack( "x${position}
            C                                   # MAGIC
            ", $$encoded_messages );
        $position += 1;

        if ( $message->{magic} )
        {
            (
                $message->{compression},
            ) = unpack( "x${position}
                C                               # COMPRESSION
                ", $$encoded_messages );
            $position += 1;
        }
        else
        {
            $message->{compression} = 0;
        }

        my $p_len = $message->{length} - 5 - ( $message->{magic} ? 1 : 0 );
        (
            $message->{checksum},
            $message->{payload},
        ) = unpack( "x${position}
            N                                   # CHECKSUM
            a${p_len}                           # PAYLOAD
            ", $$encoded_messages );
        $position += 4 + $p_len;

        $message->{error} = "";
        $message->{error} = $Kafka::ERROR[ERROR_CHECKSUM_ERROR] if $message->{checksum} != crc32( $message->{payload} );
# compression in the current version is a bug
        $message->{error} .= ( $message->{error} ? "\n" : "" ).$Kafka::ERROR[ERROR_COMPRESSED_PAYLOAD] if $message->{magic};
        $message->{valid} = !$message->{error};

        push @$decoded_messages, $message;
    }

    if ( DEBUG )
    {
        print STDERR "Messages:\n";
        for ( my $idx = 0; $idx <= $#{$decoded_messages} ; $idx++ )
        {
            my $message = $decoded_messages->[ $idx ];
            print STDERR
                 "index              = $idx\n"
                ."LENGTH             = $message->{length}\n"
                ."MAGIC              = $message->{magic}\n"
                .( $message->{magic} ? "COMPRESSION        = $message->{compression}\n" : "" )
                ."CHECKSUM           = $message->{checksum}\n"
                ."PAYLOAD            = $message->{payload}\n"
                ."valid              = $message->{valid}\n"
                ."error              = $message->{error}\n";
        }
    }

    return $decoded_messages;
}

# FETCH Request ----------------------------------------------------------------

sub fetch_request {
    my $topic           = _STRING( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );
    my $partition       = shift;
    my $offset          = shift;
    my $max_size        = _POSINT( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );

    return _error( ERROR_MISMATCH_ARGUMENT ) unless defined( _NONNEGINT( $partition ) );
    ( ref( $offset ) eq "Math::BigInt" and $offset >= 0 ) or defined( _NONNEGINT( $offset ) ) or return _error( ERROR_MISMATCH_ARGUMENT );

    my $encoded = ( BITS64 ? pack( "q>", $offset + 0 ) : Kafka::Int64::packq( $offset + 0 ) )   # OFFSET
        .pack( "
            N                                   # MAX_SIZE
            ",
            $max_size,
            );

    if ( DEBUG )
    {
        print STDERR "Fetch request:\n"
            ."OFFSET             = $offset\n"
            ."MAX_SIZE           = $max_size\n";
    }

    $encoded = _request_header_encode(
            bytes::length( $encoded ),
            REQUESTTYPE_FETCH,
            $topic,
            $partition
            ).$encoded;

    return $encoded;
}

# OFFSETS Request --------------------------------------------------------------

sub offsets_request {
    my $topic           = _STRING( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );
    my $partition       = shift;
    my $time            = shift;
    my $max_number      = _POSINT( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );

    return _error( ERROR_MISMATCH_ARGUMENT ) unless defined( _NONNEGINT( $partition ) );
    ( ref( $time ) eq "Math::BigInt" ) or defined( _NUMBER( $time ) ) or return _error( ERROR_MISMATCH_ARGUMENT );
    $time = int( $time );
    return _error( ERROR_MISMATCH_ARGUMENT ) if $time < -2;

    my $encoded = ( BITS64 ? pack( "q>", $time + 0 ) : Kafka::Int64::packq( $time + 0 ) )   # TIME
        .pack( "
            N                                   # MAX_NUMBER
            ",
            $max_number,
            );

    if ( DEBUG )
    {
        print STDERR "Offsets request:\n"
            ."TIME               = $time\n"
            ."MAX_NUMBER         = $max_number\n";
    }

    $encoded = _request_header_encode(
            bytes::length( $encoded ),
            REQUESTTYPE_OFFSETS,
            $topic,
            $partition
            ).$encoded;

    return $encoded;
}

################################################################################
# Responses Wire Format

# Response Header

sub _response_header_decode {
    my $response = shift;                       # requires a reference

    my $header = {};

    # 6 = length( RESPONSE_LENGTH + ERROR_CODE )
    if ( bytes::length( $$response ) >= 6 )
    {
# will unpack exception if the message structure disrupted
        $position = 0;
        (
            $header->{response_length},
            $header->{error_code},
        ) = unpack( "
            N                                       # RESPONSE_LENGTH
            n                                       # ERROR_CODE
            ", $$response );
        $position += 6;

        if ( DEBUG )
        {
            print STDERR "Response Header:\n"
                ."RESPONSE_LENGTH    = $header->{response_length}\n"
                ."ERROR_CODE         = $header->{error_code}\n";
        }
    }

    return $header;
}

# PRODUCE Response

#   None

# FETCH Response

sub fetch_response {
    my $response = _SCALAR( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );

    _STRING( $$response ) or return _error( ERROR_MISMATCH_ARGUMENT );
    # 6 = length( RESPONSE_LENGTH + ERROR_CODE )
    return _error( ERROR_MISMATCH_ARGUMENT ) if bytes::length( $$response ) < 6;

    my $decoded = {};

    if ( DEBUG )
    {
        print STDERR "Fetch response:\n";
    }

    if ( scalar keys %{$decoded->{header}      = _response_header_decode( $response )} )
    {
        $decoded->{messages}    = _messages_decode( $response ) unless $decoded->{header}->{error_code};
    }

    return $decoded;
}

# OFFSETS Response

sub offsets_response {
    my $response = _SCALAR( shift ) or return _error( ERROR_MISMATCH_ARGUMENT );

    _STRING( $$response ) or return _error( ERROR_MISMATCH_ARGUMENT );
    # 6 = length( RESPONSE_LENGTH + ERROR_CODE )
    return _error( ERROR_MISMATCH_ARGUMENT ) if bytes::length( $$response ) < 6;

    my $decoded = {};
    my $len = bytes::length( $$response );

    $decoded->{header} = _response_header_decode( $response );

    unless ( $decoded->{header}->{error_code} )
    {
        (
            $decoded->{number_offsets},
        ) = unpack( "
            x".$position
            ."N                                   # NUMBER_OFFSETS
            ", $$response );
        $position += 4;

        $decoded->{offsets} = [];
        while ( $position < $len )
        {
            my $offset;

            $offset = BITS64 ?                  # OFFSET
                unpack( "x${position}q>", $$response )
                : Kafka::Int64::unpackq( unpack( "x${position}a8", $$response ) );
            $position += 8;

            push @{$decoded->{offsets}}, $offset;
        }

        $decoded->{error} = ( $decoded->{number_offsets} == scalar( @{$decoded->{offsets}} ) ) ? "" : $Kafka::ERROR[ERROR_NUMBER_OF_OFFSETS];

        if ( DEBUG )
        {
            print STDERR "Offsets response:\n"
                ."NUMBER_OFFSETS     = $decoded->{number_offsets}\n"
                ."error              = $decoded->{error}\n";
            for ( my $idx = 0; $idx <= $#{$decoded->{offsets}} ; $idx++ )
            {
                print STDERR "OFFSET             = $decoded->{offsets}->[ $idx ]\n";
            }
        }
    }

    return $decoded;
}

################################################################################

1;

__END__

=head1 NAME

Kafka::Protocol - functions to process messages in the
Apache Kafka's Wire Format

=head1 VERSION

This documentation refers to C<Kafka::Consumer> version 0.07

=head1 SYNOPSIS

Setting up:

    #-- Export
    use Kafka::Protocol qw(
        DEFAULT_MAX_SIZE
        REQUESTTYPE_PRODUCE
        REQUESTTYPE_FETCH
        REQUESTTYPE_MULTIFETCH
        REQUESTTYPE_MULTIPRODUCE
        REQUESTTYPE_OFFSETS
        produce_request
        fetch_request
        offsets_request
        fetch_response
        offsets_response
        );
    
    print "REQUEST_TYPE(s):\n";
    print
        REQUESTTYPE_PRODUCE,        " ",
        REQUESTTYPE_FETCH           " ",
        REQUESTTYPE_MULTIFETCH      " ",
        REQUESTTYPE_MULTIPRODUCE    " ",
        REQUESTTYPE_OFFSETS         "\n";
    
    #-- declaration of variables to test
    my $topic       = "test";
    my $partition   = 0;
    my $single_message = "The first message";
    my $series_of_messages = [
        "The first message",
        "The second message",
        "The third message",
        ];
    my $offset      = 0;
    my $max_size    = DEFAULT_MAX_SIZE;
    my $time        = -2;
    my $max_number  = 100;
    my ( $str, $hsh_ref, $arr_ref );

Requests:

    #-- Producer request:
    $str = unpack( "H*",
        produce_request( $topic, $partition, $single_message );
    $str = unpack( "H*",
        produce_request( $topic, $partition, $series_of_messages );
    
    #-- Offsets request:
    $str = unpack( "H*",
        offsets_request( $topic, $partition, $time, $max_number );
    
    #-- Fetch request:
    $str = unpack( "H*",
        fetch_request( $topic, $partition, $offset, $max_size );

Responses (look at the L<Sample Data|Kafka::Mock/"Sample Data"> section of
the L<Kafka::Mock|Kafka::Mock> module for a C<%responses> example):

    #-- Offsets response
    $arr_ref = offsets_response( \$responses{4} );
    
    #-- Fetch response
    $hsh_ref = fetch_response( \$responses{1} );

An error:

    eval { fetch_response( [] ) };  # expecting to die
                                    # 'Mismatch argument'
    print STDERR
            "(", Kafka::Protocol::last_error(), ") ",
            $Kafka::Protocol::last_error(), "\n";

=head1 DESCRIPTION

When producing messages, the driver has to specify what topic and partition
to send the message to. When requesting messages, the driver has to specify
what topic, partition, and offset it wants them pulled from.

While you can request "old" messages if you know their topic, partition, and
offset, Kafka does not have a message index. You cannot efficiently query
Kafka for the N-1000th message, or ask for all messages written between
30 and 35 minutes ago.

The main features of the C<Kafka::Protocol> module are:

=over 3

=item *

Supports parsing the Apache Kafka Wire Format protocol.

=item *

Supports Apache Kafka Requests and Responses (PRODUCE and FETCH with
no compression codec attribute now). Within this package we currently support
access to PRODUCE Request, FETCH Request, OFFSETS Request, FETCH Response,
OFFSETS Response.

=item *

Support for working with 64 bit elements of the Kafka Wire Format protocol
on 32 bit systems.

=back

=head2 FUNCTIONS

The following functions are available for C<Kafka::Protocol> module.

=over 3

=item *

B<offset>, B<max_size> or B<time>, B<max_number> are the additional information
that might be encoded parameters of the messages we want to access.

=back

=head3 C<produce_request( $topic, $partition, $messages )>

Returns a binary PRODUCE request string coded according to
the Apache Kafka Wire Format protocol, or error will cause the program to
halt (C<confess>) if the argument is not valid.

C<produce_request()> takes arguments. The following arguments are currently
recognized:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer (of any length).
That is, a positive integer, or zero.

=item C<$messages>

The C<$messages> is an arbitrary amount of data (a simple data string or
a reference to an array of the data strings).

=back

=head3 C<fetch_request( $topic, $partition, $offset, $max_size )>

Returns a binary FETCH request string coded according to
the Apache Kafka Wire Format protocol, or error will cause the program to
halt (C<confess>) if the argument is not valid.

C<fetch_request()> takes arguments. The following arguments are currently
recognized:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer (of any length).
That is, a positive integer, or zero.

=item C<$offset>

Offset in topic and partition to start from (64 bits).

The argument must be a non-negative integer (of any length).
That is, a positive integer, or zero. The argument may be a
L<Math::BigInt|Math::BigInt> integer on 32 bit system.

=item C<$max_size>

C<$max_number> is the maximum size of the message set to return. The argument
must be a positive integer (of any length).

=back

=head3 C<offsets_request( $topic, $partition, $time, $max_number )>

Returns a binary OFFSETS request string coded according to
the Apache Kafka Wire Format protocol, or error will cause the program to
halt (C<confess>) if the argument is not valid.

C<offsets_request()> takes arguments. The following arguments are currently
recognized:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer (of any length).
That is, a positive integer, or zero.

=item C<$time>

C<$time> is the timestamp of the offsets before this time - milliseconds since
UNIX Epoch.

The argument must be a positive number. That is, it is defined and Perl thinks
it's a number. The argument may be a L<Math::BigInt|Math::BigInt> integer on 32
bit system.

The special values -1 (latest), -2 (earliest) are allowed.

=item C<$max_number>

C<$max_number> is the maximum number of offsets to retrieve. The argument must
be a positive integer (of any length).

=back

=head3 C<offsets_response( $response )>

Decodes the argument and returns a reference to the hash representing
the structure of
the OFFSETS Response. Offsets are L<Math::BigInt|Math::BigInt> integers
on 32 bit system. Hash additionally comprises a pair of items C<{error}>
describing the possible error at line structure of the argument (now only
"Amount received offsets does not match 'NUMBER of OFFSETS'" possible). Error
will cause the program to halt (C<confess>) if the argument is not valid.

C<offsets_response()> takes arguments. The following arguments are currently
recognized:

=over 3

=item C<$response>

C<$response> is a reference to the OFFSETS Response buffer. The buffer
must be a non-empty string 6+ bytes long.

=back

=head3 C<fetch_response( $response )>

Decodes the argument and returns a reference to the hash representing
the structure of the FETCH Response. Error will cause the program to halt
(C<confess>) if the argument is not valid.

C<fetch_response()> takes arguments. The following arguments are currently
recognized:

=over 3

=item C<$response>

C<$response> is a reference to the FETCH Response buffer.
The buffer must be a non-empty string 6+ bytes long.

=back

=head3 C<last_errorcode>

This method returns an error code that specifies the position of the
description in the C<@Kafka::ERROR> array.  Analysing this information
can be done to determine the cause of the error.

The server or the resource might not be available, access to the resource
might be denied or other things might have failed for some reason.

=head3 C<last_error>

This method returns an error message that contains information about the
encountered failure.  Messages returned from this method may contain
additional details and do not comply with the C<Kafka::ERROR> array.

=head2 EXPORT

None by default.

It has an additional constants available for import, which can be used
to define the module functions, and to identify REQUEST types
(look at L</"SEE ALSO"> section):

=over 3

=item

0 - C<REQUESTTYPE_PRODUCE>

=item

1 - C<REQUESTTYPE_FETCH>

=item

2 - C<REQUESTTYPE_MULTIFETCH>

=item

3 - C<REQUESTTYPE_MULTIPRODUCE>

=item

4 - C<REQUESTTYPE_OFFSETS>

=back

=head1 DIAGNOSTICS

C<Kafka::Protocol> is not a user module and any L<functions|/FUNCTIONS> error
is FATAL.
FATAL errors will cause the program to halt (C<confess>), since the
problem is so severe that it would be dangerous to continue. (This can
always be trapped with C<eval>. Under the circumstances, dying is the best
thing to do).

=over 3

=item C<Mismatch argument>

This means that you didn't give the right argument to some of
L<functions|/FUNCTIONS>.

=item C<Invalid message>

This means that the array of messages contain a reference instead a simple data
string.

=back

For more error description, always look at the message from the L</last_error>
from the C<Kafka::Protocol::last_error> function.

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

Copyright (C) 2012 by TrackingSoft LLC.
All rights reserved.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut