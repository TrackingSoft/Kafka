package Kafka::IO::Async;

=head1 NAME

Kafka::IO::Async - Pseudo async interface to nonblocking network communication with the Apache Kafka server with Coro.
This module implemets the same interface that usual Kafka::IO module

=head1 VERSION

Read documentation for C<Kafka::IO> version 1.07 .

=cut



use 5.010;
use strict;
use warnings;



our $DEBUG = 0;

our $VERSION = 'v1.700.9';

use Carp;
use Config;
use Const::Fast;
use Fcntl;
use Params::Util qw(
    _STRING
);
use Scalar::Util qw(
    dualvar
);
use Try::Tiny;

use Kafka qw(
    $ERROR_CANNOT_BIND
    $ERROR_CANNOT_RECV
    $ERROR_CANNOT_SEND
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_INCOMPATIBLE_HOST_IP_VERSION
    $ERROR_NO_CONNECTION
    $IP_V4
    $IP_V6
    $KAFKA_SERVER_PORT
    $REQUEST_TIMEOUT
);
use Kafka::Exceptions;
use Kafka::Internals qw(
    $MAX_SOCKET_REQUEST_BYTES
    debug_level
    format_message
);

use AnyEvent::Handle;



=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka::IO::Async;

    my $io;
    try {
        $io = Kafka::IO::Async->new( host => 'localhost' );
    } catch {
        my $error = $_;
        if ( blessed( $error ) && $error->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $error->code, ') ',  $error->message, "\n";
            exit;
        } else {
            die $error;
        }
    };

    # Closes and cleans up
    $io->close;
    undef $io;

=head1 DESCRIPTION

This module is private and should not be used directly.

In order to achieve better performance, methods of this module do not
perform arguments validation.

The main features of the C<Kafka::IO::Async> class are:

=over 3

=item *

Provides an object oriented API for communication with Kafka.

=item *

This class allows you to create Kafka 0.9+ clients.

=back

=cut

our $_hdr;

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Establishes TCP connection to given host and port, creates and returns C<Kafka::IO::Async> IO object.

C<new()> takes arguments in key-value pairs. The following arguments are currently recognized:

=over 3

=item C<host =E<gt> $host>

C<$host> is Kafka host to connect to. It can be a host name or an IP-address in
IPv4 or IPv6 form (for example '127.0.0.1', '0:0:0:0:0:0:0:1' or '::1').

=item C<port =E<gt> $port>

Optional, default = C<$KAFKA_SERVER_PORT>.

C<$port> is integer attribute denoting the port number of to access Apache Kafka.

C<$KAFKA_SERVER_PORT> is the default Apache Kafka server port that can be imported
from the L<Kafka|Kafka> module.

=item C<timeout =E<gt> $timeout>

C<$REQUEST_TIMEOUT> is the default timeout that can be imported from the L<Kafka|Kafka> module.

Special behavior when C<timeout> is set to C<undef>:

=back

=over 3

=item *

Alarms are not used internally (namely when performing C<gethostbyname>).

=item *

Default C<$REQUEST_TIMEOUT> is used for the rest of IO operations.

=back

=over 3

=item C<ip_version =E<gt> $ip_version>

Force version of IP protocol for resolving host name (or interpretation of passed address).

Optional, undefined by default, which works in the following way: version of IP address
is detected automatically, host name is resolved into IPv4 address.

See description of L<$IP_V4|Kafka::IO/$IP_V4>, L<$IP_V6|Kafka::IO/$IP_V6>
in C<Kafka> L<EXPORT|Kafka/EXPORT>.

=back

=cut
sub new {
    my ( $class, %p ) = @_;

    my $self = bless {
        host        => '',
        timeout     => $REQUEST_TIMEOUT,
        port        => $KAFKA_SERVER_PORT,
        ip_version  => undef,
        af          => '',  # Address family constant
        pf          => '',  # Protocol family constant
        ip          => '',  # Human-readable textual representation of the ip address
    }, $class;

    exists $p{$_} and $self->{$_} = $p{$_} foreach keys %$self;

    # we trust it: make it untainted
    ( $self->{host} ) = $self->{host} =~ /\A(.+)\z/;
    ( $self->{port} ) = $self->{port} =~ /\A(.+)\z/;

    $self->{socket}     = undef;
    $self->{_io_select} = undef;
    my $error;
    try {
        $self->_connect();
    } catch {
        $error = $_;
    };

    $self->_error( $ERROR_CANNOT_BIND, format_message("Kafka::IO::Async(%s:%s)->new: %s", $self->{host}, $self->{port}, $error ) )
        if defined $error
    ;
    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are provided by C<Kafka::IO::Async> class:

=cut

=head3 C<< send( $message <, $timeout> ) >>

Sends a C<$message> to Kafka.

The argument must be a bytes string.

Use optional C<$timeout> argument to override default timeout for this request only.

Returns the number of characters sent.

=cut
sub send {
    my ( $self, $message, $timeout ) = @_;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->send' )
        unless defined( _STRING( $message ) )
    ;
    my $length = length( $message );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->send' )
        unless $length <= $MAX_SOCKET_REQUEST_BYTES
    ;
    $timeout = $self->{timeout} // $REQUEST_TIMEOUT unless defined $timeout;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->send' )
        unless $timeout > 0
    ;

    my $socket = $self->{socket};
    $self->_error( $ERROR_NO_CONNECTION, 'Attempt to work with a closed socket' ) if !$socket || $socket->destroyed;

    $socket->wtimeout_reset;
    $socket->wtimeout($timeout);
    $socket->on_wtimeout(sub {
        #my ($h) = @_;
        $self->close;
        $self->_error(
            $ERROR_CANNOT_SEND,
            format_message( "Kafka::IO::Async(%s)->send: ERROR='%s' (length=%s, timeout=%s)",
                $self->{host},
                'Write timeout fired',
                $length,
                $timeout,
            )
        );
    });

    $socket->push_write($message);

    return length($message);
}

=head3 C<< receive( $length <, $timeout> ) >>

Receives a message up to C<$length> size from Kafka.

C<$length> argument must be a positive number.

Use optional C<$timeout> argument to override default timeout for this call only.

Returns a reference to the received message.

=cut
sub receive {
    my ( $self, $length, $timeout ) = @_;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->receive' )
        unless $length > 0
    ;
    $timeout = $self->{timeout} // $REQUEST_TIMEOUT unless defined $timeout;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->receive' )
        unless $timeout > 0
    ;
    my $socket = $self->{socket};
    $self->_error( $ERROR_NO_CONNECTION, 'Attempt to work with a closed socket' ) if !$socket || $socket->destroyed;

    my $message = '';
    my $error;
    my $cv = AnyEvent->condvar;

    $socket->rtimeout_reset;
    $socket->rtimeout($timeout);
    $socket->on_rtimeout(sub {
        #my ($h) = @_;
        $self->close;
        $error = format_message( "Kafka::IO::Async(%s)->receive: ERROR='%s' (timeout=%s)",
            $self->{host},
            'Read timeout fired',
            $timeout,
        );
        $cv->send;
    });

    $socket->on_error(sub {
        my ($h, $fatal, $message) = @_;
        $self->close if $fatal;
        $error = format_message( "Kafka::IO::Async(%s)->handle: ERROR='%s' FATAL=%s",
            $self->{host},
            $message,
            $fatal,
        );
        $cv->send;
    });

    $socket->push_read(chunk => $length, sub {
        my ($h, $data) = @_;
        $message = $data;
        $cv->send;
    });

    $cv->recv;
    $socket->rtimeout(0);
    die $error if $error;

    return \$message;
}

=head3 C<< try_receive( $length <, $timeout> ) >>
Receives a message up to C<$length> size from Kafka.

C<$length> argument must be a positive number.

Use optional C<$timeout> argument to override default timeout for this call only.

Returns a reference to the received message.

=cut

sub try_receive {
    my ( $self, $length, $timeout ) = @_;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->receive' )
        unless $length > 0
    ;
    $timeout = $self->{timeout} // $REQUEST_TIMEOUT unless defined $timeout;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->receive' )
        unless $timeout > 0
    ;
    my $socket = $self->{socket};
    $self->_error( $ERROR_NO_CONNECTION, 'Attempt to work with a closed socket' ) if !$socket || $socket->destroyed;

    my $message = '';
    my $error;
    my $cv = AnyEvent->condvar;

    $socket->rtimeout($timeout);
    $socket->on_rtimeout(sub {
        #my ($h) = @_;
        $self->close;
        $error = format_message( "Kafka::IO::Async(%s)->receive: ERROR='%s' (timeout=%s)",
            $self->{host},
            'Read timeout fired',
            $timeout,
        );
        $cv->send;
    });

    $socket->on_eof(sub {
        $message = undef;
        $cv->send;
    });

    $socket->on_read(sub {
        #my ($h, $data) = @_;
        my ($h) = @_;
        $message = substr $h->{rbuf}, 0, $length, '';
        $h->on_read();
        $h->on_eof();
        $cv->send;
    });

    $cv->recv;
    $socket->rtimeout(0);
    die $error if $error;

    return \$message;
}

=head3 C<close>

Closes connection to Kafka server.
Returns true if those operations succeed and if no error was reported by any PerlIO layer.

=cut
sub close {
    my ( $self ) = @_;

    my $ret = 1;
    if ( $self->{socket} ) {
        $self->{socket}->destroy;
        $self->{socket} = undef;
    }

    return $ret;
}


#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

# You need to have access to Kafka instance and be able to connect through TCP.
# uses http://devpit.org/wiki/Connect%28%29_with_timeout_%28in_Perl%29
sub _connect {
    my ( $self ) = @_;

    my $error;
    $self->{socket}     = undef;

    my $name    = $self->{host};
    my $port    = $self->{port};
    my $timeout = $self->{timeout};

    my $cv = AnyEvent->condvar;

    my $connection = AnyEvent::Handle->new(
        connect => [$name, $port],
        ($timeout ? (on_prepare => sub { $timeout } ) : ()),
        on_connect => sub {
            #my ($h, $host, $port, $retry) = @_;
            $cv->send;
        },
        on_connect_error => sub {
            my ($h, $message) = @_;
            $error = format_message( "connect host = %s, port = %s: %s\n", $name, $port, $message );
            $cv->send;
        },
        on_error => sub {
            my ($h, $fatal, $message) = @_;
            $self->close if $fatal;
            $self->_error(
                $ERROR_NO_CONNECTION,
                format_message( "Kafka::IO::Async(%s)->handle: ERROR='%s' FATAL=%s",
                    $self->{host},
                    $message,
                    $fatal,
                )
            );
        },
        on_drain => sub {
            my ($h) = @_;
            $h->wtimeout(0);
        }
    );

    $cv->recv;

    die $error if $error;

    $self->{socket} = $connection;
    return $connection;
}


# Show additional debugging information
sub _debug_msg {
    my ( $self, $message, $header, $colour ) = @_;

    if ( $header ) {
        unless ( $_hdr ) {
            require Data::HexDump::Range;
            $_hdr = Data::HexDump::Range->new(
                FORMAT                          => 'ANSI',  # 'ANSI'|'ASCII'|'HTML'
                COLOR                           => 'bw',    # 'bw' | 'cycle'
                OFFSET_FORMAT                   => 'hex',   # 'hex' | 'dec'
                DATA_WIDTH                      => 16,      # 16 | 20 | ...
                DISPLAY_RANGE_NAME              => 0,
#                MAXIMUM_RANGE_NAME_SIZE         => 16,
                DISPLAY_COLUMN_NAMES            => 1,
                DISPLAY_RULER                   => 1,
                DISPLAY_OFFSET                  => 1,
#                DISPLAY_CUMULATIVE_OFFSET       => 1,
                DISPLAY_ZERO_SIZE_RANGE_WARNING => 0,
                DISPLAY_ZERO_SIZE_RANGE         => 1,
                DISPLAY_RANGE_NAME              => 0,
#                DISPLAY_RANGE_SIZE              => 1,
                DISPLAY_ASCII_DUMP              => 1,
                DISPLAY_HEX_DUMP                => 1,
#                DISPLAY_DEC_DUMP                => 1,
#                COLOR_NAMES                     => {},
                ORIENTATION                     => 'horizontal',
            );
        }

        say STDERR
            "# $header ", $self->{host}, ':', $self->{port}, "\n",
            '# Hex Stream: ', unpack( 'H*', $message ), "\n",
            $_hdr->dump(
                [
                    [ 'data', length( $message ), $colour ],
                ],
                $message
            )
        ;
    } else {
        say STDERR format_message( '[%s] %s', scalar( localtime ), $message );
    }

    return;
}

# Handler for errors
sub _error {
    my $self = shift;
    my %args = throw_args( @_ );
    $self->_debug_msg( format_message( 'throwing IO error %s: %s', $args{code}, $args{message} ) )
        if $self->debug_level;
    Kafka::Exception::IO->throw( %args );
}



1;

__END__

=head1 DIAGNOSTICS

When error is detected, an exception, represented by object of C<Kafka::Exception::IO> class,
is thrown (see L<Kafka::Exceptions|Kafka::Exceptions>).

L<code|Kafka::Exceptions/code> and a more descriptive L<message|Kafka::Exceptions/message> provide
information about thrown exception. Consult documentation of the L<Kafka::Exceptions|Kafka::Exceptions>
for the list of all available methods.

Authors suggest using of L<Try::Tiny|Try::Tiny>'s C<try> and C<catch> to handle exceptions while
working with L<Kafka|Kafka> package.

Here is the list of possible error messages that C<Kafka::IO::Async> may produce:

=over 3

=item C<Invalid argument>

Invalid arguments were passed to a method.

=item C<Cannot send>

Message cannot be sent on a C<Kafka::IO::Async> object socket.

=item C<Cannot receive>

Message cannot be received.

=item C<Cannot bind>

TCP connection cannot be established on given host and port.

=back

=head2 Debug mode

Debug output can be enabled by passing desired level via environment variable
using one of the following ways:

C<PERL_KAFKA_DEBUG=1>     - debug is enabled for the whole L<Kafka|Kafka> package.

C<PERL_KAFKA_DEBUG=IO:1>  - enable debug for C<Kafka::IO::Async> only.

C<Kafka::IO::Async> supports two debug levels (level 2 includes debug output of 1):

=over 3

=item 1

Additional information about processing events/alarms.

=item 2

Dump of binary messages exchange with Kafka server.

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

L<Kafka::IO::Async|Kafka::IO::Async> - low-level interface for communication with Kafka server.

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

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2017 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
