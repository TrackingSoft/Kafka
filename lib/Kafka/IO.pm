package Kafka::IO;

=head1 NAME

Kafka::IO - Interface to network communication with the Apache Kafka server.

=head1 VERSION

This documentation refers to C<Kafka::IO> version 0.8013 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $DEBUG = 0;

our $VERSION = '0.8013';

#-- load the modules -----------------------------------------------------------

use Carp;
use Config;
use Data::Validate::Domain qw(
    is_hostname
);
use Data::Validate::IP qw(
    is_ipv4
    is_ipv6
);
use Errno;
use Fcntl;
use POSIX qw(
    ceil
);
use Scalar::Util qw(
    dualvar
);
use Socket qw(
    AF_INET
    AF_INET6
    IPPROTO_TCP
    PF_INET
    PF_INET6
    SOCK_STREAM
    SOL_SOCKET
    SO_ERROR
    SO_RCVTIMEO
    SO_SNDTIMEO
    inet_aton
    inet_pton
    inet_ntop
    pack_sockaddr_in
    pack_sockaddr_in6
);
use Sys::SigAction qw(
    set_sig_handler
);
use Try::Tiny;

use Kafka qw(
    %ERROR
    $ERROR_CANNOT_BIND
    $ERROR_CANNOT_RECV
    $ERROR_CANNOT_SEND
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NOT_BINARY_STRING
    $KAFKA_SERVER_PORT
    $REQUEST_TIMEOUT
);
use Kafka::Exceptions;
use Kafka::Internals qw(
    $MAX_SOCKET_REQUEST_BYTES
    debug_level
);

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka::IO;

    my $io;
    try {
        $io = Kafka::IO->new( host => 'localhost' );
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

The main features of the C<Kafka::IO> class are:

=over 3

=item *

Provides an object oriented API for communication with Kafka.

=item *

This class allows you to create Kafka 0.8 clients.

=back

=cut

our $_hdr;

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Establishes TCP connection to given host and port, creates and returns C<Kafka::IO> IO object.

C<new()> takes arguments in key-value pairs. The following arguments are currently recognized:

=over 3

=item C<host =E<gt> $host>

C<$host> is an Apache Kafka host to connect to. It can be a hostname or the
IP-address in the IPv4 or IPv6 form (for example '127.0.0.1', '0:0:0:0:0:0:0:1' or '::1').

=item C<port =E<gt> $port>

Optional, default = C<$KAFKA_SERVER_PORT>.

C<$port> is integer attribute denoting the port number of to access Apache Kafka.

C<$KAFKA_SERVER_PORT> is the default Apache Kafka server port that can be imported
from the L<Kafka|Kafka> module.

=item C<timeout =E<gt> $timeout>

Optional, default = C<$REQUEST_TIMEOUT>.

C<$timeout> specifies how long we wait for remote server to respond before
the IO object disconnects and throws internal exception.
The C<$timeout> is specified in seconds (could be any integer or floating-point type)
and supported by C<gethostbyname()>, connect, blocking receive and send calls.

C<$REQUEST_TIMEOUT> is the default timeout that can be imported from the L<Kafka|Kafka> module.

Special behavior when C<timeout> is set to C<undef>:

=back

=over 3

=item *

Alarms are not used internally (namely when performing C<gethostbyname>).

=item *

Default C<$REQUEST_TIMEOUT> is used for the rest of IO operations.

=back

=cut
sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        host        => q{},
        timeout     => $REQUEST_TIMEOUT,
        port        => $KAFKA_SERVER_PORT,
        af          => '',  # Address family constant
        pf          => '',  # Protocol family constant
        ip          => '',  # Human-readable textual representation of the ip address
    }, $class;

    while ( @args ) {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    # we trust it: make it untainted
    ( $self->{host} ) = $self->{host} =~ /\A(.+)\z/;
    ( $self->{port} ) = $self->{port} =~ /\A(.+)\z/;

    $self->{not_accepted} = 0;
    $self->{socket} = undef;
    try {
        $self->_connect();
    } catch {
        my $error = $_;
        $self->_error( $ERROR_CANNOT_BIND, "->new - $error" );
    };

    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are provided by C<Kafka::IO> class:

=cut

=head3 C<send( $message )>

Sends a C<$message> to Kafka.

The argument must be a bytes string.

Returns the number of characters sent.

=cut
sub send {
    my ( $self, $message ) = @_;

    ( my $len = length( $message ) ) <= $MAX_SOCKET_REQUEST_BYTES
        or $self->_error( $ERROR_MISMATCH_ARGUMENT, '->send' );

    $self->_debug_msg( $message, 'Request to', 'green' )
        if $self->debug_level >= 2;

    # accept not accepted earlier
    while ( select( my $mask = $self->{_select}, undef, undef, 0 ) ) {
        my $received = $self->receive( $self->{not_accepted} || 1 );
        return unless ( $received && defined( $$received ) );
        $self->{not_accepted} = 0;
    }
    $self->{not_accepted} = 0;

    my ( $sent, $mask );
    {
        last unless ( select( undef, $mask = $self->{_select}, undef, $self->{timeout} // $REQUEST_TIMEOUT ) );
        $sent += send( $self->{socket}, $message, 0 ) // 0;
        redo if $sent < $len;
    }

    ( defined( $sent ) && $sent == $len )
        or $self->_error( $ERROR_CANNOT_SEND, "->send - $!" );

    return $sent;
}

=head3 C<receive( $length )>

Receives a message up to C<$length> size from Kafka.

C<$length> argument must be a positive number.

Returns a reference to the received message.

=cut
sub receive {
    my ( $self, $length ) = @_;

    my ( $from_recv, $message, $buf, $mask );
    $message = q{};
    {
        last unless ( select( $mask = $self->{_select}, undef, undef, $self->{timeout} // $REQUEST_TIMEOUT ) );
        $from_recv = recv( $self->{socket}, $buf = q{}, $length, 0 );
        last if !defined( $from_recv ) || $buf eq q{};
        $message .= $buf;
        redo if length( $message ) < $length;
    }
    $self->{not_accepted} = ( $length - length( $message ) ) * ( $self->{not_accepted} >= 0 );

    ( defined( $from_recv ) && !$self->{not_accepted} )
        or $self->_error( $ERROR_CANNOT_RECV, "->receive - $!" );

    $self->_debug_msg( $message, 'Response from', 'yellow' )
        if $self->debug_level >= 2;

    # returns tainted data
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
        $self->{_select} = undef;
        $ret = CORE::close( $self->{socket} );
        $self->{socket} = undef;
    }

    return $ret;
}

=head3 C<is_alive>

The method verifies whether we are connected to Kafka server.

=cut
sub is_alive {
    my ( $self ) = @_;

    my $socket = $self->{socket};
    return unless $socket;

    socket( my $tmp_socket, $self->{pf}, SOCK_STREAM, IPPROTO_TCP );
    my $is_alive = connect( $tmp_socket, getpeername( $socket ) );
    CORE::close( $tmp_socket );

    return $is_alive;
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

# You need to have access to Kafka instance and be able to connect through TCP.
# uses http://devpit.org/wiki/Connect%28%29_with_timeout_%28in_Perl%29
sub _connect {
    my ( $self ) = @_;

    $self->{socket} = undef;

    my $name    = $self->{host};
    my $port    = $self->{port};
    my $timeout = $self->{timeout};

    my $ip = '';
    if ( is_ipv4( $name ) || is_ipv6( $name ) ) {
        $self->_get_family( $name );
        $ip = $self->{ip} = $name;
    } else {
        if ( defined $timeout ) {
            my $remaining;
            my $start = time();

            $self->_debug_msg( "name = '$name', number of wallclock seconds = ".ceil( $timeout ) )
                if $self->debug_level;

            # DNS lookup.
            local $@;
            my $h = set_sig_handler( 'ALRM', sub { die 'alarm clock restarted' },
                {
                    mask    => [ 'ALRM' ],
                    safe    => 0,   # perl 5.8+ uses safe signal delivery so we need unsafe signal for timeout to work
                }
            );
            eval {
                $remaining = alarm( ceil( $timeout ) );
                $ip = $self->_gethostbyname( $name );
                alarm 0;
            };
            alarm 0;                                # race condition protection
            my $error = $@;
            undef $h;

            $self->_debug_msg( "_connect: ip = '".( $ip || '<undef>' ).", error = '$error', \$? = $?, \$! = '$!'" )
                if $self->debug_level;

            die $error if $error;
            die( "gethostbyname $name: \$? = '$?', \$! = '$!'\n" ) unless $ip;

            my $elapsed = time() - $start;
            # $SIG{ALRM} restored automatically, but we need to restart previous alarm manually

            $self->_debug_msg( "_connect: ".( $remaining // '<undef>' )." (remaining) - $elapsed (elapsed) = ".( $remaining - $elapsed ) )
                if $self->debug_level;
            if ( $remaining ) {
                if ( $remaining - $elapsed > 0 ) {
                    $self->_debug_msg( '_connect: remaining - elapsed > 0 (to alarm restart)' )
                        if $self->debug_level;
                    alarm( ceil( $remaining - $elapsed ) );
                } else {
                    $self->_debug_msg( '_connect: remaining - elapsed < 0 (to alarm function call)' )
                        if $self->debug_level;
                    # $SIG{ALRM}->();
                    kill ALRM => $$;
                }
                $self->_debug_msg( "_connect: after alarm 'recalled'" )
                    if $self->debug_level;
            }
        } else {
            $ip = $self->_gethostbyname( $name );
        }
    }

    # Create socket.
    socket( my $connection, $self->{pf}, SOCK_STREAM, getprotobyname( 'tcp' ) ) or die( "socket: $!\n" );

    # Set autoflushing.
    $_ = select( $connection ); $| = 1; select $_;

    # Set FD_CLOEXEC.
    my $flags = fcntl( $connection, F_GETFL, 0 ) or die "fcntl: $!\n";
    fcntl( $connection, F_SETFL, $flags | FD_CLOEXEC ) or die "fnctl: $!\n";

    $flags = fcntl( $connection, F_GETFL, 0 ) or die "fcntl F_GETFL: $!\n"; # 0 for error, 0e0 for 0.
    fcntl( $connection, F_SETFL, $flags | O_NONBLOCK ) or die "fcntl F_SETFL O_NONBLOCK: $!\n"; # 0 for error, 0e0 for 0.

    # Connect returns immediately because of O_NONBLOCK.
    my $sockaddr = $self->{af} eq AF_INET
#        ? pack_sockaddr_in(  $port, inet_pton( $self->{af}, $ip ) )
        ? pack_sockaddr_in(  $port, inet_aton( $ip ) )
        : pack_sockaddr_in6( $port, inet_pton( $self->{af}, $ip ) )
    ;
    connect( $connection, $sockaddr ) || $!{EINPROGRESS} || die( "connect ip = $ip, port = $port: $!\n" );

    $self->{socket}     = $connection;
    $self->{_select}    = undef;

    # Reset O_NONBLOCK.
    $flags = fcntl( $connection, F_GETFL, 0 ) or die "fcntl F_GETFL: $!\n";  # 0 for error, 0e0 for 0.
    fcntl( $connection, F_SETFL, $flags & ~ O_NONBLOCK ) or die "fcntl F_SETFL not O_NONBLOCK: $!\n";  # 0 for error, 0e0 for 0.

    # Use select() to poll for completion or error. When connect succeeds we can write.
    my $vec = q{};
    vec( $vec, fileno( $connection ), 1 ) = 1;
    select( undef, $vec, undef, $timeout // $REQUEST_TIMEOUT );
    unless ( vec( $vec, fileno( $connection ), 1 ) ) {
        # If no response yet, impose our own timeout.
        $! = Errno::ETIMEDOUT();
        die( "connect ip = $ip, port = $port: $!\n" );
    }

    # This is how we see whether it connected or there was an error. Document Unix, are you kidding?!
    $! = unpack( q{L}, getsockopt( $connection, SOL_SOCKET, SO_ERROR ) );
    die( "connect ip = $ip, port = $port: $!\n" ) if $!;

    # Set timeout on all reads and writes.
    #
    # Note the difference between Perl's sysread() and read() calls: sysread()
    # queries the kernel exactly once, with max delay specified here. read()
    # queries the kernel repeatedly until there's a read error (such as this
    # timeout), EOF, or a full buffer. So when using read() with a timeout of one
    # second, if the remote server sends 1 byte repeatedly at 1 second intervals,
    # read() will read the whole buffer very slowly and sysread() will return only
    # the first byte. The print() and syswrite() calls are similarly different.
    # <> is of course similar to read() but delimited by newlines instead of buffer
    # sizes.
    my $timeval = _get_timeval( $timeout // $REQUEST_TIMEOUT );
    setsockopt( $connection, SOL_SOCKET, SO_SNDTIMEO, $timeval ) // die "setsockopt SOL_SOCKET, SO_SNDTIMEO: $!\n";
    setsockopt( $connection, SOL_SOCKET, SO_RCVTIMEO, $timeval ) // die "setsockopt SOL_SOCKET, SO_RCVTIMEO: $!\n";

    vec( $self->{_select} = q{}, fileno( $self->{socket} ), 1 ) = 1;

    return $connection;
}

# Packing timeval
# uses http://trinitum.org/wp/packing-timeval/
sub _get_timeval {
    my $timeout = shift;

    my $intval = int( $timeout );                               # sec
    my $fraction = int( ( $timeout - $intval ) * 1_000_000 );   # ms

    if ( $Config{osname} eq 'netbsd' && _major_osvers() >= 6 && $Config{longsize} == 4 ) {
        if ( defined $Config{use64bitint} ) {
            $timeout = pack( 'QL', int( $timeout ), $fraction );
        } else {
            $timeout = pack(
                'LLL',
                (
                    $Config{byteorder} eq '1234'
                        ? ( $timeout, 0, $fraction )
                        : ( 0, $timeout, $fraction )
                )
            );
        }
    } else {
        $timeout = pack( 'L!L!', $timeout, $fraction );
    }

    return $timeout;
}

sub _major_osvers {
    my $osvers = $Config{osvers};
    my ( $major_osvers ) = $osvers =~ /^(\d+)/;
    $major_osvers += 0;

    return $major_osvers;
}

sub _gethostbyname {
    my ( $self, $name ) = @_;

    $self->_get_family( $name );
    my $ipaddr = gethostbyname( $name );
    $self->{ip} = $ipaddr ? inet_ntop( $self->{af}, $ipaddr ) : '';

    return $self->{ip};
}

sub _get_family {
    my ( $self, $name ) = @_;

    if ( is_ipv6( $name ) ) {
        $self->{af} = AF_INET6;
        $self->{pf} = PF_INET6;
    } else {
        $self->{af} = AF_INET;
        $self->{pf} = PF_INET;
    }

    return;
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
            '# Hex Stream: ', unpack( q{H*}, $message ), "\n",
            $_hdr->dump(
                [
                    [ 'data', length( $message ), $colour ],
                ],
                $message
            )
        ;
    } else {
        say STDERR '[', scalar( localtime ), ' ] ', $message;
    }

    return;
}

# Handler for errors
sub _error {
    my $self = shift;

    Kafka::Exception::IO->throw( throw_args( @_ ) );

    return;
}

#-- Closes and cleans up -------------------------------------------------------

1;

__END__

=head1 DIAGNOSTICS

When error is detected, an exception, represented by object of C<Kafka::Exception::Producer> class,
is thrown (see L<Kafka::Exceptions|Kafka::Exceptions>).

L<code|Kafka::Exceptions/code> and a more descriptive L<message|Kafka::Exceptions/message> provide
information about thrown exception. Consult documentation of the L<Kafka::Exceptions|Kafka::Exceptions>
for the list of all available methods.

Authors suggest using of L<Try::Tiny|Try::Tiny>'s C<try> and C<catch> to handle exceptions while
working with L<Kafka|Kafka> package.

Here is the list of possible error messages that C<Kafka::IO> may produce:

=over 3

=item C<Invalid argument>

Invalid arguments were passed to a method.

=item C<Can't send>

Message can't be sent on a C<Kafka::IO> object socket.

=item C<Can't recv>

Message can't be received.

=item C<Can't bind>

TCP connection can't be established on given host and port.

=back

=head2 Debug mode

Debug output can be enabled by passing desired level via environment variable
using one of the following ways:

C<PERL_KAFKA_DEBUG=1>     - debug is enabled for the whole L<Kafka|Kafka> package.

C<PERL_KAFKA_DEBUG=IO:1>  - enable debug for C<Kafka::IO> only.

C<Kafka::IO> supports two debug levels (level 2 includes debug output of 1):

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
