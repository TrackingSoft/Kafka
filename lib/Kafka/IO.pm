package Kafka::IO;

use 5.008003;
use strict;
use warnings;
use sigtrap;

our $VERSION = '0.02';

use bytes;
use Carp;
use Params::Util qw( _STRING _POSINT _NONNEGINT _NUMBER _ARRAY0 );
use Errno;
use Fcntl;
use Socket;
use Time::HiRes qw( alarm );

use Kafka qw(
    KAFKA_SERVER_PORT
    DEFAULT_TIMEOUT
    MAX_SOCKET_REQUEST_BYTES
    ERROR_MISMATCH_ARGUMENT
    ERROR_CANNOT_SEND
    ERROR_CANNOT_RECV
    ERROR_CANNOT_BIND
    );

our $_last_error;
our $_last_errorcode;

sub new {
    my $class = shift;

    my $self = bless {
        host        => "",
        port        => KAFKA_SERVER_PORT,
        timeout     => DEFAULT_TIMEOUT,
        RaiseError  => 0,
        }, $class;

    $@ = "";
    $self->{last_error} = $self->{last_errorcode} = $_last_error = $_last_errorcode = undef;

    my @args = @_;
    while ( @args )
    {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    unless ( defined( _NONNEGINT( $self->{RaiseError} ) ) )
    {
        $self->{RaiseError} = 0;
        return $self->_error( ERROR_MISMATCH_ARGUMENT );
    }

    (
        _STRING( $self->{host} ) and
        _POSINT( $self->{port} ) and
        ( _NUMBER( $self->{timeout} ) and $self->{timeout} > 0 )
    ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );

    $self->{not_accepted} = 0;
    eval { $self->_connect() };
    return $self->_error( ERROR_CANNOT_BIND, $@ ) if ( $@ );
    return $self;
}

sub last_error {
    my $self = shift;

    return $self->{last_error} if defined $self;
    return $_last_error;
}

sub last_errorcode {
    my $self = shift;

    return $self->{last_errorcode} if defined $self;
    return $_last_errorcode;
}

# You need to have access to your Kafka instance and be able to connect through TCP
# uses http://devpit.org/wiki/Connect%28%29_with_timeout_%28in_Perl%29
sub _connect {
    my $self = shift;

    $@ = "";
    $self->{socket} = undef;
    $_last_error        = $_last_errorcode          = undef;
    $self->{last_error} = $self->{last_errorcode}   = undef;

    my $name    = $self->{host};
    my $port    = $self->{port};
    my $timeout = $self->{timeout};

    my $ip;
    if( $name =~ qr~[a-zA-Z]~s )
    {
        # DNS lookup.
        eval {
            local $SIG{ALRM} = sub { die "alarm clock restarted"};
            alarm $self->{timeout};
            $ip = gethostbyname( $name );
            alarm 0;
        };
        alarm 0;                                # race condition protection
        die $@ if $@;
        die( "gethostbyname ${name}: $?\n" ) unless defined $ip;
        $ip = inet_ntoa( $ip );
    }
    else
    {
        $ip = $name;
    }

    # Create socket.
    socket( my $connection, PF_INET, SOCK_STREAM, getprotobyname( "tcp" ) ) or die( "socket: $!\n" );

    # Set autoflushing.
    $_ = select( $connection ); $| = 1; select $_;

    # Set FD_CLOEXEC.
    $_ = fcntl( $connection, F_GETFL, 0 ) or die "fcntl: $!\n";
    fcntl( $connection, F_SETFL, $_ | FD_CLOEXEC ) or die "fnctl: $!\n";

#    if( $timeout )
#    {
        # Set O_NONBLOCK so we can time out connect().
        $_ = fcntl( $connection, F_GETFL, 0 ) or die "fcntl F_GETFL: $!\n"; # 0 for error, 0e0 for 0.
        fcntl( $connection, F_SETFL, $_ | O_NONBLOCK ) or die "fcntl F_SETFL O_NONBLOCK: $!\n"; # 0 for error, 0e0 for 0.
#    }

    # Connect returns immediately because of O_NONBLOCK.
    connect( $connection, pack_sockaddr_in( $port, inet_aton( $ip ) ) ) or $!{EINPROGRESS} or die( "connect ${ip}:${port} (${name}): $!\n" );

    $self->{socket}     = $connection;
    $self->{_select}    = undef;

#    return $connection unless $timeout;

    # Reset O_NONBLOCK.
    $_ = fcntl( $connection, F_GETFL, 0 ) or die "fcntl F_GETFL: $!\n";  # 0 for error, 0e0 for 0.
    fcntl( $connection, F_SETFL, $_ & ~ O_NONBLOCK ) or die "fcntl F_SETFL not O_NONBLOCK: $!\n";  # 0 for error, 0e0 for 0.

    # Use select() to poll for completion or error. When connect succeeds we can write.
    my $vec = "";
    vec( $vec, fileno( $connection ), 1 ) = 1;
    select( undef, $vec, undef, $timeout );
    unless( vec( $vec, fileno( $connection ), 1 ) )
    {
        # If no response yet, impose our own timeout.
        $! = Errno::ETIMEDOUT();
        die("connect ${ip}:${port} (${name}): $!\n");
    }

    # This is how we see whether it connected or there was an error. Document Unix, are you kidding?!
    $! = unpack( "L", getsockopt( $connection, SOL_SOCKET, SO_ERROR ) );
    die( "connect ${ip}:${port} (${name}): $!\n" ) if $!;

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
    setsockopt( $connection, SOL_SOCKET, SO_SNDTIMEO, pack( "L!L!", $timeout, 0 ) ) or die "setsockopt SOL_SOCKET, SO_SNDTIMEO: $!\n";
    setsockopt( $connection, SOL_SOCKET, SO_RCVTIMEO, pack( "L!L!", $timeout, 0 ) ) or die "setsockopt SOL_SOCKET, SO_RCVTIMEO: $!\n";

    vec( $self->{_select} = "", fileno( $self->{socket} ), 1 ) = 1;

    return $connection;
}

sub _disconnect {
    my $self = shift;

    # Close socket
    if ( $self->{socket} )
    {
        $self->{_select} = undef;
        close( $self->{socket} );
        $self->{socket} = undef;
    }
}

#sub _reconnect {
#    my $self = shift;
#
#    $self->_disconnect() if $self->{socket};
#    $self->_connect();
#}

sub close {
    my $self = shift;

    $self->_disconnect() if $self and $self->{socket};

    delete $self->{$_} foreach keys %$self;
    return;
}

sub _error {
    my $self        = shift;
    my $error_code  = shift;
    my $description = shift;

    $self->{last_errorcode} = $_last_errorcode  = $error_code;
    $self->{last_error}     = $_last_error      = $Kafka::ERROR[$self->{last_errorcode}].( $description ? ": ".$description : "" );
    confess $self->{last_error} if $self->{RaiseError} and $self->{last_errorcode} == ERROR_MISMATCH_ARGUMENT;
    die $self->{last_error} if $self->{RaiseError};
    return;
}

sub send {
    my $self        = shift;

    $@ = "";
    my $message     = _STRING( shift ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );
    $message .= "";
    $self->_error( ERROR_MISMATCH_ARGUMENT ) if bytes::length( $message ) > MAX_SOCKET_REQUEST_BYTES;

#    eval { $self->_reconnect() } if ( !$self->{socket} or !getpeername( $self->{socket} ) );
#    return $self->_error( ERROR_CANNOT_BIND, $@ ) if ( $@ );

    # accept not accepted earlier
    while ( select( my $mask = $self->{_select}, undef, undef, 0 ) )
    {
        my $received = $self->receive( $self->{not_accepted} || 1 );
        return unless ( $received and defined( $$received ) );
        $self->{not_accepted} = 0;
    }
    $self->{not_accepted} = 0;

    my ( $sent, $from_send, $mask, $len );
    $len = bytes::length( $message );
    {
        last unless ( select( undef, $mask = $self->{_select}, undef, $self->{timeout} ) );
        $sent += ( $from_send = send( $self->{socket}, $message, 0 ) ) || 0;
        redo if $sent < $len;
    }
    return $sent if ( defined( $sent ) and $sent == bytes::length( $message ) );
    return $self->_error( ERROR_CANNOT_SEND, "".$! );
}

sub receive {
    my $self    = shift;

    $@ = "";
    my $length  = _POSINT( shift ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );

#    eval { $self->_reconnect() } if ( !$self->{socket} or !getpeername( $self->{socket} ) );
#    return $self->_error( ERROR_CANNOT_BIND, $@ ) if ( $@ );

    my ( $from_recv, $message, $buf, $mask );
    $message = "";
    {
        last unless ( select( $mask = $self->{_select}, undef, undef, $self->{timeout} ) );
        $from_recv = recv( $self->{socket}, $buf = "", $length, 0 );
        last if !defined( $from_recv ) or $buf eq "";
        $message .= $buf;
        redo if bytes::length( $message ) < $length;
    }
    $self->{not_accepted} = $length - bytes::length( $message );
    $self->{not_accepted} *= $self->{not_accepted} >= 0;
    return \$message if ( defined( $from_recv ) and !$self->{not_accepted} );
    return $self->_error( ERROR_CANNOT_RECV, "".$! );
}

sub RaiseError {
    my $self = shift;

    return $self->{RaiseError};
}

sub DESTROY {
    my $self = shift;

    $self->close();
}

1;

__END__

=head1 NAME

Kafka::IO - object interface to socket communications with the Apache Kafka
server without using the Apache ZooKeeper

=head1 VERSION

This documentation refers to C<Kafka::IO> version 0.02

=head1 SYNOPSIS

Setting up:

    use Kafka qw( KAFKA_SERVER_PORT DEFAULT_TIMEOUT );
    use Kafka::IO;
    
    my $io;
    
    eval { $io = Kafka::IO->new(
        host        => "localhost",
        port        => KAFKA_SERVER_PORT,
        timeout     => "bad thing",
        RaiseError  => 1
        ) };
    print "expecting to die: (",
        Kafka::IO::last_errorcode, ") ",
        Kafka::IO::last_error, "\n" if $@;
    
    unless ( $io = Kafka::IO->new(
        host        => "localhost",
        port        => KAFKA_SERVER_PORT,
        timeout     => DEFAULT_TIMEOUT, # Optional,
                                        # default = DEFAULT_TIMEOUT
        RaiseError  => 0                # Optional, default = 0
        ) )
    {
        print "unexpecting to die: (",
            Kafka::IO::last_errorcode, ") ",
            Kafka::IO::last_error, "\n" if $@;
    }

Producer:

    use Kafka::Producer;
    
    my $producer = Kafka::Producer->new(
        IO          => $io,
        RaiseError  => 0                # Optional, default = 0
        );
    
    # ... the application body
    
    # Closes the producer and cleans up
    $producer->close;

Or Consumer:

    use Kafka::Consumer;
    
    my $consumer = Kafka::Consumer->new(
        IO          => $io,
        RaiseError  => 0                # Optional, default = 0
        );
    
    # ... the application body
    
    # Closes the consumer and cleans up
    $consumer->close;

=head1 DESCRIPTION

The main features of the C<Kafka::IO> class are:

=over 3

=item *

Provides an object oriented model of communication.

=item *

To provide the class that allows you to write the Apache Kafka clients without
using the Apache ZooKeeper service.

=back

=head2 CONSTRUCTOR

=head3 C<new>

Establishes socket TCP connection on given host and port, creates
a C<Kafka::IO> IO object. Returns the created a C<Kafka::IO> object.

An error will cause the program to halt or the constructor will return the
undefined value, depending on the value of the C<RaiseError> attribute.

You can use the methods of the C<Kafka::IO> class - L</last_errorcode>
and L</last_error> for the information about the error.

C<new()> takes arguments in key-value pairs.
The following arguments are currently recognized:

=over 3

=item C<host =E<gt> $host>

C<$host> is an Apache Kafka host to connect to. It can be a hostname or the
IP-address in the "xx.xx.xx.xx" form.

=item C<port =E<gt> $port>

Optional, default = KAFKA_SERVER_PORT .

C<$port> is the attribute denoting the port number of the service we want to
access (Apache Kafka service). The C<$port> should be a number.

KAFKA_SERVER_PORT is the default Apache Kafka server port = 9092.

=item C<timeout =E<gt> $timeout>

Optional, default = DEFAULT_TIMEOUT .

DEFAULT_TIMEOUT is the default timeout that can be imported from the
L<Kafka|Kafka> module.

C<$timeout> specifies how much time we give remote server to respond before
the IO object disconnects and creates an internal exception.
The C<$timeout> in secs, for gethostbyname, connect, blocking receive and send
calls (could be any integer or floating-point type).

The first connect will never fail with a timeout as the connect call
will not block.

=item C<RaiseError =E<gt> $mode>

Optional, default = 0 .

An error will cause the program to halt if L</RaiseError> is true: C<confess>
if the argument is not valid or C<die> in the other error case.
Returns the undefined value if L</RaiseError> is not true and any error occured.

=back

=head2 METHODS

The following methods are defined for the C<Kafka::IO> class:

=head3 C<send( $message )>

Sends a message on a C<Kafka::IO> IO object socket. Reconnects on unconnected
sockets.

The argument must be a bytes string.

Returns the number of characters sent. If there's an error, returns
the undefined value if the L</RaiseError> is not true.

=head3 C<receive( $length )>

Receives a message on a IO object socket. Attempts to receive the C<$length>
bytes of data.

Returns a reference to the received message. If there's an error, returns
the undefined value if the L</RaiseError> is not true.

The argument must be a value that is a positive number. That is, it is defined
and Perl thinks it's a number.

=head3 C<close>

The method to close the C<Kafka::IO> IO object and clean up.

=head3 C<last_errorcode>

This method returns an error code that specifies the position of the
description in the C<@Kafka::ERROR> array.  Analysing this information
can be done to determine the cause of the error.

The server or the resource might not be available, access to the resource
might be denied or other things might have failed for some reason.

Complies with an array of descriptions C<@Kafka::ERROR>.

=head3 C<last_error>

This method returns an error message that contains information about the
encountered failure.  Messages returned from this method may contain
additional details and do not comply with the C<Kafka::ERROR> array.

=head3 C<RaiseError>

The method which causes the undefined value to be returned when an error
is detected if L</RaiseError> set to false, or to die automatically if
L</RaiseError> set to true (this can always be trapped with C<eval>).

It must be a non-negative integer. That is, a positive integer, or zero.

You should always check for errors, when not establishing the L</RaiseError>
mode to true.

=head1 DIAGNOSTICS

Look at the C<RaiseError> description for additional information on
error handeling.

The methods for the possible error to analyse: L</last_errorcode> and
more descriptive L</last_error>.

=over 3

=item C<Mismatch argument>

This means that you didn't give the right argument to a C<new>
L<constructor|/CONSTRUCTOR> or to other L<method|/METHODS>.

=item C<Can't send>

This means that the message can't be sent on a C<Kafka::IO> IO object socket.

=item C<Can't recv>

This means that the message can't be received on a C<Kafka::IO>
IO object socket.

=item C<Can't bind>

This means that the socket TCP connection can't be established on on given host
and port.

=back

For more error description, always look at the message from L</last_error>
method or from C<Kafka::IO::last_error> class method.

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