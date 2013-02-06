package Kafka::Mock;

use 5.010;
use strict;
use warnings;

our $VERSION = '0.10';

# kafka server imitation (non-blocking mode)

use bytes;
use Socket;
use IO::Handle;
use IPC::Open3;
use Config;
use POSIX ":sys_wait_h";
use Time::HiRes qw( sleep gettimeofday alarm );
use Carp;
use Params::Util qw( _STRING _NONNEGINT _NUMBER _HASH0 );

use Kafka qw(
    ERROR_MISMATCH_ARGUMENT
    );

use constant {
    DEFAULT_TIMEOUT     => 0.1,                 # The timeout in secs, could be any integer or floating-point type
    ATTEMPTS            => 100,

    ERROR_SOCKET        => 0,
    ERROR_REUSADDR      => 1,
    ERROR_BIND          => 2,
    ERROR_LISTEN        => 3,
    ERROR_EMPTYPORT     => 4,
    ERROR_CONVERTHOST   => 5,
    ERROR_OPENPORT      => 6,
    ERROR_SOCKETPAIR    => 7,
    ERROR_CHILD         => 8,
    ERROR_FORK          => 9,
    ERROR_SIG           => 10,
};

our @ERROR = (
    "socket() failed",
    "Can't set SO_REUSADDR",
    "bind() failed",
    "listen() failed",
    "empty port not found",
    "Couldn't convert host into an Internet address",
    "cannot open port",
    "socketpair",
    "Child process does not block",
    "fork failed",
    "your server received SIG",
    );

# Child #######################################################################

my $sock_name;
my $parent;
my $timeout;
my $quit;

sub _server {
    my $self        = shift;

    $SIG{TERM} = sub { $quit = 1; };

    select( ( select( STDOUT ), $| = 1 )[0] );
    select( ( select( STDERR ), $| = 1 )[0] );

    my $protocol = getprotobyname( 'tcp' );
    $sock_name = sockaddr_in( $self->{port}, inet_aton( $self->{host} ) );

    socket( my $server, AF_INET, SOCK_STREAM, $protocol ) or die $ERROR[ERROR_SOCKET].": $!";
    setsockopt( $server, SOL_SOCKET, SO_REUSEADDR, 1 ) or die $ERROR[ERROR_REUSADDR].": $!" ;

    $self->{server} = $server;
    bind( $server, $sock_name ) or die $ERROR[ERROR_BIND].": $!";
    listen( $server, SOMAXCONN) or die $ERROR[ERROR_LISTEN].": $!";

    while (1)
    {
        if ( $quit ) { exit }
        my $client;
        next unless my $remote_addr = $self->_accept( \$client, $server );

        $client->autoflush(1);
        $client->blocking(0);

        $self->{client} = $client;
        $self->_client();

        CORE::close $client;
        $self->{client} = undef;
    }
    CORE::close $server;
}

sub _accept {
    my $self        = shift;
    my $client      = shift;
    my $server      = shift;

#    my $addr;
#    eval {
#        local $SIG{ALRM} = sub { die "alarm clock restarted"};
## 0.03 Without the theoretical justification
#        alarm 0.03;
#        $addr = accept( $$client, $server );
#        alarm 0;
#    };
#    alarm 0;                                    # race condition protection
#    return $addr;

        return accept( $$client, $server );
}

my $_recv_pos;
sub _client {
    my $self        = shift;

# kafka server imitation
    while (1)
    {
        if ( $quit ) { exit }
        $self->_get_cmd();
        $_recv_pos = 0;
        $self->{sleep} = "";
        return unless defined( my $head = $self->_receive( 6 ) );
        if ( $head )
        {
            my ( $message, $request_type, $request_length );
            $message = "";
            if ( bytes::length( $head ) >= 6 )
            {
                ( $request_length, $request_type ) = unpack( "
                    N                               # REQUEST_LENGTH
                    n                               # REQUEST_TYPE
                    ", $head );
                if ( $request_length > 2 ) { return unless defined( $message = $self->_receive( $request_length - 2, 6 ) ); }
                $self->{last_request} = $head.$message;

                if ( exists $self->{responses}->{ $request_type } )
                {
                    return unless defined( $self->_send( pack( "H*", $self->{responses}->{ $request_type } ) ) );
                }
                else
                {
                    syswrite( $self->{client}, "\n" );
                }
            }
            else
            {
                $self->{last_request} = $head;
                syswrite( $self->{client}, "\n" );
            }
        }
        sleep( $self->{timeout} );
    }
}

sub _receive {
    my $self        = shift;
    my $length      = shift;
    my $start       = shift || 0;

    my $message = "";
    $self->{sleep} = "receive:" unless $_recv_pos;
    foreach my $part ( @{$self->_order( $length, $self->{recv_delay}, $start )} )
    {
        my $len = $part->[0];
        $len = ( $length - bytes::length( $message ) < $len ) ? $length - bytes::length( $message ) : $len;
        unless ( $len <= 0 )
        {
            my $msg = "";
            my ( $status, $buf );
            $msg .= $buf while ( $len - bytes::length( $msg ) && ( $status = sysread( $self->{client}, $buf, $len - bytes::length( $msg ) ) ) );
            $message .= $msg;
            return if defined( $status ) && $status == 0 && !$buf;
        }
        $_recv_pos += $len;
        $self->{sleep} .= " $_recv_pos ".$self->_sleep( $part->[1] )if $part->[1];
    }
    return $message;
}

sub _send {
    my $self        = shift;
    my $str         = shift;

    return 0 unless $str;

    my ( $msg, $buf ) = ( "" );
    ( $msg .= $buf ) while sysread( $self->{client}, $buf, 1 );

    my $total;
    my $pos = 0;
    $self->{sleep} .= ( $self->{sleep} ? " " : "" )."send:";
    foreach my $part ( @{$self->_order( $str, $self->{send_delay} )} )
    {
        my $message = $part->[0];
        my $len = bytes::length( $message );
        if ( my $len = bytes::length( $message ) )
        {
            my $sent = 0;
            my $status;
            $sent += $status while ( bytes::length( $message ) - $sent ) && ( $status = syswrite( $self->{client}, $message ) );
            return if defined( $status ) && $status == 0;
            $total += $sent;
        }
        $pos += $len;
        $self->{sleep} .= " $pos ".$self->_sleep( $part->[1] ) if $part->[1];
    }
    return $total;
}

sub _order {
    my $self        = shift;
    my $val         = shift;
    my $delay       = shift;
    my $start       = shift || 0;

    my @order;
    my $frst = 0;
    my $rest = $val;
    foreach my $pos ( sort { $a <=> $b } keys %$delay )
    {
        if ( defined _NONNEGINT( $val ) )       # receive
        {
            next if $pos < $start || $pos > $val + $start;
            push @order, [ $pos - $start - $frst, $delay->{ $pos } ];
            $rest -= $pos - $start - $frst;
            $start = 0;
        }
        else                                    # send
        {
            last if $pos >= bytes::length $val;
            ( my $substr, $rest ) = unpack( "a".( $pos - $frst )."a*", $rest );
            push @order, [ $substr, $delay->{ $pos } ];
        }
        $frst = $pos;
    }
    push @order, [ $rest, 0 ];

    return \@order;
}

sub _get_cmd {
    my $self        = shift;

    while ( sysread( $parent, my $cmd, 1 ) )
    {
        $cmd = unpack( "C", $cmd );
        if ( $cmd == 1 )                        # clear
        {
            $self->{send_delay} = {};
            $self->{recv_delay} = {};
            $self->{note} = "delay response: @{[ %{$self->{send_delay}} ]} delay request: @{[ %{$self->{recv_delay}} ]}";
        }
        elsif ( $cmd == 2 )                     # last_request
        {
            my $msg = ( $self->{last_request} ? unpack( "H*", $self->{last_request} ) : "" )."\n";
            syswrite( $parent, $msg, bytes::length( $msg ) );
            $self->{note} = "last_request";
        }
        elsif ( $cmd == 3 )                     # note
        {
            my $msg = $self->{note}."\n";
            syswrite( $parent, $msg, bytes::length( $msg ) );
            $self->{note} = "note";
        }
        elsif ( $cmd == 4 )                     # sleep
        {
            my $msg = $self->{sleep}."\n";
            syswrite( $parent, $msg, bytes::length( $msg ) );
            $self->{note} = "sleep";
        }
        elsif ( $cmd == 5 or $cmd == 6 )        # response or request
        {
            if ( sysread( $parent, my $buf = "", 15 ) )
            {
                my ( $position, $delay ) = split " ", $buf;
                $self->{ $cmd == 5 ? "send_delay" : "recv_delay" }->{ $position } = $delay;
                $self->{note} = "delay response: @{[ %{$self->{send_delay}} ]} delay request: @{[ %{$self->{recv_delay}} ]}";
            }
        }
    }
}

sub _sleep {
    my $self        = shift;
    my $delay       = shift;

    return 0 unless $delay;

    my $start = gettimeofday;
    sleep( $delay );
    return gettimeofday - $start;
}

# Parent #######################################################################

sub new {
    my $class       = shift;

    my $self = bless {
        requests            => {},
        responses           => {},
        timeout             => DEFAULT_TIMEOUT,
        }, $class;

    my @args = @_;
    while ( @args )
    {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    (
        _HASH0( $self->{requests} ) and
        _HASH0( $self->{responses} ) and
        defined( _NUMBER( $self->{timeout} ) ) and $self->{timeout} > 0
    ) or return $self->_error( ERROR_MISMATCH_ARGUMENT );

    return $self->_error( ERROR_MISMATCH_ARGUMENT )
        if "@{[ sort keys %{$self->{requests}} ]}" ne "@{[ sort keys %{$self->{responses}} ]}";

    $self->{_my_pid}        = $$;
    $self->{host}           = '127.0.0.1';
    $self->{port}           = $self->_empty_port();
    $self->{child}          = undef;
    $self->{send_delay}     = {};
    $self->{recv_delay}     = {};
    $self->{last_request}   = "";
    $self->{note}           = "";
    $self->{sleep}          = "";
    $timeout = $self->{timeout};

    $self->_start();

    return $self;
}

sub port {
    my $self        = shift;

    return $self->{port};
}

sub delay {
    my $self        = shift;
    my $operation   = _STRING( shift ) or $self->_error( ERROR_MISMATCH_ARGUMENT );
    my $position    = shift;
    my $delay       = shift;

    $self->_error( ERROR_MISMATCH_ARGUMENT ) unless ( $operation eq "response" or $operation eq "request" );
    ( defined( _NONNEGINT( $position ) ) and defined( _NUMBER( $delay ) ) and $delay > 0 ) or $self->_error( ERROR_MISMATCH_ARGUMENT );


# delay the start of the reception or transmission timeout should be managed
    $self->_error( ERROR_MISMATCH_ARGUMENT ) if $position == 0;
# request identifier should be holistic (the first 6 bytes)
    $self->_error( ERROR_MISMATCH_ARGUMENT ) if $operation eq "request" and $position < 6;

    $self->_send_cmd( "$operation $position $delay" );
}

my %_cmd = (
    clear           => 1,
    last_request    => 2,
    note            => 3,
    sleep           => 4,
    response        => 5,
    request         => 6,
    );

sub last_request {
    my $self        = shift;
    my $mode        = shift;

    $self->_error( ERROR_MISMATCH_ARGUMENT ) unless ( !defined( $mode ) or $mode =~ /^(?:note|pid|sleep)$/ );

    return $self->{pid} if defined( $mode ) and $mode eq "pid";

    my $child = $self->{child};
    $self->_send_cmd( $mode || "last_request" );
    sleep $self->{timeout};
    my $request = <$child>;
    chomp $request;
    return $request;
}

sub clear {
    my $self        = shift;

    $self->_send_cmd( "clear" );
}

sub _send_cmd {
    my $self        = shift;
    my $cmd         = shift;

    my @arr = split " ", $cmd;
    if ( ( $arr[0] eq "response" ) or ( $arr[0] eq "request" ) )
    {
        print {$self->{child}} pack( "C", $_cmd{ $arr[0] } ).sprintf( "%7s %7s", $arr[1], $arr[2] );
    }
    else
    {
        print {$self->{child}} pack( "C", $_cmd{ $arr[0] } ) if exists $_cmd{ $arr[0] };
    }
}

sub _error {
    my $self        = shift;
    my $error_code  = shift;

    confess $Kafka::ERROR[ $error_code ];
    return;
}

#-------------------------------------------------------------------------------
# Below functions based on Test::TCP are used

# get a empty port on 49152 .. 65535
# http://www.iana.org/assignments/port-numbers
sub _empty_port {
    my $self        = shift;

    my $port = 50000 + int( rand() * 1000 );

    while ( $port++ < 60000 )
    {
        next if $self->_check_port( $port );
        socket( my $sock, PF_INET, SOCK_STREAM, getprotobyname( 'tcp' ) );
        setsockopt( $sock, SOL_SOCKET, SO_REUSEADDR, 1 );
        my $my_addr = sockaddr_in( $port, INADDR_ANY );
        next if !bind( $sock, $my_addr ) || !listen( $sock, SOMAXCONN );
        return $port;
    }
    die $ERROR[ERROR_EMPTYPORT];
}

sub _check_port {
    my $self        = shift;
    my $port        = shift;

    socket( my $remote, PF_INET, SOCK_STREAM, getprotobyname( 'tcp' ) );
    my $internet_addr = inet_aton( $self->{host} ) or die $ERROR[ERROR_CONVERTHOST].": $!\n";
    my $paddr = sockaddr_in( $port, $internet_addr );
    if ( connect( $remote, $paddr ) )
    {
        CORE::close $remote;
        return 1;
    }
    else
    {
        return 0;
    }
}

sub _wait_port {
    my $self        = shift;
    my $port        = shift;

    my $retry = ATTEMPTS;
    while ( $retry-- )
    {
        return if $self->_check_port( $port );
        sleep( $self->{timeout} );
    }
    die $ERROR[ERROR_OPENPORT].": $port";
}

sub _start {
    my $self        = shift;

# bidirectional communication using socketpair "the best ones always go both ways"
    socketpair( my $child, $parent, AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die $ERROR[ERROR_SOCKETPAIR].": $!";

    # Set autoflushing.
    $child->autoflush(1);
    $child->blocking(1);
    $parent->autoflush(1);
    $parent->blocking(0);

    if ( my $pid = fork() )
    {
        # parent.
        $self->{pid} = $pid;
        CORE::close $parent;
        $self->{child} = $child;
        $self->_wait_port( $self->port );
        return;
    }
    elsif ( $pid == 0 )
    {
        # child process
        CORE::close $child;
        $self->_server();
        # should not reach here
        if ( kill 0, $self->{_my_pid} )
        {
            # warn only parent process still exists
            warn( "[".__PACKAGE__."] ".$ERROR[ERROR_CHILD]." ( PID: $$, PPID: $self->{_my_pid} )" );
        }
        exit 0;
    }
    else
    {
        die $ERROR[ERROR_FORK].": $!";
    }
}

sub close {
    my $self        = shift;

    return unless defined $self->{pid};
    return unless $self->{_my_pid} == $$;

    local $SIG{CHLD} = 'IGNORE';
#    kill 'TERM' => $self->{pid};

    local $?;                                   # waitpid modifies original $?.
    while (1)
    {
        kill 'TERM' => $self->{pid};
        sleep $timeout;
        my $kid = waitpid( $self->{pid}, &WNOHANG );
        if ( POSIX::WIFSIGNALED( $? ) )
        {
            my $signame = ( split( ' ', $Config{sig_name} ) )[ POSIX::WTERMSIG( $? ) ];
            if ( $signame =~ /^(ABRT|PIPE)$/ )
            {
                print STDERR $ERROR[ERROR_SIG]."$signame\n";
            }
        }
        last unless $kid != -1;
    }
    undef $self->{pid};
    CORE::close $self->{child};
    $self->{child} = undef;
    delete $self->{$_} foreach keys %$self;
    return;
}

sub DESTROY {
    my $self        = shift;

    local $@;
    $self->close;
}

1;

__END__

=head1 NAME

Kafka::Mock - object interface to the TCP mock server for testing

=head1 VERSION

This documentation refers to C<Kafka::Mock> version 0.10

=head1 SYNOPSIS

To use the Mock server, the application might be started and finished as below:

    use Kafka qw( DEFAULT_TIMEOUT );
    use Kafka::Mock;
    
    # Mock server
    my $mock = Kafka::Mock->new(
        requests    => \%requests,
        responses   => \%responses,
        timeout     => 0.1  # Optional, secs float
        );
    
    #-- IO
    my $io = Kafka::IO->new(
        host        => "localhost",
        port        => $mock->port
        );
    
    # ... the application body
    
    # to kill the mock server process
    $mock->close;

Look at the L</Sample Data> section for the C<%requests> and C<%responses>
sample.

Use only one C<Kafka::Mock> object at the same time.

=head1 DESCRIPTION

This C<Kafka::Mock> mock server provides a local TCP server with a set of
behaviours for use in testing of Kafka clients with the L<Kafka::IO|Kafka::IO>
object.
It is intended to be use when testing the L<Kafka::Producer|Kafka::Producer>
and L<Kafka::Consumer|Kafka::Consumer> client interfaces.
Testing allows you to determine the settings for the Apache Kafka server
instance and timeouts for your clients.

Kafka mock server API is implemented by C<Kafka::Mock> class.

The main features of the C<Kafka::Mock> class are:

=over 3

=item *

Provides an object oriented model of communication.

=item *

Simple mock server instance for testing without an Apache Kafka server.

=item *

Allows you to set and clear the delay in these positions in the reception
and transmission of data.

=item *

Provides diagnostic information on the commands received and fulfilled by
the server.

=back

=head2 CONSTRUCTOR

=head3 C<new>

Creates a local TCP server process.
Establishes bidirectional communication to the server using C<socketpair>.
The server receives the hash references for the requests and appropriate
responses to be returned.
Also, the server receives the value of the delay between taking the requests.

Returns the created a C<Kafka::Mock> object, or error will
cause the program to halt (C<confess>) if the argument is not a valid.

The Mock server returns to the IO object the response data that corresponds
to the request from the C<%requests> (based on the REQUEST_TYPE of the
request).

C<new()> takes arguments, these arguments are in key-value pairs.
The following arguments are currently recognized:

=over 3

=item C<timeout =E<gt> $timeout>

Optional, default = DEFAULT_TIMEOUT .

DEFAULT_TIMEOUT is the default timeout that can be imported from the
L<Kafka|Kafka> module.

C<$timeout> specifies how much time we give remote server before it goes into
the next reception of a request.
The C<$timeout> in secs (could be any integer or floating-point type).

=item C<requests =E<gt> \%requests>

C<%requests> is the reference to the hash denoting the control bytes strings
of the requests.

=item C<responses =E<gt> \%responses>

C<%responses> is the reference to the hash denoting the control bytes strings
of the responses.

=back

The keys of the C<%requests>, and C<%responses> must comply with the Apache Kafka
request types:

=over 3

=item

0 - PRODUCE

=item

1 - FETCH

=item

2 - MULTIFETCH

=item

3 - MULTIPRODUCE

=item

4 - OFFSETS

=back

The values of the C<%requests>, and C<%responses> should be in the C<"H*"> format
for the command C<pack>.

=head2 METHODS

The following methods are defined for the C<Kafka::Mock> class:

=head3 C<port>

The method returns the Mock server port to use for L<Kafka::IO|Kafka::IO>
object with C<"localhost"> as his L<host|Kafka::IO/host> attribute.

=head3 C<last_request>

For the last query string of bytes received by the server (when invoked with no
arguments).
The resulting string can be used for comparison with the control passed to the
query.

    # received a last request
    my $last_request = $mock->last_request;

The following modes are used only for internal testing of the mock server:

    # received a report on delays
    my $delays = $mock->last_request( "note" );
    # report completed delays
    my $sleep = $server->last_request( "sleep" );

=head3 C<delay( $mode, $position, $delay )>

    $mock->delay( "request",  10, 0.5 );
    $mock->delay( "response", 10, 0.5 );

To set a delay in the transmission or reception of data server.

To set the required number of delays should perform successive calls to
C<delay>.

C<$mode> should be set to C<"request"> or C<"response"> for the job
delays in receipt of a request or response in the transmission, respectively.

C<$position> specifies the byte position in the message will be
followed by performed delay C<$delay> seconds.
C<$position> must be a positive integer.
That is, it is defined and Perl thinks it's an integer.
For mode C<"request"> argument to C<$position> should be set not less than 6
as the request identifier should be holistic (REQUEST_LENGTH + REQUEST_TYPE).

C<$delay> must be a positive number.
That is, it is defined and Perl thinks it's a number.

Error will cause the program to halt (C<confess>) if an argument is not valid.

=head3 C<clear>

The method to delete all previously specified delays.

=head3 C<close>

The method to kill the mock server process and clean up.

=head2 Sample Data

  my %requests = (
      0   =>                                      # PRODUCE Request
          # Request Header
           '0000005f'                             # REQUEST_LENGTH
          .'0000'                                 # REQUEST_TYPE
          .'0004'                                 # TOPIC_LENGTH
          .'74657374'                             # TOPIC ("test")
          .'00000000'                             # PARTITION
          # PRODUCE Request
          .'0000004f'                             # MESSAGES_LENGTH
          # MESSAGE
          .'00000016'                             # LENGTH
          .'00'                                   # MAGIC
          .''                                     # COMPRESSION
          .'d94a22be'                             # CHECKSUM
          # "The first message"
          .'546865206669727374206d657373616765'   # PAYLOAD
          # MESSAGE
          .'00000017'                             # LENGTH
          .'00'                                   # MAGIC
          .''                                     # COMPRESSION
          .'a3810845'                             # CHECKSUM
          # "The second message"
          .'546865207365636f6e64206d657373616765' # PAYLOAD
          # MESSAGE
          .'00000016'                             # LENGTH
          .'00'                                   # MAGIC
          .''                                     # COMPRESSION
          .'58611780'                             # CHECKSUM
          # "The third message"
          .'546865207468697264206d657373616765',  # PAYLOAD
      1   =>                                      # FETCH Request
          # Request Header
           '00000018'                             # REQUEST_LENGTH
          .'0001'                                 # REQUEST_TYPE
          .'0004'                                 # TOPIC_LENGTH
          .'74657374'                             # TOPIC ("test")
          .'00000000'                             # PARTITION
          # FETCH Request
          .'0000000000000000'                     # OFFSET
          .'00100000',                            # MAX_SIZE (1MB)
      2   => '',                                  # MULTIFETCH Request
      3   => '',                                  # MULTIPRODUCE Reqst
      4   =>                                      # OFFSETS Request
          # Request Header
           '00000018'                             # REQUEST_LENGTH
          .'0004'                                 # REQUEST_TYPE
          .'0004'                                 # TOPIC_LENGTH
          .'74657374'                             # TOPIC ("test")
          .'00000000'                             # PARTITION
          # OFFSETS Request
          .'fffffffffffffffe'                     # TIME -2: earliest
          .'00000064',                            # MAX NUM OFFSTS 100
      );
  
  my %responses = (
      0   => '',                                  # PRODUCE Response
      1   =>                                      # FETCH Response
          # Response Header
           '00000051'                             # RESPONSE_LENGTH
          .'0000'                                 # ERROR_CODE
          # MESSAGE
          .'00000016'                             # LENGTH
          .'00'                                   # MAGIC
          .''                                     # COMPRESSION
          .'d94a22be'                             # CHECKSUM
          # "The first message"
          .'546865206669727374206d657373616765'   # PAYLOAD
          # MESSAGE
          .'00000017'                             # LENGTH
          .'00'                                   # MAGIC
          .''                                     # COMPRESSION
          .'a3810845'                             # CHECKSUM
          # "The second message"
          .'546865207365636f6e64206d657373616765' # PAYLOAD
          # MESSAGE
          .'00000016'                             # LENGTH
          .'00'                                   # MAGIC
          .''                                     # COMPRESSION
          .'58611780'                             # CHECKSUM
          # "The third message"
          .'546865207468697264206d657373616765',  # PAYLOAD
      2   => '',                                  # MULTIFETCH Respns
      3   => '',                                  # MULTIPROD  Respns
      4   =>                                      # OFFSETS Response
          # Response Header
           '0000000e'                             # RESPONSE_LENGTH
          .'0000'                                 # ERROR_CODE
          # OFFSETS Response
          .'00000001'                             # NUMBER of OFFSETS
          .'0000000000000000'                     # OFFSET
      );

=head1 DIAGNOSTICS

C<Kafka::Mock> is not an end user module and any error is FATAL.
FATAL errors will cause the program to halt (C<confess>), since the
problem is so severe that it would be dangerous to continue. (This
can always be trapped with C<eval>. Under the circumstances, dying is the best
thing to do).

=over 3

=item C<Mismatch argument>

This means that you didn't give the right argument to the C<new>
L<constructor|/CONSTRUCTOR>.

=item Any other errors

When working with the C<Kafka::Mock> module errors may occur are listed in
the array C<@Kafka::Mock::ERROR>.

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