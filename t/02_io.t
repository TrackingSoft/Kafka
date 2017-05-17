#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
);

use Test::More;

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval 'use Test::TCP';           ## no critic
    plan skip_all => "because Test::TCP required for testing" if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use IO::Socket::INET;
use Net::EmptyPort qw(
    empty_port
);
use POSIX ':signal_h';
use Socket qw(
    AF_INET
    AF_INET6
    PF_INET
    PF_INET6
    inet_aton
    inet_ntop
);
use Sub::Install;
use Sys::SigAction qw(
    set_sig_handler
);
use Time::HiRes qw();

use Kafka qw(
    $IP_V4
    $IP_V6
    $KAFKA_SERVER_PORT
    $REQUEST_TIMEOUT
);
use Kafka::IO;
use Kafka::TestInternals qw(
    @not_posint
    @not_posnumber
    @not_string
);

# See Kafka::IO
use constant DEBUG  => 0;
#use constant DEBUG  => 1;
#use constant DEBUG  => 2;

Kafka::IO->debug_level( DEBUG ) if DEBUG;

STDOUT->autoflush;

my ( $server, $port, $io, $sig_handler, $marker_signal_handling, $original, $timer, $timeout, $sent, $resp, $test_message, $inet_aton, $hostname );

$inet_aton = inet_aton( '127.0.0.1' );  # localhost
$hostname = gethostbyaddr( $inet_aton, AF_INET );

my $server_code = sub {
    my ( $port ) = @_;

    my $sock = IO::Socket::INET->new(
        LocalPort   => $port,
        LocalAddr   => $hostname,
        Proto       => 'tcp',
        Listen      => 5,
        Type        => SOCK_STREAM,
        ReuseAddr   => 1,
    ) or die "Cannot open server socket $hostname:$port : $!";

    $SIG{TERM} = sub { exit };

    while ( my $remote = $sock->accept ) {
        while ( my $line = <$remote> ) {
            print { $remote } $line;
        }
    }
};

sub debug_msg {
    my ( $message ) = @_;

    return if Kafka::IO->debug_level != 2;

    diag '[ time = ', Time::HiRes::time(), ' ] ', $message;
}

my $server_port = empty_port( $KAFKA_SERVER_PORT );
$server = Test::TCP->new(
    code    => $server_code,
    port    => $server_port,
);
$port = $server->port;
ok $port, "server port = $port";
wait_port( $port );

$test_message = "Test message\n";

# NOTE: Kafka::IO->new uses alarm clock internally

#-- ALRM handler

debug_msg( 'ALRM handler verification' );

# cancel the previous timer
alarm 0;

$sig_handler = set_sig_handler( SIGALRM ,sub {
        ++$marker_signal_handling;
        debug_msg( 'SIGALRM: signal handler triggered' );
    }
);
ok( !defined( $marker_signal_handling ), 'marker signal handling not defined' );

throws_ok {
    debug_msg( "ALRM handler: host => 'something bad'" );
    $io = Kafka::IO->new(
        host    => 'something bad',
        port    => $port,
        timeout => $REQUEST_TIMEOUT,
    );
} 'Kafka::Exception::IO', 'error thrown';

debug_msg( "ALRM handler: host => $hostname" );
eval {
    $io = Kafka::IO->new(
        host    => $hostname,
        port    => $port,
        timeout => $REQUEST_TIMEOUT,
    );
};
SKIP: {
    skip "gethostbyname( '$hostname' ) takes too long: $@" if $@;

    isa_ok( $io, 'Kafka::IO' );

    ok( !defined( $marker_signal_handling ), 'marker signal handling not defined' );
    # signal handler triggered
    kill ALRM => $$;
    is $marker_signal_handling, 1, 'the signal handler to be reset to the previous value';

    #-- ALRM timer

    # Kafka::IO->new is badly ended before 'timer' and before 'timeout'

    # cancel the previous timer
    alarm 0;

    $SIG{ALRM} = sub {
        ++$marker_signal_handling;
        debug_msg( 'SIGALRM: signal handler triggered' );
    };
    $timer      = 10;
    $timeout    = $timer;

    debug_msg( "Kafka::IO->new is badly ended before 'timer' and before 'timeout'" );
    debug_msg( "timer = $timer, timeout = $timeout, host => 'something bad'" );
    $marker_signal_handling = 0;
    eval {
        alarm $timer;
        eval {
            $io = Kafka::IO->new(
                host    => 'something bad',
                port    => $port,
                timeout => $timeout,
            );
        };
        alarm 0;
    };
    ok !$marker_signal_handling, 'signal handler is not triggered';

    # Kafka::IO->new is correctly ended before 'timer' and before 'timeout'

    # cancel the previous timer
    alarm 0;

    $SIG{ALRM} = sub {
        ++$marker_signal_handling;
        debug_msg( 'SIGALRM: signal handler triggered' );
    };
    $timer      = 10;
    $timeout    = $timer + 5;

    $original = \&Kafka::IO::_gethostbyname;
    Sub::Install::reinstall_sub( {
        code    => sub {
            my $self = shift;

            $self->{af} = AF_INET;
            $self->{pf} = PF_INET;
            $self->{ip} = '127.0.0.1';

            debug_msg( '_gethostbyname called (without sleep)' );

            return $self->{ip};
        },
        into    => 'Kafka::IO',
        as      => '_gethostbyname',
    } );

    debug_msg( "Kafka::IO->new is correctly ended before 'timer' and before 'timeout'" );
    debug_msg( "timer = $timer, timeout = $timeout, host => 'something bad'" );
    $marker_signal_handling = 0;
    eval {
        alarm $timer;
        eval {
            $io = Kafka::IO->new(
                host    => 'something bad',
                port    => $port,
                timeout => $timeout,
            );

            ok !$marker_signal_handling, 'signal handler is not triggered yet';
            # 'sleep' to be interrupted by an external signal
            sleep $timeout * 2;
        };
        alarm 0;
    };
    is $marker_signal_handling, 1, 'signal handler is triggered';

    Sub::Install::reinstall_sub( {
        code    => $original,
        into    => 'Kafka::IO',
        as      => '_gethostbyname',
    } );

    # Kafka::IO->new is correctly ended after 'timer' and before 'timeout'

    # cancel the previous timer
    alarm 0;

    $SIG{ALRM} = sub {
        ++$marker_signal_handling;
        debug_msg( 'SIGALRM: signal handler triggered' );
    };
    $timer      = 10;
    $timeout    = $timer + 10;

    Sub::Install::reinstall_sub( {
        code    => sub {
            my $self = shift;

            $self->{af} = AF_INET;
            $self->{pf} = PF_INET;
            $self->{ip} = '127.0.0.1';

            debug_msg( '_gethostbyname called (sleep ', $timer + 5, ')' );
            # 'sleep' should not be interrupted by an external signal
            sleep $timer + 5;

            return $self->{ip};
        },
        into    => 'Kafka::IO',
        as      => '_gethostbyname',
    } );

    debug_msg( "Kafka::IO->new is correctly ended after 'timer' and before 'timeout'" );
    debug_msg( "timer = $timer, timeout = $timeout, host => $hostname" );
    $marker_signal_handling = 0;
    eval {
        alarm $timer;
        eval {
            $io = Kafka::IO->new(
                host    => $hostname,
                port    => $port,
                timeout => $timeout,
            );
        };
        alarm 0;
    };
    is $marker_signal_handling, 1, 'signal handler is triggered';

    Sub::Install::reinstall_sub( {
        code    => $original,
        into    => 'Kafka::IO',
        as      => '_gethostbyname',
    } );

    debug_msg( "external 'alarm' tested" );

    #-- _is_alive

    $io = Kafka::IO->new(
        host    => $hostname,
        port    => $port,
        timeout => $REQUEST_TIMEOUT,
    );

    ok $io->_is_alive, 'socket alive';

    #-- close

    ok $io->{socket}, 'socket defined';
    $io->close;
    ok !$io->{socket}, 'socket not defined';

    #-- _is_alive

    ok !$io->_is_alive, 'socket not alive';

    #-- send

    $io = Kafka::IO->new(
        host    => $hostname,
        port    => $port,
        timeout => $REQUEST_TIMEOUT,
    );

    lives_ok { $sent = $io->send( $test_message ); } 'expecting to live';
    is $sent, length( $test_message ), 'sent '.length( $test_message ).' bytes';

    #-- receive

    lives_ok { $resp = $io->receive( length( $test_message ) ); } 'expecting to live';
    is( $$resp, $test_message, 'receive OK' );

    # ip_version

    foreach my $ip_version ( undef, $IP_V4 ) {
        $io = Kafka::IO->new(
            host        => '127.0.0.1',
            port        => $port,
            ip_version  => $ip_version,
        );
        is $io->{af}, AF_INET, 'af OK';
        is $io->{pf}, PF_INET, 'pf OK';
        is $io->{ip}, '127.0.0.1', 'ip OK';
    }

    foreach my $ip_version ( undef, $IP_V4 ) {
        $io = Kafka::IO->new(
            host        => 'localhost',
            port        => $port,
            ip_version  => $ip_version,
        );
        is $io->{af}, AF_INET, 'af OK';
        is $io->{pf}, PF_INET, 'pf OK';
        is $io->{ip}, inet_ntop( AF_INET, scalar( gethostbyname( 'localhost' ) ) ), 'ip OK';
    }

    my $host = '127.0.0.1';
    throws_ok {
        Kafka::IO->new(
            host        => $host,
            port        => $port,
            ip_version  => $IP_V6,
        );
    } 'Kafka::Exception::IO', "bad ip_version for $host";

    $host = 'localhost';
    dies_ok {
        Kafka::IO->new(
            host        => $host,
            port        => $port,
            ip_version  => $IP_V6,
        );
    } "bad ip_version for $host";

    #-- close connection

    undef $server;
    ok $io, 'IO exists';
    ok !$io->_is_alive, 'socket not alive';
#    throws_ok { $sent = $io->send( $test_message ); } 'Kafka::Exception::IO', 'error thrown';
}

undef $server;

