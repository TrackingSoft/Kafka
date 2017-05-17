#!/usr/bin/env perl

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

use Socket qw(
    getaddrinfo
    AF_INET
    AF_INET6
    PF_INET
    PF_INET6

    SOCK_STREAM
    IPPROTO_TCP

    inet_aton
    inet_ntop
    pack_sockaddr_in
);
use Net::EmptyPort qw(
    can_bind
);

use Kafka qw(
    $IP_V4
    $IP_V6
    $REQUEST_TIMEOUT
);
use Kafka::IO;

{
    package TestServer;

    use 5.010;
    use strict;
    use warnings;

    use base qw(
        Exporter
    );

    our @EXPORT = qw(
        new_sock
    );

    use IO::Socket::IP;

    sub new_sock {
        my ( $host, $port ) = @_;

        my $sock = IO::Socket::IP->new(
            LocalPort   => $port,
            LocalAddr   => $host,
            Proto       => 'tcp',
            Listen      => 5,
            Type        => SOCK_STREAM,
            V6Only      => 1,
            ReuseAddr   => 1,
        ) or die "Cannot open server socket: $!";

        return $sock;
    }

    sub new {
        my ( $class, $host, $port ) = @_;

        my $sock = new_sock( $host, $port );

        return bless { sock => $sock }, $class;
    }

    sub run {
        my ( $self, $code ) = @_;

        while ( my $remote = $self->{sock}->accept ) {
            while ( my $line = <$remote> ) {
                $code->( $remote, $line );
            }
        }
    }

    1;
}

sub is_alive_v4 {
    my ( $ip, $port ) = @_;

    socket( my $tmp_socket, PF_INET, SOCK_STREAM, IPPROTO_TCP );
    my $is_alive = connect( $tmp_socket, pack_sockaddr_in(  $port, inet_aton( $ip ) ) );
    CORE::close( $tmp_socket );

    return $is_alive;
}

sub doit {
    my ( $host, $af, $pf ) = @_;

    ok 1, 'starting the test';
    test_tcp(
        client => sub {
            my $port = shift;

            ok $port, 'test case for sharedfork (client)';

            my $io = Kafka::IO->new(
                host    => $host,
                port    => $port,
                #timeout => $REQUEST_TIMEOUT,
            );
            ok $io->_is_alive, 'socket alive';
            is $io->{af}, $af, 'Address family ok';
            is $io->{pf}, $pf, 'Protocol family ok';

            my $test_message = "Test message\n";
            my ( $sent, $resp );

            lives_ok { $sent = $io->send( $test_message ); } 'expecting to live';
            is $sent, length( $test_message ), 'sent '.length( $test_message ).' bytes';

            lives_ok { $resp = $io->receive( length( $test_message ) ); } 'expecting to live';
            is( $$resp, $test_message, 'receive OK' );

            foreach my $ip_version ( undef, $IP_V6 ) {
                $io = Kafka::IO->new(
                    host        => $host,
                    port        => $port,
                    ip_version  => $ip_version,
                );
                is $io->{af}, AF_INET6, 'af OK';
                is $io->{pf}, PF_INET6, 'pf OK';
                is $io->{ip}, $host, 'ip OK';
            }

            foreach my $hostname ( '127.0.0.1', 'localhost' ) {
                my $ip_v4;
                if ( my $ipaddr = gethostbyname( $hostname ) ) {
                    $ip_v4 = inet_ntop( AF_INET, $ipaddr );
                }
                if ( $ip_v4 && !is_alive_v4( $ip_v4, $port ) ) {
                    dies_ok {
                        my $bad_io = Kafka::IO->new(
                            host        => $hostname,
                            port        => $port,
                            ip_version  => $IP_V4,
                        );
                    } "bad ip_version for $hostname";
                }
            }

            my $host_v6 = 'ip6-localhost';

            my ( $err, @addrs ) = getaddrinfo(
                $host_v6,
                '',     # not interested in the service name
                {
                    family      => AF_INET6,
                    socktype    => SOCK_STREAM,
                    protocol    => IPPROTO_TCP,
                },
            );
            unless ( $err ) {
                # /etc/hosts contains:
                # ::1     ip6-localhost
                my $v6_io = Kafka::IO->new(
                    host        => $host_v6,
                    port        => $port,
                    ip_version  => $IP_V6,
                );
                is $v6_io->{af}, AF_INET6, 'af OK';
                is $v6_io->{pf}, PF_INET6, 'pf OK';
                is $v6_io->{ip}, '::1', 'ip OK';
            }

            foreach my $ip_version ( undef, $IP_V4 ) {
                dies_ok {
                    $io = Kafka::IO->new(
                        host        => $host_v6,
                        port        => $port,
                        ip_version  => $ip_version,
                    );
                } "bad ip_version for host_v6";
            }

            throws_ok {
                $io = Kafka::IO->new(
                    host        => '::1',
                    port        => $port,
                    ip_version  => $IP_V4,
                );
            } 'Kafka::Exception::IO', "bad ip_version for IPv6";

            ok $io->close, 'Socket closed';
        },
        server => sub {
            my $port = shift;

            ok $port, 'test case for sharedfork (server)';
            TestServer->new( $host, $port )->run( sub {
                    my ( $remote, $line, $sock ) = @_;
                    note 'new request';
                    print { $remote } $line;
                }
            );
        },
        host => $host,
    );
}





subtest 'v6' => sub {
    plan skip_all => 'IPv6 not supported'
        unless eval { Socket::IPV6_V6ONLY } && can_bind( '::1' );

    foreach my $host_name (
        '0:0:0:0:0:0:0:1',
        '::1',
        #TODO: v6 fqdn resolve test
    ) {
        doit( $host_name, AF_INET6, PF_INET6 );
    }
};

