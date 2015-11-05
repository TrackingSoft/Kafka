#!/usr/bin/env perl

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
);

# ENVIRONMENT ------------------------------------------------------------------

use Test::More;

#-- verify load the module

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

#-- load the modules -----------------------------------------------------------

use Socket qw(
    AF_INET
    AF_INET6
    PF_INET
    PF_INET6
);
use Net::EmptyPort qw(
    can_bind
);

use Kafka::IO;

#-- setting up facilities ------------------------------------------------------

package TestServer {
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

#-- declarations ---------------------------------------------------------------

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
            );
            ok $io->is_alive, 'socket alive';
            is $io->{af}, $af, 'Address family ok';
            is $io->{pf}, $pf, 'Protocol family ok';

            my $test_message = "Test message\n";
            my ( $sent, $resp );

            lives_ok { $sent = $io->send( $test_message ); } 'expecting to live';
            is $sent, length( $test_message ), 'sent '.length( $test_message ).' bytes';

            lives_ok { $resp = $io->receive( length( $test_message ) ); } 'expecting to live';
            is( $$resp, $test_message, 'receive OK' );

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

#-- Global data ----------------------------------------------------------------

# INSTRUCTIONS -----------------------------------------------------------------

#subtest 'v4' => sub {
#    foreach my $host_name (
#        'localhost',
#        '127.0.0.1',
#    ) {
#        doit( $host_name, AF_INET, PF_INET );
#    }
#};

subtest 'v6' => sub {
    plan skip_all => 'IPv6 not supported'
        unless eval { Socket::IPV6_V6ONLY } && can_bind( '::1' );

    foreach my $host_name (
        '0:0:0:0:0:0:0:1',
        '::1',
    ) {
        doit( $host_name, AF_INET6, PF_INET6 );
    }
};

# POSTCONDITIONS ---------------------------------------------------------------
