package Kafka::Connection;

=head1 NAME

Kafka::Connection - Object interface to connect to a kafka cluster.

=head1 VERSION

This documentation refers to C<Kafka::Connection> version 0.800_5 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.800_5';

#-- load the modules -----------------------------------------------------------

use Carp;
use Const::Fast;
use List::MoreUtils qw(
    all
);
use List::Util qw(
    shuffle
);
use Params::Util qw(
    _ARRAY
    _ARRAY0
    _HASH
    _NONNEGINT
    _NUMBER
    _POSINT
    _STRING
);
use Scalar::Util::Numeric qw(
    isint
);
use Sys::Hostname;
use Time::HiRes qw(
    sleep
);
use Try::Tiny;

use Kafka qw(
    %ERROR
    $ERROR_CANNOT_GET_METADATA
    $ERROR_LEADER_NOT_FOUND
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_MISMATCH_CORRELATIONID
    $ERROR_NO_ERROR
    $ERROR_NO_KNOWN_BROKERS
    $ERROR_UNKNOWN_APIKEY
    $ERROR_UNKNOWN_TOPIC_OR_PARTITION
    $KAFKA_SERVER_PORT
    $NOT_SEND_ANY_RESPONSE
    $REQUEST_TIMEOUT
    $RETRY_BACKOFF
    $SEND_MAX_RETRIES
);
use Kafka::Exceptions;
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_METADATA
    $APIKEY_OFFSET
    $APIKEY_PRODUCE
    _get_CorrelationId
);
use Kafka::IO;
use Kafka::Protocol qw(
    $BAD_OFFSET
    decode_fetch_response
    decode_metadata_response
    decode_offset_response
    decode_produce_response
    encode_fetch_request
    encode_metadata_request
    encode_offset_request
    encode_produce_request
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

    # A simple example of Kafka::Connection usage:
    use Kafka::Connection;

    # connect to local cluster with the defaults
    my $connection;
    try {
        $connection = Kafka::Connection->new( host => 'localhost' );
    } catch {
        if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
            warn $_->message, "\n", $_->trace->as_string, "\n";
            exit;
        } else {
            die $_;
        }
    };

    # Closes the connection and cleans up
    undef $connection;

=head1 DESCRIPTION

The main features of the C<Kafka::Connection> class are:

=over 3

=item *

Provides API for communication with Kafka 0.8 cluster.

=item *

Performs requests encoding and responses decoding, provides automatic
selection or promotion of a leader server from Kafka cluster.

=item *

Provides information about Kafka cluster.

=back

=cut

my %protocol = (
    "$APIKEY_PRODUCE"   => {
        decode                  => \&decode_produce_response,
        encode                  => \&encode_produce_request,
    },
    "$APIKEY_FETCH"     => {
        decode                  => \&decode_fetch_response,
        encode                  => \&encode_fetch_request,
    },
    "$APIKEY_OFFSET"    => {
        decode                  => \&decode_offset_response,
        encode                  => \&encode_offset_request,
    },
    "$APIKEY_METADATA"  => {
        decode                  => \&decode_metadata_response,
        encode                  => \&encode_metadata_request,
    },
);

my %known_api_keys = map { $_ => 1 } (
    $APIKEY_FETCH,
    $APIKEY_OFFSET,
    $APIKEY_PRODUCE,
);

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Creates C<Kafka::Connection> object for interaction with Kafka cluster.
Returns created C<Kafka::Connection> object.

C<new()> takes arguments in key-value pairs. The following arguments are currently recognized:

=over 3

=item C<host =E<gt> $host>

C<$host> is any Apache Kafka cluster host to connect to. It can be a hostname or the
IP-address in the "xx.xx.xx.xx" form.

Optional. Either C<host> or C<broker_list> must be supplied.

=item C<port =E<gt> $port>

Optional, default = C<$KAFKA_SERVER_PORT>.

C<$port> is the attribute denoting the port number of the service we want to
access (Apache Kafka service). C<$port> should be an integer number.

C<$KAFKA_SERVER_PORT> is the default Apache Kafka server port constant (C<9092>) that can
be imported from the L<Kafka|Kafka> module.

=item C<broker_list =E<gt> $broker_list>

Optional, C<$broker_list> is a reference to array of the host:port strings, defining the list
of Kafka servers. This list will be used to locate the new leader if the server specified
via C<host =E<gt> $host> and C<port =E<gt> $port> arguments becomes unavailable. Either C<host>
or C<broker_list> must be supplied.

=item C<timeout =E<gt> $timeout>

Optional, default = C<$REQUEST_TIMEOUT>.

C<$timeout> specifies how long we wait for the remote server to respond.
C<$timeout> is in seconds, could be a positive integer or a floating-point number.

C<$REQUEST_TIMEOUT> is the default timeout that can be imported from the
L<Kafka|Kafka> module.

=item C<CorrelationId =E<gt> $correlation_id>

Optional, default = C<undef> .

C<Correlation> is a user-supplied integer. It will be passed back with the response by
the server, unmodified. The C<$correlation_id> should be an integer number.

An exception is thrown if C<CorrelationId> in response does not match the one supplied
in request.

If C<CorrelationId> is not provided, it is set to a random negative integer.

=item C<SEND_MAX_RETRIES =E<gt> $retries>

Optional, default = C<$SEND_MAX_RETRIES> .

C<$SEND_MAX_RETRIES> is the default number of retries that can be imported from the
L<Kafka|Kafka> module and = 3 .

The leader may be unavailable transiently, which can fail the sending of a message.
This property specifies the number of retries when such failures occur.
The C<$retries> should be an integer number.

=item C<RETRY_BACKOFF =E<gt> $backoff>

Optional, default = C<$RETRY_BACKOFF> .

C<$RETRY_BACKOFF> is the default timeout that can be imported from the
L<Kafka|Kafka> module and = 100 ms.

Since leader election takes a bit of time, this property specifies the amount of time,
in milliseconds, that the producer waits before refreshing the metadata.
The C<$backoff> should be an integer number.

=back

=cut
sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        host                => q{},
        port                => $KAFKA_SERVER_PORT,
        broker_list         => [],
        timeout             => $REQUEST_TIMEOUT,
        CorrelationId       => undef,
        SEND_MAX_RETRIES    => $SEND_MAX_RETRIES,
        RETRY_BACKOFF       => $RETRY_BACKOFF,
    }, $class;

    while ( @args ) {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    $self->{CorrelationId} //= _get_CorrelationId;

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'host' )
        unless defined( $self->{host} ) && ( $self->{host} eq q{} || defined( _STRING( $self->{host} ) ) ) && !utf8::is_utf8( $self->{host} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'port' )
        unless _POSINT( $self->{port} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'timeout' )
        unless _NUMBER( $self->{timeout} ) && $self->{timeout} > 0;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'broker_list' )
        unless _ARRAY0( $self->{broker_list} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'CorrelationId' )
        unless isint( $self->{CorrelationId} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'SEND_MAX_RETRIES' )
        unless _POSINT( $self->{SEND_MAX_RETRIES} );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'RETRY_BACKOFF' )
        unless _POSINT( $self->{RETRY_BACKOFF} );

    $self->{_metadata} = {};                # {
                                            #   TopicName => {
                                            #       Partition   => {
                                            #           'Leader'    => ...,
                                            #           'Replicas'  => [
                                            #               ...,
                                            #           ],
                                            #           'Isr'       => [
                                            #               ...,
                                            #           ],
                                            #       },
                                            #       ...,
                                            #   },
                                            #   ...,
                                            # }
    $self->{_leaders} = {};                 # {
                                            #   NodeId  => host:port,
                                            #   ...,
                                            # }
    my $IO_cache = $self->{_IO_cache} = {}; # host:port => {
                                            #       'NodeId'    => ...,
                                            #       'IO'        => ...,
                                            #       'timeout'   => ...,
                                            #       'host'      => ...,
                                            #       'port'      => ...,
                                            #       'error'     => ...,
                                            #   },
                                            #   ...,

    # init IO cache
    foreach my $server ( ( $self->{host} ? $self->_build_server_name( $self->{host}, $self->{port} ) : (), @{ $self->{broker_list} } ) ) {
        $self->_error( $ERROR_MISMATCH_ARGUMENT, 'bad host:port or broker_list element' )
            unless $self->_is_like_server( $server );
        my ( $host, $port ) = split /:/, $server;
        $host = $self->_localhost_to_hostname( $host );
        my $correct_server = $self->_build_server_name( $host, $port );
        $IO_cache->{ $correct_server } = {
            NodeId  => undef,
            IO      => undef,
            host    => $host,
            port    => $port,
        };
    }

    $self->_error( $ERROR_MISMATCH_ARGUMENT, 'server is not specified' )
        unless keys( %$IO_cache );

    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are defined for the C<Kafka::Producer> class:

=cut

#-- public methods -------------------------------------------------------------

=head3 C<get_known_servers>

Returns the list of known Kafka servers (in host:port format).

=cut
sub get_known_servers {
    my ( $self ) = @_;

    return keys %{ $self->{_IO_cache} };
}

=head3 C<is_server_known( $server )>

Returns true, if C<$server> (host:port) is known in cluster.

=cut
sub is_server_known {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT )
        unless $self->_is_like_server( $server );

    return exists $self->{_IO_cache}->{ $server };
}

=head3 C<is_server_alive( $server )>

Returns true, if successful connection is established with C<$server> (host:port).

=cut
sub is_server_alive {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_MISMATCH_ARGUMENT )
        unless $self->_is_like_server( $server );

    my $io_cache = $self->{_IO_cache};
    my $io;
    unless ( exists( $io_cache->{ $server } ) && ( $io = $io_cache->{ $server }->{IO} ) ) {
        return;
    }

    return $io->is_alive;
}

=head3 C<receive_response_to_request( $request )>

C<$request> is a reference to the hash representing
the structure of the request.

This method encodes C<$request>, passes it to the leader of cluster, receives reply, decodes and returns
it in a form of hash reference.

WARNING:

=over 3

=item *

This method should be considered private and should not be called by an end user.

=item *

In order to achieve better performance, this method does not perform arguments validation.

=back

=cut
sub receive_response_to_request {
    my ( $self, $request ) = @_;

    my $api_key = $request->{ApiKey};

# WARNING: The current version of the module limited to the following:
# No clear answer to the question, one leader for any combination of topic + partition, or at the same time, there are several different leaders?
# Therefore supports queries with only one combination of topic + partition (first and only).

    my $topic_data  = $request->{topics}->[0];
    my $topic_name  = $topic_data->{TopicName};
    my $partition   = $topic_data->{partitions}->[0]->{Partition};

    unless ( %{ $self->{_metadata} } ) {    # the first request
        $self->_update_metadata( $topic_name )  # hash metadata could be updated
            or $self->_error( $ERROR_CANNOT_GET_METADATA );
    }
    my $encoded_request = $protocol{ $api_key }->{encode}->( $request );

    my $CorrelationId = $request->{CorrelationId} // _get_CorrelationId;

    my $retries = $self->{SEND_MAX_RETRIES};
    my $partition_data;
    ATTEMPTS:
    while ( $retries-- ) {
        REQUEST:
        {
            if ( defined( my $leader = $self->{_metadata}->{ $topic_name }->{ $partition }->{Leader} ) ) {   # hash metadata could be updated
                my $server = $self->{_leaders}->{ $leader }
                    or $self->_error( $ERROR_LEADER_NOT_FOUND );

                # Send a request to the leader
                last REQUEST unless
                       $self->_connectIO( $server )
                    && $self->_sendIO( $server, $encoded_request );

                my $response;
                if ( $api_key == $APIKEY_PRODUCE && $request->{RequiredAcks} == $NOT_SEND_ANY_RESPONSE ) {

                    # Do not receive a response, self-forming own response
                    $response = {
                        CorrelationId                           => $CorrelationId,
                        topics                                  => [
                            {
                                TopicName                       => $topic_name,
                                partitions                      => [
                                    {
                                        Partition               => $partition,
                                        ErrorCode               => 0,
                                        Offset                  => $BAD_OFFSET,
                                    },
                                ],
                            },
                        ],
                    };
                }
                else {
                    my $encoded_response_ref = $self->_receiveIO( $server )
                        or last REQUEST;
                    $response = $protocol{ $api_key }->{decode}->( $encoded_response_ref );
                }

                $response->{CorrelationId} == $CorrelationId
                    or $self->_error( $ERROR_MISMATCH_CORRELATIONID );
                $topic_data     = $response->{topics}->[0];
                $partition_data = $topic_data->{ $api_key == $APIKEY_OFFSET ? 'PartitionOffsets' : 'partitions' }->[0];
                if ( ( my $ErrorCode = $partition_data->{ErrorCode} ) != $ERROR_NO_ERROR ) {
                    $self->_error( $ErrorCode, "topic = '".$topic_data->{TopicName}."', partition = ".$partition_data->{Partition} );
                }

                return $response;
            }
        }

        sleep $self->{RETRY_BACKOFF} / 1000;
        $self->_update_metadata( $topic_name )
            or $self->_error( $ERROR_CANNOT_GET_METADATA );
    }

    # NOTE: it is possible to repeat the operation here
    return;
}

=head3 C<close_connection( $server )>

Closes connection with C<$server> (defined as host:port).

=cut
sub close_connection {
    my ( $self, $server ) = @_;

    unless ( $self->is_server_known( $server ) ) {
        return;
    }

    $self->_closeIO( $server );
    return 1;
}

=head3 C<close>

Closes connection with all known Kafka servers.

=cut
sub close {
    my ( $self ) = @_;

    foreach my $server ( $self->get_known_servers ) {
        $self->_closeIO( $server );
    }
}

=head3 C<cluster_errors>

Returns a reference to a hash.

Each hash key is the identifier of the server (host:port), and the value is the last communication error
with that server.

An empty hash is returned if there were no communication errors.

=cut
sub cluster_errors {
    my ( $self ) = @_;

    my %errors;
    my $io_cache = $self->{_IO_cache};
    foreach my $server ( keys %$io_cache ) {
        if ( my $error = $io_cache->{ $server }->{error} ) {
            $errors{ $server } = $error;
        }
    }
    return \%errors;
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

# Returns identifier of the cluster leader (host:port)
sub _find_leader_server {
    my ( $self, $node_id ) = @_;

    my $leader_server;
    my $IO_cache = $self->{_IO_cache};
    my $NodeId;
    foreach my $server ( keys %$IO_cache ) {
        $NodeId = $IO_cache->{ $server }->{NodeId};
        if ( defined( $NodeId ) && $NodeId == $node_id ) {
            $leader_server = $server;
            last;
        }
    }

    return $leader_server;
}

# Form a list of servers to attempt querying of the metadata
sub _get_interviewed_servers {
    my ( $self ) = @_;

    my ( @priority, @secondary, @rest );
    my $IO_cache = $self->{_IO_cache};
    my $server_data;
    foreach my $server ( $self->get_known_servers ) {
        $server_data = $IO_cache->{ $server };
        if ( defined $server_data->{NodeId} ) {
            if ( $server_data->{IO} ) {
                push @priority, $server;
            }
            else {
                push @secondary, $server;
            }
        }
        else {
            push @rest, $server;
        }
    }

    return( shuffle( @priority ), shuffle( @secondary ), shuffle( @rest ) );
}

# Refresh metadata for given topic
sub _update_metadata {
    my ( $self, $topic ) = @_;

    my $CorrelationId = $self->{CorrelationId};
    my $encoded_request = $protocol{ $APIKEY_METADATA }->{encode}->( {
            CorrelationId   => $CorrelationId,
            ClientId        => q{},
            topics          => [
                $topic,
            ],
        } );

    my $encoded_response_ref;
    my @brokers = $self->_get_interviewed_servers;

    # receive metadata
    foreach my $broker ( @brokers ) {
        last if  $self->_connectIO( $broker )
            &&   $self->_sendIO( $broker, $encoded_request )
            && ( $encoded_response_ref = $self->_receiveIO( $broker ) );
    }

    unless ( $encoded_response_ref ) {
        # NOTE: it is possible to repeat the operation here
        return;
    }

    my $decoded_response = $protocol{ $APIKEY_METADATA }->{decode}->( $encoded_response_ref );
    $decoded_response->{CorrelationId} == $CorrelationId
        or $self->_error( $ERROR_MISMATCH_CORRELATIONID );

    _ARRAY( $decoded_response->{Broker} )
        or $self->_error( $ERROR_NO_KNOWN_BROKERS );

    my $IO_cache = $self->{_IO_cache};

    # Clear the previous information about the NodeId in the IO cache
    $IO_cache->{ $_ }->{NodeId} = undef for @brokers;

    #  In the IO cache update/add obtained server information
    my $server;
    foreach my $received_broker ( @{ $decoded_response->{Broker} } ) {
        $server = $self->_build_server_name( @{ $received_broker }{ 'Host', 'Port' } );
        $IO_cache->{ $server } = {                      # can add new servers
            IO      => $IO_cache->{ $server }->{IO},    # IO or undef
            NodeId  => $received_broker->{NodeId},
            host    => $received_broker->{Host},
            port    => $received_broker->{Port},
        };
    }

    #NOTE: IO cache does not remove server that's missing in metadata

    # Collect the received metadata
    my $received_metadata   = {};
    my $leaders             = {};
    foreach my $topic_metadata ( @{ $decoded_response->{TopicMetadata} } ) {
        my $TopicName = $topic_metadata->{TopicName};
        if ( ( my $topic_ErrorCode = $topic_metadata->{ErrorCode} ) != $ERROR_NO_ERROR ) {
            $self->_error( $topic_ErrorCode, "topic = '$TopicName'" );
        }

        foreach my $partition_metadata ( @{ $topic_metadata->{PartitionMetadata} } ) {
            my $partition = $partition_metadata->{Partition};
            if ( ( my $partition_ErrorCode = $partition_metadata->{ErrorCode} ) != $ERROR_NO_ERROR ) {
                $self->_error( $partition_ErrorCode, "topic = '$TopicName', partition = $partition" );
            }

            my $received_partition_data = $received_metadata->{ $TopicName }->{ $partition } = {};
            my $leader = $received_partition_data->{Leader} = $partition_metadata->{Leader};
            $received_partition_data->{Replicas}            = [ @{ $partition_metadata->{Replicas} } ];
            $received_partition_data->{Isr}                 = [ @{ $partition_metadata->{Isr} } ];

            $leaders->{ $leader } = $self->_find_leader_server( $leader );
        }
    }

    %$received_metadata
        or $self->_error( $ERROR_CANNOT_GET_METADATA );

    # Replace the information in the metadata
    $self->{_metadata}  = $received_metadata;
    $self->{_leaders}   = $leaders;

    return 1;
}

# forms server identifier using supplied $host, $port
sub _build_server_name {
    my ( $self, $host, $port ) = @_;

    return "$host:$port";
}

# remembers error communicating with the server
sub _on_io_error {
    my ( $self, $server_data, $error ) = @_;

    $server_data->{error}   = $error;
    $server_data->{IO}      = undef;
}

# connects to a server (host:port)
sub _connectIO {
    my ( $self, $server ) = @_;

    my $server_data = $self->{_IO_cache}->{ $server };
    my $io;
    unless ( $server_data && ( $io = $server_data->{IO} ) && $io->is_alive ) {
        try {
            $server_data->{IO} = Kafka::IO->new(
                host        => $server_data->{host},
                port        => $server_data->{port},
                timeout     => $self->{timeout},
            );
            $server_data->{error}   = undef;
        } catch {
            # NOTE: it is possible to repeat the operation here
            $self->_on_io_error( $server_data, $_ );
            return;
        };
    }
    return $server_data->{IO};
}

# Send encoded request ($encoded_request) to server ($server)
sub _sendIO {
    my ( $self, $server, $encoded_request ) = @_;

    my $server_data = $self->{_IO_cache}->{ $server };
    my $sent;
    try {
        $sent = $server_data->{IO}->send( $encoded_request );
    } catch {
        # NOTE: it is possible to repeat the operation here
        $self->_on_io_error( $server_data, $_ );
    };
    return $sent;
}

# Receive response from a given server
sub _receiveIO {
    my ( $self, $server ) = @_;

    my $server_data = $self->{_IO_cache}->{ $server };
    my $response_ref;
    try {
        my $io = $server_data->{IO};
        $response_ref   = $io->receive( 4 );
        $$response_ref .= ${ $io->receive( unpack( 'l>', $$response_ref ) ) };
    } catch {
        # NOTE: it is possible to repeat the operation here
        $self->_on_io_error( $server_data, $_ );
    };
    return $response_ref;
}

# Close connectino to $server
sub _closeIO {
    my ( $self, $server ) = @_;

    if ( my $server_data = $self->{_IO_cache}->{ $server } ) {
        if ( my $io = $server_data->{IO} ) {
            $io->close;
            $server_data->{error}   = undef;
            $server_data->{IO}      = undef;
        }
    }
}

# check validity of an argument (to match host:port format)
sub _is_like_server {
    my ( $self, $server ) = @_;

    unless (
           defined( $server )
        && defined( _STRING( $server ) )
        && !utf8::is_utf8( $server )
        && $server =~ /^[^:]+:\d+$/
        ) {
        return;
    }

    return $server;
}

# transforms localhost because metadata using the 'Host' defined by hostname
sub _localhost_to_hostname {
    my ( $self, $host ) = @_;

    return $host =~ /^(?:localhost|127\.0\.0\.1)$/i ? hostname : $host;
}

# Handler for errors
sub _error {
    my $self = shift;

    Kafka::Exception::Connection->throw( throw_args( @_ ) );
}

#-- Closes and cleans up -------------------------------------------------------

1;

__END__

=head1 DIAGNOSTICS

When error is detected, an exception, represented by object of C<Kafka::Exception::Connection> class,
is thrown (see L<Kafka::Exceptions|Kafka::Exceptions>).

L<code|Kafka::Exceptions/code> and a more descriptive L<message|Kafka::Exceptions/message> provide
information about exception. Consult documentation of the L<Kafka::Exceptions|Kafka::Exceptions>
for the list of all available methods.

=over 3

=item C<Invalid argument>

Invalid argument was provided to C<new> L<constructor|/CONSTRUCTOR> or to other L<method|/METHODS>.

=item C<Can't send>

Request cannot be sent to Kafka.

=item C<Can't recv>

Response cannot be received from Kafka.

=item C<Can't bind>

A successful TCP connection can't be established on given host and port.

=item C<Can't get metadata>

Error detected during parsing of response from Kafka.

=item C<Leader not found>

Failed to locate leader of Kafka cluster.

=item C<Mismatch CorrelationId>

Mismatch of C<CorrelationId> of request and response.

=item C<There are no known brokers>

Failed to locate cluster broker.

=item C<Can't get metadata>

Received meta data is incorrect or missing.

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

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

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
