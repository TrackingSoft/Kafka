package Kafka::Connection;

# Kafka allows you to produce and consume messages using the Apache Kafka distributed publish/subscribe messaging service.

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# PRECONDITIONS ----------------------------------------------------------------

our $VERSION = '0.8001';

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
    _INSTANCE
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
    usleep
    );

use Kafka qw(
    %ERROR
    $ERROR_CANNOT_GET_METADATA
    $ERROR_DESCRIPTION_LEADER_NOT_FOUND
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
use Kafka::Internals qw(
    $APIKEY_FETCH
    $APIKEY_METADATA
    $APIKEY_OFFSET
    $APIKEY_PRODUCE
    _get_CorrelationId
    last_error
    last_errorcode
    RaiseError
    _error
    _set_error
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

our $_package_error;

#-- constructor ----------------------------------------------------------------

sub new {
    my ( $class, @args ) = @_;

    my $self = bless {
        host                => q{},
        port                => $KAFKA_SERVER_PORT,
        broker_list         => [],
        timeout             => $REQUEST_TIMEOUT,
        RaiseError          => 0,
        CorrelationId       => undef,
        SEND_MAX_RETRIES    => $SEND_MAX_RETRIES,
        RETRY_BACKOFF       => $RETRY_BACKOFF,
    }, $class;

    while ( @args ) {
        my $k = shift @args;
        $self->{ $k } = shift @args if exists $self->{ $k };
    }

    $self->_error( $ERROR_NO_ERROR );
    $self->{CorrelationId} //= _get_CorrelationId;

    if    ( !defined _NONNEGINT( $self->RaiseError ) )                          { $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - RaiseError' ); }
    elsif ( !( $self->{host} eq q{} || _STRING( $self->{host} ) ) )             { $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - host' ); }
    elsif ( !_POSINT( $self->{port} ) )                                         { $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - port' ); }
    elsif ( !( _NUMBER( $self->{timeout} ) && $self->{timeout} > 0 ) )          { $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - timeout' ); }
    elsif ( !_ARRAY0( $self->{broker_list} ) )                                  { $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - broker_list' ); }
    elsif ( !all { $self->_is_like_server( $_ ) } @{ $self->{broker_list} } )   { $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - broker_list' ); }
    elsif ( !isint( $self->{CorrelationId} ) )                                  { $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - CorrelationId' ); }
    elsif ( !_POSINT( $self->{SEND_MAX_RETRIES} ) )                             { $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - SEND_MAX_RETRIES' ); }
    elsif ( !_POSINT( $self->{RETRY_BACKOFF} ) )                                { $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - RETRY_BACKOFF' ); }
    else {
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
                                                #   },
                                                #   ...,

        # init IO cache
        foreach my $server ( ( $self->{host} ? $self->_build_server_name( $self->{host}, $self->{port} ) : (), @{ $self->{broker_list} } ) ) {
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

        keys( %$IO_cache )
            or return $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->new - server is not specified' );
    }

    return $self;
}

#-- public attributes ----------------------------------------------------------

#-- public methods -------------------------------------------------------------

sub get_known_servers {
    my ( $self ) = @_;

    return keys %{ $self->{_IO_cache} };
}

sub is_server_known {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_NO_ERROR );

    if ( $self->_is_like_server( $server ) ) {
        return exists $self->{_IO_cache}->{ $server };
    }
    else {
        return $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->is_server_known' );
    }
}

sub is_server_alive {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_NO_ERROR );

    if ( $self->_is_like_server( $server ) ) {
        if ( $self->is_server_known( $server ) && ( my $io = $self->{_IO_cache}->{ $server }->{IO} ) ) {
            return $io->is_alive;
        }
    }
    else {
        $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->is_server_alive' );
    }
    return;
}

sub receive_response_to_request {
    my ( $self, $request ) = @_;

    _HASH( $request ) && exists( $request->{ApiKey} )   # The ApiKey must be present in the structure of the request
        or return $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->receive_response_to_request' );

    my $api_key = $request->{ApiKey};
    $known_api_keys{ $api_key }
        or return $self->_error( $ERROR_UNKNOWN_APIKEY );

# WARNING: The current version of the module limited to the following:
# No clear answer to the question, one leader for any combination of topic + partition, or at the same time, there are several different leaders?
# Therefore supports queries with only one combination of topic + partition (first and only).
    my ( $topic, $partition );
    $topic = $request->{topics}->[0]->{TopicName}
        // return $self->_error( $ERROR_UNKNOWN_TOPIC_OR_PARTITION );
    $partition = $request->{topics}->[0]->{partitions}->[0]->{Partition}
        // return $self->_error( $ERROR_UNKNOWN_TOPIC_OR_PARTITION );

    $self->_error( $ERROR_NO_ERROR );


    $self->_update_metadata( $topic ) unless %{ $self->{_metadata} };   # the first request
    %{ $self->{_metadata} } # hash metadata could be updated
        or return $self->_error( $ERROR_CANNOT_GET_METADATA );
    my $encoded_request = $protocol{ $api_key }->{encode}->( $request )
        or return $self->_error( Kafka::Protocol::last_errorcode, Kafka::Protocol::last_error );

    my $CorrelationId = $request->{CorrelationId} // _get_CorrelationId;

    my $retries = $self->{SEND_MAX_RETRIES};
    ATTEMPTS:
    while ( $retries-- ) {
        REQUEST:
        {
            if ( defined( my $leader = $self->{_metadata}->{ $topic }->{ $partition }->{Leader} ) ) {   # hash metadata could be updated
                my $server = $self->{_leaders}->{ $leader }
                    or return $self->_error( $ERROR_DESCRIPTION_LEADER_NOT_FOUND );

                # Send a request to the leader
                last REQUEST if
                       !$self->_connectIO( $server )
                    || !$self->_sendIO( $server, $encoded_request );

                my $response;
                if ( $api_key == $APIKEY_PRODUCE && $request->{RequiredAcks} == $NOT_SEND_ANY_RESPONSE ) {

                    # Do not receive a response, self-forming own response
                    $response = {
                        CorrelationId                           => $CorrelationId,
                        topics                                  => [
                            {
                                TopicName                       => $topic,
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
                    $response = $protocol{ $api_key }->{decode}->( $encoded_response_ref )
                        or return $self->_error( Kafka::Protocol::last_errorcode, Kafka::Protocol::last_error );
                }

                $response->{CorrelationId} == $CorrelationId
                    or return $self->_error( $ERROR_MISMATCH_CORRELATIONID );
                my $topic_data      = $response->{topics}->[0];
                my $partition_data  = $topic_data->{ $api_key == $APIKEY_OFFSET ? 'PartitionOffsets' : 'partitions' }->[0];
                if ( ( my $ErrorCode = $partition_data->{ErrorCode} ) != $ERROR_NO_ERROR ) {
                    return $self->_error( $ErrorCode, "topic = '".$topic_data->{TopicName}."', partition = ".$partition_data->{Partition} );
                }

                return $response;
            }
        }

        sleep $self->{RETRY_BACKOFF} / 1000;
        $self->_update_metadata( $topic );
    }

# NOTE: Here is possible, for example to repeat the operation
    return;     # IO error and !RaiseError
}

sub close_connection {
    my ( $self, $server ) = @_;

    $self->_error( $ERROR_NO_ERROR );

    return $self->_error( $ERROR_MISMATCH_ARGUMENT, __PACKAGE__.'->close_connection' )
        unless $self->_is_like_server( $server );

    if ( $self->is_server_known( $server ) ) {
        $self->_closeIO( $server );
        return 1;
    }
    return;
}

# WARNING: the connections can be used by other instances of the class Kafka::Connection
sub close {
    my ( $self ) = @_;

    $self->_error( $ERROR_NO_ERROR );

    foreach my $server ( $self->get_known_servers ) {
        $self->_closeIO( $server );
    }
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

sub _find_leader_server {
    my ( $self, $node_id ) = @_;

    my $leader_server;
    my $IO_cache = $self->{_IO_cache};
    foreach my $server ( keys %$IO_cache ) {
        my $NodeId = $IO_cache->{ $server }->{NodeId};
        if ( defined( $NodeId ) && $NodeId == $node_id ) {
            $leader_server = $server;
            last;
        }
    }

    return $leader_server;
}

# Form a list of servers to attempt to query the metadata
sub _get_interviewed_servers {
    my ( $self ) = @_;

    my ( @priority, @secondary, @rest );
    my $IO_cache = $self->{_IO_cache};
    foreach my $server ( $self->get_known_servers ) {
        my $server_data = $IO_cache->{ $server };
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

sub _update_metadata {
    my ( $self, $topic ) = @_;

    my $CorrelationId = $self->{CorrelationId};
    my $encoded_request = $protocol{ $APIKEY_METADATA }->{encode}->( {
            CorrelationId   => $CorrelationId,
            ClientId        => q{},
            topics          => [
                $topic,
            ],
        } )
        or return $self->_error( Kafka::Protocol::last_errorcode, Kafka::Protocol::last_error );

    my $encoded_response_ref;
    my @brokers = $self->_get_interviewed_servers;

    # receive metadata
    foreach my $broker ( @brokers ) {
        last if  $self->_connectIO( $broker )
            &&   $self->_sendIO( $broker, $encoded_request )
            && ( $encoded_response_ref = $self->_receiveIO( $broker ) );
    }

    unless ( $encoded_response_ref ) {  # IO error and !RaiseError
# NOTE: Here is possible, for example to repeat the operation
        return;
    }

    my $decoded_response = $protocol{ $APIKEY_METADATA }->{decode}->( $encoded_response_ref )
        or return $self->_error( Kafka::Protocol::last_errorcode, Kafka::Protocol::last_error );
    $decoded_response->{CorrelationId} == $CorrelationId
        or return $self->_error( $ERROR_MISMATCH_CORRELATIONID );

    return $self->_error( $ERROR_NO_KNOWN_BROKERS )
        unless _ARRAY( $decoded_response->{Broker} );

    my $IO_cache = $self->{_IO_cache};

    # Clear the previous information about the NodeId in the IO cache
    $IO_cache->{ $_ }->{NodeId} = undef for @brokers;

    #  In the IO cache update/add obtained server information
    foreach my $received_broker ( @{ $decoded_response->{Broker} } ) {
        my $server = $self->_build_server_name( @{ $received_broker }{ 'Host', 'Port' } );
        $IO_cache->{ $server } = {                      # can add new servers
            IO      => $IO_cache->{ $server }->{IO},    # IO or undef
            NodeId  => $received_broker->{NodeId},
            host    => $received_broker->{Host},
            port    => $received_broker->{Port},
        };
    }

    #NOTE: In the IO cache not remove server records missing in the received metadata

    # Collect the received metadata
    my ( $received_metadata, $leaders ) = ( {}, {} );
    foreach my $topic_metadata ( @{ $decoded_response->{TopicMetadata} } ) {
        my $TopicName = $topic_metadata->{TopicName};
        if ( ( my $topic_ErrorCode = $topic_metadata->{ErrorCode} ) != $ERROR_NO_ERROR ) {
            return $self->_error( $topic_ErrorCode, "topic = '$TopicName'" );
        }

        foreach my $partition_metadata ( @{ $topic_metadata->{PartitionMetadata} } ) {
            my $partition = $partition_metadata->{Partition};
            if ( ( my $partition_ErrorCode = $partition_metadata->{ErrorCode} ) != $ERROR_NO_ERROR ) {
                return $self->_error( $partition_ErrorCode, "topic = '$TopicName', partition = $partition" );
            }

            my $received_partition_data = $received_metadata->{ $TopicName }->{ $partition } = {};
            my $leader = $received_partition_data->{Leader} = $partition_metadata->{Leader};
            $received_partition_data->{Replicas}            = [ @{ $partition_metadata->{Replicas} } ];
            $received_partition_data->{Isr}                 = [ @{ $partition_metadata->{Isr} } ];

            $leaders->{ $leader } = $self->_find_leader_server( $leader );
        }
    }

    return $self->_error( $ERROR_CANNOT_GET_METADATA )
        unless %$received_metadata;

    # Replace the information in the metadata
    $self->{_metadata}  = $received_metadata;
    $self->{_leaders}   = $leaders;
    return 1;
}

sub _build_server_name {
    my ( $self, $host, $port ) = @_;

    return "$host:$port";
}

sub _connectIO {
    my ( $self, $server ) = @_;

    my $server_data = $self->{_IO_cache}->{ $server };
    unless ( $server_data && $server_data->{IO} && $self->is_server_alive( $server ) ) {
        my $io = $server_data->{IO} = Kafka::IO->new(
            host        => $server_data->{host},
            port        => $server_data->{port},
            timeout     => $self->{timeout},
        );
        if ( $io->last_errorcode != $ERROR_NO_ERROR ) {
# NOTE: Here is possible, for example to repeat the operation
            return $self->_io_error( $server );
        }
    }

    return $server_data->{IO};
}

sub _sendIO {
    my ( $self, $server, $encoded_request ) = @_;

    if ( $self->{_IO_cache}->{ $server }->{IO}->send( $encoded_request ) ) {
        return 1;
    }
    else {
# NOTE: Here is possible, for example to repeat the operation
        return $self->_io_error( $server );
    }
}

sub _receiveIO {
    my ( $self, $server ) = @_;

    my $io = $self->{_IO_cache}->{ $server }->{IO};
    my ( $response_ref, $tail_ref );
    if (   ( $response_ref  = $io->receive( 4 ) )                               && $$response_ref
        && ( $tail_ref      = $io->receive( unpack( 'l>', $$response_ref ) ) )  && $$tail_ref
        ) {

        $$response_ref .= $$tail_ref;
        return $response_ref;
    }
    else {
# NOTE: Here is possible, for example to repeat the operation
        return $self->_io_error( $server );
    }
}

sub _closeIO {
    my ( $self, $server ) = @_;

    if ( my $server_data = $self->{_IO_cache}->{ $server } ) {
        if ( my $io = $server_data->{IO} ) {
            $io->close;
            $server_data->{IO} = undef;
        }
    }
}

sub _is_like_server {
    my ( $self, $server ) = @_;

    return $server if _STRING( $server ) && $server =~ /^[^:]+:\d+$/;
}

# necessary because metadata using the 'Host' defined by hostname
sub _localhost_to_hostname {
    my ( $self, $host ) = @_;

    return $host =~ /^(localhost|127\.0\.0\.1)$/i ? hostname : $host;
}

sub _io_error {
    my ( $self, $server ) = @_;

    my $server_data = $self->{_IO_cache}->{ $server };
    my $io          = $server_data->{IO};
    my $errorcode   = $io->last_errorcode;

    return if $errorcode == $ERROR_NO_ERROR;

    $self->_set_error( $errorcode, $io->last_error.' ('.$self->_build_server_name( $server_data->{host}, $server_data->{port} ).')' );

    $self->_closeIO( $server );

    return
        unless $self->RaiseError;

    if    ( $errorcode == $ERROR_MISMATCH_ARGUMENT )    { confess $self->last_error; }
    elsif ( $errorcode == $ERROR_NO_ERROR )             { return; }
    else                                                { die $self->last_error; }
}

#-- Closes and cleans up -------------------------------------------------------

1;

__END__
