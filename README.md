# NAME

Kafka - Apache Kafka low-level synchronous API, which does not use Zookeeper.

# VERSION

This documentation refers to `Kafka` package version 1.07 .

# SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka qw(
        $BITS64
    );
    use Kafka::Connection;
    use Kafka::Producer;
    use Kafka::Consumer;

    # A simple example of Kafka usage

    # common information
    say 'This is Kafka package ', $Kafka::VERSION;
    say 'You have a ', $BITS64 ? '64' : '32', ' bit system';

    my ( $connection, $producer, $consumer );
    try {

        #-- Connect to local cluster
        $connection = Kafka::Connection->new( host => 'localhost' );
        #-- Producer
        $producer = Kafka::Producer->new( Connection => $connection );
        #-- Consumer
        $consumer = Kafka::Consumer->new( Connection  => $connection );

    } catch {
        my $error = $_;
        if ( blessed( $error ) && $error->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $error->code, ') ',  $error->message, "\n";
            exit;
        } else {
            die $error;
        }
    };

    # cleaning up
    undef $consumer;
    undef $producer;
    $connection->close;
    undef $connection;

    # another brief code example of the Kafka package
    # is provided in the "An Example" section.

# ABSTRACT

The Kafka package is a set of Perl modules which provides a simple and
consistent application programming interface (API) to Apache Kafka 0.9+,
a high-throughput distributed messaging system.

# DESCRIPTION

The user modules in this package provide an object oriented API.
The IO agents, requests sent, and responses received from the Apache Kafka
or mock servers are all represented by objects.
This makes a simple and powerful interface to these services.

The main features of the package are:

- Contains various reusable components (modules) that can be used separately
or together.
- Provides an object oriented model of communication.
- Supports parsing the Apache Kafka protocol.
- Supports the Apache Kafka Requests and Responses. Within this package the
following implements of Kafka's protocol are implemented: PRODUCE, FETCH,
OFFSETS, and METADATA.
- Simple producer and consumer clients.
- A simple interface to control the test Kafka server cluster
(in the test directory).
- Simple mock server instance (located in the test directory) for testing without
Apache Kafka server.
- Support for working with 64 bit elements of the Kafka protocol on 32 bit systems.
- Taint mode support.
The input data is not checked for tainted.
Returns untainted data.

# APACHE KAFKA'S STYLE COMMUNICATION

The Kafka package is based on Kafka's 0.9+ Protocol specification document at
[https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)

- The Kafka's protocol is based on a request/response paradigm.
A client establishes a connection with a server and sends a request to the
server in the form of a request method, followed by a messages containing
request modifiers. The server responds with a success or error code,
followed by a messages containing entity meta-information and content.

    Messages are the fundamental unit of communication. They are published to
    a topic by a producer, which means they are physically sent to a server acting
    as a broker. Some number of consumers subscribe to a topic, and each published
    message is delivered to all the consumers.
    The messages stream is partitioned on the brokers as a set of distinct
    partitions. The semantic meaning of these partitions is left up to the producer
    and the producer specifies which partition a message belongs to. Within
    a partition the messages are stored in the order in which they arrive at the
    broker, and will be given out to consumers in that same order.
    In Apache Kafka, the consumers are responsible for maintaining state information
    (offset) on what has been consumed.
    A consumer can deliberately rewind back to an old offset and re-consume data.
    Each message is uniquely identified by a 64-bit integer offset giving the
    position of the start of this message in the stream of all messages ever sent
    to that topic on that partition.
    Reads are done by giving the 64-bit logical offset of a message and a max
    chunk size.

    The request is then passed through the client to a server and we get the
    response in return to a consumer request that we can examine.
    A request is always independent of any previous requests, i.e. the service
    is stateless.
    This API is completely stateless, with the topic and partition being passed in
    on every request.

## The Connection Object

Clients use the Connection object to communicate with the Apache Kafka cluster.
The Connection object is an interface layer between your application code and
the Apache Kafka cluster.

Connection object is required to create instances of classes
[Kafka::Producer](https://metacpan.org/pod/Kafka%3A%3AProducer) or [Kafka::Consumer](https://metacpan.org/pod/Kafka%3A%3AConsumer).

Kafka Connection API is implemented by [Kafka::Connection](https://metacpan.org/pod/Kafka%3A%3AConnection) class.

    use Kafka::Connection;

    # connect to local cluster with the defaults
    my $connection = Kafka::Connection->new( host => 'localhost' );

The main attributes of the Connection object are:

- **host** and **port** are the IO object attributes denoting any server from the Kafka
cluster a client wants to connect.
- **timeout** specifies how much time remote servers is given to respond before
disconnection occurs and internal exception is thrown.

## The IO Object

The [Kafka::Connection](https://metacpan.org/pod/Kafka%3A%3AConnection) object use internal class [Kafka::IO](https://metacpan.org/pod/Kafka%3A%3AIO)
to maintain communication with the particular server of Kafka cluster
The IO object is an interface layer between [Kafka::Connection](https://metacpan.org/pod/Kafka%3A%3AConnection) object and
the network.

Kafka IO API is implemented by [Kafka::IO](https://metacpan.org/pod/Kafka%3A%3AIO) class. Note that end user
normally should have no need to use [Kafka::IO](https://metacpan.org/pod/Kafka%3A%3AIO) but work with
[Kafka::Connection](https://metacpan.org/pod/Kafka%3A%3AConnection) instead.

    use Kafka::IO;

    # connect to local server with the defaults
    my $io = Kafka::IO->new( host => 'localhost' );

The main attributes of the IO object are:

- **host** and **port** are the IO object attributes denoting the server and
the port of Apache Kafka server.
- **timeout** specifies how much time is given remote servers to respond before
the IO object disconnects and generates an internal exception.

## The Producer Object

Kafka producer API is implemented by [Kafka::Producer](https://metacpan.org/pod/Kafka%3A%3AProducer) class.

    use Kafka::Producer;

    #-- Producer
    my $producer = Kafka::Producer->new( Connection => $connection );

    # Sending a single message
    $producer->send(
        'mytopic',          # topic
        0,                  # partition
        'Single message'    # message
    );

    # Sending a series of messages
    $producer->send(
        'mytopic',          # topic
        0,                  # partition
        [                   # messages
            'The first message',
            'The second message',
            'The third message',
        ]
    );

The main methods and attributes of the producer request are:

- The request method of the producer object is `send()`.
- **topic** and **partition** define respective parameters of the **messages** we
want to send.
- **messages** is an arbitrary amount of data (a simple data string or reference to
an array of the data strings).

## The Consumer Object

Kafka consumer API is implemented by [Kafka::Consumer](https://metacpan.org/pod/Kafka%3A%3AConsumer) class.

    use Kafka::Consumer;

    $consumer = Kafka::Consumer->new( Connection => $connection );

The request methods of the consumer object are `offsets()` and `fetch()`.

`offsets` method returns a reference to the list of offsets of received messages.

`fetch` method returns a reference to the list of received
[Kafka::Message](https://metacpan.org/pod/Kafka%3A%3AMessage) objects.

    use Kafka qw(
        $DEFAULT_MAX_BYTES
        $DEFAULT_MAX_NUMBER_OF_OFFSETS
        $RECEIVE_EARLIEST_OFFSET
    );

    # Get a list of valid offsets up to max_number before the given time
    my $offsets = $consumer->offsets(
        'mytopic',                      # topic
        0,                              # partition
        $RECEIVE_EARLIEST_OFFSET,      # time
        $DEFAULT_MAX_NUMBER_OF_OFFSETS  # max_number
    );
    say "Received offset: $_" foreach @$offsets;

    # Consuming messages
    my $messages = $consumer->fetch(
        'mytopic',                      # topic
        0,                              # partition
        0,                              # offset
        $DEFAULT_MAX_BYTES              # Maximum size of MESSAGE(s) to receive
    );
    foreach my $message ( @$messages ) {
        if ( $message->valid ) {
            say 'payload    : ', $message->payload;
            say 'key        : ', $message->key;
            say 'offset     : ', $message->offset;
            say 'next_offset: ', $message->next_offset;
        } else {
            say 'error      : ', $message->error;
        }
    }

See [Kafka::Consumer](https://metacpan.org/pod/Kafka%3A%3AConsumer) for additional information and documentation about
class methods and arguments.

## The Message Object

Kafka message API is implemented by [Kafka::Message](https://metacpan.org/pod/Kafka%3A%3AMessage) class.

    if ( $message->valid ) {
        say 'payload    : ', $message->payload;
        say 'key        : ', $message->key;
        say 'offset     : ', $message->offset;
        say 'next_offset: ', $message->next_offset;
    } else {
        say 'error      : ', $message->error;
    }

Methods available for [Kafka::Message](https://metacpan.org/pod/Kafka%3A%3AMessage) object :

- `payload` A simple message received from the Apache Kafka server.
- `key` An optional message key that was used for partition assignment.
- `valid` A message entry is valid.
- `error` A description of the message inconsistence.
- `offset` The offset beginning of the message in the Apache Kafka server.
- `next_offset` The offset beginning of the next message in the Apache Kafka
server.

## The Exception Object

A designated class `Kafka::Exception` is used to provide a more detailed and
structured information when error is detected.

The following attributes are declared within `Kafka::Exception`:
[code](https://metacpan.org/pod/Kafka%3A%3AExceptions#code), [message](https://metacpan.org/pod/Kafka%3A%3AExceptions#message).

Additional subclasses of `Kafka::Exception` designed to report errors in respective
Kafka classes:
`Kafka::Exception::Connection`,
`Kafka::Exception::Consumer`,
`Kafka::Exception::IO`,
`Kafka::Exception::Int64`,
`Kafka::Exception::Producer`.

Authors suggest using of [Try::Tiny](https://metacpan.org/pod/Try%3A%3ATiny)'s `try` and `catch` to handle exceptions while
working with Kafka module.

# EXPORT

None by default.

## Additional constants

Additional constants are available for import, which can be used to define some
type of parameters, and to identify various error cases.

- `$KAFKA_SERVER_PORT`

    default Apache Kafka server port - 9092.

- `$REQUEST_TIMEOUT`

    1.5 sec - timeout in secs, for `gethostbyname`, `connect`, blocking `receive` and
    `send` calls (could be any integer or floating-point type).

- `$DEFAULT_MAX_BYTES`

    1MB - maximum size of message(s) to receive.

- `$SEND_MAX_ATTEMPTS`

    4 - The leader may be unavailable transiently, which can fail the sending of a message.
    This property specifies the number of attempts to send of a message.

    Do not use `$Kafka::SEND_MAX_ATTEMPTS` in `Kafka::Producer-<gt`send> request to prevent duplicates.

- `$RETRY_BACKOFF`

    200 - (ms)

    According to Apache Kafka documentation:

    Producer Configs -
    Before each retry, the producer refreshes the metadata of relevant topics.
    Since leader election takes a bit of time, this property specifies the amount of time
    that the producer waits before refreshing the metadata.

    Consumer Configs -
    Backoff time to wait before trying to determine the leader of a partition that has just lost its leader.

- `$RECEIVE_LATEST_OFFSET`

    **DEPRECATED**: please use `$RECEIVE_LATEST_OFFSETS`, as when using this
    constant to retrieve offsets, you can get more than one. It's kept for backward
    compatibility.

    \-1 : special value that denotes latest available offset.

- `$RECEIVE_LATEST_OFFSETS`

    \-1 : special value that denotes latest available offsets.

- `$RECEIVE_EARLIEST_OFFSET`

    \-2 : special value that denotes earliest available offset.

- `$RECEIVE_EARLIEST_OFFSETS`

    **DEPRECATED**: please use `$RECEIVE_EARLIEST_OFFSET`, as when using this
    constant to retrieve offset, you can get only one. It's kept for backward
    compatibility.

    \-2 : special value that denotes earliest available offset.

- `$DEFAULT_MAX_NUMBER_OF_OFFSETS`

    100 - maximum number of offsets to retrieve.

- `$MIN_BYTES_RESPOND_IMMEDIATELY`

    The minimum number of bytes of messages that must be available to give a response.

    0 - the server will always respond immediately.

- `$MIN_BYTES_RESPOND_HAS_DATA`

    The minimum number of bytes of messages that must be available to give a response.

    10 - the server will respond as soon as at least one partition has at least 10 bytes of data
    (Offset => int64 + MessageSize => int32)
    or the specified timeout occurs.

- `$NOT_SEND_ANY_RESPONSE`

    Indicates how many acknowledgements the servers should receive before responding to the request.

    0 - the server does not send any response.

- `$WAIT_WRITTEN_TO_LOCAL_LOG`

    Indicates how long the servers should wait for the data to be written to the local long before responding to the request.

    1 - the server will wait the data is written to the local log before sending a response.

- `$BLOCK_UNTIL_IS_COMMITTED`

    Wait for message to be committed by all sync replicas.

    \-1 - the server will block until the message is committed by all in sync replicas before sending a response.

- `$DEFAULT_MAX_WAIT_TIME`

    The maximum amount of time (seconds, may be fractional) to wait when no sufficient amount of data is available
    at the time the request is dispatched.

    0.1 - allow the server to wait up to 0.1s to try to accumulate data before responding.

- `$MESSAGE_SIZE_OVERHEAD`

    34 - size of protocol overhead (data added by protocol) for each message.

## IP version

Specify IP protocol version for resolving of IP address and host names.

- `$IP_V4`

    Interpret address as IPv4 and force resolving of host name in IPv4.

- `$IP_V6`

    Interpret address as IPv6 and force resolving of host name in IPv6.

## Compression

According to Apache Kafka documentation:

Kafka currently supports three compression codecs with the following codec numbers:

- `$COMPRESSION_NONE`

    None = 0

- `$COMPRESSION_GZIP`

    GZIP = 1

- `$COMPRESSION_SNAPPY`

    Snappy = 2

- `$COMPRESSION_LZ4`

    LZ4 = 3
    (That module supports only Kafka 0.10 or higher, as initial implementation of LZ4 in Kafka did not follow the standard LZ4 framing specification).

## Error codes

Possible error codes (corresponds to descriptions in `%ERROR`):

- `$ERROR_MISMATCH_ARGUMENT`

    \-1000 - Invalid argument

- `$ERROR_CANNOT_SEND`

    \-1001 - Cannot send

- `$ERROR_SEND_NO_ACK`

    \-1002 - No acknowledgement for sent request

- `ERROR_CANNOT_RECV`

    \-1003 - Cannot receive

- `ERROR_CANNOT_BIND`

    \-1004 - Cannot connect to broker

- `$ERROR_METADATA_ATTRIBUTES`

    \-1005 - Unknown metadata attributes

- `$ERROR_UNKNOWN_APIKEY`

    \-1006 - Unknown ApiKey

- `$ERROR_CANNOT_GET_METADATA`

    \-1007 - Cannot get Metadata

- `$ERROR_LEADER_NOT_FOUND`

    \-1008 - Leader not found

- `$ERROR_MISMATCH_CORRELATIONID`

    \-1009 - Mismatch CorrelationId

- `$ERROR_NO_KNOWN_BROKERS`

    \-1010 - There are no known brokers

- `$ERROR_REQUEST_OR_RESPONSE`

    \-1011 - Bad request or response element

- `$ERROR_TOPIC_DOES_NOT_MATCH`

    \-1012 - Topic does not match the requested

- `$ERROR_PARTITION_DOES_NOT_MATCH`

    \-1013 - Partition does not match the requested

- `$ERROR_NOT_BINARY_STRING`

    \-1014 - Unicode data is not allowed

- `$ERROR_COMPRESSION`

    \-1015 - Compression error

- `$ERROR_RESPONSEMESSAGE_NOT_RECEIVED`

    \-1016 - 'ResponseMessage' not received

- `$ERROR_INCOMPATIBLE_HOST_IP_VERSION`

    \-1017 - Incompatible host name and IP version

- `$ERROR_NO_CONNECTION`

    \-1018 - No IO connection

- `$ERROR_GROUP_COORDINATOR_NOT_FOUND`

    \-1019 - Group Coordinator not found

Contains the descriptions of possible error codes obtained via ERROR\_CODE box
of Apache Kafka Wire Format protocol response.

- `$ERROR_NO_ERROR`

    0 - `q{}`

    No error - it worked!

- `$ERROR_UNKNOWN`

    \-1 - An unexpected server error.

- `$ERROR_OFFSET_OUT_OF_RANGE`

    1 - The requested offset is not within the range of offsets maintained by the server.

- `$ERROR_INVALID_MESSAGE`

    2 - This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.

    Synonym name $ERROR\_CORRUPT\_MESSAGE .

- `$ERROR_UNKNOWN_TOPIC_OR_PARTITION`

    3 - This server does not host this topic-partition.

- `$ERROR_INVALID_FETCH_SIZE`

    4 - The requested fetch size is invalid.

    Synonym name $ERROR\_INVALID\_MESSAGE\_SIZE .

- `$ERROR_LEADER_NOT_AVAILABLE`

    5 - Unable to write due to ongoing Kafka leader selection.

    This error is thrown if we are in the middle of a leadership election and there is
    no current leader for this partition, hence it is unavailable for writes.

- `$ERROR_NOT_LEADER_FOR_PARTITION`

    6 - Server is not a leader for partition.

    This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition.
    It indicates that the clients metadata is out of date.

- `$ERROR_REQUEST_TIMED_OUT`

    7 - Request time-out.

    This error is thrown if the request exceeds the user-specified time limit in the request.

- `$ERROR_BROKER_NOT_AVAILABLE`

    8 - Broker is not available.

    This is not a client facing error and is used mostly by tools when a broker is not alive.

- `$ERROR_REPLICA_NOT_AVAILABLE`

    9 - The replica is not available for the requested topic-partition.

    If replica is expected on a broker, but is not (this can be safely ignored).

- `$ERROR_MESSAGE_TOO_LARGE`

    10 - The request included a message larger than the max message size the server will accept.

    The server has a configurable maximum message size to avoid unbounded memory allocation.
    This error is thrown if the client attempt to produce a message larger than this maximum.

    Synonym name $ERROR\_MESSAGE\_SIZE\_TOO\_LARGE .

- `$ERROR_STALE_CONTROLLER_EPOCH`

    11 - The controller moved to another broker.

    According to Apache Kafka documentation:
    Internal error code for broker-to-broker communication.

    Synonym name $ERROR\_STALE\_CONTROLLER\_EPOCH\_CODE .

- `$ERROR_OFFSET_METADATA_TOO_LARGE`

    12 - Specified metadata offset is too big

    If you specify a value larger than configured maximum for offset metadata.

    Synonym name $ERROR\_OFFSET\_METADATA\_TOO\_LARGE\_CODE .

- `$ERROR_NETWORK_EXCEPTION`

    13 - The server disconnected before a response was received.

- `$ERROR_GROUP_LOAD_IN_PROGRESS`

    14 - The coordinator is loading and hence can't process requests for this group.

    Synonym name $ERROR\_GROUP\_LOAD\_IN\_PROGRESS\_CODE, $ERROR\_LOAD\_IN\_PROGRESS\_CODE .

- `$ERROR_GROUP_COORDINATOR_NOT_AVAILABLE`

    15 - The group coordinator is not available.

    Synonym name $ERROR\_GROUP\_COORDINATOR\_NOT\_AVAILABLE\_CODE, $ERROR\_CONSUMER\_COORDINATOR\_NOT\_AVAILABLE\_CODE .

- `$ERROR_NOT_COORDINATOR_FOR_GROUP`

    16 - This is not the correct coordinator for this group.

    Synonym name $ERROR\_NOT\_COORDINATOR\_FOR\_GROUP\_CODE, $ERROR\_NOT\_COORDINATOR\_FOR\_CONSUMER\_CODE .

- `$ERROR_INVALID_TOPIC_EXCEPTION`

    17 - The request attempted to perform an operation on an invalid topic.

    Synonym name $ERROR\_INVALID\_TOPIC\_CODE .

- `$ERROR_RECORD_LIST_TOO_LARGE`

    18 - The request included message batch larger than the configured segment size on the server.

    Synonym name $ERROR\_RECORD\_LIST\_TOO\_LARGE\_CODE .

- `$ERROR_NOT_ENOUGH_REPLICAS`

    19 - Messages are rejected since there are fewer in-sync replicas than required.

    Synonym name $ERROR\_NOT\_ENOUGH\_REPLICAS\_CODE .

- `$ERROR_NOT_ENOUGH_REPLICAS_AFTER_APPEND`

    20 - Messages are written to the log, but to fewer in-sync replicas than required.

    Synonym name $ERROR\_NOT\_ENOUGH\_REPLICAS\_AFTER\_APPEND\_CODE .

- `$ERROR_INVALID_REQUIRED_ACKS`

    21 - Produce request specified an invalid value for required acks.

    Synonym name $ERROR\_INVALID\_REQUIRED\_ACKS\_CODE .

- `$ERROR_ILLEGAL_GENERATION`

    22 - Specified group generation id is not valid.

    Synonym name $ERROR\_ILLEGAL\_GENERATION\_CODE .

- `$ERROR_INCONSISTENT_GROUP_PROTOCOL`

    23 - The group member's supported protocols are incompatible with those of existing members.

    Synonym name $ERROR\_INCONSISTENT\_GROUP\_PROTOCOL\_CODE .

- `$ERROR_INVALID_GROUP_ID`

    24 - The configured groupId is invalid.

    Synonym name $ERROR\_INVALID\_GROUP\_ID\_CODE .

- `$ERROR_UNKNOWN_MEMBER_ID`

    25 - The coordinator is not aware of this member.

    Synonym name $ERROR\_UNKNOWN\_MEMBER\_ID\_CODE .

- `$ERROR_INVALID_SESSION_TIMEOUT`

    26 - The session timeout is not within the range allowed by the broker
    (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).

    Synonym name $ERROR\_INVALID\_SESSION\_TIMEOUT\_CODE .

- `$ERROR_REBALANCE_IN_PROGRESS`

    27 - The group is rebalancing, so a rejoin is needed.

    Synonym name $ERROR\_REBALANCE\_IN\_PROGRESS\_CODE .

- `$ERROR_INVALID_COMMIT_OFFSET_SIZE`

    28 - The committing offset data size is not valid.

    Synonym name $ERROR\_INVALID\_COMMIT\_OFFSET\_SIZE\_CODE .

- `$ERROR_TOPIC_AUTHORIZATION_FAILED`

    29 - Not authorized to access topics: \[Topic authorization failed.\].

    Synonym name $ERROR\_TOPIC\_AUTHORIZATION\_FAILED\_CODE .

- `$ERROR_GROUP_AUTHORIZATION_FAILED`

    30 - Not authorized to access group: Group authorization failed.

    Synonym name $ERROR\_GROUP\_AUTHORIZATION\_FAILED\_CODE .

- `$ERROR_CLUSTER_AUTHORIZATION_FAILED`

    31 - Cluster authorization failed.

    Synonym name $ERROR\_CLUSTER\_AUTHORIZATION\_FAILED\_CODE .

- `$ERROR_INVALID_TIMESTAMP`

    32 - The timestamp of the message is out of acceptable range.

- `$ERROR_UNSUPPORTED_SASL_MECHANISM`

    33 - The broker does not support the requested SASL mechanism.

- `$ERROR_ILLEGAL_SASL_STATE`

    34 - Request is not valid given the current SASL state.

- `$ERROR_UNSUPPORTED_VERSION`

    35 - The version of API is not supported.

- `%ERROR`

    Contains the descriptions for possible error codes.

- `BITS64`

    Know you are working on 64 or 32 bit system

# An Example

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka qw(
        $KAFKA_SERVER_PORT
        $REQUEST_TIMEOUT
        $RECEIVE_EARLIEST_OFFSET
        $DEFAULT_MAX_NUMBER_OF_OFFSETS
        $DEFAULT_MAX_BYTES
    );
    use Kafka::Connection;
    use Kafka::Producer;
    use Kafka::Consumer;

    my ( $connection, $producer, $consumer );
    try {

        #-- Connection
        $connection = Kafka::Connection->new( host => 'localhost' );

        #-- Producer
        $producer = Kafka::Producer->new( Connection => $connection );

        # Sending a single message
        $producer->send(
            'mytopic',                      # topic
            0,                              # partition
            'Single message'                # message
        );

        # Sending a series of messages
        $producer->send(
            'mytopic',                      # topic
            0,                              # partition
            [                               # messages
                'The first message',
                'The second message',
                'The third message',
            ]
        );

        #-- Consumer
        $consumer = Kafka::Consumer->new( Connection => $connection );

        # Get a list of valid offsets up max_number before the given time
        my $offsets = $consumer->offsets(
            'mytopic',                      # topic
            0,                              # partition
            $RECEIVE_EARLIEST_OFFSET,      # time
            $DEFAULT_MAX_NUMBER_OF_OFFSETS  # max_number
        );

        if ( @$offsets ) {
            say "Received offset: $_" foreach @$offsets;
        } else {
            warn "Error: Offsets are not received\n";
        }

        # Consuming messages
        my $messages = $consumer->fetch(
            'mytopic',                      # topic
            0,                              # partition
            0,                              # offset
            $DEFAULT_MAX_BYTES              # Maximum size of MESSAGE(s) to receive
        );

        if ( $messages ) {
            foreach my $message ( @$messages ) {
                if ( $message->valid ) {
                    say 'payload    : ', $message->payload;
                    say 'key        : ', $message->key;
                    say 'offset     : ', $message->offset;
                    say 'next_offset: ', $message->next_offset;
                } else {
                    say 'error      : ', $message->error;
                }
            }
        }

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
    undef $consumer;
    undef $producer;
    $connection->close;
    undef $connection;

# DEPENDENCIES

In order to install and use this package you will need Perl version
5.10 or later. Some modules within this package depend on other
packages that are distributed separately from Perl. We recommend that
you have the following packages installed before you install
Kafka:

    Compress::Snappy
    Compress::LZ4Frame
    Const::Fast
    Data::Compare
    Data::HexDump::Range
    Data::Validate::Domain
    Data::Validate::IP
    Exception::Class
    List::Utils
    Params::Util
    Scalar::Util::Numeric
    String::CRC32
    Sys::SigAction
    Try::Tiny

Kafka package has the following optional dependencies:

    Capture::Tiny
    Clone
    Config::IniFiles
    File::HomeDir
    Proc::Daemon
    Proc::ProcessTable
    Sub::Install
    Test::Deep
    Test::Exception
    Test::NoWarnings
    Test::TCP

If the optional modules are missing, some "prereq" tests are skipped.

# DIAGNOSTICS

Debug output can be enabled by setting level via one of the following
environment variables:

`PERL_KAFKA_DEBUG=1`               - debug is enabled for the whole `Kafka` package.

`PERL_KAFKA_DEBUG=IO:1`            - enable debug only for [Kafka::IO](https://metacpan.org/pod/Kafka%3A%3AIO) only.

`PERL_KAFKA_DEBUG=Connection:1`    - enable debug only for particular [Kafka::Connection](https://metacpan.org/pod/Kafka%3A%3AConnection).

It's possible to set different debug levels, like in the following example:

`PERL_KAFKA_DEBUG=Connection:1,IO:2`

See documentation for a particular module for explanation of various debug levels.

# BUGS AND LIMITATIONS

[Connection](https://metacpan.org/pod/Kafka%3A%3AConnection) constructor:

Make sure that you always connect to brokers using EXACTLY the same address or host name
as specified in broker configuration (host.name in server.properties).
Avoid using default value (when host.name is commented) in server.properties - always use explicit value instead.

[Producer](https://metacpan.org/pod/Kafka%3A%3AProducer) and [Consumer](https://metacpan.org/pod/Kafka%3A%3AConsumer) methods
only work with one topic and one partition at a time.
Also module does not implement the Offset Commit/Fetch API.

[Producer](https://metacpan.org/pod/Kafka%3A%3AProducer)'s, [Consumer](https://metacpan.org/pod/Kafka%3A%3AConsumer)'s, [Connection](https://metacpan.org/pod/Kafka%3A%3AConnection)'s
string arguments must be binary strings.
Using Unicode strings may cause an error or data corruption.

This module does not support Kafka protocol versions earlier than 0.8.

[Kafka::IO->new](https://metacpan.org/pod/Kafka%3A%3AIO#new)' uses [Sys::SigAction](https://metacpan.org/pod/Sys%3A%3ASigAction) and `alarm()`
to limit some internal operations. This means that if an external `alarm()` was set, signal
delivery may be delayed.

With non-empty timeout, we use `alarm()` internally in [Kafka::IO](https://metacpan.org/pod/Kafka%3A%3AIO) and try preserving existing `alarm()` if possible.
However, if [Time::HiRes::ualarm()](https://metacpan.org/pod/Time%3A%3AHiRes#ualarm) is set before calling Kafka modules,
its behaviour is unspecified (i.e. it could be reset or preserved etc.).

For `gethostbyname` operations the non-empty timeout is rounded to the nearest greater positive integer;
any timeouts less than 1 second are rounded to 1 second.

You can disable the use of `alarm()` by setting `timeout => undef` in the constructor.

The Kafka package was written, tested, and found working on recent Linux distributions.

There are no known bugs in this package.

Please report problems to the ["AUTHOR"](#author).

Patches are welcome.

# MORE DOCUMENTATION

All modules contain detailed information on the interfaces they provide.

# SEE ALSO

The basic operation of the Kafka package modules:

[Kafka](https://metacpan.org/pod/Kafka) - constants and messages used by the Kafka package modules.

[Kafka::Connection](https://metacpan.org/pod/Kafka%3A%3AConnection) - interface to connect to a Kafka cluster.

[Kafka::Producer](https://metacpan.org/pod/Kafka%3A%3AProducer) - interface for producing client.

[Kafka::Consumer](https://metacpan.org/pod/Kafka%3A%3AConsumer) - interface for consuming client.

[Kafka::Message](https://metacpan.org/pod/Kafka%3A%3AMessage) - interface to access Kafka message
properties.

[Kafka::Int64](https://metacpan.org/pod/Kafka%3A%3AInt64) - functions to work with 64 bit elements of the
protocol on 32 bit systems.

[Kafka::Protocol](https://metacpan.org/pod/Kafka%3A%3AProtocol) - functions to process messages in the
Apache Kafka's Protocol.

[Kafka::IO](https://metacpan.org/pod/Kafka%3A%3AIO) - low-level interface for communication with Kafka server.

[Kafka::Exceptions](https://metacpan.org/pod/Kafka%3A%3AExceptions) - module designated to handle Kafka exceptions.

[Kafka::Internals](https://metacpan.org/pod/Kafka%3A%3AInternals) - internal constants and functions used
by several package modules.

A wealth of detail about the Apache Kafka and the Kafka Protocol:

Main page at [http://kafka.apache.org/](http://kafka.apache.org/)

Kafka Protocol at [https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)

# SOURCE CODE

Kafka package is hosted on GitHub:
[https://github.com/TrackingSoft/Kafka](https://github.com/TrackingSoft/Kafka)

# AUTHOR

Sergey Gladkov

Please use GitHub project link above to report problems or contact authors.

# CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Sergiy Zuban

Vlad Marchenko

Damien Krotkine

Greg Franklin

# COPYRIGHT AND LICENSE

Copyright (C) 2012-2017 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See _perlartistic_ at
[http://dev.perl.org/licenses/artistic.html](http://dev.perl.org/licenses/artistic.html).

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.
