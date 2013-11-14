#!/usr/bin/perl -w

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

BEGIN {
    plan skip_all => 'Unknown base directory of Kafka server'
        unless defined $ENV{KAFKA_BASE_DIR};
}

#-- verify load the module

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

plan 'no_plan';

#-- load the modules -----------------------------------------------------------

use Config::IniFiles;
use Const::Fast;
use File::Copy;
use FindBin qw(
    $Bin
);
use File::Spec::Functions qw(
    catdir
    catfile
);
use Params::Util qw(
    _ARRAY
    _ARRAY0
    _HASH
);

use Kafka qw(
    $DEFAULT_MAX_BYTES
    $RECEIVE_LATEST_OFFSET
);
use Kafka::Cluster qw(
    $DEFAULT_REPLICATION_FACTOR
);
use Kafka::Connection;
use Kafka::Consumer;
use Kafka::MockIO;
use Kafka::Producer;

#-- setting up facilities ------------------------------------------------------

const my $KAFKA_PROPERTIES_FILE => 'server.properties';

my ( $properties_file, $bak_properties_file );
{
    my $start_dir           = ( substr( $Bin, -1 ) eq 't' ) ? $Bin : catdir( $Bin, 't' );
    my $kafka_config_dir    = catdir( $start_dir, 'config' );
    $properties_file        = catfile( $kafka_config_dir, $KAFKA_PROPERTIES_FILE );
}
$bak_properties_file = $properties_file.'.bak';

unlink $bak_properties_file;
copy( $properties_file, $bak_properties_file );

#-- declarations ---------------------------------------------------------------

# WARNING: must match the settings of your system
const my $KAFKA_BASE_DIR        => $ENV{KAFKA_BASE_DIR};
const my $TOPIC_PATTERN         => 'stranger0';
const my $INI_SECTION           => 'GENERAL';

my ( $cluster, $port, $connection, $topic, $partition, $producer, $response, $consumer, $offsets, $messages );

sub restore_properties {
    unlink $properties_file;
    rename $bak_properties_file, $properties_file;
}

# Setting the server configuration file
sub setup {
    my ( $auto_create_topics_mode ) = @_;

    if ( !( my $cfg = Config::IniFiles->new(
            -file       => $properties_file,
            -fallback   => $INI_SECTION,
        ) ) ) {
        restore_properties();
        my $error = q{};
        map { $error .= "\n$_" } @Config::IniFiles::errors;
        BAIL_OUT "$properties_file error: $error";
    }
    else {
        $cfg->setval( $INI_SECTION, 'auto.create.topics.enable'     => $auto_create_topics_mode );
        $cfg->setval( $INI_SECTION, 'default.replication.factor'    => $DEFAULT_REPLICATION_FACTOR );
        $cfg->RewriteConfig( $properties_file );
    }
}

sub sending {
    return $producer->send(
        ++$topic,                   # unknown topic
        $partition,
        'Single message'            # message
    );
}

sub getting_offsets {
    return $consumer->offsets(
        ++$topic,
        $partition,
        $RECEIVE_LATEST_OFFSET,         # time
    );
}

sub fetching {
    return $consumer->fetch(
        ++$topic,
        $partition,
        0,                  # offset
        $DEFAULT_MAX_BYTES  # Maximum size of MESSAGE(s) to receive
    );
}

#-- Global data ----------------------------------------------------------------

$partition  = $Kafka::MockIO::PARTITION;;
$topic      = $TOPIC_PATTERN;

# INSTRUCTIONS -----------------------------------------------------------------

for my $auto_create_topics_enable ( 'true', 'false' ) {
    setup( $auto_create_topics_enable );

    $cluster = Kafka::Cluster->new(
        kafka_dir           => $KAFKA_BASE_DIR,
        replication_factor  => $DEFAULT_REPLICATION_FACTOR,
    );

    #-- Connecting to the Kafka server port (for example for node_id = 0)
    ( $port ) =  $cluster->servers;

    for my $AutoCreateTopicsEnable ( 0, 1 ) {
        #-- Connecting to the Kafka server port
        $connection = Kafka::Connection->new(
            host                    => 'localhost',
            port                    => $port,
            AutoCreateTopicsEnable  => $AutoCreateTopicsEnable,
        );
        $producer = Kafka::Producer->new(
            Connection  => $connection,
        );
        $consumer = Kafka::Consumer->new(
            Connection  => $connection,
        );

        # Sending a single message
        undef $response;
        if ( $auto_create_topics_enable eq 'true' && $AutoCreateTopicsEnable ) {
            lives_ok    { $response = sending() } 'expecting to live';
            ok _HASH( $response ), 'response is received';
        } else {
            dies_ok     { $response = sending() } 'expecting to die';
            ok !defined( $response ), 'response is not received';
        }

        # Get a list of valid offsets up max_number before the given time
        undef $offsets;
        if ( $auto_create_topics_enable eq 'true' && $AutoCreateTopicsEnable ) {
            lives_ok    { $offsets = getting_offsets() } 'expecting to live';
            ok _ARRAY( $offsets ), 'offsets are received';
        } else {
            dies_ok     { $offsets = getting_offsets() } 'expecting to die';
            ok !defined( $offsets ), 'offsets are not received';
        }

        # Consuming messages
        undef $messages;
        if ( $auto_create_topics_enable eq 'true' && $AutoCreateTopicsEnable ) {
            lives_ok    { $messages = fetching() } 'expecting to live';
            ok _ARRAY0( $messages ), 'messages are received';
        } else {
            dies_ok     { $messages = fetching() } 'expecting to die';
            ok !defined( $messages ), 'messages are not received';
        }
    }

    $cluster->close;
}

# POSTCONDITIONS ---------------------------------------------------------------

restore_properties();
