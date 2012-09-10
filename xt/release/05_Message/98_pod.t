#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use Test::More;
use File::Basename;

eval "use Test::Pod";
plan skip_all => "Test::Pod required for testing POD" if $@;
chdir dirname( $0 )."/../../../lib";
all_pod_files_ok( "Kafka/Message.pm" );