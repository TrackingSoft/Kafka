#!/usr/bin/perl -w

use 5.009004;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 4;

BEGIN { use_ok 'Kafka::Int64' }

can_ok( 'Kafka::Int64', 'intsum' );
can_ok( 'Kafka::Int64', 'packq' );
can_ok( 'Kafka::Int64', 'unpackq' );
