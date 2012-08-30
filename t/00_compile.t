#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 4;

BEGIN { use_ok 'Kafka', qw( BITS64 ERROR_MISMATCH_ARGUMENT ) }

ok( defined( my $error_id = ERROR_MISMATCH_ARGUMENT ),  "import error codes" );
ok( my $error = $Kafka::ERROR[ERROR_MISMATCH_ARGUMENT], "receive an error message" );
ok( my $error_code = $Kafka::ERROR_CODE{-1},            "receive an error_code message" );
