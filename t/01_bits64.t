#!/usr/bin/perl -w

use 5.008003;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 1;

use Kafka qw ( BITS64 );

eval { my $ret = unpack( 'Q', ( 255 ) x 8 )."\n" };

if      ( $@ )  { ok( !BITS64,  "Your system not supports 64-bit integer values" ); }
else            { ok(  BITS64,  "Your system supports 64-bit integer values" ); }
