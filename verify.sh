#!/bin/sh

RELEASE_TESTS=1
perl Makefile.PL --no_full_tests && 		\
make test

