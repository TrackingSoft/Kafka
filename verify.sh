#!/bin/sh

# install dependencies locally (requires local::lib and cpanm to be installed already)
eval "$(perl -Mlocal::lib)"
cpanm --installdeps .

RELEASE_TESTS=1
perl Makefile.PL --no_full_tests && 		\
make test

