#!/bin/sh

# install dependencies locally (requires local::lib and cpanm to be installed already)
eval "$(perl -Mlocal::lib)"
cpanm --installdeps .
cpanm --notest Test::Distribution
cpanm --notest Test::Kwalitee
cpanm --notest Test::Kwalitee::Extra

export KAFKA_BASE_DIR="$( dirname $0 )/kafka"

export RELEASE_TESTS=1
perl Build.PL && \
./Build build && \
./Build test

