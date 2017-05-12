#!/bin/bash
set -eux

MIRROR=http://mirrors.ibiblio.org/apache/kafka/0.10.2.1
DIST=kafka_2.12-0.10.2.1.tgz

if [[ ! -d vendor ]]; then
    mkdir -p vendor
fi

SOURCE="vendor/${DIST}"

if [[ ! -e $SOURCE ]]; then
    if ! wget -O $SOURCE "${MIRROR}/${DIST}" ; then
        rm $SOURCE
        exit 1
    fi
fi

if [[ -e kafka ]]; then
   rm -r kafka
fi

mkdir -p kafka
tar xzf $SOURCE -C kafka --strip-components 1

