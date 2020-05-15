#!/bin/bash
set -eux

MIRROR=https://archive.apache.org/dist/kafka/

VERSION=${1:-0.10.2.2}

if [[ $VERSION == "1.0.0" ]]; then
    DIST="kafka_2.11-${VERSION}.tgz"
elif [[ $VERSION == "0.11.0.2" ]]; then
    DIST="kafka_2.11-${VERSION}.tgz"
elif [[ $VERSION == "0.10.2.2" ]]; then
    DIST="kafka_2.12-${VERSION}.tgz"
elif [[ $VERSION == "0.9.0.1" ]]; then
    DIST="kafka_2.11-${VERSION}.tgz"
else
    >&2 echo "ERROR: unknown version '${VERSION}'"
    exit 1
fi

if [[ ! -d vendor ]]; then
    mkdir -p vendor
fi

SOURCE="vendor/${DIST}"

if [[ ! -e $SOURCE ]]; then
    if ! wget -O $SOURCE "${MIRROR}/${VERSION}/${DIST}" ; then
        rm $SOURCE
        exit 1
    fi
fi

if [[ -e kafka ]]; then
   rm -r kafka
fi

mkdir -p kafka
tar xzf $SOURCE -C kafka --strip-components 1

# fix java options issue: https://stackoverflow.com/questions/36970622/kafka-unrecognized-vm-option-printgcdatestamps
find kafka -name \*.sh -exec sed -i'' 's/-XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps//g' {} \;
