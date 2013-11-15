#!/bin/sh

cd ..
cover -delete

#find t -name \*.t -print -exec perl -MDevel::Cover {} \;

perl -MDevel::Cover t/00_compile.t
perl -MDevel::Cover t/00_api.t
perl -MDevel::Cover t/01_bits64.t
perl -MDevel::Cover t/02_io.t
perl -MDevel::Cover t/03_mockio.t
perl -MDevel::Cover t/04_protocol.t
perl -MDevel::Cover t/05_decode_encode.t
perl -MDevel::Cover t/06_cluster.t
perl -MDevel::Cover t/07_cluster_start.t
perl -MDevel::Cover t/08_connection.t
perl -MDevel::Cover t/09_message.t
perl -MDevel::Cover t/10_producer.t
perl -MDevel::Cover t/11_consumer.t
perl -MDevel::Cover t/20_kafka_usage.t
perl -MDevel::Cover t/21_kafka_bench.t
perl -MDevel::Cover t/30_cluster_stop.t
perl -MDevel::Cover t/40_autocreate_topics.t
perl -MDevel::Cover t/41_fork.t
perl -MDevel::Cover t/90_mock_io.t
perl -MDevel::Cover t/91_mock_usage.t
perl -MDevel::Cover t/92_mock_bench.t

cover
