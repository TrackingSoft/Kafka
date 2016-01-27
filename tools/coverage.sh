#!/bin/sh

cd ..
cover -delete

#find t -name \*.t -print -exec perl -MDevel::Cover {} \;

perl -MDevel::Cover t/00_api.t
perl -MDevel::Cover t/01_bits64.t
perl -MDevel::Cover t/02_io.t
perl -MDevel::Cover t/03_io_ipv6.t
perl -MDevel::Cover t/04_mockio.t
perl -MDevel::Cover t/05_protocol.t
perl -MDevel::Cover t/06_decode_encode.t
perl -MDevel::Cover t/07_cluster.t
perl -MDevel::Cover t/08_cluster_start.t
perl -MDevel::Cover t/09_connection.t
perl -MDevel::Cover t/10_message.t
perl -MDevel::Cover t/11_producer.t
perl -MDevel::Cover t/12_consumer.t
perl -MDevel::Cover t/13_leader_not_found.t
perl -MDevel::Cover t/20_kafka_usage.t
perl -MDevel::Cover t/21_kafka_bench.t
perl -MDevel::Cover t/30_cluster_stop.t
perl -MDevel::Cover t/40_autocreate_topics.t
perl -MDevel::Cover t/41_fork.t
perl -MDevel::Cover t/42_nonfatal_errors.t
perl -MDevel::Cover t/43_competition.t
perl -MDevel::Cover t/44_bad_sending.t
perl -MDevel::Cover t/45_compression.t
perl -MDevel::Cover t/46_destroy_connection.t
perl -MDevel::Cover t/47_kafka_usage_ipv6.t
perl -MDevel::Cover t/50_debug_level.t
perl -MDevel::Cover t/90_mock_io.t
perl -MDevel::Cover t/91_mock_usage.t
perl -MDevel::Cover t/92_mock_bench.t
perl -MDevel::Cover t/99_data_cleanup.t

cover
