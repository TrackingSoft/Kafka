cover -delete

#find t -name \*.t -print -exec perl -MDevel::Cover {} \;

perl -MDevel::Cover t/00_compile.t
perl -MDevel::Cover t/01_bits64.t
perl -MDevel::Cover xt/author/92_vars.t
perl -MDevel::Cover xt/author/93_versions.t
perl -MDevel::Cover xt/author/94_fixme.t
perl -MDevel::Cover xt/author/95_critic.t
perl -MDevel::Cover xt/release/96_kwalitee.t
perl -MDevel::Cover xt/release/97_distribution.t
perl -MDevel::Cover xt/release/98_pod.t
perl -MDevel::Cover xt/release/99_pod_coverage.t

perl -MDevel::Cover t/00_Int64/00_compile.t
perl -MDevel::Cover t/00_Int64/01_intsum.t
perl -MDevel::Cover t/00_Int64/02_packq.t
perl -MDevel::Cover t/00_Int64/03_unpackq.t
perl -MDevel::Cover xt/release/00_Int64/98_pod.t
perl -MDevel::Cover xt/release/00_Int64/99_pod_coverage.t

perl -MDevel::Cover t/01_IO/00_compile.t
perl -MDevel::Cover t/01_IO/01_new.t
perl -MDevel::Cover t/01_IO/02_RaiseError.t
perl -MDevel::Cover t/01_IO/03_close.t
perl -MDevel::Cover t/01_IO/04_last_error.t
perl -MDevel::Cover t/01_IO/05_last_errorcode.t
perl -MDevel::Cover t/01_IO/06_send.t
perl -MDevel::Cover t/01_IO/07_receive.t
perl -MDevel::Cover xt/release/01_IO/98_pod.t
perl -MDevel::Cover xt/release/01_IO/99_pod_coverage.t

perl -MDevel::Cover xt/release/02_Mock/00_compile.t
perl -MDevel::Cover xt/release/02_Mock/01_new.t
perl -MDevel::Cover xt/release/02_Mock/02_port.t
perl -MDevel::Cover xt/release/02_Mock/03_delay.t
perl -MDevel::Cover xt/release/02_Mock/04_clear.t
perl -MDevel::Cover xt/release/02_Mock/05_close.t
perl -MDevel::Cover xt/release/02_Mock/06_last_request.t
perl -MDevel::Cover xt/release/02_Mock/07_usage.t
perl -MDevel::Cover xt/release/02_Mock/98_pod.t
perl -MDevel::Cover xt/release/02_Mock/99_pod_coverage.t

perl -MDevel::Cover t/03_Protocol/00_compile.t
perl -MDevel::Cover t/03_Protocol/01_last_error.t
perl -MDevel::Cover t/03_Protocol/02_last_errorcode.t
perl -MDevel::Cover t/03_Protocol/03_produce_request.t
perl -MDevel::Cover t/03_Protocol/04_fetch_request.t
perl -MDevel::Cover t/03_Protocol/05_offsets_request.t
perl -MDevel::Cover t/03_Protocol/06_fetch_response.t
perl -MDevel::Cover t/03_Protocol/07_offsets_response.t
perl -MDevel::Cover xt/release/03_Protocol/98_pod.t
perl -MDevel::Cover xt/release/03_Protocol/99_pod_coverage.t

perl -MDevel::Cover t/04_Producer/00_compile.t
perl -MDevel::Cover t/04_Producer/01_new.t
perl -MDevel::Cover t/04_Producer/02_last_error.t
perl -MDevel::Cover t/04_Producer/03_last_errorcode.t
perl -MDevel::Cover t/04_Producer/04_send.t
perl -MDevel::Cover t/04_Producer/05_close.t
perl -MDevel::Cover xt/release/04_Producer/98_pod.t
perl -MDevel::Cover xt/release/04_Producer/99_pod_coverage.t

perl -MDevel::Cover t/05_Message/00_compile.t
perl -MDevel::Cover t/05_Message/01_new.t
perl -MDevel::Cover t/05_Message/02_methods.t
perl -MDevel::Cover xt/release/05_Message/98_pod.t
perl -MDevel::Cover xt/release/05_Message/99_pod_coverage.t

perl -MDevel::Cover t/06_Consumer/00_compile.t
perl -MDevel::Cover t/06_Consumer/01_new.t
perl -MDevel::Cover t/06_Consumer/02_last_error.t
perl -MDevel::Cover t/06_Consumer/03_last_errorcode.t
perl -MDevel::Cover t/06_Consumer/04_fetch.t
perl -MDevel::Cover t/06_Consumer/05_offsets.t
perl -MDevel::Cover t/06_Consumer/06_close.t
perl -MDevel::Cover xt/release/06_Consumer/98_pod.t
perl -MDevel::Cover xt/release/06_Consumer/99_pod_coverage.t

perl -MDevel::Cover t/99_usage/00_mock.t
perl -MDevel::Cover t/99_usage/01_kafka.t
perl -MDevel::Cover t/99_usage/02_mock_error.t
perl -MDevel::Cover t/99_usage/03_kafka_bench.t

cover
