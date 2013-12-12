#!/bin/sh

cd ../t
perl 07_cluster_start.t
cd ../tools
perl -d:NYTProf profiling.pl "$@"
cd ../t
perl 30_cluster_stop.t
cd ../tools
nytprofhtml --open
