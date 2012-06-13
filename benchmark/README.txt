Copyright 2012 TripAdvisor, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

======================================================================

OVERVIEW

The purpose of this tool is to assess the relative performance of
machines in your Hadoop cluster.  It is not intended to assess the
overall performance of the cluster as a whole.

It works by running the same job multiple times and looking at how
long it took each task to complete on different machines.  You can
use this to tune your map and reduce slots (keep going up until
the machine with the higher settings shows worse performance) or
to evaluate new hardware purchases.

So far it has been used only with Hive jobs, but it should be possible to
use it with any other job where the assignment of work to tasks is
determinstic.  The log fetching script is specific to Hive and will
need to be extended or bypassed for other mapreduce jobs.

======================================================================

# Example invocation steps:

# Run three copies of the job
rm /tmp/benchmark_query.log
hive -f benchmark_query.hql >> /tmp/benchmark_query.log 2>&1 &
hive -f benchmark_query.hql >> /tmp/benchmark_query.log 2>&1 &
hive -f benchmark_query.hql >> /tmp/benchmark_query.log 2>&1 &

# Wait for them to finish, and then fetch their logs
wait
mkdir logdir/
./fetchlogs.pl -u root logdir/ < /tmp/benchmark_query.log

# Generate the report
./relperf.pl logdir/*
