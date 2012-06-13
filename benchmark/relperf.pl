#!/usr/bin/perl
#
#########################################################################
#Copyright 2012 TripAdvisor, LLC
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
#
#########################################################################

=head1 relperf.pl

This script works by reading in jobtracker logs, such as the ones from
/var/log/hadoop/history/done.  It expects to see multiple runs of the
SAME job - it relies on a mapper of the same number doing exactly the
same work each time and the conclusions will be completely wrong if this
is not the case.

It calibrates each mapper and reducer for the amount of work it does,
then uses this normalized value to compare the speed of execution on
each machine which has run jobs.  It also calculates how many input
shards were local, and obtains separate statistics for local and
non-local execution.

It is a work in progress and has many rough edges, missing features,
and quite possibly outright errors.

Invocation:

=over 4

./relperf.pl [options] input_files

   -v N    Set verbosity level to N
   -m      Mappers only
   -r      Reducers only

=back

Author: Terran Melconian

=cut

use Getopt::Std;
use Pod::Usage;
use Socket;
use strict;

our %opt;
getopts("hv:mr", \%opt);

if ($opt{h}) {
    pod2usage(-verbose => 2);
    exit;
}

# Structure of this hash:
# TASKID
#  SPLITS
#  number: more information for attempt number N
#   START_TIME
#   FINISH_TIME (only if state was SUCCESS; absent otherwise)
#   HOSTNAME
our %tasks;

our (%pertask, %avgpertask);
our (%perbox);
our (%perlocal);

foreach my $file (@ARGV)
{
    print "Processing $file" if $opt{v};
    open FILE, $file or die;
    while (<FILE>)
    {
        # The format of these lines is mosty that they have one token describing
        # the type of entry, then arbitrarily many key-value pairs with an
        # equal sign.

        chomp;
        # There's an edge case where these lines have an extra unexpected space
        s/^((FROM )?\S+) //;
        my $type=$1;
        print "$type:\n" if $opt{v} > 1;
        my %params;
        while (s/^([^"]+)="([^"]+)"\s+//) {
            my ($key, $value) = ($1, $2);
            $value =~ s/\\(.)/$1/g;
            print "  $key: $value\n" if $opt{v} > 1;
            $params{$key}=$value;
        }

        # We don't care about setup or cleanup tasks here.
        next if $params{TASK_TYPE} ne "MAP" and $params{TASK_TYPE} ne "REDUCE";

        # If the user specified only map or only reduce, ignore the other
        next if $params{TASK_TYPE} eq "MAP" and $opt{r};
        next if $params{TASK_TYPE} eq "REDUCE" and $opt{m};

        # We need the ID *WITHOUT* the job part, so that we can store
        # information about the same task and match it up across jobs.
        $params{TASKID}=~/task_\d+_(\d+)_._(\d+)/ or die;
        my ($jobpart, $taskpart) = ($1, $2);

        # Each map task will be logged several times.  First, it will appear
        # before a specific attempt is created, and we must get the SPLITS
        # field here.  Then, there will be a version with TASK_ATTEMPT_ID,
        # and we must save the START_TIME and maybe TRACKER_NAME of that attempt.
        # Then the attempt will either succeed or fail, and if it succeeds,
        # we must store its FINISH_TIME.  Hostname is available on either
        # HOSTNAME with the finish event or TRACKER_NAME with the start
        # event, in incompatible formats.  STATE_STRING with the end
        # event gives us the input file - if we want, we can save this
        # and make sure it's the same across jobs.
        #
        # It's similar for reduces, except that there are multiple
        # finish types for each part, and STATE_STRING doesn't have a file
        # name.
        #
        # We don't use the counters for anything at this time.  If we want
        # do, they will require additional parsing because they have a
        # nested structure
        #
        # Gotchas:
        #
        # Sometimes splits are hostnames and sometimes they're
        # IP addresses.  I don't know why; I think CombineHiveInputFormat
        # may do it one way and BucketizedHiveInputFormat may do the other.
        # Thus we have to parse both and convert them.  Sometimes three
        # hosts appear, and sometimes only one.  We assume without having
        # verified it in the Hadoop source that if any copies exited on the
        # local machine, that one was used.

        my $id;
        if ($params{TASK_ATTEMPT_ID})
        {
            $params{TASK_ATTEMPT_ID} =~ /(.)$/ or die;
            $id = $1;
        }

        # Types are MapAttempt or ReduceAttempt; we treat them identically.
        if ($params{SPLITS}) {
            $tasks{$taskpart}{SPLITS} = $params{SPLITS};
        } elsif ($type =~ /Attempt$/ and $params{START_TIME}) {
            $tasks{$taskpart}{$id}{START_TIME} = $params{START_TIME};
        } elsif ($type =~ /Attempt$/ and $params{TASK_STATUS} eq "SUCCESS") {
            $tasks{$taskpart}{$id}{FINISH_TIME} = $params{FINISH_TIME};
            # Remove the rack prefix
            $params{HOSTNAME} =~ /(?:.*\/)*(.*)/;
            $tasks{$taskpart}{$id}{HOSTNAME} = $1;
            $tasks{$taskpart}{$id}{TASK_TYPE} = $params{TASK_TYPE};
        }
    }
    for my $task (sort keys %tasks) {
        my $data=$tasks{$task};
        print "$task: " if $opt{v};

        my $attempt;
        # Find the first attempt which succeeded.  TODO: what if there
        # are more than 9?  Does it go to letters?  This can be unlimited
        # because if a task is killed, it can be retried more times than
        # the failure limit.
        for ($attempt=0; $attempt<9; $attempt++) {
            last if $$data{$attempt}{FINISH_TIME};
        }

        # This is an issue only when processing partial or truncated logs.
        # The rest of the time, some attempt DID finish.
        next if not $$data{$attempt}{FINISH_TIME};

        my $duration = $$data{$attempt}{FINISH_TIME} -
                $$data{$attempt}{START_TIME};

        my $islocal = $$data{SPLITS} =~ $$data{$attempt}{HOSTNAME};
        # If we didn't find the hostname, try the IP instead.
        if (not $islocal) {
            my $ip;
            my $packed_ip = gethostbyname($$data{$attempt}{HOSTNAME});
            if (defined $packed_ip) {
                $ip = inet_ntoa($packed_ip); 
                $islocal = $$data{SPLITS} =~ $ip;
           }
        }

        # Duration is in millis
        printf "%4d %2s\n", $duration/1000, $islocal if $opt{v};

        if ($opt{v}) {
            print "  SPLITS: $$data{SPLITS}\n";
            while (my ($key, $data) = each %{$$data{$attempt}}) {
                print "    $key: $data\n";
            }
        }

        # Accumulate information by task
        push @{$pertask{$task}{$$data{$attempt}{HOSTNAME}}}, $duration;

        # Accumulate information by machine
        push @{$perbox{$$data{$attempt}{HOSTNAME}}}, [$task, $duration, $islocal];

        # Accumulate information by locality (maybe someday add rack-local
        # here as well - we don't have multiple racks so we can't do that
        # right now)
        push @{$perlocal{$islocal}}, [$task, $duration];
    }
}

# Now calculate the average time a task took to normalize it, and see
# how each machine which ran it performed against the others.
for my $task (sort keys %pertask) {

    # If the task only ever ran on one machine, don't use it; it's devoid
    # of useful information, and just falsely offset the average towards 1.
    print "Task $task ran on " . join(',', keys(%{$pertask{$task}})) . "\n" if $opt{v};
    next if (keys %{$pertask{$task}} == 1);

    # If the task ran more than once on the same machine, take the mean
    # of those times, but do not increase the weight of that machine's
    # contribution towards the per-task average.
    my ($sum, $count);
    for my $box(keys %{$pertask{$task}}) {
        my ($perboxsum, $perboxcount);
        for my $v (@{$pertask{$task}{$box}}) {
            $perboxsum += $v;
            $perboxcount++;
        }
        die "Internal consistency error" if not $perboxsum;
        $sum += $perboxsum/$perboxcount;
        $count++;
    }
    my $avgpertask = $sum/$count;
    print "$task $avgpertask\n" if $opt{v};
    $avgpertask{$task}=$avgpertask;
}

# Note that Tasks is the TOTAL number of tasks which ran on the machine
# as part of these job.  Comps is the number which we were able to use
# for comparisons.  If the same task ran on the same machine each time,
# we get no data from it.  The number of tasks which could be used for
# comparison is printed to give you a hint about whether you have
# enough data for a meaningful result.
printf "%-12s %5s %5s %4s %s\n", "Machine", "Tasks", "Comps", "Lcl%", "Relative Perf (larger is better)";
print  "~~~~~~~~~~~~ ~~~~~ ~~~~~ ~~~~ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";

# Set the size of the baseline ASCII bar for average performance
my $SCALE=20;

for my $box (sort keys %perbox) {
    # Strip the domain for readability
    $box =~ /^([^.]+)/;
    my $hostwithoutdomain=$1;

    my ($sum, $count, $local);
    for my $v (@{$perbox{$box}}) {
        # If a task only ever ran on one machine, don't try to use
        # the information.
        next if not defined $avgpertask{$$v[0]};
        my $normalized = $$v[1] / $avgpertask{$$v[0]};
        $sum += $normalized;
        $count++;
        $local += $$v[2];
    }
    my ($avg, $barchart);
    if ($sum) {
        $avg = 1.0/$sum*$count; # Invert so larger means better performance
        $barchart = "="x(($avg+1.0/$SCALE/2)*$SCALE);
    } else {
        $avg = 0;
        $barchart = "No Data; Do More Job Runs";
    }
    printf "%-12s %5d %5d %4d %.2f %s\n", $hostwithoutdomain, $#{$perbox{$box}}+1, $count, 100*$local/$count, $avg, $barchart;
}

print "\n";

# After bucketing by machine, repeat the same process but instead bucket based
# on whether the input data was available locally.  This gives you a hint about
# the extent to which you are network bound.
for my $local (keys %perlocal) {
    my $localstring = $local ? "local" : "remote";
    my ($sum, $count);
    for my $v (@{$perlocal{$local}}) {
        next if not defined $avgpertask{$$v[0]};
        my $normalized = $$v[1] / $avgpertask{$$v[0]};
        $sum += $normalized;
        $count++;
    }
    my $avg = 1.0/$sum*$count;
    my $barchart = "="x(($avg+1.0/$SCALE/2)*$SCALE);
    printf "%-12s %5d %5d %4d %.2f %s\n", $localstring, $#{$perlocal{$local}}+1, $count,
            $local*100, $avg, $barchart;
}
