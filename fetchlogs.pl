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

=head1 fetchlogs.pl

This is a simple script to automate the process of reading the job name
from the Hive output and fetching the corresponding script from the
master.  It does not support any other kind of mapreduce jobs.

It reads your Hive log and fetches the job files it refers to.
The log is read from stdin.  The output directory must be passed
as a command-line argument.

Options:
    -u USERNAME  Use specified username in scp command
    -p PATH      Path on the master

author: Terran Melconian
=cut

use Getopt::Std;
use Pod::Usage;

$PATH="/var/log/hadoop/history/done";

our %opt;
getopts("u:p:h", \%opt);

if ($opt{h}) {
    pod2usage(-verbose => 2);
    exit;
}


$DESTDIR=$ARGV[0] or ".";
$PATH=$opt{p} if $opt{p};

while (<STDIN>) {
    if (/Kill Command.*mapred\.job\.tracker=([^:]+):.*(job_\w+)/) {
        $hostname=$1;
        push @jobs, $2;
    }
}

$opt{u} .= "@" if $opt{u};
for $j (@jobs) {
    # the _$USERNAME part is needed to avoid matching _conf.xml files
    system "scp $opt{u}$hostname:$PATH/*${j}_$ENV{USER}* $DESTDIR";
}
