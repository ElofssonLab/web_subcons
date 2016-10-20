#!/usr/bin/perl -w
# Filename:  check_v2.topcons.net.pl

# Description: check whether v2.topcons.net is accessable and also check the status
#              of the qd_topcons_fe.pl

# Created 2015-04-10, updated 2015-04-10, Nanjiang Shu

use File::Temp;

use Cwd 'abs_path';
use File::Basename;

use LWP::Simple qw($ua head);
$ua->timeout(10);

my $rundir = dirname(abs_path(__FILE__));
my $basedir = "$rundir/../";
require "$rundir/nanjianglib.pl";

my @to_email_list = (
    "nanjiang.shu\@gmail.com");

my $date = localtime();
print "Date: $date\n";
my $url = "http://subcons.bioinfo.se";
my @urllist = ("http://v2.topcons.net");
my $target_qd_script_name = "qd_topcons2_fe.py";
my $computenodelistfile = "$basedir/static/computenode.txt";
my $from_email = "nanjiang.shu\@scilifelab.se";
my $title = "";
my $output = "";

foreach $url (@urllist){ 
# first: check if $url is accessable
    if (!head($url)){
        $title = "$url un-accessible";
        $output = "$url un-accessible";
        foreach my $to_email(@to_email_list) {
            sendmail($to_email, $from_email, $title, $output);
        }
    }

# second: check if qd_topcons2_fe.pl running at pcons1.scilifelab.se frontend
    my $num_running=`curl $url/cgi-bin/check_qd_topcons2_fe.cgi 2> /dev/null | html2text | grep  "$target_qd_script_name" | wc -l`;
    chomp($num_running);

    if ($num_running < 1){
        $output=`curl $url/cgi-bin/restart_qd_topcons2_fe.cgi 2>&1 | html2text`;
        $title = "$target_qd_script_name restarted for $url";
        foreach my $to_email(@to_email_list) {
            sendmail($to_email, $from_email, $title, $output);
        }
    }
}

# third, check if the suq queue is blocked at the compute node and try to clean
# it if blocked
my @computenodelist = ();
open(IN, "<", $computenodelistfile) or die;
while(<IN>) {
    chomp;
    push @computenodelist, $_;
}
close IN;
foreach my $computenode(@computenodelist){
    print "curl http://$computenode/cgi-bin/clean_blocked_suq.cgi 2>&1 | html2text\n";
    $output=`curl http://$computenode/cgi-bin/clean_blocked_suq.cgi 2>&1 | html2text`;
    if ($output =~ /Try to clean the queue/){
        $title = "Cleaning the queue at $computenode";
        `curl http://$computenode/cgi-bin/set_suqntask.cgi?ntask=5 `;
        foreach my $to_email(@to_email_list) {
            sendmail($to_email, $from_email, $title, $output);
        }
    }
}
