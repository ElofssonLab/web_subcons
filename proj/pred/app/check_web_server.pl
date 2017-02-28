#!/usr/bin/perl -w
# Filename:  check_web_server.pl

# Description: check whether web server is accessable and also check the status
#              of the qd_fe.py

# Created 2016-12-07, updated 2017-02-28, Nanjiang Shu

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
my $url = "";
my $servername = "SubCons";
my @urllist = ("http://subcons.bioinfo.se");
my $target_qd_script_name = "qd_fe.py";
my $computenodelistfile = "$basedir/static/computenode.txt";
my $alert_emaillist_file = "$basedir/static/alert_email.txt";
my $from_email = "nanjiang.shu\@scilifelab.se";
my $title = "";
my $output = "";

my @to_email_list = ();
open(IN, "<", $alert_emaillist_file) or die;
while(<IN>) {
    chomp;
    push @to_email_list, $_;
}
close IN;


my %computenodelist = ();
open(IN, "<", $computenodelistfile) or die;
while(<IN>) {
    chomp;
    if (substr($_, 0, 1) ne '#'){
        my @items = split(' ', $_);
        $computenodelist{$items[0]} = $items[1];
    }
}
close IN;
foreach $url (@urllist){ 
# First: check if the $url is accessable
    if (!head($url)){
        $title = "[$servername] $url un-accessible";
        $output = "$url un-accessible";
        foreach my $to_email(@to_email_list) {
            sendmail($to_email, $from_email, $title, $output);
        }
    }

# Second: check if qd is running at the front-end
    my $num_running=`curl $url/cgi-bin/check_qd_fe.cgi 2> /dev/null | html2text | grep  "$target_qd_script_name" | wc -l`;
    chomp($num_running);

    if ($num_running < 1){
        $output=`curl $url/cgi-bin/restart_qd_fe.cgi 2>&1 | html2text`;
        $title = "[$servername] $target_qd_script_name restarted for $url";
        foreach my $to_email(@to_email_list) {
            sendmail($to_email, $from_email, $title, $output);
        }
    }
}

# Third, check if the suq queue is blocked at the compute node and try to clean
# it if blocked
foreach (sort keys %computenodelist){
    my $computenode = $_;
    my $max_parallel_job = $computenodelist{$_};
    print "curl http://$computenode/cgi-bin/clean_blocked_suq.cgi 2>&1 | html2text\n";
    $output=`curl http://$computenode/cgi-bin/clean_blocked_suq.cgi 2>&1 | html2text`;
    `curl http://$computenode/cgi-bin/set_suqntask.cgi?ntask=$max_parallel_job `;
    if ($output =~ /Try to clean the queue/){
        $title = "[$servername] Cleaning the queue at $computenode";
        foreach my $to_email(@to_email_list) {
            sendmail($to_email, $from_email, $title, $output);
        }
    }
}
