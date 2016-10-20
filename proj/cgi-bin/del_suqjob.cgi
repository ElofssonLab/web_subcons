#!/usr/bin/perl -w
# delete suq job
#Created 2015-03-20, updated 2015-03-20, Nanjiang Shu
use CGI qw(:standard);
use CGI qw(:cgi-lib);
use CGI qw(:upload);

use Cwd 'abs_path';
use File::Basename;
my $rundir = dirname(abs_path(__FILE__));
my $suq = "/usr/bin/suq";
# at proj
my $basedir = "$rundir/../";
my $auth_ip_file = "$basedir/auth_iplist.txt";#ip address which allows to run cgi script

print header();
print start_html(-title => "delete an suq job",
    -author => "nanjiang.shu\@scilifelab.se",
    -meta   => {'keywords'=>''});

if(!param())
{
    print "<pre>\n";
    print "usage: curl del_suqjob.cgi -d jobid=jobid\n\n";
    print "       or in the browser\n\n";
    print "       del_suqjob.cgi?jobid=jobid\n\n";
    print "Examples:\n";
    print "       del_suqjob.cgi?jobid=jobid\n";
    print "</pre>\n";
    print end_html();
}
if(param())
{
    my $suqbase = "/scratch";
    my $jobid=param('jobid');
    my $remote_host = $ENV{'REMOTE_ADDR'};

    my @auth_iplist = ();
    open(IN, "<", $auth_ip_file) or die;
    while(<IN>) {
        chomp;
        push @auth_iplist, $_;
    }
    close IN;

    if (grep { $_ eq $remote_host } @auth_iplist) {
        if ($jobid=~/rst_/){
            $jobid =`$suq -b $suqbase ls | grep $jobid | awk '{print \$1}'`;
            chomp($jobid);
        }
        print "<pre>";
        print "Host IP: $remote_host\n\n";
# set ntask
        if ($jobid ne "" ){
            print "$suq -b $suqbase del $jobid\n";
            `$suq -b $suqbase del $jobid`;
            $suqlist = `$suq -b $suqbase ls`;
            print "Suq list after deletion:\n\n";
            print "$suqlist\n";
        }else{
            print "Empty jobid\n";
        }

        print "</pre>";
    }else{
        print "Permission denied!\n";
    }

    print '<br>';
    print end_html();
}
