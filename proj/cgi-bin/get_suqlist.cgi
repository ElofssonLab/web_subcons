#!/usr/bin/perl -w
# get set suq ntask
#Created 2015-03-20, updated 2015-03-20, Nanjiang Shu
use CGI qw(:standard);
use CGI qw(:cgi-lib);
use CGI qw(:upload);

use Cwd 'abs_path';
use File::Basename;
my $rundir = dirname(abs_path(__FILE__));
# at proj
my $basedir = abs_path("$rundir/../");
my $progname = basename(__FILE__);
my $logpath = "$basedir/pred/static/log";
my $errfile = "$logpath/$progname.err";
my $auth_ip_file = "$basedir/auth_iplist.txt";#ip address which allows to run cgi script
my $suq = "/usr/bin/suq";
my $suqbase = "/scratch";

print header();
print start_html(-title => "get suq list",
    -author => "nanjiang.shu\@scilifelab.se",
    -meta   => {'keywords'=>''});

# if(!param())
# {
#     print "<pre>\n";
#     print "usage: curl get_suqlist.cgi -d base=suqbasedir \n\n";
#     print "       or in the browser\n\n";
#     print "       get_suqlist.cgi?base=suqbasedir\n\n";
#     print "Example\n";
#     print "       get_suqlist.cgi?base=log\n";
#     print "</pre>\n";
#     print end_html();
# }
my $remote_host = $ENV{'REMOTE_ADDR'};

my @auth_iplist = ();
open(IN, "<", $auth_ip_file) or die;
while(<IN>) {
    chomp;
    push @auth_iplist, $_;
}
close IN;

if (grep { $_ eq $remote_host } @auth_iplist) {
    my $command =  "$suq -b $suqbase ls 2>>$errfile";
    $suqlist = `$command`;
    print "<pre>";
    print "Host IP: $remote_host\n\n";
    print "command: $command\n\n";
    print "Suq list:\n\n";
    print "$suqlist\n";

    print "</pre>";
}else{
    print "Permission denied!\n";
}

print '<br>';
print end_html();

