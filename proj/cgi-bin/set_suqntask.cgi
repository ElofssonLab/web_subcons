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
my $basedir = "$rundir/../";
my $auth_ip_file = "$basedir/auth_iplist.txt";#ip address which allows to run cgi script

print header();
print start_html(-title => "set suq ntask",
    -author => "nanjiang.shu\@scilifelab.se",
    -meta   => {'keywords'=>''});

# if(!param())
# {
#     print "<pre>\n";
#     print "usage: curl set_suqntask.cgi -d base=suqbasedir -d ntask=N\n\n";
#     print "       or in the browser\n\n";
#     print "       set_suqntask.cgi?base=suqbasedir&ntask=N\n";
#     print "</pre>\n";
#     print end_html();
# }
my $suqbase = "/scratch";
my $ntask=param('ntask');
my $remote_host = $ENV{'REMOTE_ADDR'};
my @auth_iplist = ();
open(IN, "<", $auth_ip_file) or die;
while(<IN>) {
    chomp;
    push @auth_iplist, $_;
}
close IN;

if (grep { $_ eq $remote_host } @auth_iplist) {
    $suqlist = `suq -b $suqbase ls`;
    $current_ntask =`suq -b $suqbase ls | grep "max tasks" | awk '{print \$NF}'`;
    chomp($current_ntask);

    print "<pre>";
    print "Host IP: $remote_host\n\n";
    print "ntask before re-setting: $current_ntask\n\n";
# set ntask
    `suq -b $suqbase ntask $ntask`;
    print "suq -b $suqbase ntask $ntask\n\n";

    $after_ntask =`suq -b $suqbase ls | grep "max tasks" | awk '{print \$NF}'`;

    print "ntask after re-setting: $after_ntask\n\n";

    print "Suq list:\n\n";
    print "$suqlist\n";

    print "</pre>";
}else{
    print "Permission denied!\n";
}

print '<br>';
print end_html();

