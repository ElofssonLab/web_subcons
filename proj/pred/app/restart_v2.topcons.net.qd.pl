#!/usr/bin/perl -w
# restart qd
use File::Temp;

use Cwd 'abs_path';
use File::Basename;

use LWP::Simple qw($ua head);
$ua->timeout(10);

my $rundir = dirname(abs_path(__FILE__));
my $basedir = "$rundir/../";

my @to_email_list = (
    "nanjiang.shu\@gmail.com");

my $date = localtime();
print "Date: $date\n";
my $url = "http://v2.topcons.net";
my @urllist = ("http://v2.topcons.net");
my $target_qd_script_name = "qd_topcons2_fe.py";

foreach $url (@urllist){ 
# first: check if $url is accessable
    $output=`curl $url/cgi-bin/restart_qd_topcons2_fe.cgi 2>&1 | html2text`;
    my $title = "$target_qd_script_name restarted for $url";
    (my $tmpfh, my $tmpmessagefile) = File::Temp::tempfile("/tmp/message.XXXXXXX", SUFFIX=>".txt");
    print $tmpfh  "$output"."\n";
    close($tmpfh);
    foreach my $to_email(@to_email_list)
    {
        print "mutt -s \"$title\" \"$to_email\" < $tmpmessagefile"."\n";
        `mutt -s "$title" "$to_email"  < $tmpmessagefile`;
    }
    `rm -f $tmpmessagefile`;
}
