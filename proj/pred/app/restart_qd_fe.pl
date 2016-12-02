#!/usr/bin/perl -w
# restart qd
use File::Temp;

use Cwd 'abs_path';
use File::Basename;

use LWP::Simple qw($ua head);
$ua->timeout(10);

my $rundir = dirname(abs_path(__FILE__));
my $basedir = "$rundir/../";
my $urlfile = abs_path("$basedir/static/log/base_www_url.txt");

my @to_email_list = (
    "nanjiang.shu\@gmail.com");

my $date = localtime();

my @urllist = ();
open my $fpin, '<', $urlfile;
chomp(@urllist = <$fpin>);
close $fpin;

print "Date: $date\n";

my $target_qd_script_name = "qd_fe.py";

foreach $url (@urllist){ 
# first: check if $url is accessable
    $output=`curl $url/cgi-bin/restart_qd_fe.cgi 2>&1 | html2text`;
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
