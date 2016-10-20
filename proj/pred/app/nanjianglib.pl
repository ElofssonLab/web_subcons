#!/usr/bin/perl -w
#
use Cwd 'abs_path';
use File::Basename;
use Time::Local;
use POSIX qw(strftime);
$ENV{'TZ'} = 'Europe/Stockholm';

my $rundir = dirname(abs_path( __FILE__ ));
my $progname = basename(__FILE__);

sub exec_cmd{#{{{ #debug nanjiang
    # write the date and content of the command and then execute the command
    my $date = strftime "%Y-%m-%d %H:%M:%S", localtime;
    print "[$date] @_"."\n";
    system( "@_");
}#}}}
sub pcons_result_path_to_folder_nr{#{{{ #debug nanjiang
    # convert pcons result path to folder number
    # e.g. /data3/server/www_from_dany/pcons_nanjiang/PCONSMETA/400 -> 400
    my $folder_nr = shift;
    $folder_nr =~ s/\/+$//;
    $folder_nr = basename($folder_nr);
    return $folder_nr;
}#}}}
sub age{ #{{{
#age in days
#-M does not work since it counts from program start.
    my $file=shift;
    my $age=-M $file;
    return $age;
}#}}}
sub date_str_to_epoch{#{{{
    my $date = shift;
    my ($year,$mon, $mday, $hour,$min,$sec) = split(/[\s.:-]+/, $date);
    # note mon starts from 0, therefore, $mon-1
    my $epochtime = timelocal($sec,$min,$hour,$mday,$mon-1,$year);
    return $epochtime;
}#}}}
sub WriteFile{ #content, outfile#{{{
    #Description: Write the content to outfile
    my $content = shift;
    my $outfile = shift;
    open (OUT, ">$outfile");
    print OUT $content;
    close(OUT);
}#}}}
sub WriteDateTagFile{ #outfile #{{{
    # Write the current date to outfile
    my $outfile = shift;
    my $date = strftime "%Y-%m-%d %H:%M:%S", localtime;
    WriteFile($date, $outfile);
}#}}}
sub ReadContent{#infile#{{{
    #read the content of the file
    my $infile = shift;
    open(FH, $infile);
    my $content = do{local $/; <FH>;};
    close FH;
    return $content;
}#}}}
sub ReadContent_chomp{#infile#{{{
    #read the content of the file
    my $infile = shift;
    my $content = ReadContent($infile);
    chomp($content);
    return $content;
}#}}}
sub sendmail{ #{{{
    my ($to, $from, $subject, $message) = @_;
    open(MAIL, "|/usr/sbin/sendmail -t");
# Email Header
    print MAIL "To: $to\n";
    print MAIL "From: $from\n";
    print MAIL "Subject: $subject\n\n";
# Email Body
    print MAIL $message;
    close(MAIL);
    #print "Email Sent Successfully\n";
} #}}}
1;
