#!/usr/bin/perl -w

(($infile,$prefix) = @ARGV) || die("Syntax: fasta_total_split.pl fasta_file prefix\n");

$number_of_sequences=`cat $infile|grep ">"|wc -l`;
for ($i=0;$i<length($number_of_sequences-1);$i++) {
    $nr .= "0";
}

open(IN,$ARGV[0]) || die("Could not open input file: $infile\n");
while($name=<IN>) {
    $sequence="";
    while($seq=<IN>) {
	($seq =~ /^\s*$/) && next;
	($seq =~ /^>/) && (seek(IN,-1*length($seq),1) && last || die);
	$sequence .= $seq;
    }
    open(OUT,">".$prefix.$nr) || die;
    print OUT $name.$sequence;
    close(OUT);
    $nr++;
}
close(IN);
