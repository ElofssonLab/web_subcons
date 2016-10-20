#!/bin/bash
# plot average waittime (finish time) vs numseq_in_job
# #
progname=`basename $0`
usage="
Usage: $progname datafile

Description:
plot average (or median) waiting time (or finish time) vs numseq_in_job

Options:
    -format  STR     Set the format of output, (default: eps)
                     Can be one of png, terminal or eps
    -outpath DIR     Set the output path, default =./
    -h, --help       Print this help message and exit

Created 2015-09-07, updated 2015-09-07, Nanjiang Shu 
"

PrintHelp() {
    echo "$usage"
}

if [ $# -lt 1 ]; then
    PrintHelp
    exit
fi


Makeplot(){ #dataFile#{{{
    local basename=`basename "$dataFile"` 
    local outputSetting=
    case $outputStyle in
        term*)
        outputSetting=
        ;;
        png)
        outfile=$outpath/$basename.png
        outputSetting="
set terminal png enhanced
set output '$outfile' 
        "
        ;;
        eps)
        outfile=$outpath/$basename.eps
        pdffile=$outpath/$basename.pdf
        outputSetting="
set term postscript eps enhanced
set output '$outfile' 
        "
        ;;
    esac


/usr/bin/gnuplot -persist<<EOF 
$outputSetting
set style line 1 lt 1 pt 0 ps 0 lc rgb "red" lw 2
set style line 2 lt 1 pt 0 ps 0 lc rgb "blue" lw 2
set style line 3 lt 1 pt 0 ps 0 lc rgb "green" lw 2
set style line 11 lt 1 pt 7 ps 2 lc rgb "red" lw 1
set style line 12 lt 1 pt 7 ps 2 lc rgb "blue" lw 1
set style line 13 lt 1 pt 7 ps 2 lc rgb "green" lw 1
set style line 21 lt 1 pt 7 ps 2 lc rgb "grey10" lw 2
set style line 22 lt 1 pt 7 ps 2 lc rgb "grey30" lw 2
set style line 33 lt 1 pt 7 ps 2 lc rgb "grey50" lw 2

set title ""
set xlabel "Number of sequences of jobs"
set ylabel "$ylabel"
set key left spacing 1.25
set style data points
set logscale x
set logscale y
plot "$dataFile"   using 1:2 ls $linestyle title "$keytitle" with lines

EOF

    case $outputStyle in
        eps)
            $eps2pdf $outfile
            convert -density 200 -background white $outpath/$basename.eps $outpath/$basename.png
            echo "Histogram image output to $pdffile"
            ;;
        *) echo "Histogram image output to $outfile" ;;
    esac
}
#}}}

outputStyle=eps
outpath=
dataFile=
isNonOptionArg=false
while [ "$1" != "" ]; do
    if [ "$isNonOptionArg" == "true" ]; then 
        dataFile=$1
        isNonOptionArg=false
    elif [ "$1" == "--" ]; then
        isNonOptionArg=true
    elif [ "${1:0:1}" == "-" ]; then
        case $1 in
            -h|--help) PrintHelp; exit;;
            -format|--format) outputStyle=$2;shift;;
            -outpath|--outpath) outpath=$2;shift;;
            -*) echo "Error! Wrong argument: $1"; exit;;
        esac
    else
        dataFile=$1
    fi
    shift
done

if [ "$outpath" == "" ]; then
    outpath=`dirname $dataFile`
elif [ ! -d "$outpath" ]; then
    mkdir -p $outpath
fi

if [ ! -f "$dataFile" ]; then 
    echo "Error! dataFile = \"$dataFile\" does not exist. Exit..."
    exit
fi

osname=`uname -s`
eps2pdf=eps2pdf
case $osname in 
    *Darwin*) eps2pdf=epstopdf;;
    *Linux*) eps2pdf=eps2pdf;;
esac

case $dataFile in 
    *waittime*)
        ylabel="Waiting time (s)"
        linestyle=1
        ;;
    *finishtime*)
        ylabel="Total time to finish the job (s)"
        linestyle=2
        ;;
esac
case $dataFile in 
    *avg*)
        keytitle=Average
        ;;
    *median*)
        keytitle=Median
        ;;
esac

Makeplot $dataFile
