#!/bin/bash
# draw histogram of the number of submitted jobs
# #
progname=`basename $0`
usage="
Usage: $progname datafile

Description:
    Draw histogram of numjob and numseq vs time series

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

Makeplot_numjob(){ #dataFile#{{{
    local dataFile=$1
    local basename=`basename "$dataFile"` 
    basename=${basename}.numjob
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

/usr/bin/env gnuplot -persist<<EOF 
$outputSetting
set style line 1 lt 1 pt 7 ps 1 lc rgb "red" lw 1
set style line 2 lt 1 pt 7 ps 1 lc rgb "blue" lw 1
set style line 3 lt 1 pt 7 ps 1 lc rgb "green" lw 1
set style line 11 lt 1 pt 7 ps 2 lc rgb "red" lw 1
set style line 12 lt 1 pt 7 ps 2 lc rgb "blue" lw 1
set style line 13 lt 1 pt 7 ps 2 lc rgb "green" lw 1

set key autotitle columnhead

set tmargin at screen 0.95
set bmargin at screen 0.25
set rmargin at screen 0.90
set lmargin at screen 0.10
set title ""
set xdata time
set xlabel ""
set ylabel "Count"
set xtics rotate by -45 offset 0,-0
$xticfreqsetting
set style fill solid 0.5 border -1
set boxwidth 0.5 relative
set logscale y
set timefmt "$timeformat_in"
set format x "$timeformat_out"
set xdata time
set grid y
plot "$dataFile" using 1:2 ls 1 title "Number of jobs" $plot_setting

EOF

    case $outputStyle in
        eps)
            $eps2pdf $outfile
            convert -density 200 -background white $outpath/$basename.pdf $outpath/$basename.png
            echo "Histogram image output to $pdffile"
            ;;
        *) echo "Histogram image output to $outfile" ;;
    esac
}
#}}}
Makeplot_numseq(){ #dataFile#{{{
    local dataFile=$1
    local basename=`basename "$dataFile"` 
    basename=${basename}.numseq
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


/usr/bin/env gnuplot -persist<<EOF 
$outputSetting
set style line 1 lt 1 pt 7 ps 1 lc rgb "red" lw 1
set style line 2 lt 1 pt 7 ps 1 lc rgb "blue" lw 1
set style line 3 lt 1 pt 7 ps 1 lc rgb "green" lw 1
set style line 11 lt 1 pt 7 ps 2 lc rgb "red" lw 1
set style line 12 lt 1 pt 7 ps 2 lc rgb "blue" lw 1
set style line 13 lt 1 pt 7 ps 2 lc rgb "green" lw 1

set key autotitle columnhead

set tmargin at screen 0.95
set bmargin at screen 0.25
set rmargin at screen 0.90
set lmargin at screen 0.10
set title ""
set xlabel ""
set ylabel "Count"
set xtics rotate by -45 offset 0,-0
$xticfreqsetting
set style fill solid 0.5 border -1
set boxwidth 0.5 relative
set logscale y
set timefmt "$timeformat_in"
set format x "$timeformat_out"
set xdata time
set grid y
plot "$dataFile" using 1:3 ls 2 title "Number of seqs" $plot_setting

EOF

    case $outputStyle in
        eps)
            $eps2pdf $outfile
            convert -density 200 -background white $outpath/$basename.pdf $outpath/$basename.png
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
eps2pdf=epstopdf
case $osname in 
    *Darwin*) eps2pdf=epstopdf;;
    *Linux*) eps2pdf=epstopdf;;
esac


xticfreqsetting=
case $dataFile in 
    *day*)
        timeformat_in="%Y-%m-%d"
        timeformat_out="%Y-%m-%d"
        ;;
    *week*)
        timeformat_in="%Y-%m-%d"
        timeformat_out="%Y-W%W"
        ;;
    *month*)
        timeformat_in="%Y-%m-%d"
        timeformat_out="%Y-%b"
        ;;
    *year*)
        timeformat_in="%Y-%m-%d"
        timeformat_out="%Y"
        xticfreqsetting="set xtics 60*60*24*365"
        ;;
esac

plot_setting="with boxes"
case $dataFile in 
    *day*)
        plot_setting="with linespoints"
        ;;
    *)
        plot_setting="with boxes"
        ;;
esac

Makeplot_numjob $dataFile
Makeplot_numseq $dataFile
