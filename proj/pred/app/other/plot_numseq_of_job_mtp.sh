#!/bin/bash
# draw histogram of numjob vs numseq_of_job
# #
progname=`basename $0`
usage="
Usage: $progname -web FILE -wsdl FILE

Description:
    Draw histogram of numjob vs numseq_of_job (X axis)

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
    local basename=`basename "$dataFile_web"` 
    basename=${basename}.mtp
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

set title ""
set xlabel "Number of sequences of jobs" 
set ylabel "Number of jobs"
set style data linespoints
set logscale x
set logscale y
plot "$dataFile_web" using 1:2 ls 1  title "Web", \
     "$dataFile_wsdl" using 1:2 ls 2 title "WSDL"
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
dataFile_web=
dataFile_wsdl=
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
            -web|--web) dataFile_web=$2;shift;;
            -wsdl|--wsdl) dataFile_wsdl=$2;shift;;
            -outpath|--outpath) outpath=$2;shift;;
            -*) echo "Error! Wrong argument: $1"; exit;;
        esac
    else
        dataFile=$1
    fi
    shift
done

if [ ! -f "$dataFile_web" ]; then 
    echo "Error! dataFile_web = \"$dataFile_web\" does not exist. Exit..."
    exit
fi

if [ "$outpath" == "" ]; then
    outpath=`dirname $dataFile_web`
elif [ ! -d "$outpath" ]; then
    mkdir -p $outpath
fi


osname=`uname -s`
eps2pdf=epstopdf
case $osname in 
    *Darwin*) eps2pdf=epstopdf;;
    *Linux*) eps2pdf=epstopdf;;
esac
Makeplot
