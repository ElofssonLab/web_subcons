#!/bin/bash
# draw histogram of length vs runtime
# #
usage="
Usage: $0 datafile

Description:
    Draw histogram of runtime vs length of sequence (X axis)

OPTIONS:
    -format  STR    Set the format of output, (default: eps)
                    Can be one of png, terminal or eps
    -outpath DIR    Set the output path, (default: ./)
    -h, --help      Print this help message and exit

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
    local dataFile=$1
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

    local linestyle1=1
    local linestyle2=11
    local keytitle=
    case $dataFile in
        *pfam*)
            linestyle1=1
            linestyle2=11
            keytitle=Pfam
            ;;
        *cdd*)
            linestyle1=2
            linestyle2=12
            keytitle=CDD
            ;;
        *uniref*)
            linestyle1=3
            linestyle2=13
            keytitle=UniRef
            ;;
    esac


/usr/bin/env gnuplot -persist<<EOF 
$outputSetting
set style line 1 lt 1 pt 0 ps 0 lc rgb "red" lw 1
set style line 2 lt 1 pt 0 ps 0 lc rgb "blue" lw 1
set style line 3 lt 1 pt 0 ps 0 lc rgb "green" lw 1
set style line 11 lt 1 pt 7 ps 2 lc rgb "red" lw 1
set style line 12 lt 1 pt 7 ps 2 lc rgb "blue" lw 1
set style line 13 lt 1 pt 7 ps 2 lc rgb "green" lw 1

set key autotitle columnhead

set title ""
set xlabel "Length of sequence" 
set ylabel "Running time (s)"
set style data points
set logscale x
set logscale y
plot 1/0 ls $linestyle2 with points t "$keytitle", \
     "$dataFile" using 1:2 ls $linestyle1 notitle

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
Makeplot $dataFile
