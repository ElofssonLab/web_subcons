#!/bin/bash
# draw histogram of average runtime
# #
progname=`basename $0`
usage="
Usage: $progname datafile

Description:
    Draw histogram of average runtime

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


    local max_yrange=`cat $dataFile | awk 'BEGIN{max=-1}{if($2>max) {max=$2}}END{print int(max)+10}'`
/usr/bin/env gnuplot -persist<<EOF 
$outputSetting
set style line 1 lt 1 pt 7 ps 1 lc rgb "red" lw 1
set style line 2 lt 1 pt 7 ps 1 lc rgb "blue" lw 1
set style line 3 lt 1 pt 7 ps 1 lc rgb "green" lw 1
set style line 11 lt 1 pt 7 ps 2 lc rgb "red" lw 1
set style line 12 lt 1 pt 7 ps 2 lc rgb "blue" lw 1
set style line 13 lt 1 pt 7 ps 2 lc rgb "green" lw 1

set bmargin at screen 0.20


set title ""
set ylabel ""
set xlabel ""
set xtic rotate by 90 scale 0 offset $offset_xtic
unset ytics
set style data histograms
set style fill solid 0.9 border -1
set boxwidth 0.9
set y2tics rotate by 90 offset $offset_y2tic
set yrange[0:$max_yrange]
set y2label "Running time (s)" offset $offset_y2label
set size 0.5, 1
set grid y2
plot "$dataFile" using 2:xtic(1) ls 2 notitle

EOF

    case $outputStyle in
        eps)
            $eps2pdf $outfile
            convert -density 200 -background white $outpath/$basename.pdf $outpath/$basename.png
            convert -rotate 90 $outpath/$basename.png $outpath/$basename.rot.png
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
offset_xtic="1,-3"
offset_y2tic="0,-1"
offset_y2label="-2"
if [ -f "/etc/redhat-release" ];then 
    platform="centos"
    offset_xtic="0,0"
    offset_y2tic="0,0"
    offset_y2label="0"
else
    platform="notcentos"
    offset_xtic="1,-3"
    offset_y2tic="0,-1"
    offset_y2label="-2"
fi

Makeplot $dataFile
