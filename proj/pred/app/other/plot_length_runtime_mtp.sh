#!/bin/bash
# draw histogram of length vs runtime
# #
usage="
Usage: $0 -pfam FILE -cdd FILE -uniref FILE

Description:
    Draw histogram of runtime vs length of sequence (X axis)

OPTIONS:
    -format  STR    Set the format of output, (default: eps)
                    Can be one of png, terminal or eps
    -add-avg        Overlay the average lines to the plot
    -sep-avg        Make a separate plot with average lines
    -outpath DIR    Set the output path, (default: the same folder as datafile)
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
    local basename=`basename "$dataFile_pfam"` 
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
set key left spacing 1.25
set style data points
set logscale x
set logscale y
plot 1/0 ls 11 with points t "Pfam", \
     1/0 ls 12 with points t "CDD", \
     1/0 ls 13 with points t "UniRef", \
     "$dataFile_pfam"   using 1:2 ls 1 notitle, \
     "$dataFile_cdd"    using 1:2 ls 2 notitle, \
     "$dataFile_uniref" using 1:2 ls 3 notitle 
          

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
Makeplot_add_avg(){ #dataFile#{{{
    local basename=`basename "$dataFile_pfam"` 
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
set style line 1 lt 1 pt 0 ps 0 lc rgb "red" lw 1
set style line 2 lt 1 pt 0 ps 0 lc rgb "blue" lw 1
set style line 3 lt 1 pt 0 ps 0 lc rgb "green" lw 1
set style line 11 lt 1 pt 7 ps 2 lc rgb "red" lw 1
set style line 12 lt 1 pt 7 ps 2 lc rgb "blue" lw 1
set style line 13 lt 1 pt 7 ps 2 lc rgb "green" lw 1
set style line 21 lt 1 pt 7 ps 2 lc rgb "grey10" lw 2
set style line 22 lt 1 pt 7 ps 2 lc rgb "grey30" lw 2
set style line 33 lt 1 pt 7 ps 2 lc rgb "grey50" lw 2

set key autotitle columnhead

set title ""
set xlabel "Length of sequence" 
set ylabel "Running time (s)"
set key left spacing 1.25
set style data points
set logscale x
set logscale y
plot 1/0 ls 11 with points t "Pfam", \
     1/0 ls 12 with points t "CDD", \
     1/0 ls 13 with points t "UniRef", \
     "$dataFile_pfam"   using 1:2 ls 1 notitle, \
     "$dataFile_cdd"    using 1:2 ls 2 notitle, \
     "$dataFile_uniref" using 1:2 ls 3 notitle, \
     "$dataFile_pfam_avg"   using 1:2 ls 21 title "Pfam (average)" with lines, \
     "$dataFile_cdd_avg"    using 1:2 ls 22 title "CDD (average)" with lines, \
     "$dataFile_uniref_avg" using 1:2 ls 23 title "UniRef (average)" with lines

EOF

    case $outputStyle in
        eps)
            $eps2pdf $outfile
            convert -density 200 -background white  $outpath/$basename.pdf $outpath/$basename.png
            echo "Histogram image output to $pdffile"
            ;;
        *) echo "Histogram image output to $outfile" ;;
    esac
}
#}}}
Makeplot_sep_avg(){ #dataFile#{{{
    local basename=`basename "$dataFile_pfam"` 
    basename=${basename}.mtp.avgline
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
set style line 1 lt 1 pt 0 ps 0 lc rgb "red" lw 2
set style line 2 lt 1 pt 0 ps 0 lc rgb "blue" lw 2
set style line 3 lt 1 pt 0 ps 0 lc rgb "green" lw 2
set style line 11 lt 1 pt 7 ps 2 lc rgb "red" lw 1
set style line 12 lt 1 pt 7 ps 2 lc rgb "blue" lw 1
set style line 13 lt 1 pt 7 ps 2 lc rgb "green" lw 1
set style line 21 lt 1 pt 7 ps 2 lc rgb "grey10" lw 2
set style line 22 lt 1 pt 7 ps 2 lc rgb "grey30" lw 2
set style line 33 lt 1 pt 7 ps 2 lc rgb "grey50" lw 2

set key autotitle columnhead

set title ""
set xlabel "Length of sequence" 
set ylabel "Running time (s)"
set key left spacing 1.25
set style data points
set logscale x
set logscale y
plot "$dataFile_pfam_avg"   using 1:2 ls 1 title "Pfam (average)" with lines, \
     "$dataFile_cdd_avg"    using 1:2 ls 2 title "CDD (average)" with lines, \
     "$dataFile_uniref_avg" using 1:2 ls 3 title "UniRef (average)" with lines

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
dataFile_pfam=
dataFile_cdd=
dataFile_uniref=
isAddPlotAvgLine=0
isPlotAvgLine=0
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
            -add-avg|--add-avg) isAddPlotAvgLine=1;;
            -sep-avg|--sep-avg) isPlotAvgLine=1;;
            -format|--format) outputStyle=$2;shift;;
            -pfam|--pfam) dataFile_pfam=$2;shift;;
            -cdd|--cdd) dataFile_cdd=$2;shift;;
            -uniref|--uniref) dataFile_uniref=$2;shift;;
            -outpath|--outpath) outpath=$2;shift;;
            -*) echo "Error! Wrong argument: $1"; exit;;
        esac
    else
        dataFile=$1
    fi
    shift
done

if [ ! -f "$dataFile_pfam" ]; then 
    echo "Error! dataFile_pfam = \"$dataFile_pfam\" does not exist. Exit..."
    exit
fi

if [ "$outpath" == "" ]; then
    outpath=`dirname $dataFile_pfam`
elif [ ! -d "$outpath" ]; then
    mkdir -p $outpath
fi


osname=`uname -s`
eps2pdf=epstopdf
case $osname in 
    *Darwin*) eps2pdf=epstopdf;;
    *Linux*) eps2pdf=epstopdf;;
esac

dataFile_pfam_avg=${dataFile_pfam%.*}.avg.txt
dataFile_cdd_avg=${dataFile_cdd%.*}.avg.txt
dataFile_uniref_avg=${dataFile_uniref%.*}.avg.txt

if [ $isAddPlotAvgLine -eq 0 ];then
    Makeplot
else
    Makeplot_add_avg
fi

if [ $isPlotAvgLine -eq 1 ];then
    Makeplot_sep_avg
fi
