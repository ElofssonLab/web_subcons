#!/bin/bash

# fix the broken link due to bug in the qd script
seqdirlist=$* # this is a list of seq_xx
for seqdir in $seqdirlist;do
    if [  -L $seqdir -o -d $seqdir ];then
        continue
    fi
    index=${seqdir#seq_}
    fafile=../tmpdir/splitaa/query_${index}.fa
    if [ -f $fafile ];then
        md5key=`awk '/^[^>]/ {printf ("%s", $1)}' $fafile | md5sum | awk '{print $1}'`
    fi

    sizekey=${#md5key}
    if [ $sizekey -gt 2 ];then
        cachedir=../../cache/${md5key:0:2}/$md5key
    fi
    #echo $seqdir, $md5key, $sizekey, $cachedir

    if [ -d "$cachedir" ];then
        sudo ln -sf $cachedir $seqdir; sudo chown apache:apache $seqdir
        echo "sudo ln -sf $cachedir $seqdir; sudo chown apache:apache $seqdir"
    fi
done
