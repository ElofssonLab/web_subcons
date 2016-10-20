#!/bin/bash

filelist=$*
for file in $filelist;do
    if [ ! -L $file ];then
        continue
    fi
    dirname=`dirname $file`
    basename=`basename $file`
    newlink=$(readlink $file | sed 's/^..\///')
    pushd $dirname; sudo ln -sf $newlink $basename; sudo chown apache:apache $basename; popd
    echo "pushd $dirname; sudo ln -sf $newlink $basename; sudo chown apache:apache $basename; popd"
done
