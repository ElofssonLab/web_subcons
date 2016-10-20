#!/bin/bash
# initialize the working folder
exec_cmd(){
#    echo "$*"
    eval "$*"
}
rundir=`dirname $0`

rundir=`realpath $rundir`
cd $rundir


dirlist="
$rundir/proj/pred/static/tmp
$rundir/proj/pred/static/result
$rundir/proj/pred/static/md5
$rundir/proj/pred/static/log
$rundir/proj/pred/static/log/stat
$rundir/proj/pred/static/log/divided
"

echo "setting up file permissions"
platform_info=`python -mplatform |  tr '[:upper:]' '[:lower:]'`
platform=
case $platform_info in 
    *centos*)platform=centos;;
    *ubuntu*)platform=ubuntu;;
    *)platform=other;;
esac


case $platform in 
    centos) user=apache;group=apache;;
    ubuntu) user=www-data;group=www-data;;
    other)echo Unrecognized plat form; exit 1;;
esac



for dir in  $dirlist; do
    if [ ! -d $dir ];then
        sudo mkdir -p $dir
    fi
    sudo chmod 755 $dir
    sudo chown $user:$group $dir
done

logfile_submit=$rundir/proj/pred/static/log/submitted_seq.log
if [ ! -f $logfile_submit ];then
    sudo touch $logfile_submit
fi
sudo chmod 644 $logfile_submit
sudo chown $user:$group $logfile_submit

# fix the settings.py
if [ ! -f $rundir/settings.py ];then
    pushd $rundir/proj; ln -s pro_settings.py settings.py; popd;
fi
