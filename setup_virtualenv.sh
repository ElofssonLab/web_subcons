#!/bin/bash
# install virtualenv if not installed
# first install dependencies
# make sure virtualenv is installed with Python3
if [ ! $(type -P pip3) ] ;then
    sudo python3 -m pip install --upgrade pip
fi
if [ ! $(type -P virtualenv) ] ;then
    sudo pip3 install virtualenv
fi
# then install programs in the virtual environment
mkdir -p ~/.virtualenvs
rundir=`dirname $0`
rundir=`readlink -f $rundir`
cd $rundir
virtualenv env
source ./env/bin/activate

pip3 install --ignore-installed -r requirements.txt
# below is a hack to make python3 version of geoip working
pip3 uninstall --yes python-geoip
pip3 install  python-geoip-python3==1.3

#Install gnuplot 4.2.6
gnuplot_version=4.2.6
echo -e "\nInstall gnuplot $gnuplot_version to env\n"
tmpdir=$(mktemp -d /tmp/tmpdir.setup_virtualenv.XXXXXXXXX) || { echo "Failed to create temp dir" >&2; exit 1; }
url=https://sourceforge.net/projects/gnuplot/files/gnuplot/${gnuplot_version}/gnuplot-${gnuplot_version}.tar.gz/download
filename=gnuplot-${gnuplot_version}.tar.gz
cd $tmpdir
wget $url -O $filename
tar -xzf $filename
foldername=$(find . -maxdepth 1 -type d -name "[^.]*")
if [ "$foldername" != "" ];then
    cd $foldername
    ./configure --prefix $rundir/env
    make && make install
else
    echo "fetching gnuplot package filed"
fi
cd $rundir
/bin/rm -rf $tmpdir

