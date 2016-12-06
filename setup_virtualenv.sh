#!/bin/bash
# install virtualenv if not installed
# first install dependencies
# install python2.7 if not exists by
# sudo /big/src/install_python2.7_centos.sh
# sudo pip2.7 install virtualenv

# then install programs in the virtual environment
rundir=`dirname $0`
cd $rundir
exec_virtualenv=virtualenv
if [ -f "/usr/local/bin/virtualenv" ];then
    exec_virtualenv=/usr/local/bin/virtualenv
fi
eval "$exec_virtualenv env"
source ./env/bin/activate

pip install --ignore-installed -r requirements.txt
