#!/usr/bin/env python
# Description: submit job to queue
# ChangeLog 2015-03-26 
#   1. suq ntask is set universally for each node by the qd_topcons2_fe.py
#   2. priority is re-calculated, considering waiting time and numseq_this_user
#   in the queue, note that numseq_this_user should be privoded outside of this
#   script
# ChangeLog 2015-04-15 
#   1. if suq submit failed, try MAX_TRY times, sleep 0.05 second for the next
#   try
import os
import sys
import myfunc
import subprocess
import time
import math
suq_exec = "/usr/bin/suq"
progname =  os.path.basename(__file__)
wspace = ''.join([" "]*len(progname))

vip_user_list = [
        "nanjiang.shu@scilifelab.se"
        ]

rundir = os.path.dirname(os.path.realpath(__file__))
basedir = os.path.realpath("%s/../"%(rundir))
python_exec = os.path.realpath("%s/../../env/bin/python"%(basedir))
suq_basedir = "/tmp"
if os.path.exists("/scratch"):
    suq_basedir = "/scratch"
elif os.path.exists("/tmp"):
    suq_basedir = "/tmp"
gen_errfile = "%s/static/log/%s.log"%(basedir, progname)

usage_short="""
Usage: %s -nseq INT -jobid STR -outpath DIR -datapath DIR
       %s -email EMAIL -host IP -baseurl BASE_WWW_URL
       %s -nseq-this-user INT
       %s [-force]

Description: 
    BASE_WWW_URL e.g. topcons.net
"""%(progname, wspace, wspace, wspace)

usage_ext="""
Description:
    Submit job to queue
    datapath should include query.fa

OPTIONS:
  -force            Do not use cahced result
  -nseq-this-user   Number of sequences in the queue submitted by this user
  -h, --help    Print this help message and exit

Created 2015-01-20, updated 2015-03-26, Nanjiang Shu
"""
usage_exp="""
Examples:
    %s -jobid rst_mXLDGD -outpath /data3/result/rst_mXLDGD -datapath /data3/tmp/tmp_dkgSD
"""%(progname)

def PrintHelp(fpout=sys.stdout):#{{{
    print >> fpout, usage_short
    print >> fpout, usage_ext
    print >> fpout, usage_exp#}}}

def SubmitJobToQueue(jobid, datapath, outpath, numseq, numseq_this_user, email, #{{{
        host_ip, base_www_url):
    myfunc.WriteFile("Entering SubmitJobToQueue()\n", g_params['debugfile'],
            "a", True)
    fafile = "%s/query.fa"%(datapath)

    if numseq == -1:
        numseq = myfunc.CountFastaSeq(fafile)
    if numseq_this_user == -1:
        numseq_this_user = numseq

    runjob = "%s %s/run_job.py"%(python_exec, rundir)
    scriptfile = "%s/runjobSPLIT%sSPLIT%sSPLIT%sSPLIT%d.sh"%(datapath, jobid, host_ip, email, numseq)
    code_str_list = []
    code_str_list.append("#!/bin/bash")
    cmdline = "%s %s -outpath %s -tmpdir %s -jobid %s "%(runjob, fafile, outpath, datapath, jobid)
    if email != "":
        cmdline += "-email \"%s\" "%(email)
    if base_www_url != "":
        cmdline += "-baseurl \"%s\" "%(base_www_url)
    if g_params['isForceRun']:
        cmdline += "-force "
    code_str_list.append(cmdline)

    code = "\n".join(code_str_list)

    msg = "Write scriptfile %s"%(scriptfile)
    myfunc.WriteFile(msg+"\n", g_params['debugfile'], "a", True)

    myfunc.WriteFile(code, scriptfile, mode="w", isFlush=True)
    os.chmod(scriptfile, 0755)

    myfunc.WriteFile("Getting priority"+"\n", g_params['debugfile'], "a", True)
    priority = myfunc.GetSuqPriority(numseq_this_user)

    if email in vip_user_list:
        priority = 999999999.0

    myfunc.WriteFile("priority=%d\n"%(priority), g_params['debugfile'], "a",
            True)

    st1 = SubmitSuqJob(suq_basedir, datapath, priority, scriptfile)

    return st1
#}}}
def SubmitSuqJob(suq_basedir, datapath, priority, scriptfile):#{{{
    myfunc.WriteFile("Entering SubmitSuqJob()\n", g_params['debugfile'], "a",
            True)
    rmsg = ""
    cmd = [suq_exec,"-b", suq_basedir, "run", "-d", datapath, "-p", "%d"%(priority), scriptfile]
    cmdline = " ".join(cmd)
    myfunc.WriteFile("cmdline: %s\n\n"%(cmdline), g_params['debugfile'], "a",
            True)
    MAX_TRY = 5
    cnttry = 0
    isSubmitSuccess = False
    while cnttry < MAX_TRY:
        try:
            myfunc.WriteFile("run cmd: cnttry = %d, MAX_TRY=%d\n"%(cnttry,
                MAX_TRY), g_params['debugfile'], "a", True)
            rmsg = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            isSubmitSuccess = True
            break
        except subprocess.CalledProcessError, e:
            print  e
            print rmsg
            myfunc.WriteFile(str(e)+"\n"+rmsg+"\n", g_params['debugfile'],
                    "a", True)
            pass
        cnttry += 1
        time.sleep(0.05+cnttry*0.03)
    if isSubmitSuccess:
        myfunc.WriteFile("Leaving SubmitSuqJob() with success\n\n",
                g_params['debugfile'], "a", True)
        return 0
    else:
        myfunc.WriteFile("Leaving SubmitSuqJob() with error\n\n",
                g_params['debugfile'], "a", True)
        return 1
#}}}
def main(g_params):#{{{
    argv = sys.argv
    numArgv = len(argv)
    if numArgv < 2:
        PrintHelp()
        return 1

    rmsg = ""
    outpath = ""
    jobid = ""
    datapath = ""
    numseq = -1
    numseq_this_user = -1
    email = ""
    host_ip = ""
    base_www_url = ""
    i = 1
    isNonOptionArg=False
    while i < numArgv:
        if isNonOptionArg == True:
            print >> g_params['fperr'], "Error! Wrong argument:", argv[i]
            return 1
            isNonOptionArg = False
            i += 1
        elif argv[i] == "--":
            isNonOptionArg = True
            i += 1
        elif argv[i][0] == "-":
            if argv[i] in ["-h", "--help"]:
                PrintHelp()
                return 1
            elif argv[i] in ["-outpath", "--outpath"]:
                (outpath, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-email", "--email"]:
                (email, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-host", "--host"]:
                (host_ip, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-nseq", "--nseq"]:
                (numseq, i) = myfunc.my_getopt_int(argv, i)
            elif argv[i] in ["-nseq-this-user", "--nseq-this-user"]:
                (numseq_this_user, i) = myfunc.my_getopt_int(argv, i)
            elif argv[i] in ["-baseurl", "--baseurl"]:
                (base_www_url, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-jobid", "--jobid"] :
                (jobid, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-datapath", "--datapath"] :
                (datapath, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-force", "--force"]:
                g_params['isForceRun'] = True
                i += 1
            elif argv[i] in ["-q", "--q"]:
                g_params['isQuiet'] = True
                i += 1
            else:
                print >> g_params['fperr'], "Error! Wrong argument:", argv[i]
                return 1
        else:
            print >> g_params['fperr'], "Error! Wrong argument:", argv[i]
            return 1

    if outpath == "":
        print >> g_params['fperr'], "outpath not set. exit"
        return 1
    elif not os.path.exists(outpath):
        cmd =  ["mkdir", "-p", outpath]
        try:
            rmsg = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, e:
            print e
            print rmsg
            return 1

    if jobid == "":
        print >> g_params['fperr'], "%s: jobid not set. exit"%(sys.argv[0])
        return 1

    if datapath == "":
        print >> g_params['fperr'], "%s: datapath not set. exit"%(sys.argv[0])
        return 1
    elif not os.path.exists(datapath):
        print >> g_params['fperr'], "%s: datapath does not exist. exit"%(sys.argv[0])
        return 1
    elif not os.path.exists("%s/query.fa"%(datapath)):
        print >> g_params['fperr'], "%s: file %s/query.fa does not exist. exit"%(sys.argv[0], datapath)
        return 1

    g_params['debugfile'] = "%s/debug.log"%(outpath)

    myfunc.WriteFile("Go to SubmitJobToQueue()\n", g_params['debugfile'], "a", True)
    return SubmitJobToQueue(jobid, datapath, outpath, numseq, numseq_this_user,
            email, host_ip, base_www_url)

#}}}

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['isQuiet'] = True
    g_params['isForceRun'] = False
    g_params['fperr'] = None
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    try:
        g_params['fperr'] = open(gen_errfile, "a")
    except IOError:
        g_params['fperr'] = sys.stderr
        pass
    g_params = InitGlobalParameter()
    status = main(g_params)
    if g_params['fperr'] and g_params['fperr'] != sys.stderr:
        g_params['fperr'].close()
    sys.exit(status)

