#!/usr/bin/env python

# Description: access v2.topcons.net via WSDL service
# Copyright Nanjiang Shu (nanjiang.shu@scilifelab.se)

import os
import sys
progname =  os.path.basename(sys.argv[0])
wspace = ''.join([" "]*len(progname))

no_suds_message="""\
suds is not installed!
Please install suds by

$ pip install suds
"""

try:
    from suds.client import Client
except ImportError:
    print >> sys.stderr, no_suds_message
    sys.exit(1)

import urllib

MAX_FILESIZE_IN_MB = 9
MAX_FILESIZE = MAX_FILESIZE_IN_MB*1024*1024

usage_short="""
Usage: %s -m submit|get [-seq SEQFILE] [-jobname NAME] [-email EMAIL]
       %s               [-fix FIXFILE]
       %s               [-jobid JOBID] [-outpath DIR]
"""%(progname, wspace, wspace)

usage_ext="""
Description:
    Access topcons2 web-server (http://v2.topcons.net) through WSDL service

OPTIONS:
  -m submit|get  Set the mode
                 submit - submit a job to WSDL
                 get    - retrieve the result from the server

  -seq    FILE   Supply input sequence in FASTA format

  -jobname STR   Give the job a name

  -email   STR   Send a notification to the email when the result is ready

  -jobid   STR   Retrieve the result by supplying a valid jobid

  -outpath DIR   Save the retrieved data to outpath, (default: ./)

  -fix    FILE   Supply a file with topology constraints (TO BE IMPLEMENTED)

  -h, --help     Print this help message and exit

Created 2015-02-04, updated 2015-02-06, Nanjiang Shu
"""
usage_exp="""
Examples:
    # submit test.fa with jobname 'test' to the server 
    %s -m submit -seq test.fa -jobname test

    # try to retrieve the result for jobid 'rst_TTT' and save it to the current
    # directory
    %s -m get -jobid rst_TTT

"""%(progname, progname)

def my_getopt_str(argv, i):#{{{
    """
    Get a string from the argument list, return the string and the updated
    index to the argument list
    """
    try:
        opt = argv[i+1]
        if opt[0] == "-":
            msg = "Error! option '%s' must be followed by a string"\
                    ", not an option arg."
            print >> sys.stderr, msg%(argv[i])
            sys.exit(1)
        return (opt, i+2)
    except IndexError:
        msg = "Error! option '%s' must be followed by a string"
        print >> sys.stderr, msg%(argv[i])
        sys.exit(1)
#}}}
def PrintHelp(fpout=sys.stdout):#{{{
    print >> fpout, usage_short
    print >> fpout, usage_ext
    print >> fpout, usage_exp#}}}
def ReadFile(infile, mode="r"):#{{{
    try: 
        fpin = open(infile, mode)
        content = fpin.read()
        fpin.close()
        return content
    except IOError:
        print >> sys.stderr, "Failed to read file %s with mode '%s'"%(infile,
                mode)
        return ""
#}}}

def main(g_params):#{{{
    argv = sys.argv
    numArgv = len(argv)
    if numArgv < 2:
        PrintHelp()
        return 1

    wsdl_url = "http://v2.topcons.net/pred/api_submitseq/?wsdl"
    mode = ""
    jobid = ""
    email = ""
    jobname = ""
    fixtopfile = ""
    seqfile = ""
    outpath = "./"

    i = 1
    isNonOptionArg=False
    while i < numArgv:
        if isNonOptionArg == True:
            print >> sys.stderr, "Error! Wrong argument:", argv[i]
            return 1
            isNonOptionArg = False
            i += 1
        elif argv[i] == "--":
            isNonOptionArg = True
            i += 1
        elif argv[i][0] == "-":
            if argv[i] in ["-h", "--help"]:
                PrintHelp()
                return 0
            elif argv[i] in ["-m", "--m"]:
                (mode, i) = my_getopt_str(argv, i)
            elif argv[i] in ["-seq", "--seq"]:
                (seqfile, i) = my_getopt_str(argv, i)
            elif argv[i] in ["-jobname", "--jobname"]:
                (jobname, i) = my_getopt_str(argv, i)
            elif argv[i] in ["-email", "--email"]:
                (email, i) = my_getopt_str(argv, i)
            elif argv[i] in ["-fix", "--fix"]:
                (fixtopfile, i) = my_getopt_str(argv, i)
            elif argv[i] in ["-jobid", "--jobid"]:
                (jobid, i) = my_getopt_str(argv, i)
            elif argv[i] in ["-outpath", "--outpath"]:
                (outpath, i) = my_getopt_str(argv, i)
            else:
                print >> sys.stderr, "Error! Wrong argument:", argv[i]
                return 1
        else:
            print >> sys.stderr, "Error! Wrong argument:", argv[i]
            return 1

    if mode == "":
        print >> sys.stderr, "mode not set. exit!"
        print usage_short
        return 1
    elif not mode in ["submit", "get"]:
        print >> sys.stderr, "unrecognized mode. exit!"
        print usage_short
        return 1

    if mode == "submit":
        if seqfile == "":
            print >> sys.stderr, "You want to submit a job but seqfile not set. exit!"
            print usage_short
            return 1
        elif not os.path.exists(seqfile):
            print >> sys.stderr, "seqfile %s does not exist. exit!"%(seqfile)
            return 1

        try:
            filesize = os.path.getsize(seqfile)
        except OSError:
            print >> sys.stderr, "failed to get the size of seqfile %s. exit"%(seqfile)
            return 1

        if filesize >= MAX_FILESIZE:
            print >> sys.stderr, "You input seqfile %s exceeds the "\
                    "upper limit %d Mb."%(seqfile, MAX_FILESIZE_IN_MB)
            print >> sys.stderr, "Please split your seqfile and submit again."
            return 1
        seq = ReadFile(seqfile)

        fixtop = ""
        if fixtopfile != "":
            fixtop = ReadFile(fixtopfile)
        myclient = Client(wsdl_url, cache=None)
        retValue = myclient.service.submitjob(seq, fixtop, jobname, email)
        if len(retValue) >= 1:
            strs = retValue[0]
            jobid = strs[0]
            result_url = strs[1]
            numseq_str = strs[2]
            errinfo = strs[3]
            warninfo = strs[4]
            if jobid != "None" and jobid != "":
                print "You have successfully submitted your job "\
                        "with %s sequences. jobid = %s"%(numseq_str, jobid)
                if warninfo != "" and warninfo != "None":
                    print "Warning message:\n", warninfo
            else:
                print "Failed to submit job!"
                if errinfo != "" and errinfo != "None":
                    print "Error message:\n", errinfo
                if warninfo != "" and warninfo != "None":
                    print "Warning message:\n", warninfo
        else:
            print "Failed to submit job!"
            return 1
    else:
        if jobid == "":
            print >> sys.stderr, "You want to get the result of a job but jobid not set. exit!"
            return 1
        myclient = Client(wsdl_url, cache=None)
        retValue = myclient.service.checkjob(jobid)
        if len(retValue) >= 1:
            strs = retValue[0]
            status = strs[0]
            result_url = strs[1]
            errinfo = strs[2]
            if status == "Failed":
                print "Your job with jobid %s is failed!"%(jobid)
                if errinfo != "" and errinfo != "None":
                    print "Error message:\n", errinfo
            elif status == "Finished":
                print "Your job with jobid %s is finished!"%(jobid)
                if not os.path.exists(outpath):
                    try:
                        os.makedirs(outpath)
                    except OSError:
                        print "Failed to create the outpath %s"%(outpath)
                        return 1
                outfile = "%s/%s.zip"%(outpath, jobid)
                urllib.urlretrieve (result_url, outfile)
                if os.path.exists(outfile):
                    print "The result file %s has been retrieved for jobid %s"%(outfile, jobid)
                else:
                    print "Failed to retrieve result for jobid %s"%(jobid)
            elif status == "None":
                print "Your job with jobid %s does not exist! Please check you typing!"%(jobid)
            else:
                print "Your job with jobid %s is not ready, status = %s"%(jobid, status)
        else:
            print "Failed to get job!"
            return 1

    return 0

#}}}

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['isQuiet'] = True
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    sys.exit(main(g_params))

