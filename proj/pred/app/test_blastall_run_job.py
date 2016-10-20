#!/usr/bin/env python
# Description: run job
import os
import sys
import subprocess
import time
import myfunc
import glob
progname =  os.path.basename(sys.argv[0])
wspace = ''.join([" "]*len(progname))
rundir = os.path.dirname(os.path.realpath(__file__))

os.environ['BLASTMAT'] = "/data3/usr/share/blast/blast-2.2.24/data"
os.environ['BLASTBIN'] = "/data3/usr/share/blast/blast-2.2.24/bin"
os.environ['BLASTDB'] = "/data3/data/blastdb"
blastall = "/data3/usr/share/blast/blast-2.2.24/bin/blastall"
script_getseqlen = "/data3/wk/MPTopo/src/getseqlen.py"

contact_email = "nanjiang.shu@scilifelab.se"

# note that here the url should be without http://

usage_short="""
Usage: %s seqfile_in_fasta -jobid JOBID -outpath DIR -tmpdir DIR -email EMAIL -baseurl BASE_WWW_URL
"""%(progname)

usage_ext="""
Description:
    run job

OPTIONS:
  -h, --help    Print this help message and exit

Created 2015-01-20, updated 2015-01-30, Nanjiang Shu
"""
usage_exp="""
Examples:
    %s /data3/tmp/tmp_dkgSD/query.fa -outpath /data3/result/rst_mXLDGD -tmpdir /data3/tmp/tmp_dkgSD
"""%(progname)

def PrintHelp(fpout=sys.stdout):#{{{
    print >> fpout, usage_short
    print >> fpout, usage_ext
    print >> fpout, usage_exp#}}}

def Sendmail(from_email, to_email, subject, bodytext):#{{{
    sendmail_location = "/usr/sbin/sendmail" # sendmail location
    p = os.popen("%s -t" % sendmail_location, "w")
    p.write("From: %s\n" % from_email)
    p.write("To: %s\n" % to_email)
    p.write("Subject: %s\n"%(subject))
    p.write("\n") # blank line separating headers from body
    p.write(bodytext)
    status = p.close()
    if status != 0:
        print "Sendmail to %s failed with status"%(to_email), status
    else:
        print "Sendmail to %s succeeded"%(to_email)
    return status

#}}}
def RunJob(infile, outpath, tmpdir, email, jobid, g_params):#{{{
    blastdb = "/data3/data/blastdb/swissprot"
    rootname = os.path.basename(os.path.splitext(infile)[0])
    starttagfile   = "%s/runjob.start"%(outpath)
    runjob_errfile = "%s/runjob.err"%(outpath)
    runjob_logfile = "%s/runjob.log"%(outpath)
    finishtagfile = "%s/runjob.finish"%(outpath)
    tmp_outfile = "%s/query.result" %(tmpdir)
    resultpathname = jobid
    outpath_result = "%s/%s"%(outpath, resultpathname)
    outfile = "%s/query.result" %(outpath_result)
    tarball = "%s.tar.gz"%(resultpathname)
    tarball_fullpath = "%s.tar.gz"%(outpath_result)
    isOK = True
    try:
        os.makedirs(outpath_result)
        isOK = True
    except OSError:
        msg = "Failed to create folder %s"%(outpath_result)
        myfunc.WriteFile(msg+"\n", runjob_errfile, "a")
        isOK = False

    if isOK:
        g_params['runjob_log'].append("tmpdir = %s"%(tmpdir))
        #cmd = [script_getseqlen, infile, "-o", tmp_outfile , "-printid"]
        datetime = time.strftime("%Y-%m-%d %H:%M:%S")
        rt_msg = myfunc.WriteFile(datetime, starttagfile)
        if rt_msg:
            g_params['runjob_err'].append(rt_msg)

        cmd = [blastall, "-i", infile,  "-p", "blastp","-o", tmp_outfile,  "-d", blastdb  ]

        g_params['runjob_log'].append(" ".join(cmd))
        try:
            myfunc.check_output(cmd)
        except subprocess.CalledProcessError, e:
            g_params['runjob_err'].append(str(e))
            suqoutfilelist = glob.glob("%s/*.sh.*.out"%(tmpdir))
            if len(suqoutfilelist)>0:
                suqoutfile = suqoutfilelist[0]
            g_params['runjob_err'].append(myfunc.ReadFile(suqoutfile))

        if os.path.exists(tmp_outfile):
            cmd = ["cp","-f", tmp_outfile, outfile]
            try:
                myfunc.check_output(cmd)
            except subprocess.CalledProcessError, e:
                g_params['runjob_err'].append(str(e))

        if len(g_params['runjob_log']) > 0 :
            rt_msg = myfunc.WriteFile("\n".join(g_params['runjob_log']), runjob_logfile, "w")
            if rt_msg:
                g_params['runjob_err'].append(rt_msg)

        datetime = time.strftime("%Y-%m-%d %H:%M:%S")
        if os.path.exists(outfile):
            rt_msg = myfunc.WriteFile(datetime, finishtagfile)
            if rt_msg:
                g_params['runjob_err'].append(rt_msg)

        # make the tarball
        pwd = os.getcwd()
        os.chdir(outpath)
        cmd = ["tar", "-czf", tarball, resultpathname]
        try:
            myfunc.check_output(cmd)
        except subprocess.CalledProcessError, e:
            g_params['runjob_err'].append(str(e))
        os.chdir(pwd)

    isSuccess = False
    if (os.path.exists(finishtagfile) and os.path.exists(outfile) and
            os.path.exists(tarball_fullpath)):
        isSuccess = True
    else:
        isSuccess = False
        failtagfile = "%s/runjob.failed"%(outpath)
        datetime = time.strftime("%Y-%m-%d %H:%M:%S")
        rt_msg = myfunc.WriteFile(datetime, failtagfile)
        if rt_msg:
            g_params['runjob_err'].append(rt_msg)

# send the result to email
    if myfunc.IsValidEmailAddress(email):
        from_email = "info@topcons.net"
        to_email = email
        subject = "Your result for TOPCONS2 JOBID=%s"%(jobid)
        if isSuccess:
            bodytext = """
Your result is ready at %s/pred/result/%s

Thanks for using TOPCONS2

        """%(g_params['base_www_url'], jobid)
        else:
            bodytext="""
We are sorry that your job with jobid %s is failed.

Please contact %s if you have any questions.

Attached below is the error message:
%s
            """%(jobid, contact_email, "\n".join(g_params['runjob_err']))
        g_params['runjob_log'].append("Sendmail %s -> %s, %s"% (from_email, to_email, subject)) #debug
        rtValue = Sendmail(from_email, to_email, subject, bodytext)
        if rtValue != 0:
            g_params['runjob_err'].append("Sendmail to %s failed with status %d"%(to_email, rtValue))

    if len(g_params['runjob_err']) > 0:
        rt_msg = myfunc.WriteFile("\n".join(g_params['runjob_err']), runjob_errfile, "w")
        return 1
    return 0
#}}}
def main(g_params):#{{{
    argv = sys.argv
    numArgv = len(argv)
    if numArgv < 2:
        PrintHelp()
        return 1

    outpath = ""
    infile = ""
    tmpdir = ""
    email = ""
    jobid = ""

    i = 1
    isNonOptionArg=False
    while i < numArgv:
        if isNonOptionArg == True:
            infile = argv[i]
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
            elif argv[i] in ["-tmpdir", "--tmpdir"] :
                (tmpdir, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-jobid", "--jobid"] :
                (jobid, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-baseurl", "--baseurl"] :
                (g_params['base_www_url'], i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-email", "--email"] :
                (email, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-q", "--q"]:
                g_params['isQuiet'] = True
                i += 1
            else:
                print >> sys.stderr, "Error! Wrong argument:", argv[i]
                return 1
        else:
            infile = argv[i]
            i += 1

    if jobid == "":
        print >> sys.stderr, "%s: jobid not set. exit"%(sys.argv[0])
        return 1

    if myfunc.checkfile(infile, "infile"):
        return 1
    if outpath == "":
        print >> sys.stderr, "outpath not set. exit"
        return 1
    elif not os.path.exists(outpath):
        try:
            myfunc.check_output(["mkdir", "-p", outpath])
        except subprocess.CalledProcessError, e:
            print >> sys.stderr, e
            return 1
    if tmpdir == "":
        print >> sys.stderr, "tmpdir not set. exit"
        return 1
    elif not os.path.exists(tmpdir):
        try:
            myfunc.check_output(["mkdir", "-p", tmpdir])
        except subprocess.CalledProcessError, e:
            print >> sys.stderr, e
            return 1

    return RunJob(infile, outpath, tmpdir, email, jobid, g_params)

#}}}

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['isQuiet'] = True
    g_params['runjob_log'] = []
    g_params['runjob_err'] = []
    g_params['base_www_url'] = ""
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    sys.exit(main(g_params))
