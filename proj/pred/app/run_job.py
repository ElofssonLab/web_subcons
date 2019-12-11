#!/usr/bin/env python
# Description: run job
import os
import sys
import subprocess
import time
from libpredweb import myfunc
from libpredweb import webserver_common as webcom
import glob
import hashlib
import shutil
import datetime
import site
import fcntl
progname =  os.path.basename(sys.argv[0])
wspace = ''.join([" "]*len(progname))
rundir = os.path.dirname(os.path.realpath(__file__))
webserver_root = os.path.realpath("%s/../../../"%(rundir))
activate_env="%s/env/bin/activate_this.py"%(webserver_root)
exec(compile(open(activate_env, "rb").read(), activate_env, 'exec'), dict(__file__=activate_env))

site.addsitedir("%s/env/lib/python2.7/site-packages/"%(webserver_root))
sys.path.append("/usr/local/lib/python2.7/dist-packages")

runscript = "%s/%s"%(rundir, "soft/subcons/master_subcons.sh")
#runscript = "%s/%s"%(rundir, "soft/subcons/dummyrun.sh")

basedir = os.path.realpath("%s/.."%(rundir)) # path of the application, i.e. pred/
path_cache = "%s/static/result/cache"%(basedir)
path_result = "%s/static/result/"%(basedir)
gen_errfile = "%s/static/log/%s.err"%(basedir, progname)
gen_logfile = "%s/static/log/%s.log"%(basedir, progname)

contact_email = "nanjiang.shu@scilifelab.se"
vip_user_list = [
        "nanjiang.shu@scilifelab.se"
        ]

# note that here the url should be without http://

FORMAT_DATETIME = webcom.FORMAT_DATETIME
TZ = webcom.TZ


usage_short="""
Usage: %s seqfile_in_fasta 
       %s -jobid JOBID -outpath DIR -tmpdir DIR
       %s -email EMAIL -baseurl BASE_WWW_URL
       %s -only-get-cache [-force]
"""%(progname, wspace, wspace, wspace)

usage_ext="""\
Description:
    run job

OPTIONS:
  -only-get-cache   Only get the cached results, this will be run on the front-end
  -force            Do not use cahced result
  -h, --help        Print this help message and exit

Created 2016-12-01, updated 2016-12-07, Nanjiang Shu
"""
usage_exp="""
Examples:
    %s /data3/tmp/tmp_dkgSD/query.fa -outpath /data3/result/rst_mXLDGD -tmpdir /data3/tmp/tmp_dkgSD
"""%(progname)

def PrintHelp(fpout=sys.stdout):#{{{
    print(usage_short, file=fpout)
    print(usage_ext, file=fpout)
    print(usage_exp, file=fpout)#}}}

def RunJob(infile, outpath, tmpdir, email, jobid, g_params):#{{{
    all_begin_time = time.time()

    rootname = os.path.basename(os.path.splitext(infile)[0])
    starttagfile   = "%s/runjob.start"%(outpath)
    runjob_errfile = "%s/runjob.err"%(outpath)
    runjob_logfile = "%s/runjob.log"%(outpath)
    app_logfile = "%s/app.log"%(outpath)
    finishtagfile = "%s/runjob.finish"%(outpath)
    rmsg = ""


    resultpathname = jobid

    outpath_result = "%s/%s"%(outpath, resultpathname)
    tmp_outpath_result = "%s/%s"%(tmpdir, resultpathname)

    tarball = "%s.tar.gz"%(resultpathname)
    zipfile = "%s.zip"%(resultpathname)
    tarball_fullpath = "%s.tar.gz"%(outpath_result)
    zipfile_fullpath = "%s.zip"%(outpath_result)
    resultfile_text = "%s/%s"%(outpath_result, "query.result.txt")
    mapfile = "%s/seqid_index_map.txt"%(outpath_result)
    finished_seq_file = "%s/finished_seqs.txt"%(outpath_result)
    finished_idx_file = "%s/finished_seqindex.txt"%(outpath)

    for folder in [outpath_result, tmp_outpath_result]:
        try:
            os.makedirs(folder)
        except OSError:
            msg = "Failed to create folder %s"%(folder)
            myfunc.WriteFile(msg+"\n", gen_errfile, "a")
            return 1

    try:
        open(finished_seq_file, 'w').close()
    except:
        pass
#first getting result from caches
# ==================================

    maplist = []
    maplist_simple = []
    toRunDict = {}
    hdl = myfunc.ReadFastaByBlock(infile, method_seqid=0, method_seq=0)
    if hdl.failure:
        isOK = False
    else:
        webcom.WriteDateTimeTagFile(starttagfile, runjob_logfile, runjob_errfile)

        recordList = hdl.readseq()
        cnt = 0
        origpath = os.getcwd()
        while recordList != None:
            for rd in recordList:
                isSkip = False
                # temp outpath for the sequence is always seq_0, and I feed
                # only one seq a time to the workflow
                tmp_outpath_this_seq = "%s/%s"%(tmp_outpath_result, "seq_%d"%0)
                outpath_this_seq = "%s/%s"%(outpath_result, "seq_%d"%cnt)
                subfoldername_this_seq = "seq_%d"%(cnt)
                if os.path.exists(tmp_outpath_this_seq):
                    try:
                        shutil.rmtree(tmp_outpath_this_seq)
                    except OSError:
                        pass

                maplist.append("%s\t%d\t%s\t%s"%("seq_%d"%cnt, len(rd.seq),
                    rd.description, rd.seq))
                maplist_simple.append("%s\t%d\t%s"%("seq_%d"%cnt, len(rd.seq),
                    rd.description))
                if not g_params['isForceRun']:
                    md5_key = hashlib.md5(rd.seq.encode('utf-8')).hexdigest()
                    subfoldername = md5_key[:2]
                    cachedir = "%s/%s/%s"%(path_cache, subfoldername, md5_key)
                    if os.path.exists(cachedir):
                        # create a symlink to the cache
                        rela_path = os.path.relpath(cachedir, outpath_result) #relative path
                        os.chdir(outpath_result)
                        os.symlink(rela_path, subfoldername_this_seq)

                        if os.path.exists(outpath_this_seq):
                            runtime = 0.0 #in seconds
                            info_finish = webcom.GetInfoFinish_Subcons(outpath_this_seq,
                                    cnt, len(rd.seq), rd.description,
                                    source_result="cached", runtime=runtime)
                            myfunc.WriteFile("\t".join(info_finish)+"\n", finished_seq_file, "a", isFlush=True)
                            myfunc.WriteFile("%d\n"%(cnt), finished_idx_file, "a", isFlush=True)
                            isSkip = True

                if not isSkip:
                    # first try to delete the outfolder if exists
                    if os.path.exists(outpath_this_seq):
                        try:
                            shutil.rmtree(outpath_this_seq)
                        except OSError:
                            pass
                    origIndex = cnt
                    numTM = 0
                    toRunDict[origIndex] = [rd.seq, numTM, rd.description] #init value for numTM is 0

                cnt += 1
            recordList = hdl.readseq()
        hdl.close()
    myfunc.WriteFile("\n".join(maplist_simple)+"\n", mapfile)


    if not g_params['isOnlyGetCache']:
        torun_all_seqfile = "%s/%s"%(tmp_outpath_result, "query.torun.fa")
        dumplist = []
        for key in toRunDict:
            top = toRunDict[key][0]
            dumplist.append(">%s\n%s"%(str(key), top))
        myfunc.WriteFile("\n".join(dumplist)+"\n", torun_all_seqfile, "w")
        del dumplist


        sortedlist = sorted(list(toRunDict.items()), key=lambda x:x[1][1], reverse=True)
        #format of sortedlist [(origIndex: [seq, numTM, description]), ...]

        # submit sequences one by one to the workflow according to orders in
        # sortedlist

        for item in sortedlist:
            origIndex = item[0]
            seq = item[1][0]
            description = item[1][2]

            subfoldername_this_seq = "seq_%d"%(origIndex)
            outpath_this_seq = "%s/%s"%(outpath_result, subfoldername_this_seq)
            tmp_outpath_this_seq = "%s/%s"%(tmp_outpath_result, "seq_%d"%(0))
            if os.path.exists(tmp_outpath_this_seq):
                try:
                    shutil.rmtree(tmp_outpath_this_seq)
                except OSError:
                    pass

            seqfile_this_seq = "%s/%s"%(tmp_outpath_result, "query_%d.fa"%(origIndex))
            seqcontent = ">query_%d\n%s\n"%(origIndex, seq)
            myfunc.WriteFile(seqcontent, seqfile_this_seq, "w")

            if not os.path.exists(seqfile_this_seq):
                msg =  "Failed to generate seq file for index %d"%(origIndex)
                date_str = time.strftime(FORMAT_DATETIME)
                myfunc.WriteFile("[%s] %s\n"%(date_str, msg), runjob_errfile, "a", True)
                continue


            cmd = ["bash", runscript, seqfile_this_seq,  tmp_outpath_this_seq, "-verbose"]
            (t_success, runtime_in_sec) = webcom.RunCmd(cmd, runjob_logfile, runjob_errfile, True)

            aaseqfile = "%s/seq.fa"%(tmp_outpath_this_seq)
            if not os.path.exists(aaseqfile):
                seqcontent = ">%s\n%s\n"%(description, seq)
                myfunc.WriteFile(seqcontent, aaseqfile, "w")


            if os.path.exists(tmp_outpath_this_seq):
                cmd = ["mv","-f", tmp_outpath_this_seq, outpath_this_seq]
                (isCmdSuccess, t_runtime) = webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)
                timefile = "%s/time.txt"%(tmp_outpath_result)
                targetfile = "%s/time.txt"%(outpath_this_seq)
                if os.path.exists(timefile) and os.path.exists(outpath_this_seq):
                    try:
                        shutil.move(timefile, targetfile)
                    except:
                        msg = "Failed to move %s/time.txt"%(tmp_outpath_result)
                        date_str = time.strftime(FORMAT_DATETIME)
                        myfunc.WriteFile("[%s] %s\n"%(date_str, msg), runjob_errfile, "a", True)
                        pass


                if isCmdSuccess:
                    runtime = runtime_in_sec #in seconds
                    info_finish = webcom.GetInfoFinish_Subcons(outpath_this_seq,
                            origIndex, len(seq), description,
                            source_result="newrun", runtime=runtime)
                    myfunc.WriteFile("\t".join(info_finish)+"\n", finished_seq_file, "a", isFlush=True)
                    # now write the text output for this seq

                    info_this_seq = "%s\t%d\t%s\t%s"%("seq_%d"%origIndex, len(seq), description, seq)
                    resultfile_text_this_seq = "%s/%s"%(outpath_this_seq, "query.result.txt")
                    webcom.WriteSubconsTextResultFile(resultfile_text_this_seq,
                            outpath_result, [info_this_seq], runtime_in_sec, g_params['base_www_url'])
                    # create or update the md5 cache
                    # create cache only on the front-end
                    figurefile = "%s/plot/query_0.png"%(outpath_this_seq)
                    # Note: do not create cache is figure file does not exist
                    if webcom.IsFrontEndNode(g_params['base_www_url']) and os.path.exists(figurefile):
                        md5_key = hashlib.md5(seq.encode('utf-8')).hexdigest()
                        subfoldername = md5_key[:2]
                        md5_subfolder = "%s/%s"%(path_cache, subfoldername)
                        cachedir = "%s/%s/%s"%(path_cache, subfoldername, md5_key)
                        if os.path.exists(cachedir):
                            try:
                                shutil.rmtree(cachedir)
                            except:
                                msg = "Failed to shutil.rmtree(%s)"%(cachedir)
                                date_str = time.strftime(FORMAT_DATETIME)
                                myfunc.WriteFile("[%s] %s\n"%(date_str, msg), runjob_errfile, "a", True)
                                pass

                        if not os.path.exists(md5_subfolder):
                            try:
                                os.makedirs(md5_subfolder)
                            except:
                                pass

                        if os.path.exists(md5_subfolder) and not os.path.exists(cachedir):
                            cmd = ["mv","-f", outpath_this_seq, cachedir]
                            webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)


                        if not os.path.exists(outpath_this_seq) and os.path.exists(cachedir):
                            rela_path = os.path.relpath(cachedir, outpath_result) #relative path
                            try:
                                os.chdir(outpath_result)
                                os.symlink(rela_path,  subfoldername_this_seq)
                            except:
                                pass

    all_end_time = time.time()
    all_runtime_in_sec = all_end_time - all_begin_time

    if not g_params['isOnlyGetCache'] or len(toRunDict) == 0:
        # now write the text output to a single file
        statfile = "%s/%s"%(outpath_result, "stat.txt")
        webcom.WriteSubconsTextResultFile(resultfile_text, outpath_result, maplist,
                all_runtime_in_sec, g_params['base_www_url'], statfile=statfile)

        # now making zip instead (for windows users)
        # note that zip rq will zip the real data for symbolic links
        os.chdir(outpath)
        cmd = ["zip", "-rq", zipfile, resultpathname]
        webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)

        # write finish tag file
        if os.path.exists(finished_seq_file):
            webcom.WriteDateTimeTagFile(finishtagfile, runjob_logfile, runjob_errfile)

        isSuccess = False
        if (os.path.exists(finishtagfile) and os.path.exists(zipfile_fullpath)):
            isSuccess = True
        else:
            isSuccess = False
            failedtagfile = "%s/runjob.failed"%(outpath)
            webcom.WriteDateTimeTagFile(failedtagfile, runjob_logfile, runjob_errfile)

# send the result to email
# do not sendmail at the cloud VM
        if webcom.IsFrontEndNode(g_params['base_www_url']) and myfunc.IsValidEmailAddress(email):
            if isSuccess:
                finish_status = "success"
            else:
                finish_status = "failed"
            webcom.SendEmail_on_finish(jobid, g_params['base_www_url'],
                    finish_status, name_server="SubCons", from_email="no-reply.SubCons@bioinfo.se)",
                    to_email=email, contact_email=contact_email,
                    logfile=runjob_logfile, errfile=runjob_errfile)

    if os.path.exists(runjob_errfile) and os.path.getsize(runjob_errfile) > 1:
        return 1
    else:
        date_str = time.strftime(FORMAT_DATETIME)
        try:
            shutil.rmtree(tmpdir)
            msg = "rmtree(%s)"%(tmpdir)
            myfunc.WriteFile("[%s] %s\n"%(date_str, msg), runjob_logfile, "a", True)
        except Exception as e:
            msg = "Failed to rmtree(%s)"%(tmpdir)
            myfunc.WriteFile("[%s] %s\n"%(date_str, msg), runjob_errfile, "a", True)
            pass
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
            elif argv[i] in ["-force", "--force"]:
                g_params['isForceRun'] = True
                i += 1
            elif argv[i] in ["-only-get-cache", "--only-get-cache"]:
                g_params['isOnlyGetCache'] = True
                i += 1
            else:
                print("Error! Wrong argument:", argv[i], file=sys.stderr)
                return 1
        else:
            infile = argv[i]
            i += 1

    if jobid == "":
        print("%s: jobid not set. exit"%(sys.argv[0]), file=sys.stderr)
        return 1

    g_params['jobid'] = jobid
    # create a lock file in the resultpath when run_job.py is running for this
    # job, so that daemon will not run on this folder
    lockname = "runjob.lock"
    lock_file = "%s/%s/%s"%(path_result, jobid, lockname)
    g_params['lockfile'] = lock_file
    fp = open(lock_file, 'w')
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("Another instance of %s is running"%(progname), file=sys.stderr)
        return 1


    if myfunc.checkfile(infile, "infile") != 0:
        return 1
    if outpath == "":
        print("outpath not set. exit", file=sys.stderr)
        return 1
    elif not os.path.exists(outpath):
        try:
            subprocess.check_output(["mkdir", "-p", outpath])
        except subprocess.CalledProcessError as e:
            print(e, file=sys.stderr)
            return 1
    if tmpdir == "":
        print("tmpdir not set. exit", file=sys.stderr)
        return 1
    elif not os.path.exists(tmpdir):
        try:
            subprocess.check_output(["mkdir", "-p", tmpdir])
        except subprocess.CalledProcessError as e:
            print(e, file=sys.stderr)
            return 1

    numseq = myfunc.CountFastaSeq(infile)
    g_params['debugfile'] = "%s/debug.log"%(outpath)
    return RunJob(infile, outpath, tmpdir, email, jobid, g_params)

#}}}

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['isQuiet'] = True
    g_params['isForceRun'] = False
    g_params['isOnlyGetCache'] = False
    g_params['base_www_url'] = ""
    g_params['jobid'] = ""
    g_params['lockfile'] = ""
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    status = main(g_params)
    if os.path.exists(g_params['lockfile']):
        try:
            os.remove(g_params['lockfile'])
        except:
            myfunc.WriteFile("Failed to delete lockfile %s\n"%(g_params['lockfile']), gen_errfile, "a", True)

    sys.exit(status)
