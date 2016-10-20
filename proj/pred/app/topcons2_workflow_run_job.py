#!/usr/bin/env python
# Description: run job

# ChangeLog 
#
# ChangeLog 2015-02-12 
#   submit individual sequences to the workflow, so that the result of each
#   sequence can be cached and the progress can be shown for a job with many
#   sequences
# ChangeLog 2015-03-26
#   the tmpdir is removed if RunJob is succeeded
# ChangeLog 2015-04-01 
#   result from cache just make a soft link, 
#   zip -rq will replace the symbolic link with the actual data when making the
#   zip file
# ChangeLog 2016-06-30
#   cache results is saved at static/result/cache using md5 keys
#   the folder static/md5 is not used anymore

# how to create md5
# import hashlib
# md5_key = hashlib.md5(string).hexdigest()
# subfolder = md5_key[:2]

# 

import os
import sys
import subprocess
import time
import myfunc
import glob
import hashlib
import shutil
import datetime
import site
progname =  os.path.basename(sys.argv[0])
wspace = ''.join([" "]*len(progname))
rundir = os.path.dirname(os.path.realpath(__file__))
webserver_root = os.path.realpath("%s/../../../"%(rundir))
activate_env="%s/env/bin/activate_this.py"%(webserver_root)
execfile(activate_env, dict(__file__=activate_env))

blastdir = "%s/%s"%(rundir, "soft/topcons2_webserver/tools/blast-2.2.26")
os.environ['SCAMPI_DIR'] = "/server/scampi"
os.environ['MODHMM_BIN'] = "/server/modhmm/bin"
os.environ['BLASTMAT'] = "%s/data"%(blastdir)
os.environ['BLASTBIN'] = "%s/bin"%(blastdir)
os.environ['BLASTDB'] = "%s/%s"%(rundir, "soft/topcons2_webserver/database/blast/")
blastdb = "%s/%s"%(os.environ['BLASTDB'], "uniref90.fasta" )
runscript = "%s/%s"%(rundir, "soft/topcons2_webserver/workflow/pfam_workflow.py")
script_scampi = "%s/%s"%(rundir, "mySCAMPI_run.pl")

basedir = os.path.realpath("%s/.."%(rundir)) # path of the application, i.e. pred/
path_md5cache = "%s/static/md5"%(basedir)
path_cache = "%s/static/result/cache"%(basedir)

contact_email = "nanjiang.shu@scilifelab.se"
vip_user_list = [
        "nanjiang.shu@scilifelab.se"
        ]

# note that here the url should be without http://

usage_short="""
Usage: %s seqfile_in_fasta 
       %s -jobid JOBID -outpath DIR -tmpdir DIR
       %s -email EMAIL -baseurl BASE_WWW_URL
       %s [-force]
"""%(progname, wspace, wspace, wspace)

usage_ext="""\
Description:
    run job

OPTIONS:
  -force        Do not use cahced result
  -h, --help    Print this help message and exit

Created 2015-02-05, updated 2015-02-12, Nanjiang Shu
"""
usage_exp="""
Examples:
    %s /data3/tmp/tmp_dkgSD/query.fa -outpath /data3/result/rst_mXLDGD -tmpdir /data3/tmp/tmp_dkgSD
"""%(progname)

def PrintHelp(fpout=sys.stdout):#{{{
    print >> fpout, usage_short
    print >> fpout, usage_ext
    print >> fpout, usage_exp#}}}

def IsFrontEndNode(base_www_url):#{{{
    """
    check if the base_www_url is front-end node
    if base_www_url is ip address, then not the front-end
    otherwise yes
    """
    if base_www_url == "":
        return False
    else:
        arr =  [x.isdigit() for x in base_www_url.split('.')]
        if all(arr):
            return False
        else:
            return True
#}}}
def RunJob(infile, outpath, tmpdir, email, jobid, g_params):#{{{
    all_begin_time = time.time()

    rootname = os.path.basename(os.path.splitext(infile)[0])
    starttagfile   = "%s/runjob.start"%(outpath)
    runjob_errfile = "%s/runjob.err"%(outpath)
    runjob_logfile = "%s/runjob.log"%(outpath)
    finishtagfile = "%s/runjob.finish"%(outpath)
    rmsg = ""


    resultpathname = jobid

    outpath_result = "%s/%s"%(outpath, resultpathname)
    tarball = "%s.tar.gz"%(resultpathname)
    zipfile = "%s.zip"%(resultpathname)
    tarball_fullpath = "%s.tar.gz"%(outpath_result)
    zipfile_fullpath = "%s.zip"%(outpath_result)
    outfile = "%s/%s/Topcons/topcons.top"%(outpath_result, "seq_%d"%(0))
    resultfile_text = "%s/%s"%(outpath_result, "query.result.txt")
    mapfile = "%s/seqid_index_map.txt"%(outpath_result)
    finished_seq_file = "%s/finished_seqs.txt"%(outpath_result)



    tmp_outpath_result = "%s/%s"%(tmpdir, resultpathname)
    isOK = True
    try:
        os.makedirs(tmp_outpath_result)
        isOK = True
    except OSError:
        msg = "Failed to create folder %s"%(tmp_outpath_result)
        myfunc.WriteFile(msg+"\n", runjob_errfile, "a")
        isOK = False
        pass

    try:
        os.makedirs(outpath_result)
        isOK = True
    except OSError:
        msg = "Failed to create folder %s"%(outpath_result)
        myfunc.WriteFile(msg+"\n", runjob_errfile, "a")
        isOK = False
        pass


    if isOK:
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
            datetime = time.strftime("%Y-%m-%d %H:%M:%S")
            rt_msg = myfunc.WriteFile(datetime, starttagfile)

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
                        md5_key = hashlib.md5(rd.seq).hexdigest()
                        subfoldername = md5_key[:2]
                        cachedir = "%s/%s/%s"%(path_cache, subfoldername, md5_key)
                        if os.path.exists(cachedir):
                            # create a symlink to the cache
                            rela_path = os.path.relpath(cachedir, outpath_result) #relative path
                            os.chdir(outpath_result)
                            os.symlink(rela_path, subfoldername_this_seq)

                            if os.path.exists(outpath_this_seq):
                                runtime = 0.0 #in seconds
                                topfile = "%s/%s/topcons.top"%(
                                        outpath_this_seq, "Topcons")
                                top = myfunc.ReadFile(topfile).strip()
                                numTM = myfunc.CountTM(top)
                                posSP = myfunc.GetSPPosition(top)
                                if len(posSP) > 0:
                                    isHasSP = True
                                else:
                                    isHasSP = False
                                info_finish = [ "seq_%d"%cnt,
                                        str(len(rd.seq)), str(numTM),
                                        str(isHasSP), "cached", str(runtime),
                                        rd.description]
                                myfunc.WriteFile("\t".join(info_finish)+"\n",
                                        finished_seq_file, "a", isFlush=True)
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


        # run scampi single to estimate the number of TM helices and then run
        # the query sequences in the descending order of numTM
        torun_all_seqfile = "%s/%s"%(tmp_outpath_result, "query.torun.fa")
        dumplist = []
        for key in toRunDict:
            top = toRunDict[key][0]
            dumplist.append(">%s\n%s"%(str(key), top))
        myfunc.WriteFile("\n".join(dumplist)+"\n", torun_all_seqfile, "w")
        del dumplist

        topfile_scampiseq = "%s/%s"%(tmp_outpath_result, "query.torun.fa.topo")
        if os.path.exists(torun_all_seqfile):
            # run scampi to estimate the number of TM helices
            cmd = [script_scampi, torun_all_seqfile, "-outpath", tmp_outpath_result]
            try:
                rmsg = subprocess.check_output(cmd)
            except subprocess.CalledProcessError, e:
                g_params['runjob_err'].append(str(e)+"\n")
                pass
        if os.path.exists(topfile_scampiseq):
            (idlist_scampi, annolist_scampi, toplist_scampi) = myfunc.ReadFasta(topfile_scampiseq)
            for jj in xrange(len(idlist_scampi)):
                numTM = myfunc.CountTM(toplist_scampi[jj])
                try:
                    toRunDict[int(idlist_scampi[jj])][1] = numTM
                except (KeyError, ValueError, TypeError):
                    pass

        sortedlist = sorted(toRunDict.items(), key=lambda x:x[1][1], reverse=True)
        #format of sortedlist [(origIndex: [seq, numTM, description]), ...]

        # submit sequences one by one to the workflow according to orders in
        # sortedlist

        for item in sortedlist:
#             g_params['runjob_log'].append("tmpdir = %s"%(tmpdir))
            #cmd = [script_getseqlen, infile, "-o", tmp_outfile , "-printid"]
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
            seqcontent = ">%d\n%s\n"%(origIndex, seq)
            myfunc.WriteFile(seqcontent, seqfile_this_seq, "w")

            if not os.path.exists(seqfile_this_seq):
                g_params['runjob_err'].append("failed to generate seq index %d"%(origIndex))
                continue


            cmd = ["python", runscript, seqfile_this_seq,  tmp_outpath_result, blastdir, blastdb]
            cmdline = " ".join(cmd)
            g_params['runjob_log'].append(" ".join(cmd))
            begin_time = time.time()
            try:
                rmsg = subprocess.check_output(cmd)
                g_params['runjob_log'].append("workflow:\n"+rmsg+"\n")
            except subprocess.CalledProcessError, e:
                g_params['runjob_err'].append(str(e)+"\n")
                g_params['runjob_err'].append("cmdline: "+ cmdline +"\n")
                g_params['runjob_err'].append(rmsg + "\n")
                pass
            end_time = time.time()
            runtime_in_sec = end_time - begin_time

            if os.path.exists(tmp_outpath_this_seq):
                cmd = ["mv","-f", tmp_outpath_this_seq, outpath_this_seq]
                isCmdSuccess = False
                try:
                    subprocess.check_output(cmd)
                    isCmdSuccess = True
                except subprocess.CalledProcessError, e:
                    msg =  "Failed to run prediction for sequence No. %d\n"%(origIndex)
                    g_params['runjob_err'].append(msg)
                    g_params['runjob_err'].append(str(e)+"\n")
                    pass
                timefile = "%s/time.txt"%(tmp_outpath_result)
                targetfile = "%s/time.txt"%(outpath_this_seq)
                if os.path.exists(timefile) and os.path.exists(outpath_this_seq):
                    try:
                        shutil.move(timefile, targetfile)
                    except:
                        g_params['runjob_err'].append("Failed to move %s/time.txt"%(tmp_outpath_result)+"\n")
                        pass

                if isCmdSuccess:
                    runtime = runtime_in_sec #in seconds
                    topfile = "%s/%s/topcons.top"%(
                            outpath_this_seq, "Topcons")
                    top = myfunc.ReadFile(topfile).strip()
                    numTM = myfunc.CountTM(top)
                    posSP = myfunc.GetSPPosition(top)
                    if len(posSP) > 0:
                        isHasSP = True
                    else:
                        isHasSP = False
                    info_finish = [ "seq_%d"%origIndex, str(len(seq)), str(numTM),
                            str(isHasSP), "newrun", str(runtime), description]
                    myfunc.WriteFile("\t".join(info_finish)+"\n",
                            finished_seq_file, "a", isFlush=True)
                    # now write the text output for this seq

                    info_this_seq = "%s\t%d\t%s\t%s"%("seq_%d"%origIndex, len(seq), description, seq)
                    resultfile_text_this_seq = "%s/%s"%(outpath_this_seq, "query.result.txt")
                    myfunc.WriteTOPCONSTextResultFile(resultfile_text_this_seq,
                            outpath_result, [info_this_seq], runtime_in_sec, g_params['base_www_url'])
                    # create or update the md5 cache
                    # create cache only on the front-end
                    if IsFrontEndNode(g_params['base_www_url']):
                        md5_key = hashlib.md5(seq).hexdigest()
                        subfoldername = md5_key[:2]
                        md5_subfolder = "%s/%s"%(path_cache, subfoldername)
                        cachedir = "%s/%s/%s"%(path_cache, subfoldername, md5_key)
                        if os.path.exists(cachedir):
                            try:
                                shutil.rmtree(cachedir)
                            except:
                                g_params['runjob_err'].append("failed to shutil.rmtree(%s)"%(cachedir)+"\n")
                                pass

                        if not os.path.exists(md5_subfolder):
                            try:
                                os.makedirs(md5_subfolder)
                            except:
                                pass

                        if os.path.exists(md5_subfolder) and not os.path.exists(cachedir):
                            cmd = ["mv","-f", outpath_this_seq, cachedir]
                            cmdline = " ".join(cmd)
                            g_params['runjob_log'].append("cmdline: %s"%(cmdline))
                            try:
                                subprocess.check_call(cmd)
                            except CalledProcessError,e:
                                g_params['runjob_err'].append(str(e)+"\n")


                        if not os.path.exists(outpath_this_seq) and os.path.exists(cachedir):
                            rela_path = os.path.relpath(cachedir, outpath_result) #relative path
                            try:
                                os.chdir(outpath_result)
                                os.symlink(rela_path,  subfoldername_this_seq)
                            except:
                                pass

        all_end_time = time.time()
        all_runtime_in_sec = all_end_time - all_begin_time

        if len(g_params['runjob_log']) > 0 :
            rt_msg = myfunc.WriteFile("\n".join(g_params['runjob_log'])+"\n", runjob_logfile, "a")
            if rt_msg:
                g_params['runjob_err'].append(rt_msg)

        datetime = time.strftime("%Y-%m-%d %H:%M:%S")
        if os.path.exists(finished_seq_file):
            rt_msg = myfunc.WriteFile(datetime, finishtagfile)
            if rt_msg:
                g_params['runjob_err'].append(rt_msg)

# now write the text output to a single file
        statfile = "%s/%s"%(outpath_result, "stat.txt")
        myfunc.WriteTOPCONSTextResultFile(resultfile_text, outpath_result, maplist,
                all_runtime_in_sec, g_params['base_www_url'], statfile=statfile)

        # now making zip instead (for windows users)
        # note that zip rq will zip the real data for symbolic links
        os.chdir(outpath)
#             cmd = ["tar", "-czf", tarball, resultpathname]
        cmd = ["zip", "-rq", zipfile, resultpathname]
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError, e:
            g_params['runjob_err'].append(str(e))
            pass


    isSuccess = False
    if (os.path.exists(finishtagfile) and os.path.exists(zipfile_fullpath)):
        isSuccess = True
        # delete the tmpdir if succeeded
        if g_params['runjob_err'] == "":
            shutil.rmtree(tmpdir) #DEBUG, keep tmpdir
    else:
        isSuccess = False
        failtagfile = "%s/runjob.failed"%(outpath)
        datetime = time.strftime("%Y-%m-%d %H:%M:%S")
        rt_msg = myfunc.WriteFile(datetime, failtagfile)
        if rt_msg:
            g_params['runjob_err'].append(rt_msg)

# send the result to email
# do not sendmail at the cloud VM
    if (g_params['base_www_url'].find("topcons.net") != -1 and
            myfunc.IsValidEmailAddress(email)):
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
        rtValue = myfunc.Sendmail(from_email, to_email, subject, bodytext)
        if rtValue != 0:
            g_params['runjob_err'].append("Sendmail to {} failed with status {}".format(to_email, rtValue))

    if len(g_params['runjob_err']) > 0:
        rt_msg = myfunc.WriteFile("\n".join(g_params['runjob_err'])+"\n", runjob_errfile, "w")
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
            elif argv[i] in ["-force", "--force"]:
                g_params['isForceRun'] = True
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

    if myfunc.checkfile(infile, "infile") != 0:
        return 1
    if outpath == "":
        print >> sys.stderr, "outpath not set. exit"
        return 1
    elif not os.path.exists(outpath):
        try:
            subprocess.check_output(["mkdir", "-p", outpath])
        except subprocess.CalledProcessError, e:
            print >> sys.stderr, e
            return 1
    if tmpdir == "":
        print >> sys.stderr, "tmpdir not set. exit"
        return 1
    elif not os.path.exists(tmpdir):
        try:
            subprocess.check_output(["mkdir", "-p", tmpdir])
        except subprocess.CalledProcessError, e:
            print >> sys.stderr, e
            return 1

    numseq = myfunc.CountFastaSeq(infile)
    g_params['debugfile'] = "%s/debug.log"%(outpath)
    return RunJob(infile, outpath, tmpdir, email, jobid, g_params)

#}}}

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['isQuiet'] = True
    g_params['runjob_log'] = []
    g_params['runjob_err'] = []
    g_params['isForceRun'] = False
    g_params['base_www_url'] = ""
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    sys.exit(main(g_params))
