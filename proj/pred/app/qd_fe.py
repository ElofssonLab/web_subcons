#!/usr/bin/env python
# Description: daemon to submit jobs and retrieve results to/from remote
#              servers
# 
import os
import sys
import site

rundir = os.path.dirname(os.path.realpath(__file__))
webserver_root = os.path.realpath("%s/../../../"%(rundir))

activate_env="%s/env/bin/activate_this.py"%(webserver_root)
exec(compile(open(activate_env, "rb").read(), activate_env, 'exec'), dict(__file__=activate_env))

from libpredweb import myfunc
from libpredweb import dataprocess
from libpredweb import webserver_common as webcom
from libpredweb import qd_fe_common as qdcom
import time
from datetime import datetime
from dateutil import parser as dtparser
from pytz import timezone
import requests
import json
import urllib.request, urllib.parse, urllib.error
import shutil
import hashlib
import subprocess
from suds.client import Client
import numpy

from geoip import geolite2
import pycountry


# make sure that only one instance of the script is running
# this code is working 
progname = os.path.basename(__file__)
rootname_progname = os.path.splitext(progname)[0]
lockname = os.path.realpath(__file__).replace(" ", "").replace("/", "-")
import fcntl
lock_file = "/tmp/%s.lock"%(lockname)
fp = open(lock_file, 'w')
try:
    fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
except IOError:
    print("Another instance of %s is running"%(progname), file=sys.stderr)
    sys.exit(1)

contact_email = "nanjiang.shu@scilifelab.se"

threshold_logfilesize = 20*1024*1024

usage_short="""
Usage: %s
"""%(sys.argv[0])

usage_ext="""
Description:
    Daemon to submit jobs and retrieve results to/from remote servers
    run periodically
    At the end of each run generate a runlog file with the status of all jobs

OPTIONS:
  -h, --help    Print this help message and exit

Created 2015-03-25, updated 2015-03-25, Nanjiang Shu
"""
usage_exp="""
"""

basedir = os.path.realpath("%s/.."%(rundir)) # path of the application, i.e. pred/
path_static = "%s/static"%(basedir)
path_log = "%s/static/log"%(basedir)
path_stat = "%s/stat"%(path_log)
path_result = "%s/static/result"%(basedir)
path_cache = "%s/static/result/cache"%(basedir)

# format of the computenodefile is 
# each line is a record and contains two items
# hostname MAX_ALLOWED_PARALLEL_JOBS
computenodefile = "%s/config/computenode.txt"%(basedir)

gen_errfile = "%s/static/log/%s.err"%(basedir, progname)
gen_logfile = "%s/static/log/%s.log"%(basedir, progname)
black_iplist_file = "%s/config/black_iplist.txt"%(basedir)
finished_date_db = "%s/cached_job_finished_date.sqlite3"%(path_log)
vip_email_file = "%s/config/vip_email.txt"%(basedir)

def PrintHelp(fpout=sys.stdout):#{{{
    print(usage_short, file=fpout)
    print(usage_ext, file=fpout)
    print(usage_exp, file=fpout)#}}}

def CreateRunJoblog(path_result, submitjoblogfile, runjoblogfile,#{{{
        finishedjoblogfile, loop, isOldRstdirDeleted):
    myfunc.WriteFile("CreateRunJoblog...\n", gen_logfile, "a", True)
    # Read entries from submitjoblogfile, checking in the result folder and
    # generate two logfiles: 
    #   1. runjoblogfile 
    #   2. finishedjoblogfile
    # when loop == 0, for unfinished jobs, re-generate finished_seqs.txt
    hdl = myfunc.ReadLineByBlock(submitjoblogfile)
    if hdl.failure:
        return 1

    finished_jobid_list = []
    finished_job_dict = {}
    if os.path.exists(finishedjoblogfile):
        finished_job_dict = myfunc.ReadFinishedJobLog(finishedjoblogfile)

    new_finished_list = []  # Finished or Failed
    new_submitted_list = []  # 

    new_runjob_list = []    # Running
    new_waitjob_list = []    # Queued
    lines = hdl.readlines()
    while lines != None:
        for line in lines:
            strs = line.split("\t")
            if len(strs) < 8:
                continue
            submit_date_str = strs[0]
            jobid = strs[1]
            ip = strs[2]
            numseq_str = strs[3]
            jobname = strs[5]
            email = strs[6].strip()
            method_submission = strs[7]
            start_date_str = ""
            finish_date_str = ""
            rstdir = "%s/%s"%(path_result, jobid)

            numseq = 1
            try:
                numseq = int(numseq_str)
            except:
                pass

            isRstFolderExist = False
            if not isOldRstdirDeleted or os.path.exists(rstdir):
                isRstFolderExist = True

            if isRstFolderExist:
                new_submitted_list.append([jobid,line])

            if jobid in finished_job_dict:
                if isRstFolderExist:
                    li = [jobid] + finished_job_dict[jobid]
                    new_finished_list.append(li)
                continue

            status = webcom.get_job_status(jobid)
            if 'DEBUG_JOB_STATUS' in g_params and g_params['DEBUG_JOB_STATUS']:
                webcom.loginfo("status(%s): %s"%(jobid, status), gen_logfile)

            starttagfile = "%s/%s"%(rstdir, "runjob.start")
            finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
            if os.path.exists(starttagfile):
                start_date_str = myfunc.ReadFile(starttagfile).strip()
            if os.path.exists(finishtagfile):
                finish_date_str = myfunc.ReadFile(finishtagfile).strip()

            li = [jobid, status, jobname, ip, email, numseq_str,
                    method_submission, submit_date_str, start_date_str,
                    finish_date_str]
            if status in ["Finished", "Failed"]:
                new_finished_list.append(li)

            # single-sequence job submitted from the web-page will be
            # submmitted by suq
            UPPER_WAIT_TIME_IN_SEC = 60
            isValidSubmitDate = True
            try:
                submit_date = webcom.datetime_str_to_time(submit_date_str)
            except ValueError:
                isValidSubmitDate = False

            if isValidSubmitDate:
                current_time = datetime.now(timezone(g_params['TZ']))
                timeDiff = current_time - submit_date
                queuetime_in_sec = timeDiff.seconds
            else:
                queuetime_in_sec = UPPER_WAIT_TIME_IN_SEC + 1

            #if numseq > 1 or method_submission == "wsdl" or queuetime_in_sec > UPPER_WAIT_TIME_IN_SEC:
            # note that all jobs are handled by the qd
            if 1:
                if status == "Running":
                    new_runjob_list.append(li)
                elif status == "Wait":
                    new_waitjob_list.append(li)
        lines = hdl.readlines()
    hdl.close()

# re-write logs of submitted jobs
    li_str = []
    for li in new_submitted_list:
        li_str.append(li[1])
    if len(li_str)>0:
        myfunc.WriteFile("\n".join(li_str)+"\n", submitjoblogfile, "w", True)
    else:
        myfunc.WriteFile("", submitjoblogfile, "w", True)

# re-write logs of finished jobs
    li_str = []
    for li in new_finished_list:
        li = [str(x) for x in li]
        li_str.append("\t".join(li))
    if len(li_str)>0:
        myfunc.WriteFile("\n".join(li_str)+"\n", finishedjoblogfile, "w", True)
    else:
        myfunc.WriteFile("", finishedjoblogfile, "w", True)
# re-write logs of finished jobs for each IP
    new_finished_dict = {}
    for li in new_finished_list:
        ip = li[3]
        if not ip in new_finished_dict:
            new_finished_dict[ip] = []
        new_finished_dict[ip].append(li)
    for ip in new_finished_dict:
        finished_list_for_this_ip = new_finished_dict[ip]
        divide_finishedjoblogfile = "%s/divided/%s_finished_job.log"%(path_log,
                ip)
        li_str = []
        for li in finished_list_for_this_ip:
            li = [str(x) for x in li]
            li_str.append("\t".join(li))
        if len(li_str)>0:
            myfunc.WriteFile("\n".join(li_str)+"\n", divide_finishedjoblogfile, "w", True)
        else:
            myfunc.WriteFile("", divide_finishedjoblogfile, "w", True)

# update all_submitted jobs
    allsubmitjoblogfile = "%s/all_submitted_seq.log"%(path_log)
    allsubmitted_jobid_set = set(myfunc.ReadIDList2(allsubmitjoblogfile, col=1, delim="\t"))
    li_str = []
    for li in new_submitted_list:
        jobid = li[0]
        if not jobid in allsubmitted_jobid_set:
            li_str.append(li[1])
    if len(li_str)>0:
        myfunc.WriteFile("\n".join(li_str)+"\n", allsubmitjoblogfile, "a", True)


# update allfinished jobs
    allfinishedjoblogfile = "%s/all_finished_job.log"%(path_log)
    allfinished_jobid_set = set(myfunc.ReadIDList2(allfinishedjoblogfile, col=0, delim="\t"))
    li_str = []
    for li in new_finished_list:
        li = [str(x) for x in li]
        jobid = li[0]
        if not jobid in allfinished_jobid_set:
            li_str.append("\t".join(li))
    if len(li_str)>0:
        myfunc.WriteFile("\n".join(li_str)+"\n", allfinishedjoblogfile, "a", True)

# write logs of running and queuing jobs
# the queuing jobs are sorted in descending order by the suq priority
# frist get numseq_this_user for each jobs
# format of numseq_this_user: {'jobid': numseq_this_user}
    numseq_user_dict = webcom.GetNumSeqSameUserDict(new_runjob_list + new_waitjob_list)

# now append numseq_this_user and priority score to new_waitjob_list and
# new_runjob_list

    for joblist in [new_waitjob_list, new_runjob_list]:
        for li in joblist:
            jobid = li[0]
            ip = li[3]
            email = li[4].strip()
            rstdir = "%s/%s"%(path_result, jobid)
            outpath_result = "%s/%s"%(rstdir, jobid)

            # if loop == 0 , for new_waitjob_list and new_runjob_list
            # re-generate finished_seqs.txt
            if loop == 0 and os.path.exists(outpath_result):#{{{
                finished_seq_file = "%s/finished_seqs.txt"%(outpath_result)
                finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
                finished_idx_set = set([])

                finished_seqs_idlist = []
                if os.path.exists(finished_seq_file):
                    finished_seqs_idlist = myfunc.ReadIDList2(finished_seq_file, col=0, delim="\t")
                finished_seqs_idset = set(finished_seqs_idlist)
                finished_info_list = []
                queryfile = "%s/query.fa"%(rstdir)
                (seqidlist, seqannolist, seqlist) = myfunc.ReadFasta(queryfile)
                try:
                    dirlist = os.listdir(outpath_result)
                except:
                    webcom.loginfo("Failed to os.listdir(%s)"%(outpath_result), gen_errfile)
                for dd in dirlist:
                    if dd.find("seq_") == 0:
                        origIndex_str = dd.split("_")[1]
                        finished_idx_set.add(origIndex_str)

                    if dd.find("seq_") == 0 and dd not in finished_seqs_idset:
                        origIndex = int(dd.split("_")[1])
                        outpath_this_seq = "%s/%s"%(outpath_result, dd)
                        timefile = "%s/time.txt"%(outpath_this_seq)
                        seq = seqlist[origIndex]
                        description = seqannolist[origIndex]
                        runtime = webcom.ReadRuntimeFromFile(timefile, default_runtime=0.0)
                        info_finish = webcom.GetInfoFinish_Subcons(outpath_this_seq,
                                origIndex, len(seq), description,
                                source_result="newrun", runtime=runtime)
                        finished_info_list.append("\t".join(info_finish))
                if len(finished_info_list)>0:
                    myfunc.WriteFile("\n".join(finished_info_list)+"\n", finished_seq_file, "a", True)
                if len(finished_idx_set) > 0:
                    myfunc.WriteFile("\n".join(list(finished_idx_set))+"\n", finished_idx_file, "w", True)
                else:
                    myfunc.WriteFile("", finished_idx_file, "w", True)
            #}}}

            try:
                numseq = int(li[5])
            except:
                numseq = 1
                pass
            try:
                numseq_this_user = numseq_user_dict[jobid]
            except:
                numseq_this_user = numseq
                pass
            # note that the priority is deducted by numseq so that for jobs
            # from the same user, jobs with fewer sequences are placed with
            # higher priority
            priority = myfunc.GetSuqPriority(numseq_this_user) - numseq

            if ip in g_params['blackiplist']:
                priority = priority/1000.0

            if email in g_params['vip_user_list']:
                numseq_this_user = 1
                priority = 999999999.0
                myfunc.WriteFile("email %s in vip_user_list\n"%(email), gen_logfile, "a", True)

            li.append(numseq_this_user)
            li.append(priority)


    # sort the new_waitjob_list in descending order by priority
    new_waitjob_list = sorted(new_waitjob_list, key=lambda x:x[11], reverse=True)
    new_runjob_list = sorted(new_runjob_list, key=lambda x:x[11], reverse=True)

    # write to runjoblogfile
    li_str = []
    for joblist in [new_waitjob_list, new_runjob_list]:
        for li in joblist:
            li2 = li[:10]+[str(li[10]), str(li[11])]
            li_str.append("\t".join(li2))
#     print "write to", runjoblogfile
#     print "\n".join(li_str)
    if len(li_str)>0:
        myfunc.WriteFile("\n".join(li_str)+"\n", runjoblogfile, "w", True)
    else:
        myfunc.WriteFile("", runjoblogfile, "w", True)

#}}}
def SubmitJob(jobid, cntSubmitJobDict, numseq_this_user):#{{{
# for each job rstdir, keep three log files, 
# 1.seqs finished, finished_seq log keeps all information, finished_index_log
#   can be very compact to speed up reading, e.g.
#   1-5 7-9 etc
# 2.seqs queued remotely , format:
#       index node remote_jobid
# 3. format of the torun_idx_file
#    origIndex

    rmsg = ""
    myfunc.WriteFile("SubmitJob for %s, numseq_this_user=%d\n"%(jobid,
        numseq_this_user), gen_logfile, "a", True)
    rstdir = "%s/%s"%(path_result, jobid)
    outpath_result = "%s/%s"%(rstdir, jobid)
    if not os.path.exists(outpath_result):
        os.mkdir(outpath_result)

    finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
    failed_idx_file = "%s/failed_seqindex.txt"%(rstdir)
    remotequeue_idx_file = "%s/remotequeue_seqindex.txt"%(rstdir)
    torun_idx_file = "%s/torun_seqindex.txt"%(rstdir) # ordered seq index to run
    cnttry_idx_file = "%s/cntsubmittry_seqindex.txt"%(rstdir)#index file to keep log of tries

    runjob_errfile = "%s/%s"%(rstdir, "runjob.err")
    runjob_logfile = "%s/%s"%(rstdir, "runjob.log")
    finished_seq_file = "%s/finished_seqs.txt"%(outpath_result)
    tmpdir = "%s/tmpdir"%(rstdir)
    qdinittagfile = "%s/runjob.qdinit"%(rstdir)
    failedtagfile = "%s/%s"%(rstdir, "runjob.failed")
    starttagfile = "%s/%s"%(rstdir, "runjob.start")
    cache_process_finish_tagfile = "%s/cache_processed.finish"%(rstdir)
    fafile = "%s/query.fa"%(rstdir)
    split_seq_dir = "%s/splitaa"%(tmpdir)
    forceruntagfile = "%s/forcerun"%(rstdir)
    lastprocessed_cache_idx_file = "%s/lastprocessed_cache_idx.txt"%(rstdir)

    if os.path.exists(forceruntagfile):
        isForceRun = True
    else:
        isForceRun = False

    finished_idx_list = []
    failed_idx_list = []    # [origIndex]
    if os.path.exists(finished_idx_file):
        finished_idx_list = list(set(myfunc.ReadIDList(finished_idx_file)))
    if os.path.exists(failed_idx_file):
        failed_idx_list = list(set(myfunc.ReadIDList(failed_idx_file)))

    processed_idx_set = set(finished_idx_list) | set(failed_idx_list)

    jobinfofile = "%s/jobinfo"%(rstdir)
    jobinfo = ""
    if os.path.exists(jobinfofile):
        jobinfo = myfunc.ReadFile(jobinfofile).strip()
    jobinfolist = jobinfo.split("\t")
    email = ""
    if len(jobinfolist) >= 8:
        email = jobinfolist[6]
        method_submission = jobinfolist[7]


    # the first time when the this jobid is processed, do the following
    # 1. generate a file with sorted seqindex
    # 2. generate splitted sequence files named by the original seqindex
    if not os.path.exists(qdinittagfile): #initialization#{{{
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)
        if isForceRun or os.path.exists(cache_process_finish_tagfile):
            isCacheProcessingFinished = True
        else:
            isCacheProcessingFinished = False

        # ==== 1.dealing with cached results 
        (seqIDList, seqAnnoList, seqList) = myfunc.ReadFasta(fafile)
        if len(seqIDList) <= 0:
            webcom.WriteDateTimeTagFile(failedtagfile, runjob_logfile, runjob_errfile)
            webcom.loginfo("Read query seq file failed. Zero sequence read in", runjob_errfile)
            return 1
        if g_params['DEBUG']:
            msg = "jobid = %s, isCacheProcessingFinished=%s, MAX_CACHE_PROCESS=%d"%(
                    jobid, str(isCacheProcessingFinished), g_params['MAX_CACHE_PROCESS'])
            webcom.loginfo(msg, gen_logfile)

        if not isCacheProcessingFinished:
            finished_idx_set = set(finished_idx_list)

            lastprocessed_idx = -1
            if os.path.exists(lastprocessed_cache_idx_file):
                try:
                    lastprocessed_idx = int(myfunc.ReadFile(lastprocessed_cache_idx_file))
                except:
                    lastprocessed_idx = -1

            cnt_processed_cache = 0
            for i in range(lastprocessed_idx+1, len(seqIDList)):
                if i in finished_idx_set:
                    continue
                outpath_this_seq = "%s/%s"%(outpath_result, "seq_%d"%i)
                subfoldername_this_seq = "seq_%d"%(i)
                md5_key = hashlib.md5(seqList[i].encode('utf-8')).hexdigest()
                subfoldername = md5_key[:2]
                cachedir = "%s/%s/%s"%(path_cache, subfoldername, md5_key)
                zipfile_cache = cachedir + ".zip"

                if os.path.exists(cachedir) or os.path.exists(zipfile_cache):
                    if os.path.exists(cachedir):
                        try:
                            shutil.copytree(cachedir, outpath_this_seq)
                        except Exception as e:
                            msg = "Failed to copytree  %s -> %s"%(cachedir, outpath_this_seq)
                            webcom.loginfo("%s with errmsg=%s"%(msg, str(e)), runjob_errfile)
                    elif os.path.exists(zipfile_cache):
                        cmd = ["unzip", zipfile_cache, "-d", outpath_result]
                        webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)
                        if os.path.exists(outpath_this_seq):
                            shutil.rmtree(outpath_this_seq)
                        shutil.move("%s/%s"%(outpath_result, md5_key), outpath_this_seq)

                    if os.path.exists(outpath_this_seq):
                        if not os.path.exists(starttagfile): #write start tagfile
                            webcom.WriteDateTimeTagFile(starttagfile, runjob_logfile, runjob_errfile)

                        info_finish = webcom.GetInfoFinish_Subcons(outpath_this_seq,
                                i, len(seqList[i]), seqAnnoList[i], source_result="cached", runtime=0.0)
                        myfunc.WriteFile("\t".join(info_finish)+"\n",
                                finished_seq_file, "a", isFlush=True)
                        myfunc.WriteFile("%d\n"%(i), finished_idx_file, "a", True)

                    if g_params['DEBUG']:
                        msg = "Get result from cache for seq_%d"%(i)
                        webcom.loginfo(msg, gen_logfile)
                    if cnt_processed_cache+1 >= g_params['MAX_CACHE_PROCESS']:
                        myfunc.WriteFile(str(i), lastprocessed_cache_idx_file, "w", True)
                        return 0
                    cnt_processed_cache += 1

            webcom.WriteDateTimeTagFile(cache_process_finish_tagfile, runjob_logfile, runjob_errfile)

        # Regenerate toRunDict
        toRunDict = {}
        for i in range(len(seqIDList)):
            if not i in processed_idx_set:
                toRunDict[i] = [seqList[i], 0, seqAnnoList[i].replace('\t', ' ')]
        sortedlist = sorted(list(toRunDict.items()), key=lambda x:x[1][1], reverse=True)

        # Write splitted fasta file and write a torunlist.txt
        if not os.path.exists(split_seq_dir):
            os.mkdir(split_seq_dir)

        torun_index_str_list = [str(x[0]) for x in sortedlist]
        if len(torun_index_str_list)>0:
            myfunc.WriteFile("\n".join(torun_index_str_list)+"\n", torun_idx_file, "w", True)
        else:
            myfunc.WriteFile("", torun_idx_file, "w", True)

        # write cnttry file for each jobs to run
        cntTryDict = {}
        for idx in torun_index_str_list:
            cntTryDict[int(idx)] = 0
        json.dump(cntTryDict, open(cnttry_idx_file, "w"))

        for item in sortedlist:
            origIndex = item[0]
            seq = item[1][0]
            description = item[1][2]
            seqfile_this_seq = "%s/%s"%(split_seq_dir, "query_%d.fa"%(origIndex))
            seqcontent = ">%s\n%s\n"%(description, seq)
            myfunc.WriteFile(seqcontent, seqfile_this_seq, "w", True)
        # qdinit file is written at the end of initialization, to make sure
        # that initialization is either not started or completed
        webcom.WriteDateTimeTagFile(qdinittagfile, runjob_logfile, runjob_errfile)
#}}}


    # 3. try to submit the job 
    toRunIndexList = [] # index in str
    processedIndexSet = set([]) #seq index set that are already processed
    submitted_loginfo_list = []
    if os.path.exists(torun_idx_file):
        toRunIndexList = myfunc.ReadIDList(torun_idx_file)
        # unique the list but keep the order
        toRunIndexList = myfunc.uniquelist(toRunIndexList)
    if len(toRunIndexList) > 0:
        iToRun = 0
        numToRun = len(toRunIndexList)
        for node in cntSubmitJobDict:
            if iToRun >= numToRun:
                break
            wsdl_url = "http://%s/pred/api_submitseq/?wsdl"%(node)
            try:
                myclient = Client(wsdl_url, cache=None, timeout=30)
            except:
                webcom.loginfo("Failed to access %s"%(wsdl_url), gen_errfile)
                break

            [cnt, maxnum, queue_method] = cntSubmitJobDict[node]
            cnttry = 0
            while cnt < maxnum and iToRun < numToRun:
                origIndex = int(toRunIndexList[iToRun])
                seqfile_this_seq = "%s/%s"%(split_seq_dir, "query_%d.fa"%(origIndex))
                # ignore already existing query seq, this is an ugly solution,
                # the generation of torunindexlist has a bug
                outpath_this_seq = "%s/%s"%(outpath_result, "seq_%d"%origIndex)
                if os.path.exists(outpath_this_seq):
                    iToRun += 1
                    continue


                if g_params['DEBUG']:
                    myfunc.WriteFile("DEBUG: cnt (%d) < maxnum (%d) "\
                            "and iToRun(%d) < numToRun(%d)"%(cnt, maxnum, iToRun, numToRun), gen_logfile, "a", True)
                fastaseq = ""
                seqid = ""
                seqanno = ""
                seq = ""
                if not os.path.exists(seqfile_this_seq):
                    all_seqfile = "%s/query.fa"%(rstdir)
                    try:
                        (allseqidlist, allannolist, allseqlist) = myfunc.ReadFasta(all_seqfile)
                        seqid = allseqidlist[origIndex]
                        seqanno = allannolist[origIndex]
                        seq = allseqlist[origIndex]
                        fastaseq = ">%s\n%s\n"%(seqanno, seq)
                    except:
                        pass
                else:
                    fastaseq = myfunc.ReadFile(seqfile_this_seq)#seq text in fasta format
                    (seqid, seqanno, seq) = myfunc.ReadSingleFasta(seqfile_this_seq)


                isSubmitSuccess = False
                if len(seq) > 0:
                    query_para = {}
                    if wsdl_url.find("commonbackend") != -1:
                        query_para['name_software'] = "docker_subcons"
                    else:
                        query_para['name_software'] = "subcons"
                    query_para['queue_method'] = queue_method
                    if (queue_method == 'slurm'):
                        query_para['name_software'] = "singularity_subcons"

                    para_str = json.dumps(query_para, sort_keys=True)
                    jobname = ""
                    if not email in g_params['vip_user_list']:
                        useemail = ""
                    else:
                        useemail = email
                    try:
                        myfunc.WriteFile("\tSubmitting seq %4d "%(origIndex),
                                gen_logfile, "a", True)
                        rtValue = myclient.service.submitjob_remote(fastaseq, para_str,
                                jobname, useemail, str(numseq_this_user), isForceRun)
                    except:
                        webcom.loginfo("Failed to run myclient.service.submitjob_remote", gen_errfile)
                        rtValue = []
                        pass

                    cnttry += 1
                    if len(rtValue) >= 1:
                        strs = rtValue[0]
                        if len(strs) >=5:
                            remote_jobid = strs[0]
                            result_url = strs[1]
                            numseq_str = strs[2]
                            errinfo = strs[3]
                            warninfo = strs[4]
                            if remote_jobid != "None" and remote_jobid != "":
                                isSubmitSuccess = True
                                epochtime = time.time()
                                # 6 fields in the file remotequeue_idx_file
                                txt =  "%d\t%s\t%s\t%s\t%s\t%f"%( origIndex,
                                        node, remote_jobid, seqanno, seq,
                                        epochtime)
                                submitted_loginfo_list.append(txt)
                                cnttry = 0  #reset cnttry to zero
                        else:
                            webcom.loginfo("bad wsdl return value", gen_errfile)
                if isSubmitSuccess:
                    cnt += 1
                    myfunc.WriteFile(" succeeded\n", gen_logfile, "a", True)
                else:
                    myfunc.WriteFile(" failed\n", gen_logfile, "a", True)

                if isSubmitSuccess or cnttry >= g_params['MAX_SUBMIT_TRY']:
                    iToRun += 1
                    processedIndexSet.add(str(origIndex))
                    if g_params['DEBUG']:
                        myfunc.WriteFile("DEBUG: jobid %s processedIndexSet.add(str(%d))\n"%(jobid, origIndex), gen_logfile, "a", True)
            # update cntSubmitJobDict for this node
            cntSubmitJobDict[node][0] = cnt

    # finally, append submitted_loginfo_list to remotequeue_idx_file 
    if len(submitted_loginfo_list)>0:
        myfunc.WriteFile("\n".join(submitted_loginfo_list)+"\n", remotequeue_idx_file, "a", True)
    # update torun_idx_file
    newToRunIndexList = []
    for idx in toRunIndexList:
        if not idx in processedIndexSet:
            newToRunIndexList.append(idx)
    if g_params['DEBUG']:
        myfunc.WriteFile("DEBUG: jobid %s, newToRunIndexList="%(jobid) + " ".join( newToRunIndexList)+"\n", gen_logfile, "a", True)

    if len(newToRunIndexList)>0:
        myfunc.WriteFile("\n".join(newToRunIndexList)+"\n", torun_idx_file, "w", True)
    else:
        myfunc.WriteFile("", torun_idx_file, "w", True)

    return 0
#}}}
def GetResult(jobid):#{{{
    # retrieving result from the remote server for this job
    myfunc.WriteFile("GetResult for %s.\n" %(jobid), gen_logfile, "a", True)
    rstdir = "%s/%s"%(path_result, jobid)
    runjob_logfile = "%s/%s"%(rstdir, "runjob.log")
    runjob_errfile = "%s/%s"%(rstdir, "runjob.err")
    outpath_result = "%s/%s"%(rstdir, jobid)
    if not os.path.exists(outpath_result):
        os.mkdir(outpath_result)

    remotequeue_idx_file = "%s/remotequeue_seqindex.txt"%(rstdir)

    torun_idx_file = "%s/torun_seqindex.txt"%(rstdir) # ordered seq index to run
    finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
    failed_idx_file = "%s/failed_seqindex.txt"%(rstdir)

    starttagfile = "%s/%s"%(rstdir, "runjob.start")
    cnttry_idx_file = "%s/cntsubmittry_seqindex.txt"%(rstdir)#index file to keep log of tries
    tmpdir = "%s/tmpdir"%(rstdir)
    finished_seq_file = "%s/finished_seqs.txt"%(outpath_result)

    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)

    finished_info_list = [] #[info for finished record]
    finished_idx_list = [] # [origIndex]
    failed_idx_list = []    # [origIndex]
    resubmit_idx_list = []  # [origIndex]
    keep_queueline_list = [] # [line] still in queue

    cntTryDict = {}
    if os.path.exists(cnttry_idx_file):
        with open(cnttry_idx_file, 'r') as fpin:
            cntTryDict = json.load(fpin)

    # in case of missing queries, if remotequeue_idx_file is empty  but the job
    # is still not finished, force re-creating torun_idx_file
    if ((not os.path.exists(remotequeue_idx_file) or
        os.path.getsize(remotequeue_idx_file)<1)):
        idlist1 = []
        idlist2 = []
        if os.path.exists(finished_idx_file):
           idlist1 =  myfunc.ReadIDList(finished_idx_file)
        if os.path.exists(failed_idx_file):
           idlist2 =  myfunc.ReadIDList(failed_idx_file)

        completed_idx_set = set(idlist1 + idlist2)

        jobinfofile = "%s/jobinfo"%(rstdir)
        jobinfo = myfunc.ReadFile(jobinfofile).strip()
        jobinfolist = jobinfo.split("\t")
        if len(jobinfolist) >= 8:
            numseq = int(jobinfolist[3])

        if len(completed_idx_set) < numseq:
            all_idx_list = [str(x) for x in range(numseq)]
            torun_idx_str_list = list(set(all_idx_list)-completed_idx_set)
            for idx in torun_idx_str_list:
                try:
                    cntTryDict[int(idx)] += 1
                except:
                    cntTryDict[int(idx)] = 1
                    pass
            myfunc.WriteFile("\n".join(torun_idx_str_list)+"\n", torun_idx_file, "w", True)

            if g_params['DEBUG']:
                myfunc.WriteFile("recreate torun_idx_file: jobid = %s, numseq=%d, len(completed_idx_set)=%d, len(torun_idx_str_list)=%d\n"%(jobid, numseq, len(completed_idx_set), len(torun_idx_str_list)), gen_logfile, "a", True)
        else:
            myfunc.WriteFile("", torun_idx_file, "w", True)

    text = ""
    if os.path.exists(remotequeue_idx_file):
        text = myfunc.ReadFile(remotequeue_idx_file)
    if text == "":
        return 1
    lines = text.split("\n")

    nodeSet = set([])
    for i in range(len(lines)):
        line = lines[i]
        if not line or line[0] == "#":
            continue
        strs = line.split("\t")
        if len(strs) != 6:
            continue
        node = strs[1]
        nodeSet.add(node)

    myclientDict = {}
    for node in nodeSet:
        wsdl_url = "http://%s/pred/api_submitseq/?wsdl"%(node)
        try:
            myclient = Client(wsdl_url, cache=None, timeout=30)
            myclientDict[node] = myclient
        except:
            webcom.loginfo("Failed to access %s"%(wsdl_url), gen_errfile)
            pass


    for i in range(len(lines)):#{{{
        line = lines[i]

        if g_params['DEBUG']:
            myfunc.WriteFile("Process %s\n"%(line), gen_logfile, "a", True)
        if not line or line[0] == "#":
            continue
        strs = line.split("\t")
        if len(strs) != 6:
            continue
        origIndex = int(strs[0])
        node = strs[1]
        remote_jobid = strs[2]
        description = strs[3]
        seq = strs[4]
        submit_time_epoch = float(strs[5])
        subfoldername_this_seq = "seq_%d"%(origIndex)
        outpath_this_seq = "%s/%s"%(outpath_result, "seq_%d"%origIndex)

        try:
            myclient = myclientDict[node]
        except KeyError:
            continue
        try:
            rtValue = myclient.service.checkjob(remote_jobid)
        except:
            webcom.loginfo("Failed to run myclient.service.checkjob(%s)"%(remote_jobid), gen_errfile)
            rtValue = []
            pass
        isSuccess = False
        isFinish_remote = False
        if len(rtValue) >= 1:
            ss2 = rtValue[0]
            if len(ss2)>=3:
                status = ss2[0]
                result_url = ss2[1]
                errinfo = ss2[2]

                if errinfo and errinfo.find("does not exist")!=-1:
                    isFinish_remote = True

                if status == "Finished":#{{{
                    isFinish_remote = True
                    outfile_zip = "%s/%s.zip"%(tmpdir, remote_jobid)
                    isRetrieveSuccess = False
                    myfunc.WriteFile("\tFetching result for %s "%(result_url),
                            gen_logfile, "a", True)
                    if myfunc.IsURLExist(result_url,timeout=5):
                        try:
                            urllib.request.urlretrieve (result_url, outfile_zip)
                            isRetrieveSuccess = True
                            myfunc.WriteFile(" succeeded\n", gen_logfile, "a", True)
                        except Exception as e:
                            myfunc.WriteFile(" failed with %s\n"%(str(e)), gen_logfile, "a", True)
                            pass
                    if os.path.exists(outfile_zip) and isRetrieveSuccess:
                        cmd = ["unzip", outfile_zip, "-d", tmpdir]
                        webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)
                        rst_this_seq = "%s/%s/seq_0"%(tmpdir, remote_jobid)

                        if os.path.islink(outpath_this_seq):
                            os.unlink(outpath_this_seq)
                        elif os.path.exists(outpath_this_seq):
                            shutil.rmtree(outpath_this_seq)

                        if os.path.exists(rst_this_seq) and not os.path.exists(outpath_this_seq):
                            cmd = ["mv","-f", rst_this_seq, outpath_this_seq]
                            webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)
                            checkfile = "%s/plot/query_0.png"%(outpath_this_seq)
                            if os.path.exists(checkfile):
                                isSuccess = True

                            if isSuccess:
                                # delete the data on the remote server
                                try:
                                    rtValue2 = myclient.service.deletejob(remote_jobid)
                                except:
                                    webcom.loginfo("Failed to run myclient.service.deletejob(%s)"%(remote_jobid), gen_errfile)
                                    rtValue2 = []
                                    pass

                                logmsg = ""
                                if len(rtValue2) >= 1:
                                    ss2 = rtValue2[0]
                                    if len(ss2) >= 2:
                                        status = ss2[0]
                                        errmsg = ss2[1]
                                        if status == "Succeeded":
                                            logmsg = "Successfully deleted data on %s "\
                                                    "for %s"%(node, remote_jobid)
                                        else:
                                            logmsg = "Failed to delete data on %s for "\
                                                    "%s\nError message:\n%s\n"%(node, remote_jobid, errmsg)
                                else:
                                    logmsg = "Failed to call deletejob %s via WSDL on %s\n"%(remote_jobid, node)

                                # delete the downloaded temporary zip file and
                                # extracted file
                                os.remove(outfile_zip)
                                shutil.rmtree("%s/%s"%(tmpdir, remote_jobid))

                                # create or update the md5 cache
                                md5_key = hashlib.md5(seq.encode('utf-8')).hexdigest()
                                subfoldername = md5_key[:2]
                                md5_subfolder = "%s/%s"%(path_cache, subfoldername)
                                cachedir = "%s/%s/%s"%(path_cache, subfoldername, md5_key)

                                # copy the zipped folder to the cache path
                                origpath = os.getcwd()
                                os.chdir(outpath_result)
                                shutil.copytree("seq_%d"%(origIndex), md5_key)
                                cmd = ["zip", "-rq", "%s.zip"%(md5_key), md5_key]
                                webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)
                                if not os.path.exists(md5_subfolder):
                                    os.makedirs(md5_subfolder)
                                shutil.move("%s.zip"%(md5_key), "%s.zip"%(cachedir))
                                shutil.rmtree(md5_key) # delete the temp folder named as md5 hash
                                os.chdir(origpath)

                                # Add the finished date to the database
                                date_str = time.strftime(g_params['FORMAT_DATETIME'])
                                MAX_TRY_INSERT_DB = 3
                                cnttry = 0
                                while cnttry < MAX_TRY_INSERT_DB:
                                    t_rv = webcom.InsertFinishDateToDB(date_str, md5_key, seq, finished_date_db)
                                    if t_rv == 0:
                                        break
                                    cnttry += 1
                                    time.sleep(random.random()/1.0)
#}}}
                elif status in ["Failed", "None"]:
                    # the job is failed for this sequence, try to re-submit
                    isFinish_remote = True
                if status != "Wait" and not os.path.exists(starttagfile):
                    webcom.WriteDateTimeTagFile(starttagfile, runjob_logfile, runjob_errfile)

                if g_params['DEBUG_CACHE']:
                    myfunc.WriteFile("\n", gen_logfile, "a", True)

        if isSuccess:#{{{
            time_now = time.time()
            runtime1 = time_now - submit_time_epoch #in seconds
            timefile = "%s/time.txt"%(outpath_this_seq)
            runtime = webcom.ReadRuntimeFromFile(timefile, default_runtime=runtime1)
            info_finish = webcom.GetInfoFinish_Subcons(outpath_this_seq,
                    origIndex, len(seq), description,
                    source_result="newrun", runtime=runtime)
            finished_info_list.append("\t".join(info_finish))
            finished_idx_list.append(str(origIndex))#}}}

        # if the job is finished on the remote but the prediction is failed,
        # try resubmit a few times and if all failed, add the origIndex to the
        # failed_idx_file
        if isFinish_remote and not isSuccess:
            cnttry = 1
            try:
                cnttry = cntTryDict[int(origIndex)]
            except KeyError:
                cnttry = 1
            if cnttry < g_params['MAX_RESUBMIT']:
                resubmit_idx_list.append(str(origIndex))
                cntTryDict[int(origIndex)] = cnttry+1
            else:
                failed_idx_list.append(str(origIndex))

        if not isFinish_remote:
            time_in_remote_queue = time.time() - submit_time_epoch
            # for jobs queued in the remote queue more than one day (but not
            # running) delete it and try to resubmit it. This solved the
            # problem of dead jobs in the remote server due to server
            # rebooting)
            if status != "Running" and time_in_remote_queue > g_params['MAX_TIME_IN_REMOTE_QUEUE']:
                # delete the remote job on the remote server
                try:
                    rtValue2 = myclient.service.deletejob(remote_jobid)
                except Exception as e:
                    webcom.loginfo( "Failed to run myclient.service.deletejob(%s) on node %s with msg %s"%(remote_jobid, node, str(e)), gen_logfile)
                    rtValue2 = []
                    pass
            else:
                keep_queueline_list.append(line)
#}}}
    #Finally, write log files
    finished_idx_list = list(set(finished_idx_list))
    failed_idx_list = list(set(failed_idx_list))
    resubmit_idx_list = list(set(resubmit_idx_list))

    if len(finished_info_list)>0:
        myfunc.WriteFile("\n".join(finished_info_list)+"\n", finished_seq_file, "a", True)
    if len(finished_idx_list)>0:
        myfunc.WriteFile("\n".join(finished_idx_list)+"\n", finished_idx_file, "a", True)
    if len(failed_idx_list)>0:
        myfunc.WriteFile("\n".join(failed_idx_list)+"\n", failed_idx_file, "a", True)
    if len(resubmit_idx_list)>0:
        myfunc.WriteFile("\n".join(resubmit_idx_list)+"\n", torun_idx_file, "a", True)

    if len(keep_queueline_list)>0:
        myfunc.WriteFile("\n".join(keep_queueline_list)+"\n", remotequeue_idx_file, "w", True);
    else:
        myfunc.WriteFile("", remotequeue_idx_file, "w", True);

    with open(cnttry_idx_file, 'w') as fpout:
        json.dump(cntTryDict, fpout)
    return 0
#}}}

def CheckIfJobFinished(jobid, numseq, email):#{{{
    # check if the job is finished and write tagfiles
    myfunc.WriteFile("CheckIfJobFinished for %s.\n" %(jobid), gen_logfile, "a", True)
    rstdir = "%s/%s"%(path_result, jobid)
    tmpdir = "%s/tmpdir"%(rstdir)
    outpath_result = "%s/%s"%(rstdir, jobid)
    runjob_errfile = "%s/%s"%(rstdir, "runjob.err")
    runjob_logfile = "%s/%s"%(rstdir, "runjob.log")
    finished_idx_file = "%s/finished_seqindex.txt"%(rstdir)
    failed_idx_file = "%s/failed_seqindex.txt"%(rstdir)
    seqfile = "%s/query.fa"%(rstdir)

    base_www_url_file = "%s/static/log/base_www_url.txt"%(basedir)
    base_www_url = ""

    finished_idx_list = []
    failed_idx_list = []
    if os.path.exists(finished_idx_file):
        finished_idx_list = myfunc.ReadIDList(finished_idx_file)
        finished_idx_list = list(set(finished_idx_list))
    if os.path.exists(failed_idx_file):
        failed_idx_list = myfunc.ReadIDList(failed_idx_file)
        failed_idx_list = list(set(failed_idx_list))

    finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
    failedtagfile = "%s/%s"%(rstdir, "runjob.failed")
    starttagfile = "%s/%s"%(rstdir, "runjob.start")

    num_processed = len(finished_idx_list)+len(failed_idx_list)
    finish_status = "" #["success", "failed", "partly_failed"]
    if num_processed >= numseq:# finished
        if len(failed_idx_list) == 0:
            finish_status = "success"
        elif len(failed_idx_list) >= numseq:
            finish_status = "failed"
        else:
            finish_status = "partly_failed"

        if os.path.exists(base_www_url_file):
            base_www_url = myfunc.ReadFile(base_www_url_file).strip()
        if base_www_url == "":
            base_www_url = "http://subcons.bioinfo.se"

        date_str = time.strftime(g_params['FORMAT_DATETIME'])
        date_str_epoch = time.time()
        myfunc.WriteFile(date_str, finishtagfile, "w", True)

        # Now write the text output to a single file
        statfile = "%s/%s"%(outpath_result, "stat.txt")
        resultfile_text = "%s/%s"%(outpath_result, "query.result.txt")
        (seqIDList, seqAnnoList, seqList) = myfunc.ReadFasta(seqfile)
        maplist = []
        for i in range(len(seqIDList)):
            maplist.append("%s\t%d\t%s\t%s"%("seq_%d"%i, len(seqList[i]),
                seqAnnoList[i], seqList[i]))
        start_date_str = myfunc.ReadFile(starttagfile).strip()
        start_date_epoch = webcom.datetime_str_to_epoch(start_date_str)
        all_runtime_in_sec = float(date_str_epoch) - float(start_date_epoch)

        webcom.WriteSubconsTextResultFile(resultfile_text, outpath_result, maplist,
                all_runtime_in_sec, base_www_url, statfile=statfile)

        # note that zip rq will zip the real data for symbolic links
        zipfile = "%s.zip"%(jobid)
        zipfile_fullpath = "%s/%s"%(rstdir, zipfile)
        os.chdir(rstdir)
        cmd = ["zip", "-rq", zipfile, jobid]
        webcom.RunCmd(cmd, runjob_logfile, runjob_errfile)

        if len(failed_idx_list)>0:
            myfunc.WriteFile(date_str, failedtagfile, "w", True)

        if finish_status == "success":
            shutil.rmtree(tmpdir)

        # send the result to email
        if webcom.IsFrontEndNode(base_www_url) and myfunc.IsValidEmailAddress(email):
            webcom.SendEmail_on_finish(jobid, base_www_url,
                    finish_status, name_server="SubCons", from_email="no-reply.SubCons@bioinfo.se",
                    to_email=email, contact_email=contact_email,
                    logfile=runjob_logfile, errfile=runjob_errfile)
        webcom.CleanJobFolder_Subcons(rstdir)

#}}}

def main(g_params):#{{{
    submitjoblogfile = "%s/submitted_seq.log"%(path_log)
    runjoblogfile = "%s/runjob_log.log"%(path_log)
    finishedjoblogfile = "%s/finished_job.log"%(path_log)

    if not os.path.exists(path_cache):
        os.mkdir(path_cache)

    loop = 0
    while 1:
        if os.path.exists("%s/CACHE_CLEANING_IN_PROGRESS"%(path_result)):#pause when cache cleaning is in progress
            continue
        # load the config file if exists
        configfile = "%s/config/config.json"%(basedir)
        config = {}
        if os.path.exists(configfile):
            text = myfunc.ReadFile(configfile)
            config = json.loads(text)

        if rootname_progname in config:
            g_params.update(config[rootname_progname])

        if os.path.exists(black_iplist_file):
            g_params['blackiplist'] = myfunc.ReadIDList(black_iplist_file)

        os.environ['TZ'] = g_params['TZ']
        time.tzset()

        avail_computenode = webcom.ReadComputeNode(computenodefile) # return value is a dict
        g_params['vip_user_list'] = myfunc.ReadIDList2(vip_email_file,  col=0)
        num_avail_node = len(avail_computenode)

        webcom.loginfo("loop %d"%(loop), gen_logfile)

        isOldRstdirDeleted = False
        if loop % g_params['STATUS_UPDATE_FREQUENCY'][0] == g_params['STATUS_UPDATE_FREQUENCY'][1]:
            qdcom.RunStatistics_basic(webserver_root, gen_logfile, gen_errfile)
            isOldRstdirDeleted = webcom.DeleteOldResult(path_result, path_log,
                    gen_logfile, MAX_KEEP_DAYS=g_params['MAX_KEEP_DAYS'])
            webcom.CleanServerFile(path_static, gen_logfile, gen_errfile)
        webcom.ArchiveLogFile(path_log, threshold_logfilesize=threshold_logfilesize) 

        CreateRunJoblog(path_result, submitjoblogfile, runjoblogfile,
                finishedjoblogfile, loop, isOldRstdirDeleted)

        # Get number of jobs submitted to the remote server based on the
        # runjoblogfile
        runjobidlist = myfunc.ReadIDList2(runjoblogfile,0)
        remotequeueDict = {}
        for node in avail_computenode:
            remotequeueDict[node] = []
        for jobid in runjobidlist:
            rstdir = "%s/%s"%(path_result, jobid)
            remotequeue_idx_file = "%s/remotequeue_seqindex.txt"%(rstdir)
            if os.path.exists(remotequeue_idx_file):
                content = myfunc.ReadFile(remotequeue_idx_file)
                lines = content.split('\n')
                for line in lines:
                    strs = line.split('\t')
                    if len(strs)>=5:
                        node = strs[1]
                        remotejobid = strs[2]
                        if node in remotequeueDict:
                            remotequeueDict[node].append(remotejobid)

        cntSubmitJobDict = {} # format of cntSubmitJobDict {'node_ip': [INT, INT, STR]}
        for node in avail_computenode:
            queue_method = avail_computenode[node]['queue_method']
            num_queue_job = len(remotequeueDict[node])
            if num_queue_job >= 0:
                cntSubmitJobDict[node] = [num_queue_job,
                        g_params['MAX_SUBMIT_JOB_PER_NODE'], queue_method]
            else:
                cntSubmitJobDict[node] = [g_params['MAX_SUBMIT_JOB_PER_NODE'],
                        g_params['MAX_SUBMIT_JOB_PER_NODE'], queue_method]

# entries in runjoblogfile includes jobs in queue or running
        hdl = myfunc.ReadLineByBlock(runjoblogfile)
        if not hdl.failure:
            lines = hdl.readlines()
            while lines != None:
                for line in lines:
                    strs = line.split("\t")
                    if len(strs) >= 11:
                        jobid = strs[0]
                        email = strs[4]
                        try:
                            numseq = int(strs[5])
                        except:
                            numseq = 1
                        try:
                            numseq_this_user = int(strs[10])
                        except:
                            numseq_this_user = 1
                        rstdir = "%s/%s"%(path_result, jobid)
                        finishtagfile = "%s/%s"%(rstdir, "runjob.finish")
                        status = strs[1]
                        webcom.loginfo("CompNodeStatus: %s"%(str(cntSubmitJobDict)), gen_logfile)

                        runjob_lockfile = "%s/%s/%s.lock"%(path_result, jobid, "runjob.lock")
                        if os.path.exists(runjob_lockfile):
                            msg = "runjob_lockfile %s exists, ignore the job %s" %(runjob_lockfile, jobid)
                            webcom.loginfo(msg, gen_logfile)
                            continue

                        #if IsHaveAvailNode(cntSubmitJobDict):
                        if not g_params['DEBUG_NO_SUBMIT']:
                            SubmitJob(jobid, cntSubmitJobDict, numseq_this_user)
                        GetResult(jobid) # the start tagfile is written when got the first result
                        CheckIfJobFinished(jobid, numseq, email)

                lines = hdl.readlines()
            hdl.close()

        myfunc.WriteFile("sleep for %d seconds\n"%(g_params['SLEEP_INTERVAL']), gen_logfile, "a", True)
        time.sleep(g_params['SLEEP_INTERVAL'])
        loop += 1


    return 0
#}}}

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['isQuiet'] = True
    g_params['blackiplist'] = []
    g_params['DEBUG'] = False
    g_params['DEBUG_NO_SUBMIT'] = False
    g_params['DEBUG_CACHE'] = False
    g_params['DEBUG_REMOTE_QUEUE'] = False
    g_params['SLEEP_INTERVAL'] = 5    # sleep interval in seconds
    g_params['MAX_SUBMIT_JOB_PER_NODE'] = 200
    g_params['MAX_KEEP_DAYS'] = 60
    g_params['MAX_RESUBMIT'] = 2
    g_params['MAX_SUBMIT_TRY'] = 3
    g_params['MAX_TIME_IN_REMOTE_QUEUE'] = 3600*24 # one day in seconds
    g_params['FORMAT_DATETIME'] = webcom.FORMAT_DATETIME
    g_params['TZ'] = "Europe/Stockholm"
    g_params['MAX_CACHE_PROCESS'] = 200 # process at the maximum this cached sequences in one loop
    g_params['STATUS_UPDATE_FREQUENCY'] = [500, 50]  # updated by if loop%$1 == $2
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    date_str = time.strftime(g_params['FORMAT_DATETIME'])
    print("\n#%s#\n[Date: %s] qd_fe.py restarted"%('='*80,date_str))
    sys.stdout.flush()
    sys.exit(main(g_params))
