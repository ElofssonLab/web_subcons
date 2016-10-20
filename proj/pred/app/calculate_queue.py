#!/usr/bin/env python
# Description: calculate queues for topcons2 web-server
# Calculate the queue of jobs in the result folder
import os
import sys
import myfunc
import datetime

#  score = queue_time_in_sec / ((rank+addition)**2 * max(length_seq,100)**1.5)

usage_short="""
Usage: %s -resultdir DIR -runlog FILE [-o OUTFILE]
"""%(sys.argv[0])

usage_ext="""
Description:
    Calculate the queue of jobs in the result folder

OPTIONS:
  -resultdir DIR    Result directory
  -runlog   FILE    Log file for the status of all jobs
  -o OUTFILE        Output the result to OUTFILE
  -h, --help    Print this help message and exit

Created 2015-03-25, updated 2015-03-25, Nanjiang Shu
"""
usage_exp="""
Examples:
    %s -resultdir result -runlog runlog.txt -o jobqueue.txt
"""

def PrintHelp(fpout=sys.stdout):#{{{
    print >> fpout, usage_short
    print >> fpout, usage_ext
    print >> fpout, usage_exp#}}}

def get_job_status(jobdir):#{{{
    status = "";
    tagfile_start = "%s/%s"%(jobdir, "pconsc.start")
    tagfile_stop = "%s/%s"%(jobdir, "pconsc.stop")
    tagfile_success = "%s/%s"%(jobdir, "pconsc.success")
    tagfile_rerun = "%s/%s"%(jobdir, "pconsc.rerun")

    if not os.path.exists(tagfile_start):
        status = "Queued"
    else:
        if os.path.exists(tagfile_stop):
            if os.path.exists(tagfile_success):
                status = "Finished"
            else:
                status = "Failed"
        else:
            if os.path.exists(tagfile_rerun):
                status = "Rerun"
            else:
                status = "Running"
    return status
#}}}
def get_total_seconds(td): #{{{
    """
    return the total_seconds for the timedate.timedelta object
    for python version >2.7 this is not needed
    """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6
#}}}
def main(g_params):#{{{
    argv = sys.argv
    numArgv = len(argv)
    if numArgv < 2:
        PrintHelp()
        return 1

    resultdir = ""
    outfile = ""

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
                return 1
            elif argv[i] in ["-o", "--o", "-outfile"]:
                (outfile, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-resultdir", "--resultdir"]:
                (resultdir, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-l", "--l"] :
                (fileListFile, i) = myfunc.my_getopt_str(argv, i)
            elif argv[i] in ["-q", "--q"]:
                g_params['isQuiet'] = True
                i += 1
            else:
                print >> sys.stderr, "Error! Wrong argument:", argv[i]
                return 1
        else:
            print >> sys.stderr, "Error! Wrong argument:", argv[i]
            return 1
    if resultdir == "":
        print >> sys.stderr , "%s: resultdir not set. exit"%(sys.argv[0])
        return 1
    elif not os.path.exists(resultdir):
        print >> sys.stderr, "%s: resultdir %s does not exist. exit"%(sys.argv[0], resultdir)
        return 1

    CalculateQueue(resultdir, outfile)


#}}}
def ReadContent(infile):#{{{
    """
    Read content, strip all front end white spaces
    return "" if read failed
    """
    content = ""
    try:
        content = open(infile, "r").read().strip()
    except IOError:
        content = ""
    return content
#}}}
def CalculateQueue(resultdir, outfile):#{{{
# 1. get the list of working folders
    raw_folder_list = os.listdir(resultdir)
    folder_nr_list = []
    for folder in raw_folder_list:
        if os.path.isdir(resultdir+"/"+folder) and (folder.isdigit() or folder[0:2] == "r_" or folder[:4]=="rst_"):
            folder_nr_list.append(folder)
# 2. gather information for queued jobs
    job_table_in_queue = {}
    other_job_table = {}
    freq_user_in_queue = {} # count the frequency of the user of jobs in queue
    freq_user_running = {}  # count the frequency of the user for running jobs
    for folder in folder_nr_list:
        workdir = "%s/%s"%(resultdir, folder)
        status = get_job_status(workdir)
        if status in ["Queued"]: # jobs in queue
            email = ReadContent("%s/%s"%(workdir, "email"))
            host = ReadContent("%s/%s"%(workdir, "host"))
            date_str = ReadContent("%s/%s"%(workdir, "date"))
            sequence = ReadContent("%s/%s"%(workdir, "sequence")) 
            length_seq = len(sequence)
            user = ""
            if email and email != "N/A":
                user = email
            else:
                user = host
            if not user in freq_user_in_queue:
                freq_user_in_queue[user] = 0
            freq_user_in_queue[user] += 1

            try:
                date_submitted = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print >> sys.stderr, "datefile = '%s'. date = '%s'"%("%s/date"%(workdir), date_str)
                print >> sys.stderr, "Ignore %s"%folder
                continue
            date_now = datetime.datetime.now()
            queue_time = date_now - date_submitted
            queue_time_in_sec = get_total_seconds(queue_time)

            job_table_in_queue[folder] = [status, user, queue_time_in_sec, length_seq]
        else:
            email = ReadContent("%s/%s"%(workdir, "email"))
            host = ReadContent("%s/%s"%(workdir, "host"))
            user = ""
            if email and email != "N/A":
                user = email
            else:
                user = host
            if status in ["Running", "Rerun"]:
                if not user in freq_user_running:
                    freq_user_running[user] = 0
                freq_user_running[user] += 1
                other_job_table[folder] = [status, user, 0, 0]
            else:
                other_job_table[folder] = [status, user, 0, 0]


    for folder in job_table_in_queue:
        user = job_table_in_queue[folder][1]
        freq_in_queue = 1
        freq_running = 0
        if user != "":
            freq_in_queue = freq_user_in_queue[user]
        try:
            freq_running = freq_user_running[user]
        except KeyError:
            freq_running = 0

        job_table_in_queue[folder].append(freq_in_queue)
        job_table_in_queue[folder].append(freq_running)

    for folder in other_job_table:
        other_job_table[folder].append(0)
        other_job_table[folder].append(0)

# calculate the priority
# now each job_table_in_queue[folder] has five element 
# [user, queue_time_in_sec, length_seq, freq_in_queue, freq_running]
# Group the jobs in each user, and for the sublist of each user, first rank by
# the queue_time_in_sec, and then do as follows
# Note, for those target with <= 100 aa, the run time is relatively similar
# score = queue_time_in_sec / ((rank+addition)**1.5 * max(length_seq,100)**1.5)
# where addition = freq_running
    for user in freq_user_in_queue:
        sub_table = {}
        for folder in job_table_in_queue:
            if job_table_in_queue[folder][1] == user:
                sub_table[folder] = job_table_in_queue[folder]

        # in descending order by queue_time_in_sec
        sorted_sub_table = sorted(sub_table.items(), key=lambda x:x[1][2], reverse=True)

        for i in xrange(len(sorted_sub_table)):
            folder_nr = sorted_sub_table[i][0]
            queue_time_in_sec = sorted_sub_table[i][1][2]
            length_seq = sorted_sub_table[i][1][3]
            freq_in_queue = sorted_sub_table[i][1][4]
            freq_running = sorted_sub_table[i][1][5]
            rank = i+1
            addition = freq_running
            if user == "":
                rank = 1
                addition = 0

            score = queue_time_in_sec/((rank+addition)**2 * max(length_seq, 100)**1.5)
            job_table_in_queue[folder_nr].append(score)

    for folder in other_job_table:
        other_job_table[folder].append(0)

# now rank the job_table_in_queue again
    sorted_job_table_in_queue = sorted(job_table_in_queue.items(), key=lambda x:x[1][6], reverse=True)

# write the result
    fpout = myfunc.myopen(outfile,sys.stdout, "w", False)
    print >> fpout, "#%-5s %8s %4s %-30s %10s %10s %6s %6s"%("ID","Status",
            "Rank","User","PD_time(s)","Score","Count_PD","Count_R")
    for i in xrange(len(sorted_job_table_in_queue)):
        folder = sorted_job_table_in_queue[i][0]
        rank = i + 1
        status = sorted_job_table_in_queue[i][1][0]
        user = sorted_job_table_in_queue[i][1][1]
        queue_time_in_sec = sorted_job_table_in_queue[i][1][2]
        freq_in_queue = sorted_job_table_in_queue[i][1][4]
        freq_running = sorted_job_table_in_queue[i][1][5]
        score = sorted_job_table_in_queue[i][1][6]
        print >> fpout , "%-6s %8s %4d %-30s %10.1f %10.1f %6d %6d"%(folder,
                status, rank, user, queue_time_in_sec, score, freq_in_queue,
                freq_running)

# now rank the job_table_in_queue again
    sorted_other_job_table = sorted(other_job_table.items(), key=lambda x:x[1][0])# sorted by status
    for i in xrange(len(sorted_other_job_table)):
        folder = sorted_other_job_table[i][0]
        rank = 0
        status = sorted_other_job_table[i][1][0]
        user = sorted_other_job_table[i][1][1]
        queue_time_in_sec = sorted_other_job_table[i][1][2]
        freq_in_queue = sorted_other_job_table[i][1][4]
        freq_running = sorted_other_job_table[i][1][5]
        score = sorted_other_job_table[i][1][6]
        print >> fpout , "%-6s %8s %4d %-30s %10.1f %10.1f %6d %6d"%(folder,
                status, rank, user, queue_time_in_sec, score, freq_in_queue,
                freq_running)


    myfunc.myclose(fpout)


#}}}

def InitGlobalParameter():#{{{
    g_params = {}
    g_params['isQuiet'] = True
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    sys.exit(main(g_params))
