#!/usr/bin/env python

# Description:
#   A collection of classes and functions used by web-servers
#
# Author: Nanjiang Shu (nanjiang.shu@scilifelab.se)
#
# Address: Science for Life Laboratory Stockholm, Box 1031, 17121 Solna, Sweden

import os
import sys
import myfunc
import datetime
import tabulate
import logging
def WriteSubconsTextResultFile(outfile, outpath_result, maplist,#{{{
        runtime_in_sec, base_www_url, statfile=""):
    try:
        fpout = open(outfile, "w")
        if statfile != "":
            fpstat = open(statfile, "w")

        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print >> fpout, "##############################################################################"
        print >> fpout, "Subcons result file"
        print >> fpout, "Generated from %s at %s"%(base_www_url, date)
        print >> fpout, "Total request time: %.1f seconds."%(runtime_in_sec)
        print >> fpout, "##############################################################################"
        cnt = 0
        for line in maplist:
            strs = line.split('\t')
            subfoldername = strs[0]
            length = int(strs[1])
            desp = strs[2]
            seq = strs[3]
            seqid = myfunc.GetSeqIDFromAnnotation(desp)
            print >> fpout, "Sequence number: %d"%(cnt+1)
            print >> fpout, "Sequence name: %s"%(desp)
            print >> fpout, "Sequence length: %d aa."%(length)
            print >> fpout, "Sequence:\n%s\n\n"%(seq)

            rstfile1 = "%s/%s/%s/query_0_final.csv"%(outpath_result, subfoldername, "plot")
            rstfile2 = "%s/%s/query_0_final.csv"%(outpath_result, subfoldername)
            if os.path.exists(rstfile1):
                rstfile = rstfile1
            elif os.path.exists(rstfile2):
                rstfile = rstfile2
            else:
                rstfile = ""

            if os.path.exists(rstfile):
                content = myfunc.ReadFile(rstfile).strip()
                lines = content.split("\n")
                if len(lines) >= 6:
                    header_line = lines[0].split("\t")
                    if header_line[0].strip() == "":
                        header_line[0] = "Method"
                        header_line = [x.strip() for x in header_line]

                    data_line = []
                    for i in xrange(1, len(lines)):
                        strs1 = lines[i].split("\t")
                        strs1 = [x.strip() for x in strs1]
                        data_line.append(strs1)

                    content = tabulate.tabulate(data_line, header_line, 'plain')
            else:
                content = ""
            if content == "":
                content = "***No prediction could be produced with this method***"

            print >> fpout, "Prediction results:\n\n%s\n\n"%(content)

            print >> fpout, "##############################################################################"
            cnt += 1

    except IOError:
        print "Failed to write to file %s"%(outfile)
#}}}
def ReplaceDescriptionSingleFastaFile(infile, new_desp):#{{{
    """Replace the description line of the fasta file by the new_desp
    """
    if os.path.exists(infile):
        (seqid, seqanno, seq) = myfunc.ReadSingleFasta(infile)
        if seqanno != new_desp:
            myfunc.WriteFile(">%s\n%s\n"%(new_desp, seq), infile)
        return 0
    else:
        sys.stderr.write("infile %s does not exists at %s\n"%(infile, sys._getframe().f_code.co_name))
        return 1
#}}}

def GetLocDef(predfile):#{{{
    """
    Read in LocDef and its corresponding score from the subcons prediction file
    """
    content = ""
    if os.path.exists(predfile):
        content = myfunc.ReadFile(predfile)

    loc_def = None
    loc_def_score = None
    if content != "":
        lines = content.split("\n")
        if len(lines)>=2:
            strs0 = lines[0].split("\t")
            strs1 = lines[1].split("\t")
            strs0 = [x.strip() for x in strs0]
            strs1 = [x.strip() for x in strs1]
            if len(strs0) == len(strs1) and len(strs0) > 2:
                if strs0[1] == "LOC_DEF":
                    loc_def = strs1[1]
                    dt_score = {}
                    for i in xrange(2, len(strs0)):
                        dt_score[strs0[i]] = strs1[i]
                    if loc_def in dt_score:
                        loc_def_score = dt_score[loc_def]

    return (loc_def, loc_def_score)
#}}}
def IsFrontEndNode(base_www_url):#{{{
    """
    check if the base_www_url is front-end node
    if base_www_url is ip address, then not the front-end
    otherwise yes
    """
    base_www_url = base_www_url.lstrip("http://").lstrip("https://").split("/")[0]
    if base_www_url == "":
        return False
    elif base_www_url.find("computenode") != -1:
        return False
    else:
        arr =  [x.isdigit() for x in base_www_url.split('.')]
        if all(arr):
            return False
        else:
            return True
#}}}

def GetAverageNewRunTime(finished_seq_file, window=100):#{{{
    """Get average running time of the newrun tasks for the last x number of
sequences
    """
    logger = logging.getLogger(__name__)
    avg_newrun_time = -1.0
    if not os.path.exists(finished_seq_file):
        return avg_newrun_time
    else:
        indexmap_content = myfunc.ReadFile(finished_seq_file).split("\n")
        indexmap_content = indexmap_content[::-1]
        cnt = 0
        sum_run_time = 0.0
        for line in indexmap_content:
            strs = line.split("\t")
            if len(strs)>=7:
                source = strs[4]
                if source == "newrun":
                    try:
                        sum_run_time += float(strs[5])
                        cnt += 1
                    except:
                        logger.debug("bad format in finished_seq_file (%s) with line \"%s\""%(finished_seq_file, line))
                        pass

                if cnt >= window:
                    break

        if cnt > 0:
            avg_newrun_time = sum_run_time/float(cnt)
        return avg_newrun_time


#}}}
def ValidateQuery(request, query, g_params):#{{{
    query['errinfo_br'] = ""
    query['errinfo_content'] = ""
    query['warninfo'] = ""

    has_pasted_seq = False
    has_upload_file = False
    if query['rawseq'].strip() != "":
        has_pasted_seq = True
    if query['seqfile'] != "":
        has_upload_file = True

    if has_pasted_seq and has_upload_file:
        query['errinfo_br'] += "Confused input!"
        query['errinfo_content'] = "You should input your query by either "\
                "paste the sequence in the text area or upload a file."
        return False
    elif not has_pasted_seq and not has_upload_file:
        query['errinfo_br'] += "No input!"
        query['errinfo_content'] = "You should input your query by either "\
                "paste the sequence in the text area or upload a file "
        return False
    elif query['seqfile'] != "":
        try:
            fp = request.FILES['seqfile']
            fp.seek(0,2)
            filesize = fp.tell()
            if filesize > g_params['MAXSIZE_UPLOAD_FILE_IN_BYTE']:
                query['errinfo_br'] += "Size of uploaded file exceeds limit!"
                query['errinfo_content'] += "The file you uploaded exceeds "\
                        "the upper limit %g Mb. Please split your file and "\
                        "upload again."%(g_params['MAXSIZE_UPLOAD_FILE_IN_MB'])
                return False

            fp.seek(0,0)
            content = fp.read()
        except KeyError:
            query['errinfo_br'] += ""
            query['errinfo_content'] += """
            Failed to read uploaded file \"%s\"
            """%(query['seqfile'])
            return False
        query['rawseq'] = content

    query['filtered_seq'] = ValidateSeq(query['rawseq'], query, g_params)
    is_valid = query['isValidSeq']
    return is_valid
#}}}
def ValidateSeq(rawseq, seqinfo, g_params):#{{{
# seq is the chunk of fasta file
# seqinfo is a dictionary
# return (filtered_seq)
    rawseq = re.sub(r'[^\x00-\x7f]',r' ',rawseq) # remove non-ASCII characters
    rawseq = re.sub(r'[\x0b]',r' ',rawseq) # filter invalid characters for XML
    filtered_seq = ""
    # initialization
    for item in ['errinfo_br', 'errinfo', 'errinfo_content', 'warninfo']:
        if item not in seqinfo:
            seqinfo[item] = ""

    seqinfo['isValidSeq'] = True

    seqRecordList = []
    myfunc.ReadFastaFromBuffer(rawseq, seqRecordList, True, 0, 0)
# filter empty sequences and any sequeces shorter than MIN_LEN_SEQ or longer
# than MAX_LEN_SEQ
    newSeqRecordList = []
    li_warn_info = []
    isHasEmptySeq = False
    isHasShortSeq = False
    isHasLongSeq = False
    isHasDNASeq = False
    cnt = 0
    for rd in seqRecordList:
        seq = rd[2].strip()
        seqid = rd[0].strip()
        if len(seq) == 0:
            isHasEmptySeq = 1
            msg = "Empty sequence %s (SeqNo. %d) is removed."%(seqid, cnt+1)
            li_warn_info.append(msg)
        elif len(seq) < g_params['MIN_LEN_SEQ']:
            isHasShortSeq = 1
            msg = "Sequence %s (SeqNo. %d) is removed since its length is < %d."%(seqid, cnt+1, g_params['MIN_LEN_SEQ'])
            li_warn_info.append(msg)
        elif len(seq) > g_params['MAX_LEN_SEQ']:
            isHasLongSeq = True
            msg = "Sequence %s (SeqNo. %d) is removed since its length is > %d."%(seqid, cnt+1, g_params['MAX_LEN_SEQ'])
            li_warn_info.append(msg)
        elif myfunc.IsDNASeq(seq):
            isHasDNASeq = True
            msg = "Sequence %s (SeqNo. %d) is removed since it looks like a DNA sequence."%(seqid, cnt+1)
            li_warn_info.append(msg)
        else:
            newSeqRecordList.append(rd)
        cnt += 1
    seqRecordList = newSeqRecordList

    numseq = len(seqRecordList)

    if numseq < 1:
        seqinfo['errinfo_br'] += "Number of input sequences is 0!\n"
        t_rawseq = rawseq.lstrip()
        if t_rawseq and t_rawseq[0] != '>':
            seqinfo['errinfo_content'] += "Bad input format. The FASTA format should have an annotation line start with '>'.\n"
        if len(li_warn_info) >0:
            seqinfo['errinfo_content'] += "\n".join(li_warn_info) + "\n"
        if not isHasShortSeq and not isHasEmptySeq and not isHasLongSeq and not isHasDNASeq:
            seqinfo['errinfo_content'] += "Please input your sequence in FASTA format.\n"

        seqinfo['isValidSeq'] = False
    else:
        li_badseq_info = []
        if 'isForceRun' in seqinfo and seqinfo['isForceRun'] and numseq > g_params['MAX_NUMSEQ_FOR_FORCE_RUN']:
            seqinfo['errinfo_br'] += "Invalid input!"
            seqinfo['errinfo_content'] += "You have chosen the \"Force Run\" mode. "\
                    "The maximum allowable number of sequences of a job is %d. "\
                    "However, your input has %d sequences."%(g_params['MAX_NUMSEQ_FOR_FORCE_RUN'], numseq)
            seqinfo['isValidSeq'] = False
        for i in xrange(numseq):
            seq = seqRecordList[i][2].strip()
            anno = seqRecordList[i][1].strip().replace('\t', ' ')
            seqid = seqRecordList[i][0].strip()
            seq = seq.upper()
            seq = re.sub("[\s\n\r\t]", '', seq)
            li1 = [m.start() for m in re.finditer("[^ABCDEFGHIKLMNPQRSTUVWYZX*-]", seq)]
            if len(li1) > 0:
                for j in xrange(len(li1)):
                    msg = "Bad letter for amino acid in sequence %s (SeqNo. %d) "\
                            "at position %d (letter: '%s')"%(seqid, i+1,
                                    li1[j]+1, seq[li1[j]])
                    li_badseq_info.append(msg)

        if len(li_badseq_info) > 0:
            seqinfo['errinfo_br'] += "There are bad letters for amino acids in your query!\n"
            seqinfo['errinfo_content'] = "\n".join(li_badseq_info) + "\n"
            seqinfo['isValidSeq'] = False

# out of these 26 letters in the alphabet, 
# B, Z -> X
# U -> C
# *, - will be deleted
# 
        li_newseq = []
        for i in xrange(numseq):
            seq = seqRecordList[i][2].strip()
            anno = seqRecordList[i][1].strip()
            seqid = seqRecordList[i][0].strip()
            seq = seq.upper()
            seq = re.sub("[\s\n\r\t]", '', seq)
            anno = anno.replace('\t', ' ') #replace tab by whitespace


            li1 = [m.start() for m in re.finditer("[BZ]", seq)]
            if len(li1) > 0:
                for j in xrange(len(li1)):
                    msg = "Amino acid in sequence %s (SeqNo. %d) at position %d "\
                            "(letter: '%s') has been replaced by 'X'"%(seqid,
                                    i+1, li1[j]+1, seq[li1[j]])
                    li_warn_info.append(msg)
                seq = re.sub("[BZ]", "X", seq)

            li1 = [m.start() for m in re.finditer("[U]", seq)]
            if len(li1) > 0:
                for j in xrange(len(li1)):
                    msg = "Amino acid in sequence %s (SeqNo. %d) at position %d "\
                            "(letter: '%s') has been replaced by 'C'"%(seqid,
                                    i+1, li1[j]+1, seq[li1[j]])
                    li_warn_info.append(msg)
                seq = re.sub("[U]", "C", seq)

            li1 = [m.start() for m in re.finditer("[*]", seq)]
            if len(li1) > 0:
                for j in xrange(len(li1)):
                    msg = "Translational stop in sequence %s (SeqNo. %d) at position %d "\
                            "(letter: '%s') has been deleted"%(seqid,
                                    i+1, li1[j]+1, seq[li1[j]])
                    li_warn_info.append(msg)
                seq = re.sub("[*]", "", seq)

            li1 = [m.start() for m in re.finditer("[-]", seq)]
            if len(li1) > 0:
                for j in xrange(len(li1)):
                    msg = "Gap in sequence %s (SeqNo. %d) at position %d "\
                            "(letter: '%s') has been deleted"%(seqid,
                                    i+1, li1[j]+1, seq[li1[j]])
                    li_warn_info.append(msg)
                seq = re.sub("[-]", "", seq)

            # check the sequence length again after potential removal of
            # translation stop
            if len(seq) < g_params['MIN_LEN_SEQ']:
                isHasShortSeq = 1
                msg = "Sequence %s (SeqNo. %d) is removed since its length is < %d (after removal of translation stop)."%(seqid, i+1, g_params['MIN_LEN_SEQ'])
                li_warn_info.append(msg)
            else:
                li_newseq.append(">%s\n%s"%(anno, seq))

        filtered_seq = "\n".join(li_newseq) # seq content after validation
        seqinfo['numseq'] = len(li_newseq)
        seqinfo['warninfo'] = "\n".join(li_warn_info) + "\n"

    seqinfo['errinfo'] = seqinfo['errinfo_br'] + seqinfo['errinfo_content']
    return filtered_seq
#}}}
def DeleteOldResult(path_result, path_log, gen_logfile, MAX_KEEP_DAYS=180):#{{{
    """
    Delete jobdirs that are finished > MAX_KEEP_DAYS
    """
    finishedjoblogfile = "%s/finished_job.log"%(path_log)
    finished_job_dict = myfunc.ReadFinishedJobLog(finishedjoblogfile)
    for jobid in finished_job_dict:
        li = finished_job_dict[jobid]
        try:
            finish_date_str = li[8]
        except IndexError:
            finish_date_str = ""
            pass
        if finish_date_str != "":
            isValidFinishDate = True
            try:
                finish_date = datetime.datetime.strptime(finish_date_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                isValidFinishDate = False

            if isValidFinishDate:
                current_time = datetime.datetime.now()
                timeDiff = current_time - finish_date
                if timeDiff.days > MAX_KEEP_DAYS:
                    rstdir = "%s/%s"%(path_result, jobid)
                    date_str = time.strftime("%Y-%m-%d %H:%M:%S")
                    msg = "\tjobid = %s finished %d days ago (>%d days), delete."%(jobid, timeDiff.days, MAX_KEEP_DAYS)
                    myfunc.WriteFile("[Date: %s] "%(date_str)+ msg + "\n", gen_logfile, "a", True)
                    shutil.rmtree(rstdir)
#}}}
