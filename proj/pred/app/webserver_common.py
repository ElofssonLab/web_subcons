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
def WriteSubconsTextResultFile(outfile, outpath_result, maplist,#{{{
        runtime_in_sec, base_www_url, statfile=""):
    try:
        methodlist = ['SubCons', 'LocTree2', 'SherLoc2', 'MultiLoc2', 'Cello']
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

            for i in xrange(len(methodlist)):
                method = methodlist[i]
                rstfile = ""
                if method == "SubCons":
                    rstfile = "%s/%s/%s/query_0.subcons-final-pred.csv"%(outpath_result, subfoldername, "final-prediction")
                else:
                    rstfile = "%s/%s/%s/query_0.%s.csv"%(outpath_result, subfoldername, "for-dat", method.lower())

                if os.path.exists(rstfile):
                    content = myfunc.ReadFile(rstfile).strip()
                    lines = content.split("\n")
                    if len(lines) >= 2:
                        strs1 = lines[0].split("\t")
                        strs2 = lines[1].split("\t")
                        if strs1[0].strip() == "":
                            strs1[0] = "id_protein"
                        if len(strs1) < len(strs2):
                            strs1.insert(0, "id_protein")
                        if strs2[0].strip() == "query_0":
                            strs2[0] = seqid

                        strs1 = [x.strip() for x in strs1]
                        strs2 = [x.strip() for x in strs2]
                        content = tabulate.tabulate(strs2, strs1, 'plain')
                else:
                    content = ""
                if content == "":
                    content = "***No prediction could be produced with this method***"

                print >> fpout, "%s prediction:\n%s\n\n"%(method, content)

            print >> fpout, "##############################################################################"
            cnt += 1

    except IOError:
        print "Failed to write to file %s"%(outfile)
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
