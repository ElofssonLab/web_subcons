#!/usr/bin/env python

import os
import sys
import myfunc


def OutputRecord(seqnumber, jobid, idx, path_result, fpout):
    resultfile = "%s/%s/%s/%s/query.result.txt"%(path_result, jobid, jobid, idx)
    if os.path.exists(resultfile):
        buff = myfunc.ReadFile(resultfile)
        lines = buff.split("\n")
        numline = len(lines)
        i = 0
        seq=""
        seqname=""
        seq=""
        seqlength = 0
        top_topcons = ""

        while i < numline:
            if lines[i].find("Sequence name:") == 0:
                seqname=lines[i].lstrip("Sequence name:").replace(":", " ")
                i += 1
            elif lines[i].find("Sequence:") == 0:
                seq = lines[i+1].strip()
                i += 2
            elif lines[i].find("TOPCONS predicted topology:") == 0:
                top_topcons = lines[i+1].strip()
                i += 2
            elif lines[i].find("Predicted Delta-G-values") == 0:
                break
            else:
                i += 1
        seqlength = len(seq)

        fpout.write("SeqNo: %d\n"%(seqnumber))
        fpout.write("SeqLength: %d\n"%(seqlength))
        fpout.write("SeqName: %s\n"%(seqname))
        fpout.write("Seq: %s\n"%(seq))
        fpout.write("TOPCONS2: %s\n"%(top_topcons))
        fpout.write("\n")

        return 0
    else:
        print >> sys.stderr, "resultfile %s does not exist"%(resultfile)
        return 1


usage="""
Usage: %s jobruntimeFile path_result [OUTFILE]
"""%(sys.argv[0])

try:
    jobruntimeFile = sys.argv[1]
except:
    print usage
    sys.exit(1)

try:
    path_result = sys.argv[2]
except:
    print usage
    sys.exit(1)

outfile=""
try:
    outfile = sys.argv[3]
except:
    pass

fpout = myfunc.myopen(outfile, sys.stdout, "w", False)



hdl = myfunc.ReadLineByBlock(jobruntimeFile)
if hdl.failure:
    sys.exit(1)

lines = hdl.readlines()
cnt=0
while lines != None:
    for line in lines:
        strs = line.split("\t")
        if len(strs) < 8:
            continue
        jobid = strs[0]
        idx = strs[1]
        cnt += 1
        OutputRecord(cnt, jobid, idx, path_result, fpout)
    lines = hdl.readlines()
hdl.close()

myfunc.myclose(fpout)


