#!/usr/bin/python
# Filename: archive_logfile.py
# Description: archive logfile using gnu gzip
import os
import sys
import re
import gzip
progname =  os.path.basename(sys.argv[0])
wspace = ''.join([" "]*len(progname))

usage_short="""
Usage: %s FILE [FILE ...] [-maxsize STR]
"""%(progname)

usage_ext="""
Description:
    Archive (gzip) the logfile if its size is over maxsize

OPTIONS:
  -l LISTFILE   List of log files
  -maxsize STR  Set the threshold of the filesize, the logfile will be gzipped
                if its file size is >= maxsize, (default: 20M)
                e.g. 500k, 20M, 500000b, 5000, 1G
  -h, --help    Print this help message and exit

Created 2014-05-22, updated 2014-05-22, Nanjiang Shu
"""
usage_exp="""
Examples:
    %s /var/log/program.output.log
"""%(progname)

def PrintHelp(fpout=sys.stdout):#{{{
    print >> fpout, usage_short
    print >> fpout, usage_ext
    print >> fpout, usage_exp#}}}
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
        raise
#}}}
def Size_human2byte(s):#{{{
    if s.isdigit():
        return int(s)
    else:
        s = s.upper()
        match = re.match(r"([0-9]+)([A-Z]+)", s , re.I)
        if match:
            items = match.groups()
            size = int(items[0])
            if items[1] in ["B"]:
                return size
            elif items[1] in ["K", "KB"]:
                return size*1024
            elif items[1] in ["M", "MB"]:
                return size*1024*1024
            elif items[1] in ["G", "GB"]:
                return size*1024*1024*1024
            else:
                print >> sys.stderr, "Bad maxsize argument:",s
                return -1
        else:
            print >> sys.stderr, "Bad maxsize argument:",s
            return -1

#}}}
def ArchiveFile(filename, maxsize):#{{{
    """
    Archive the logfile if its size exceeds the limit
    """
    if not os.path.exists(filename):
        print >> sys.stderr, filename,  "does not exist. ignore."
        return 1
    else:
        filesize = os.path.getsize(filename)
        if filesize > maxsize:
            cnt = 0
            zipfile = ""
            while 1:
                cnt += 1
                zipfile = "%s.%d.gz"%(filename, cnt)
                if not os.path.exists(zipfile):
                    break
            # write zip file
            try:
                f_in = open(filename, 'rb')
            except IOError:
                print >> sys.stderr, "Failed to read %s"%(filename)
                return 1
            try:
                f_out = gzip.open(zipfile, 'wb')
            except IOError:
                print >> sys.stderr, "Failed to write to %s"%(zipfile)
                return 1

            f_out.writelines(f_in)
            f_out.close()
            f_in.close()
            print "%s is archived to %s"%(filename, zipfile)
            os.remove(filename)
        return 0
#}}}
def main(g_params):#{{{
    argv = sys.argv
    numArgv = len(argv)
    if numArgv < 2:
        PrintHelp()
        return 1

    fileList = []
    fileListFile = ""
    maxsize_str = ""

    i = 1
    isNonOptionArg=False
    while i < numArgv:
        if isNonOptionArg == True:
            fileList.append(argv[i])
            isNonOptionArg = False
            i += 1
        elif argv[i] == "--":
            isNonOptionArg = True
            i += 1
        elif argv[i][0] == "-":
            if argv[i] in ["-h", "--help"]:
                PrintHelp()
                return 1
            elif argv[i] in ["-maxsize", "--maxsize"]:
                (maxsize_str, i) = my_getopt_str(argv, i)
            elif argv[i] in ["-l", "--l"] :
                (fileListFile, i) = my_getopt_str(argv, i)
            elif argv[i] in ["-q", "--q"]:
                g_params['isQuiet'] = True
                i += 1
            else:
                print >> sys.stderr, "Error! Wrong argument:", argv[i]
                return 1
        else:
            fileList.append(argv[i])
            i += 1


    if maxsize_str != "":
        maxsize = Size_human2byte(maxsize_str)
        if maxsize > 0:
            g_params['maxsize'] = maxsize
        else:
            return 1
#     print "maxsize=", g_params['maxsize']

    if fileListFile != "":
        tmplist = open(fileListFile, "r").read().split('\n')
        tmplist = [x.strip() for x in tmplist]
        fileList += tmplist

    if len(fileList) < 1:
        print >> sys.stderr, "No input file is set. exit."
    for i in xrange(len(fileList)):
#         print "%d --> %s" %(i, fileList[i])
        ArchiveFile(fileList[i], g_params['maxsize'])
#}}}


def InitGlobalParameter():#{{{
    g_params = {}
    g_params['isQuiet'] = True
    g_params['maxsize'] = 20*1024*1024
    return g_params
#}}}
if __name__ == '__main__' :
    g_params = InitGlobalParameter()
    sys.exit(main(g_params))
