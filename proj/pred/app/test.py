import os
import sys
import myfunc

rundir = os.path.dirname(__file__)
basedir = os.path.realpath("%s/../"%(rundir))

if 0:#{{{
    infile = sys.argv[1]
    li = myfunc.ReadIDList2(infile, 2, None)
    print li
#}}}
if 0:#{{{
   rawseq = ">1\nseqAAAAAAAAAAAAAAAAAAAAAAAAA\n    \n>2  dad\ndfasdf  "
   #rawseq = "  >1\nskdfaskldgasdk\nf\ndadfa\n\n\nadsad   \n"
   #rawseq = ">sadjfasdkjfsalkdfsadfjasdfk"
   rawseq = "asdkfjasdg asdkfasdf\n"
   seqRecordList = []
   myfunc.ReadFastaFromBuffer(rawseq, seqRecordList, True, 0, 0)

   print seqRecordList
#}}}

if 0:#{{{
    size = float(sys.argv[1])
    print "size=",size
    print "humansize=", myfunc.Size_byte2human(size)#}}}

if 1:
    newsfile = "%s/static/doc/news.txt"%(basedir)
    newsList = myfunc.ReadNews(newsfile)
    print newsList

