#!/usr/bin/env python

# Description: moving cache data from within each job folder to a common place
#              located under static/result/cache

import os
import sys
import shutil
import subprocess
import platform
import pwd
import grp
import myfunc
import hashlib

usage="""
usage: %s MODE

MODE is md5, result, all

    md5: following static/md5 folder
    result: following static/result folder
    all: do both
"""%(sys.argv[0])

if len(sys.argv) < 2:
    print usage
    sys.exit(1)

mode = sys.argv[1]

print "Running mode = ",mode


basedir = "/var/www/html/topcons2/proj/pred/"
basedir = os.path.realpath(basedir)
path_md5   = "%s/static/md5"%(basedir)
path_result      = "%s/static/result"%(basedir)
path_cache = "%s/static/result/cache"%(basedir)

platform = platform.platform()

apacheusername = ""
if platform.lower().find("ubuntu") != -1:
    apacheusername = "www-data"
elif platform.lower().find("centos") != -1:
    apacheusername = "apache"

isChangeOwner = False
if apacheusername != "":
    try:
        apacheusername_uid = pwd.getpwnam(apacheusername).pw_uid
        apacheusername_gid = grp.getgrnam(apacheusername).gr_gid
        isChangeOwner = True
    except:
        pass

VERBOSE=1

def MoveCache_mode_md5(md5_key, sub_md5_name):#{{{
    sub_md5dir = "%s/%s"%(path_md5, sub_md5_name)
    md5_link = "%s/%s"%(sub_md5dir, md5_key)
    realpath = os.path.realpath(md5_link)
    basename_realpath = os.path.basename(realpath)
    dirname_realpath = os.path.dirname(realpath)
    cachedir = "%s/%s/%s"%(path_cache, sub_md5_name, md5_key)
    sub_cachedir = "%s/%s"%(path_cache, sub_md5_name)

    if os.path.lexists(md5_link) and not os.path.exists(md5_link): # remove the broken symbolic link
        try:
            os.unlink(md5_link)
        except:
            pass
    else:
        if not os.path.exists(sub_cachedir):
            os.makedirs(sub_cachedir)
            if isChangeOwner:
                os.chown(sub_cachedir, apacheusername_uid, apacheusername_gid)
        if not os.path.exists(cachedir):
            cmd = ["mv","-f", realpath, cachedir]
            cmdline = " ".join(cmd)
            isMoveSuccess = False
            try:
                subprocess.check_call(cmd)
                isMoveSuccess = True
            except subprocess.CalledProcessError,e:
                print e
            if VERBOSE>0:
                print cmdline

            if isMoveSuccess:
                # then change the symbolic link
                rela_path = os.path.relpath(cachedir, dirname_realpath)
                try:
                    os.chdir(dirname_realpath)
                    os.symlink(rela_path,  basename_realpath)
                    if isChangeOwner:
                        os.lchown(realpath, apacheusername_uid, apacheusername_gid)
                except:
                    pass
                if VERBOSE > 0:
                    print dirname_realpath, "os.symlink(", rela_path, ",", basename_realpath,")"

                # then change the symbolic link of md5_link to cachedir
                if os.path.lexists(md5_link):
                    try:
                        os.unlink(md5_link)
                    except:
                        pass
                if not os.path.lexists(md5_link):
                    rela_path = os.path.relpath(cachedir, sub_md5dir)
                    try:
                        os.chdir(sub_md5dir)
                        os.symlink(rela_path, md5_key)
                        if isChangeOwner:
                            os.lchown(md5_link, apacheusername_uid, apacheusername_gid)
                    except:
                        pass
                if VERBOSE > 0:
                    print sub_md5dir, "os.symlink(", rela_path, ",", md5_key,")"

#}}}
def MoveCache_mode_result(outpath_this_seq):#{{{
    subfoldername_this_seq = os.path.basename(outpath_this_seq)
    outpath_this_seq = os.path.realpath(outpath_this_seq)
    outpath_result = os.path.dirname(outpath_this_seq)
    fafile = "%s/seq.fa"%(outpath_this_seq)
    if os.path.exists(fafile):
        (seqid, seqanno,seq) = myfunc.ReadSingleFasta(fafile)
        md5_key = hashlib.md5(seq).hexdigest()
        sub_md5_name = md5_key[:2]
        sub_cachedir = "%s/%s"%(path_cache, sub_md5_name)
        cachedir = "%s/%s/%s"%(path_cache, sub_md5_name, md5_key)
        if not os.path.exists(sub_cachedir):
            os.makedirs(sub_cachedir)
            if isChangeOwner:
                os.chown(sub_cachedir, apacheusername_uid, apacheusername_gid)
        if not os.path.exists(cachedir):
            cmd = ["mv","-f", outpath_this_seq, cachedir]
            cmdline = " ".join(cmd)
            try:
                subprocess.check_call(cmd)
            except CalledProcessError,e:
                print e
                pass
            if VERBOSE>0:
                print cmdline
        else:
            print "cachedir %s already exists for %s"%(cachedir, outpath_this_seq)
            cmd = ["rm","-rf", outpath_this_seq]
            cmdline = " ".join(cmd)
            try:
                subprocess.check_call(cmd)
            except CalledProcessError,e:
                print e
                pass
            if VERBOSE>0:
                print cmdline

        # create symbolic link to the cache
        if not os.path.exists(outpath_this_seq) and os.path.exists(cachedir):
            rela_path = os.path.relpath(cachedir, outpath_result) #relative path
            try:
                os.chdir(outpath_result)
                os.symlink(rela_path,  subfoldername_this_seq)
                if isChangeOwner:
                    os.lchown(subfoldername_this_seq, apacheusername_uid, apacheusername_gid)
            except:
                pass
            if VERBOSE > 0:
                print outpath_result, "os.symlink(", rela_path, ",", subfoldername_this_seq,")"
    else:
        print "fafile %s does not exist"%(fafile)
#}}}


if mode.lower() in ["result", "all"]:
    dirlist = os.listdir(path_result)
    for jobdir in dirlist:
        if jobdir.find("rst") != 0:
            continue
        resultdir = "%s/%s"%(path_result, jobdir)
        outpath_result = "%s/%s"%(resultdir, jobdir)
        #if jobdir not in [ "rst_yybgZR", "rst_Ru2NPJ", "rst_0BqrZs"]:
#         if jobdir != "rst_fgWqQP":
#             continue

        if not os.path.exists(outpath_result):
            continue
        for seqdir in os.listdir(outpath_result):
            outpath_this_seq = "%s/%s"%(outpath_result, seqdir)
            if (seqdir.find("seq_") == 0 
                    and (not os.path.islink(outpath_this_seq)) 
                    and os.path.isdir(outpath_this_seq)):
                MoveCache_mode_result(outpath_this_seq)

if mode.lower() in ["md5", "all"]:
    dirlist = os.listdir(path_md5)
    for sub_md5_name in dirlist:
        sub_md5dir = "%s/%s"%(path_md5, sub_md5_name)
        #if sub_md5_name == "01":
        for md5_key in os.listdir(sub_md5dir):
            md5_link = "%s/%s"%(sub_md5dir, md5_key)
            if os.path.islink(md5_link):
                MoveCache_mode_md5(md5_key, sub_md5_name)
