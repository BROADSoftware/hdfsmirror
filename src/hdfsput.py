#!/usr/bin/env python

# Copyright (C) 2016 BROADSoftware
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
#limitations under the License.
#



import os
import logging.config
import lib.misc as misc
import yaml
import lib.WebHDFS as WebHDFS
import lib.buildTree as buildTree
import sys
import Queue    
from threading import Thread
import time
import lib.common as common
import atexit

logger = logging.getLogger("hdfsput.main")
tlogger = logging.getLogger("hdfsput.thread")


def applyAttrOnNewFile(webhdfs, path, p):
    if p.owner != None:
        webhdfs.setOwner(path,p.owner)
    if p.group != None:
        webhdfs.setGroup(path, p.group)
    if p.mode != None:
        webhdfs.setPermission(path, p.mode)


def applyAttrOnNewDirectory(webhdfs, path, p):
    if p.owner != None:
        webhdfs.setOwner(path, p.owner)
    if p.group != None:
        webhdfs.setGroup(path, p.group)
    # Mode is defined at creation

def adjustAttrOnExistingFile(webhdfs, filePath, fileStatus, p):
    if p.owner != None and p.owner != fileStatus['owner']:
        webhdfs.setOwner(filePath, p.owner)
    if p.group != None and p.group != fileStatus['group']:
        webhdfs.setGroup(filePath, p.group)
    if(p.mode != None and fileStatus['mode'] != p.mode):
        webhdfs.setPermission(filePath, p.mode)

def adjustAttrOnExistingDir(webhdfs, dirPath, dirStatus, p):
    if p.owner != None and p.owner != dirStatus['owner']:
        webhdfs.setOwner(dirPath, p.owner)
    if p.group != None and p.group != dirStatus['group']:
        webhdfs.setGroup(dirPath, p.group)
    if(p.directoryMode != None and p.directoryMode != dirStatus['mode']):
        webhdfs.setPermission(dirPath, p.directoryMode)


def checkAttrOnExistingFile(fileStatus, p):
    if p.owner != None and p.owner != fileStatus['owner']:
        return True
    if p.group != None and p.group != fileStatus['group']:
        return True
    if(p.mode != None and fileStatus['mode'] != p.mode):
        return True
    return False

def checkAttrOnExistingDir(dirStatus, p):
    if p.owner != None and p.owner != dirStatus['owner']:
        return True
    if p.group != None and p.group != dirStatus['group']:
        return True
    if(p.directoryMode != None and p.directoryMode != dirStatus['mode']):
        return True
    return False

def backupHdfsFile(webhdfs, path):
    #ext = time.strftime("%Y-%m-%d@%H:%M:%S", time.localtime(time.time()))
    ext = time.strftime("%Y-%m-%d_%H_%M_%S~", time.localtime(time.time()))
    backupdest = '%s.%s' % (path, ext)
    webhdfs.rename(path, backupdest)


class PutThread(Thread):
    def __init__(self, tid, queue, srcTree, destTree, webHDFS, p):
        Thread.__init__(self)
        self.tid = tid
        self.queue = queue
        self.srcTree = srcTree
        self.destTree = destTree
        self.webHDFS = webHDFS
        self.p = p
        self.fileCount = 0;
    
    def run(self):
        tlogger.debug("Thread#{0} started".format(self.tid))
        while True:
            try:
                f = self.queue.get_nowait()
            except Queue.Empty:
                tlogger.debug("Thread#{0} ended. {1} files handled".format(self.tid, self.fileCount))
                return
            srcPath = os.path.join(self.srcTree['rroot'], f)
            destPath = os.path.join(self.destTree['rroot'], f)
            if f in self.destTree['files'] and self.p.backup:
                backupHdfsFile(self.webHDFS, destPath)
            self.webHDFS.putFileToHdfs(srcPath, destPath, self.p.force)
            modTime = self.srcTree['files'][f]['modificationTime']
            self.webHDFS.setModificationTime(destPath, modTime)
            applyAttrOnNewFile(self.webHDFS, destPath, self.p)
            self.fileCount += 1
            
            
            
# To be sure we cancel kerberos delegation token, if any
webHDFS = None

def cleanup():
    if webHDFS != None:
        webHDFS.close()

atexit.register(cleanup)


def main():
    mydir =  os.path.dirname(os.path.realpath(__file__)) 
    p = common.parseArg(mydir)

    if not p.dest.startswith("/"):
        misc.ERROR("dest '{0}' is not absolute. Absolute path is required for HDFS!", p.path)
        
    logging.config.dictConfig(yaml.load(open(p.loggingConfFile)))
       
    global webHDFS
    webHDFS = WebHDFS.lookup(p)

    if not os.path.isdir(p.src):
        misc.ERROR("{0} must be an existing folder".format(p.src))
    srcTree = buildTree.buildLocalTree(p.src)
    
    (ft, _) = webHDFS.getPathTypeAndStatus(p.dest)
    if ft == "NOT_FOUND":
        misc.ERROR("Path {0} non existing on HDFS", p.dest)
    if ft == "FILE":
        misc.ERROR("HDFS path {0} is a file, not a directory", p.dest)
    elif ft == "NO_ACCESS":
        misc.ERROR("HDFS path {0}: No access", p.dest)
    elif ft != "DIRECTORY":
        misc.ERROR("HDFS path {0}: Unknown type: '{1}'", p.dest, ft)

    directoriesToCreate = []
    directoriesToAdjust = []
    filesToCreate = []
    filesToReplace = []
    filesToAdjust = []

    # If source does not end with '/', its basename will be added to target path. And directory created if not existing
    if not srcTree['slashTerminated']:
        x = os.path.basename(srcTree['rroot'])
        p.dest = os.path.join(p.dest, x)
        (ft, dirStatus) = webHDFS.getPathTypeAndStatus(p.dest)
        if ft == "NOT_FOUND":
            directoriesToCreate.append(p.dest)
            destTree = buildTree.buildEmptyTree(p.dest)
        elif ft == "DIRECTORY":
            destTree = buildTree.buildHdfsTree(webHDFS, p.dest)
            destTree['directories'][p.dest] = dirStatus  # Will need to to apply modification later on
            if checkAttrOnExistingDir(dirStatus, p):
                directoriesToAdjust.append(p.dest)
        else:
            misc.ERROR("HDFS path {0}: Invalid type: '{1}'", p.dest, ft)
    else:
        destTree = buildTree.buildHdfsTree(webHDFS, p.dest)
    

    if logger.getEffectiveLevel() <= logging.DEBUG:
        logger.debug("Source (local) files:\n" + misc.pprint2s(srcTree))
        logger.debug("Target (HDFS) files:\n" + misc.pprint2s(destTree))

    # Lookup all folder to create or adjust on target
    for dirName in srcTree['directories']:
        if dirName in destTree['directories']:
            if checkAttrOnExistingDir(destTree['directories'][dirName], p):
                directoriesToAdjust.append(dirName)
        else:
            dirPath = os.path.join(destTree['rroot'], dirName)
            directoriesToCreate.append(dirPath)

    directoriesToAdjust.sort()
    directoriesToCreate.sort()

    for fileName in srcTree['files']:
        if fileName in destTree['files']:
            srcFilesStatus = srcTree['files'][fileName]
            destFilesStatus = destTree['files'][fileName]
            if srcFilesStatus['size'] != destFilesStatus['size'] or srcFilesStatus['modificationTime'] != destFilesStatus['modificationTime']:
                filesToReplace.append(fileName)
            elif checkAttrOnExistingFile(destTree['files'][fileName], p):
                filesToAdjust.append(fileName)
        else:
            filesToCreate.append(fileName)

    if(p.report):
        print("{0} files in {1} directories present in local source".format(len(srcTree['files']), len(srcTree['directories'])))
        print("{0} files in {1} directories already present in HDFS target".format(len(destTree['files']), len(destTree['directories'])))

        print("{0} directories to be created on HDFS target".format(len(directoriesToCreate)))
        for f in directoriesToCreate:
            print "\t" + f

        print("{0} files to be created on HDFS target".format(len(filesToCreate)))
        if(p.reportFiles):
            for f in filesToCreate:
                print "\t" + os.path.join(destTree['rroot'], f)

        if p.force:
            print("{0} files to be replaced on HDFS target".format(len(filesToReplace)))
        else:
            print("{0} files differs from source in HDFS target (use --force [--backup] to overwrite)".format(len(filesToReplace)))
        if(p.reportFiles):
            for f in filesToReplace:
                print "\t" + os.path.join(destTree['rroot'], f)

        if p.forceExt:
            print("{0} files owner/group/mode will be changed on HDFS target".format(len(filesToAdjust)))
        else:
            print("{0} files owner/group/mode differs from source on HDFS target (use --forceExt to overwrite)".format(len(filesToAdjust)))
        if(p.reportFiles):
            for f in filesToAdjust:
                print "\t" + os.path.join(destTree['rroot'], f)

        if p.forceExt:
            print("{0} directories owner/group/mode will be changed on HDFS target".format(len(directoriesToAdjust)))
        else:
            print("{0} directories owner/group/mode differs from source on HDFS target (use --forceExt to overwrite)".format(len(directoriesToAdjust)))
        if(p.reportFiles):
            for f in directoriesToAdjust:
                print "\t" + os.path.join(destTree['rroot'], f)

    nbrOperations = len(directoriesToCreate) + len(filesToAdjust) + len(filesToCreate) + len(filesToReplace) + len(directoriesToAdjust)

    if not p.checkMode:
        for f in directoriesToCreate:
            webHDFS.createFolder(f, p.directoryMode)
            applyAttrOnNewDirectory(webHDFS, f, p)
        if p.forceExt:
            for f in directoriesToAdjust:
                dirPath = os.path.join(destTree['rroot'], f)
                dirStatus = destTree['directories'][f]
                adjustAttrOnExistingDir(webHDFS, dirPath, dirStatus, p)
            for f in filesToAdjust:
                filePath = os.path.join(destTree['rroot'], f)
                fileStatus = destTree['files'][f]
                adjustAttrOnExistingFile(webHDFS, filePath, fileStatus, p)
        if len(filesToCreate) > 0 or len(filesToReplace) > 0:
            queue = Queue.Queue()
            for f in filesToCreate:
                queue.put(f)
            if p.force:
                for f in filesToReplace:
                    queue.put(f)
            myThreads = []
            for i in range(0, p.nbrThreads):
                pt = PutThread(i, queue, srcTree, destTree, webHDFS, p)
                myThreads.append(pt)
                pt.start()
            st = common.StatsThread(queue, myThreads)
            myThreads.append(st)
            st.start()
            for t in myThreads:
                t.join()
            if not queue.empty():
                misc.ERROR("File Queue not empty while all threads ending!!")
        
    print("Operation count: {0}".format(nbrOperations))
    return 0

if __name__ == '__main__':
    sys.exit(main())

    
    