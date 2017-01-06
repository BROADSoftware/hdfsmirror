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
import argparse
import logging.config
import lib.misc as misc
import yaml
import lib.WebHDFS as WebHDFS
import lib.buildTree as buildTree
import sys
import Queue    
from threading import Thread
import time


logger = logging.getLogger("hdfsget.main")
tlogger = logging.getLogger("hdfsget.thread")


def applyAttrOnNewFile(path, p):
    owner = p.defaultOwner if p.owner is None else p.owner
    group = p.defaultGroup if p.group is None else p.group
    mode = p.defaultMode if p.mode is None else p.mode
    if owner != None:
        os.chown(path, misc.getUidFromName(owner), -1)
    if group != None:
        os.chown(path, -1, misc.getGidFromName(group))
    if mode != None:
        os.chmod(path, p.mode)


def applyAttrOnNewDirectory(path, p):
    owner = p.defaultOwner if p.owner is None else p.owner
    group = p.defaultGroup if p.group is None else p.group
    if owner != None:
        os.chown(path, misc.getUidFromName(owner), -1)
    if group != None:
        os.chown(path, -1, misc.getGidFromName(group))


def adjustAttrOnExistingFile(filePath, fileStatus, p):
    if p.owner != None and p.owner != fileStatus['owner']:
        os.chown(filePath, misc.getUidFromName(p.owner), -1)
    if p.group != None and p.group != fileStatus['group']:
        os.chown(filePath, -1, misc.getGidFromName(p.group))
    if(p.mode != None and fileStatus['mode'] != p.mode):
        os.chmod(filePath, p.mode)


def checkAttrOnExistingFile(fileStatus, p):
    if p.owner != None and p.owner != fileStatus['owner']:
        return True
    if p.group != None and p.group != fileStatus['group']:
        return True
    if(p.mode != None and fileStatus['mode'] != p.mode):
        return True
    return False

def backupLocalFile(webhdfs, path):
    #ext = time.strftime("%Y-%m-%d@%H:%M:%S", time.localtime(time.time()))
    ext = time.strftime("%Y-%m-%d_%H_%M_%S~", time.localtime(time.time()))
    backupdest = '%s.%s' % (path, ext)
    os.rename(path, backupdest)


class StatsThread(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        self.allFiles = queue.qsize()
    
    def run(self):
        while True:
            x = self.queue.qsize()
            print("hdfsmirror: {0}/{1} files copied".format(self.allFiles - x, self.allFiles))
            if x == 0:
                return
            time.sleep(2)

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
                backupLocalFile(self.webHDFS, destPath)
            self.webHDFS.getFileFromHdfs(srcPath, destPath)
            modTime = self.srcTree['files'][f]['modificationTime']
            os.utime(destPath, (time.time(), modTime))
            applyAttrOnNewFile(destPath, self.p)
            self.fileCount += 1

class Parameters:
    pass


def main():
    mydir =  os.path.dirname(os.path.realpath(__file__)) 
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', required=True)
    parser.add_argument('--dest', required=True)
    
    parser.add_argument('--checkMode', action='store_true')
    parser.add_argument('--report', action='store_true')
    parser.add_argument('--reportFiles', action='store_true')
    parser.add_argument('--nbrThreads', required=False)
    parser.add_argument('--yamlLoggingConf', help="Logging configuration as a yaml file")

    parser.add_argument('--backup', action='store_true')
    parser.add_argument('--defaultGroup', required=False, help="group for newly create files.")
    parser.add_argument('--defaultMode', required=False, help="mode for newly create files.")
    parser.add_argument('--defaultOwner', required=False, help="owner for newly create files.")
    parser.add_argument('--directoryMode', required=False)
    parser.add_argument('--force',  action='store_true')
    parser.add_argument('--group', required=False, help="group for all files.")
    parser.add_argument('--hadoopConfDir', required=False)
    parser.add_argument('--hdfsUser', required=False)
    parser.add_argument('--mode', required=False, help="mode for all files.")
    parser.add_argument('--owner', required=False, help="owner for all files.")
    parser.add_argument('--webhdfsEndpoint', required=False)

    params = parser.parse_args()
    
    p = Parameters()
    p.src = params.src
    p.dest = params.dest
    p.checkMode = params.checkMode
    p.report = params.report
    p.reportFiles = params.reportFiles
    p.nbrThreads = params.nbrThreads
    p.yamlLoggingConf = params.yamlLoggingConf
    
    p.backup = params.backup
    p.defaultGroup = params.defaultGroup
    p.defaultMode = params.defaultMode
    p.defaultOwner = params.defaultOwner
    p.directoryMode = params.directoryMode
    p.force = params.force
    p.group = params.group
    p.hadoopConfDir = params.hadoopConfDir
    p.hdfsUser = params.hdfsUser
    p.mode = params.mode
    p.owner = params.owner
    p.webhdfsEndpoint = params.webhdfsEndpoint
    
    
    loggingConfFile =  os.path.join(mydir, "./logging.yml")
    if  p.yamlLoggingConf != None:
        loggingConfFile = p.yamlLoggingConf
        if not os.path.isfile(loggingConfFile):
            misc.ERROR("'{0}' is not a readable file!".format(loggingConfFile))    

    if p.nbrThreads != None:
        nbrThreads = int(p.nbrThreads)
    else:
        nbrThreads = 1

    logging.config.dictConfig(yaml.load(open(loggingConfFile)))

    if p.reportFiles:
        p.report = True
    
       
    webHDFS = WebHDFS.lookup(p.webhdfsEndpoint, p.hadoopConfDir, None if p.hdfsUser == None else "user.name=" + p.hdfsUser)

    
    ft = webHDFS.getPathType(p.src)
    if ft == "NOT_FOUND":
        misc.ERROR("Path {0} non existing on HDFS", p.dest)
    if ft == "FILE":
        misc.ERROR("HDFS path {0} is a file, not a directory", p.dest)
    elif ft == "NO_ACCESS":
        misc.ERROR("HDFS path {0}: No access", p.dest)
    elif ft != "DIRECTORY":
        misc.ERROR("HDFS path {0}: Unknown type: '{1}'", p.dest, ft)

    srcTree = buildTree.buildHdfsTree(webHDFS, p.src)
    
    if not os.path.exists(p.dest):
        misc.ERROR("Path {0} non existing locally", p.dest)
    if not os.path.isdir(p.dest):
        misc.ERROR("Path {0} is not a directory", p.dest)
    
    directoriesToCreate = []
    filesToCreate = []
    filesToReplace = []
    filesToAdjust = []

    # If source does not end with '/', its basename will be added to target path. And directory created if not existing
    if not srcTree['slashTerminated']:
        x = os.path.basename(srcTree['rroot'])
        p.dest = os.path.join(p.dest, x)
        if not os.path.isdir(p.dest):
            directoriesToCreate.append(p.dest)
            destTree = buildTree.buildEmptyTree(p.dest)
        else:
            destTree = buildTree.buildLocalTree(p.dest)
    else:
        destTree = buildTree.buildLocalTree( p.dest)
    

    if logger.getEffectiveLevel() <= logging.DEBUG:
        logger.debug("Source (HDFS) files:\n" + misc.pprint2s(srcTree))
        logger.debug("Target (local) files:\n" + misc.pprint2s(destTree))
    
    # Create missing folder on target
    for dirName in srcTree['directories']:
        if dirName not in destTree['directories']:
            dirPath = os.path.join(destTree['rroot'], dirName)
            directoriesToCreate.append(dirPath)
               

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
        print("{0} directories to be created on local target".format(len(directoriesToCreate)))
        for f in directoriesToCreate:
            print "\t" + f
        print("{0} files to be created on local target".format(len(filesToCreate)))
        if(p.reportFiles):
            for f in filesToCreate:
                print "\t" + os.path.join(destTree['rroot'], f)
        if p.force:
            print("{0} files to be replaced on local target".format(len(filesToReplace)))
        else:
            print("{0} files differs from source in local target (use --force [--backup] to overwrite)".format(len(filesToReplace)))
        if(p.reportFiles):
            for f in filesToReplace:
                print "\t" + os.path.join(destTree['rroot'], f)
        print("{0} files will need chown or chmod on local target".format(len(filesToAdjust)))
        if(p.reportFiles):
            for f in filesToAdjust:
                print "\t" + os.path.join(destTree['rroot'], f)

    if not p.checkMode:
        for f in directoriesToCreate:
            if p.directoryMode != None:
                os.mkdir(f, int(p.directoryMode, 8))
            else:
                os.mkdir(f)
            applyAttrOnNewDirectory(f, p)
        for f in filesToAdjust:
            filePath = os.path.join(destTree['rroot'], f)
            fileStatus = destTree['files'][f]
            adjustAttrOnExistingFile(filePath, fileStatus, p)
        if len(filesToCreate) > 0 or len(filesToReplace) > 0:
            queue = Queue.Queue()
            for f in filesToCreate:
                queue.put(f)
            if p.force:
                for f in filesToReplace:
                    queue.put(f)
            myThreads = []
            st = StatsThread(queue)
            myThreads.append(st)
            st.start()
            for i in range(0,nbrThreads):
                pt = PutThread(i, queue, srcTree, destTree, webHDFS, p)
                myThreads.append(pt)
                pt.start()
            for t in myThreads:
                t.join()
            if not queue.empty():
                misc.ERROR("File Queue not empty while all threads ending!!")


if __name__ == '__main__':
    sys.exit(main())

    
    