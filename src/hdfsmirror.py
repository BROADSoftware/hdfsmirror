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



import sys
import os
import argparse
import lib.misc as misc
import lib.WebHDFS as WebHDFS
import yaml
import logging.config


logger = logging.getLogger("hdfsmirror.main")



def buildLocalTree(rroot):
    tree = {}
    if rroot == "/":
        tree['slashTerminated'] = False
        prefLen = len(rroot) 
    else:
        if rroot.endswith("/"):
            rroot = rroot[:-1]
            tree['slashTerminated'] = True
        else :
            tree['slashTerminated'] = False
        prefLen = len(rroot) + 1
    tree['rroot'] = rroot
    fileMap = {}
    for root, _, files in os.walk(rroot, topdown=True, onerror=None, followlinks=False):
        for fileName in files:
            key = os.path.join(root, fileName)[prefLen:]
            path = os.path.join(rroot, key)
            stat = os.stat(path)
            f = {}
            f['size'] = stat.st_size
            f['modificationTime'] = int(stat.st_mtime)
            fileMap[key] = f
    tree['files'] = fileMap
    return tree
    
def buildHdfsTree(webHdfs, rroot):
    tree = {}
    if rroot == "/":
        tree['slashTerminated'] = False
        prefLen = len(rroot) 
    else:
        if rroot.endswith("/"):
            rroot = rroot[:-1]
            tree['slashTerminated'] = True
        else :
            tree['slashTerminated'] = False
        prefLen = len(rroot) + 1
    tree['rroot'] = rroot
    fileMap = {}
    noAccess = []
    walkInHdfs(webHdfs, rroot, fileMap, noAccess, prefLen)
    tree['files'] = fileMap
    tree['noAccess'] = noAccess
    return tree

def walkInHdfs(webHdfs, current, fileMap, noAccess, prefLen):
    dirContent = webHdfs.getDirContent(current)
    if dirContent['status'] == "OK":
        for f in dirContent['files']:
            path = os.path.join(current, f['name'])[prefLen:]
            del f['name']
            fileMap[path] = f
        for d in dirContent['directories']:
            walkInHdfs(webHdfs, os.path.join(current, d), fileMap, noAccess, prefLen)
    elif dirContent['status'] == "NO_ACCESS":
        noAccess.append(current)
    else:
        raise Exception("Invalid DirContent status: {0} for path:'{1}'".format(dirContent.status, current)) 
    


def main():
    mydir =  os.path.dirname(os.path.realpath(__file__)) 
    parser = argparse.ArgumentParser()
    parser.add_argument('--local', required=True)
    parser.add_argument('--hdfs', required=True)
    parser.add_argument('--webHdfsEndpoint', required=False)
    parser.add_argument('--hadoopConfDir', required=False)
    parser.add_argument('--hdfsUser', required=False)
    parser.add_argument('--put', action='store_true')
    parser.add_argument('--get', action='store_true')
    parser.add_argument('--report', action='store_true')
    parser.add_argument('--yamlLoggingConf', help="Logging configuration as a yaml file")

    param = parser.parse_args()
    
    
    loggingConfFile =  os.path.join(mydir, "./logging.yml")
    if  param.yamlLoggingConf != None:
        loggingConfFile = param.yamlLoggingConf
        if not os.path.isfile(loggingConfFile):
            misc.ERROR("'{0}' is not a readable file!".format(loggingConfFile))    

    logging.config.dictConfig(yaml.load(open(loggingConfFile)))
    
    if param.put and param.get:
        misc.ERROR("Only one of --put or --get must be set")
    if not param.put and not param.get and not param.report:
        misc.ERROR("One of --put, --get or --report must be set")

    localPath = param.local
    hdfsPath = param.hdfs
    
    webHDFS = WebHDFS.lookup(param.webHdfsEndpoint, param.hadoopConfDir, None if param.hdfsUser == None else "user.name=" + param.hdfsUser)

    if not os.path.isdir(localPath):
        misc.ERROR("{0} must be an existing folder".format(localPath))
    localFiles = buildLocalTree(localPath)
    
    ft = webHDFS.getPathType(hdfsPath)
    if ft == "NOT_FOUND":
        misc.ERROR("Path {0} non existing on HDFS", hdfsPath)
    if ft == "FILE":
        misc.ERROR("HDFS path {0} is a file, not a directory", hdfsPath)
    elif ft == "NO_ACCESS":
        misc.ERROR("HDFS path {0}: No access", hdfsPath)
    elif ft != "DIRECTORY":
        misc.ERROR("HDFS path {0}: Unknown type: '{1}'", hdfsPath, ft)
        
    hdfsFiles = buildHdfsTree(webHDFS, hdfsPath)

    if logger.getEffectiveLevel() <= logging.DEBUG:
        logger.debug("Local files:\n" + misc.pprint2s(localFiles))
        logger.debug("HDFS files:\n" + misc.pprint2s(hdfsFiles))
    
    inLocalOnly = []
    inHdfsOnly = []
    sizeDiff = []
    timeDiff = []
    noDiff = []
    
    localFileMap = localFiles['files']
    hdfsFileMap = hdfsFiles['files']
    
    for f in localFileMap:
        if f in hdfsFileMap:
            if localFileMap[f]['size'] != hdfsFileMap[f]['size']:
                sizeDiff.append(f)
            elif  localFileMap[f]['modificationTime'] != hdfsFileMap[f]['modificationTime']:
                timeDiff.append(f)
            else:
                noDiff.append(f)
        else:
            inLocalOnly.append(f)

    for f in hdfsFileMap:
        if not f in localFileMap:
            inHdfsOnly.append(f)
            
    if param.report:
        print("On local only (Will be pushed if --put is set):\n" + misc.pprint2s(inLocalOnly))
        print("On HDFS only (Will be pulled if --get is set):\n" + misc.pprint2s(inHdfsOnly))
        print("Size diff:\n" + misc.pprint2s(sizeDiff))
        print("Time diff:\n" + misc.pprint2s(timeDiff))
        print("No diff:\n" + misc.pprint2s(noDiff))

    if param.put:
        for f in inLocalOnly:
            fileSize = localFiles['files'][f]['size']
            localPath = os.path.join(localFiles['rroot'], f)
            hdfsPath = os.path.join(hdfsFiles['rroot'], f)
            webHDFS.putFileToHdfs(localPath, hdfsPath, fileSize)
            modTime = localFiles['files'][f]['modificationTime']
            webHDFS.setModificationTime(hdfsPath, modTime)
    

if __name__ == '__main__':
    sys.exit(main())
