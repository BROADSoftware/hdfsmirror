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
    fileList = []
    for root, _, files in os.walk(rroot, topdown=True, onerror=None, followlinks=False):
        for fileName in files:
            f = os.path.join(root, fileName)
            f = f[prefLen:]
            fileList.append(f)
    fileList.sort()
    tree['files'] = fileList
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
    fileList = []
    walkInHdfs(webHdfs, rroot, fileList, prefLen)
    fileList.sort()
    tree['files'] = fileList
    return tree

def walkInHdfs(webHdfs, current, fileList, prefLen):
    (dirs, files) = webHdfs.listDirContent(current)
    for f in files:
        fileList.append(os.path.join(current, f)[prefLen:])
    for d in dirs:
        walkInHdfs(webHdfs, os.path.join(current, d), fileList, prefLen)


def main():
    #mydir =  os.path.dirname(os.path.realpath(__file__)) 
    parser = argparse.ArgumentParser()
    parser.add_argument('--local', required=True)
    parser.add_argument('--hdfs', required=True)
    parser.add_argument('--webHdfsEndpoint', required=False)
    parser.add_argument('--hadoopConfDir', required=False)
    parser.add_argument('--hdfsUser', required=False)
    parser.add_argument('--put', action='store_true')
    parser.add_argument('--get', action='store_true')
    parser.add_argument('--diff', action='store_true')

    param = parser.parse_args()
    if param.put and param.get:
        misc.ERROR("Only one of --put or --get must be set")
    if not param.put and not param.get and not param.diff:
        misc.ERROR("One of --put or --get must be set")

    localPath = param.local
    hdfsPath = param.hdfs
    
    webHdfs = WebHDFS.lookup(param.webHdfsEndpoint, param.hadoopConfDir, None if param.hdfsUser == None else "user.name=" + param.hdfsUser)

    if not os.path.isdir(localPath):
        misc.ERROR("{0} must be an existing folder".format(localPath))
    localFiles = buildLocalTree(localPath)
    fs = webHdfs.getFileStatus(hdfsPath)
    if fs == None:
        misc.ERROR("Path {0} non existing on HDFS", hdfsPath)
    if fs.type != "DIRECTORY":
        misc.ERROR("HDFS path {0} is not a directory", hdfsPath)
    hdfsFiles = buildHdfsTree(webHdfs, hdfsPath)

    misc.pprint(localFiles)
    misc.pprint(hdfsFiles)
    

if __name__ == '__main__':
    sys.exit(main())
