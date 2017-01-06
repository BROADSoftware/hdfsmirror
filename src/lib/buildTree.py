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
import misc

"""
This functions set walk in directories and return a Dict() like:

{ 
    'directories': { 
        'subtree': { 
            'group': 'staff',
            'mode': '0755',
            'owner': 'sa'
        },
        'subtree/subsubtree': { 
            'group': 'staff',
            'mode': '0755',
            'owner': 'sa'
        }
    },
    'files': { 
        'file1.txt': { 
            'group': 'staff',
            'mode': '0644',
            'modificationTime': 1480983919,
            'owner': 'sa',
            'size': 12
        },
        'subtree/subfile1.txt': { 
            'group': 'staff',
            'mode': '0644',
            'modificationTime': 1480983919,
            'owner': 'sa',
            'size': 14
        },
        'subtree/subsubtree/subsubfile1.txt': { 
            'group': 'staff',
            'mode': '0664',
            'modificationTime': 1483639831,
            'owner': 'sa',
            'size': 20
        }
    },
    'rroot': './files/tree',
    'slashTerminated': False
}

This for the following layout

    ls -lR
    total 8
    -rw-r--r--  1 sa  staff   12 Dec  6 01:25 file1.txt
    drwxr-xr-x  5 sa  staff  170 Jan  5 12:19 subtree
    
    ./subtree:
    total 16
    -rw-r--r--  1 sa  staff   14 Dec  6 01:25 subfile1.txt
    drwxr-xr-x  3 sa  staff  102 Jan  5 12:19 subsubtree
    
    ./subtree/subsubtree:
    total 8
    -rw-rw-r--  1 sa  staff  20 Jan  5 19:10 subsubfile1.txt

Works same way for local (Linux) FS and HDFS

"""


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
    dirMap = {}
    for root, dirs, files in os.walk(rroot, topdown=True, onerror=None, followlinks=False):
        for fileName in files:
            key = os.path.join(root, fileName)[prefLen:]
            path = os.path.join(rroot, key)
            stat = os.stat(path)
            f = {}
            f['size'] = stat.st_size
            f['modificationTime'] = int(stat.st_mtime)
            f['mode'] = "0" + oct(stat.st_mode)[-3:]
            f['owner'] = misc.getUserNameFromId(stat.st_uid)
            f['group'] = misc.getGroupNameGroupId(stat.st_gid)
            fileMap[key] = f
        for dirName in dirs:
            key = os.path.join(root, dirName)[prefLen:]
            path = os.path.join(rroot, key)
            stat = os.stat(path)
            f = {}
            f['mode'] = "0" + oct(stat.st_mode)[-3:]
            f['owner'] = misc.getUserNameFromId(stat.st_uid)
            f['group'] = misc.getGroupNameGroupId(stat.st_gid)
            dirMap[key] = f
    tree['files'] = fileMap
    tree['directories'] = dirMap
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
    dirMap = {}
    noAccess = []
    walkInHdfs(webHdfs, rroot, dirMap, fileMap, noAccess, prefLen)
    tree['files'] = fileMap
    tree['directories'] = dirMap
    tree['noAccess'] = noAccess
    return tree

def walkInHdfs(webHdfs, current, dirMap, fileMap, noAccess, prefLen):
    dirContent = webHdfs.getDirContent(current)
    #print misc.pprint2s(dirContent)
    if dirContent['status'] == "OK":
        for f in dirContent['files']:
            path = os.path.join(current, f['name'])[prefLen:]
            del f['name']
            fileMap[path] = f
        for d in dirContent['directories']:
            #print misc.pprint2s(d)
            path = os.path.join(current, d['name'])
            del d['name']
            dirMap[path[prefLen:]] = d
            walkInHdfs(webHdfs, path, dirMap, fileMap, noAccess, prefLen)
    elif dirContent['status'] == "NO_ACCESS":
        noAccess.append(current)
    else:
        raise Exception("Invalid DirContent status: {0} for path:'{1}'".format(dirContent.status, current)) 

def buildEmptyTree(rroot):
        tree = {}
        tree['files'] = {}
        tree['directories'] = {}
        tree['noAccess'] = []
        if rroot == "/":
            tree['slashTerminated'] = False
        else:
            if rroot.endswith("/"):
                rroot = rroot[:-1]
                tree['slashTerminated'] = True
            else :
                tree['slashTerminated'] = False
        tree['rroot'] = rroot
        return tree       

