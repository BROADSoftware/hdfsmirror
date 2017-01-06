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


import pprint
import StringIO
import errno
import os
import pwd
import grp
import sys


prettyPrinter = pprint.PrettyPrinter(indent=2)

def ppprint(obj):
    prettyPrinter.pprint(obj)


def pprint2s(obj):
    output = StringIO.StringIO()
    pp = pprint.PrettyPrinter(indent=2, stream=output)
    pp.pprint(obj)
    s = output.getvalue()
    output.close
    return s
                    

def ERROR(err, *args):
    if type(err) is str:
        message = "" + err.format(*args)
    else:
        message = err.__class__.__name__ + ": " + str(err)
    print "* * * * ERROR: " + str(message)
    #raise Exception("xx")
    sys.exit(1)



def ensureFolder(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
        else:
            pass
        

# Just a local cache, to optimize system calls
userNameFromUid = {}
def getUserNameFromId(uid):
    if uid in userNameFromUid:
        return userNameFromUid[uid]
    else:
        n = pwd.getpwuid(uid).pw_name
        userNameFromUid[uid] = n
        return n
    
    
# Just a local cache, to optimize system calls
groupNameFromGid = {}
def getGroupNameGroupId(gid):
    if gid in groupNameFromGid:
        return groupNameFromGid[gid]
    else:
        n = grp.getgrgid(gid).gr_name
        groupNameFromGid[gid] = n
        return n
    
    
# Just a local cache, to optimize system calls
uidFromName = {}
def getUidFromName(name):
    if name in uidFromName:
        return uidFromName[name]
    else:
        try:
            n = pwd.getpwnam(name).pw_uid
        except:
            ERROR("User '{0}' is not existing", name)
        uidFromName[name] = n
        return n
    
    
# Just a local cache, to optimize system calls
gidFromName = {}
def getGidFromName(name):
    if name in gidFromName:
        return gidFromName[name]
    else:
        try:
            n = grp.getgrnam(name).gr_gid
        except:
            ERROR("Group '{0}' is not existing", name)
        gidFromName[name] = n
        return n
    
    
