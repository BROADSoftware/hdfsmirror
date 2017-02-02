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
import argparse
from threading import Thread
import time



class Parameters:
    pass

def parseArg(mydir):
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', required=True)
    parser.add_argument('--dest', required=True)
    
    parser.add_argument('--checkMode', action='store_true')
    parser.add_argument('--report', action='store_true')
    parser.add_argument('--reportFiles', action='store_true')
    parser.add_argument('--nbrThreads', required=False)
    parser.add_argument('--yamlLoggingConf', help="Logging configuration as a yaml file")

    parser.add_argument('--force',  action='store_true')
    parser.add_argument('--backup', action='store_true')

    parser.add_argument('--owner', required=False, help="owner for all files.")
    parser.add_argument('--group', required=False, help="group for all files.")
    parser.add_argument('--mode', required=False, help="mode for all files.")

    parser.add_argument('--directoryMode', required=False)
    
    parser.add_argument('--forceExt',  action='store_true')
    
    parser.add_argument('--hdfsUser', required=False, default="hdfs", help="Default: 'hdfs'. Set to 'KERBEROS' to use Kerberos authentication")
    parser.add_argument('--hadoopConfDir', required=False, default="/etc/hadoop/conf")
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
    
    p.force = params.force
    p.backup = params.backup
    p.owner = params.owner
    p.group = params.group
    p.mode = params.mode
    p.directoryMode = params.directoryMode
    p.forceExt = params.forceExt
    p.hdfsUser = params.hdfsUser
    p.hadoopConfDir = params.hadoopConfDir
    p.webhdfsEndpoint = params.webhdfsEndpoint
    
    
    p.loggingConfFile =  os.path.join(mydir, "./logging.yml")
    if  p.yamlLoggingConf != None:
        p.loggingConfFile = p.yamlLoggingConf
        if not os.path.isfile(p.loggingConfFile):
            misc.ERROR("'{0}' is not a readable file!".format(p.loggingConfFile))    

    if p.nbrThreads != None:
        p.nbrThreads = int(p.nbrThreads)
    else:
        p.nbrThreads = 1
        
    if p.reportFiles:
        p.report = True

    # Some checks    
    if p.mode != None:
        if not isinstance(p.mode, int):
            try:
                p.mode = int(p.mode, 8)
            except Exception:
                misc.ERROR("mode must be in octal form")
        p.mode = oct(p.mode)
        #print '{ mode_type: "' + str(type(p.mode)) + '",  mode_value: "' + str(p.mode) + '"}'
    
    return p



class StatsThread(Thread):
    def __init__(self, queue, myThreads):
        Thread.__init__(self)
        self.queue = queue
        self.myThreads = myThreads
        self.allFiles = queue.qsize()
    
    def run(self):
        while True:
            x = self.queue.qsize()
            print("hdfsmirror: {0}/{1} new files copied".format(self.allFiles - x, self.allFiles))
            if x == 0 or  len(self.myThreads) <= 1: # If 1, this is myself. So, exit. (This is in error case, where all threads died while queue is not empty)
                return
            time.sleep(2)
        
