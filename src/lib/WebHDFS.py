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
from xml.dom import minidom
import misc
import requests



class WebHDFS:
    
    def __init__(self, endpoint, auth):
        if auth != "" and not auth.endswith("&"):
            auth = auth + "&"
        self.endpoint = endpoint
        self.auth = auth
        
         
    def test(self):
        url = "http://{0}/webhdfs/v1/?{1}op=GETFILESTATUS".format(self.endpoint, self.auth)
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                return (True, "")
            else: 
                return (False, "{0}  =>  Response code: {1}".format(url, resp.status_code))
        except Exception as e:
            return (False, "{0}  =>  Response code: {1}".format(url, e.strerror))
                
    class FileStatus:
        owner = None
        group  = None
        type = None
        permission = None
        def __str__(self):
            return "FileStatus => owner: '{0}', group: '{1}',  type:'{2}', permission:'{3}'".format(self.owner, self.group, self.type, self.permission)

    def getFileStatus(self, path):
        url = "http://{0}/webhdfs/v1{1}?{2}op=GETFILESTATUS".format(self.endpoint, path, self.auth)
        resp = requests.get(url)
        if resp.status_code == 200:
            #print content
            result = resp.json()
            fileStatus = WebHDFS.FileStatus()
            fileStatus.owner = result['FileStatus']['owner']
            fileStatus.group = result['FileStatus']['group']
            fileStatus.permission = result['FileStatus']['permission']
            fileStatus.type = result['FileStatus']['type']
            return fileStatus
        elif resp.status_code == 404:
            return None
        elif resp.status_code == 403:
            return None
        else:
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp.status_code, url))
                            
    def listDirContent(self, path):
        url = "http://{0}/webhdfs/v1{1}?{2}op=LISTSTATUS".format(self.endpoint, path, self.auth)
        print(url)
        resp = requests.get(url)
        if resp.status_code == 200:
            #print content
            result = resp.json()
            #misc.pprint(result)
            files = []
            directories = []
            for f in result['FileStatuses']['FileStatus']:
                if f['type'] == 'FILE':
                    files.append(f['pathSuffix'])
                elif f['type'] == 'DIRECTORY':
                    directories.append(f['pathSuffix'])
                else:
                    misc.ERROR("Unknown directory entry type: {0}".format(f['type']))
            return(directories, files)
        elif resp.status_code == 404:
            return None
        elif resp.status_code == 403:
            return None
        else:
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp.status_code, url))
        
                
def lookup(webHdfsEndpoint=None, hadoopConfDir=None, auth=None):          
    hadoopConfDir = hadoopConfDir if hadoopConfDir != None else "/etc/hadoop/conf"
    auth = auth if auth != None else "user.name=hdfs"
    if webHdfsEndpoint == None:
        if not os.path.isdir(hadoopConfDir):
            misc.ERROR("{0} must be an existing folder, or --hadoopConfDir  or --webHdfsEndpoint provided as parameter.".format(hadoopConfDir))
        candidates = []
        hspath = os.path.join(hadoopConfDir, "hdfs-site.xml")
        NN_HTTP_TOKEN1 = "dfs.namenode.http-address"
        NN_HTTP_TOKEN2 = "dfs.http.address"  # Deprecated
        if os.path.isfile(hspath):
            doc = minidom.parse(hspath)
            properties = doc.getElementsByTagName("property")
            for prop in properties :
                name = prop.getElementsByTagName("name")[0].childNodes[0].data
                if name.startswith(NN_HTTP_TOKEN1) or name.startswith(NN_HTTP_TOKEN2):
                    candidates.append(prop.getElementsByTagName("value")[0].childNodes[0].data)
            if not candidates:
                misc.ERROR("Unable to find {0}* or {1}* in {2}. Provide explicit 'webhdfs_endpoint'", NN_HTTP_TOKEN1, NN_HTTP_TOKEN2, hspath)
            errors = []
            for endpoint in candidates:
                webHDFS= WebHDFS(endpoint, auth)
                (x, err) = webHDFS.test()
                if x:
                    webHdfsEndpoint = webHDFS.endpoint
                    return webHDFS
                else:
                    errors.append("\n" + err)
            misc.ERROR("Unable to find a valid 'webhdfs_endpoint' in hdfs-site.xml:" + err)
        else:
            misc.ERROR("Unable to find file {0}. Provide 'webhdfs_endpoint' or 'hadoop_conf_dir' parameter", hspath)
    else:
        return WebHDFS(webHdfsEndpoint, auth)
    