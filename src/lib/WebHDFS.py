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
import logging

logger = logging.getLogger("hdfsmirror.WebHDFS")


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
                

    def getPathType(self, path):
        url = "http://{0}/webhdfs/v1{1}?{2}op=GETFILESTATUS".format(self.endpoint, path, self.auth)
        logger.debug(url)
        resp = requests.get(url)
        if resp.status_code == 200:
            result = resp.json()
            return result['FileStatus']['type']
        elif resp.status_code == 404:
            return "NOT_FOUND"
        elif resp.status_code == 403:
            return "NO_ACCESS"
        else:
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp.status_code, url))
            
                            
    def getDirContent(self, path):
        url = "http://{0}/webhdfs/v1{1}?{2}op=LISTSTATUS".format(self.endpoint, path, self.auth)
        logger.debug(url)
        resp = requests.get(url)
        dirContent = {}
        dirContent['status'] = "OK"
        dirContent['files'] = []
        dirContent['directories'] = []
        if resp.status_code == 200:
            result = resp.json()
            for f in result['FileStatuses']['FileStatus']:
                if f['type'] == 'FILE':
                    fi = {}
                    fi['name'] = f['pathSuffix']
                    fi['size'] = f['length']
                    fi['modificationTime'] = f['modificationTime']/1000
                    dirContent['files'].append(fi)
                elif f['type'] == 'DIRECTORY':
                    dirContent['directories'].append(f['pathSuffix'])
                else:
                    misc.ERROR("Unknown directory entry type: {0}".format(f['type']))
        elif resp.status_code == 404:
            dirContent['status'] = "NOT_FOUND"
        elif resp.status_code == 403:
            dirContent['status'] = "NO_ACCESS"
        else:
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp.status_code, url))
        return dirContent
        
                
    CHUNK_SIZE = 1024 * 1024
                     
    def putFileToHdfs(self, localPath, hdfsPath, fileSize=None):
        logger.debug("putFileToHdfs(localPath={0}, hdfsPath={1}, fileSize={2})".format(localPath, hdfsPath, fileSize))
        if(fileSize == None):   # Compute size if not provided
            stat = os.stat(localPath)
            fileSize = stat.st_size
            logger.debug("Lookup file size for '{0}': {1}".format(localPath, fileSize))
        url = "http://{0}/webhdfs/v1{1}?{2}op=CREATE".format(self.endpoint, hdfsPath, self.auth)
        logger.debug(url)
        resp = requests.put(url, allow_redirects=False)
        if not resp.status_code == 307:
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp.status_code, url))
        url2 = resp.headers['location']    
        logger.debug(url2)
        if fileSize < WebHDFS.CHUNK_SIZE:
            f = open(localPath, "r")
            fileData = f.read(WebHDFS.CHUNK_SIZE)
            resp2 = requests.put(url2, data=fileData, headers={'content-type': 'application/octet-stream'})
            if not resp2.status_code == 201:
                misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp2.status_code, url2))
        else:
            misc.ERROR("putFileToHdfs() chunk mode not yet implemented ")
           
           
    def setModificationTime(self, hdfsPath, modTime):
        url = "http://{0}/webhdfs/v1{1}?{2}op=SETTIMES&modificationtime={3}".format(self.endpoint, hdfsPath, self.auth, long(modTime)*1000)
        logger.debug(url)
        resp = requests.put(url)
        if not resp.status_code == 200:
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
    