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
    
    def __init__(self, endpoint, hdfsUser):
        self.endpoint = endpoint
        self.auth = "user.name=" + hdfsUser + "&"
        
         
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
    
    def put(self, url):
        resp = requests.put(url, allow_redirects=False)
        if resp.status_code != 200:  
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'", resp.status_code, url)
        
    def createFolder(self, path, permission):
        if permission != None:
            url = "http://{0}/webhdfs/v1{1}?{2}op=MKDIRS&permission={3}".format(self.endpoint, path, self.auth, permission)
        else:
            url = "http://{0}/webhdfs/v1{1}?{2}op=MKDIRS".format(self.endpoint, path, self.auth)
        self.put(url)

    def setOwner(self, path, owner):
        url = "http://{0}/webhdfs/v1{1}?{2}op=SETOWNER&owner={3}".format(self.endpoint, path, self.auth, owner)
        self.put(url)

    def setGroup(self, path, group):
        url = "http://{0}/webhdfs/v1{1}?{2}op=SETOWNER&group={3}".format(self.endpoint, path, self.auth, group)
        self.put(url)
    
    def setPermission(self, path, permission):
        url = "http://{0}/webhdfs/v1{1}?{2}op=SETPERMISSION&permission={3}".format(self.endpoint, path, self.auth, permission)
        self.put(url)
           
    def setModificationTime(self, hdfsPath, modTime):
        url = "http://{0}/webhdfs/v1{1}?{2}op=SETTIMES&modificationtime={3}".format(self.endpoint, hdfsPath, self.auth, long(modTime)*1000)
        logger.debug(url)
        self.put(url)
           
    def rename(self, hdfsPath, newName):
        url = "http://{0}/webhdfs/v1{1}?{2}op=RENAME&destination={3}".format(self.endpoint, hdfsPath, self.auth, newName)
        self.put(url)
           
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
            #print misc.pprint2s(result)
            for f in result['FileStatuses']['FileStatus']:
                if f['type'] == 'FILE':
                    fi = {}
                    fi['name'] = f['pathSuffix']
                    fi['size'] = f['length']
                    fi['modificationTime'] = f['modificationTime']/1000
                    fi['mode'] = "0" + f['permission']
                    fi['owner'] = f['owner']
                    fi['group'] = f['group']
                    dirContent['files'].append(fi)
                elif f['type'] == 'DIRECTORY':
                    di = {}
                    di['name'] = f['pathSuffix']
                    #di['modificationTime'] = f['modificationTime']/1000
                    di['mode'] = "0" + f['permission']
                    di['owner'] = f['owner']
                    di['group'] = f['group']
                    dirContent['directories'].append(di)
                else:
                    misc.ERROR("Unknown directory entry type: {0}".format(f['type']))
        elif resp.status_code == 404:
            dirContent['status'] = "NOT_FOUND"
        elif resp.status_code == 403:
            dirContent['status'] = "NO_ACCESS"
        else:
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp.status_code, url))
        return dirContent
   

    def putFileToHdfs(self, localPath, hdfsPath, overwrite):
        logger.debug("putFileToHdfs(localPath={0}, hdfsPath={1})".format(localPath, hdfsPath))
        url = "http://{0}/webhdfs/v1{1}?{2}op=CREATE&overwrite={3}".format(self.endpoint, hdfsPath, self.auth, "true" if overwrite else "false")
        logger.debug(url)
        resp = requests.put(url, allow_redirects=False)
        if not resp.status_code == 307:
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp.status_code, url))
        url2 = resp.headers['location']    
        logger.debug(url2)
        f = open(localPath, "rb")
        resp2 = requests.put(url2, data=f, headers={'content-type': 'application/octet-stream'})
        if not resp2.status_code == 201:
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp2.status_code, url2))
           
    def getFileFromHdfs(self, localPath, hdfsPath):
        logger.debug("getFileFromHdfs(localPath={0}, hdfsPath={1})".format(localPath, hdfsPath))
        if os.path.exists(localPath):
            misc.ERROR("Local file {1} already exists. Will not overwrite it!".format(localPath))
        f = open(localPath, "wb")
        url = "http://{0}/webhdfs/v1{1}?{2}op=OPEN".format(self.endpoint, hdfsPath, self.auth)
        logger.debug(url)
        resp = requests.get(url, allow_redirects=True, stream=True)
        if not resp.status_code == 200:
            misc.ERROR("Invalid returned http code '{0}' when calling '{1}'".format(resp.status_code, url))
        for chunk in resp.iter_content(chunk_size=None, decode_unicode=False):
            f.write(chunk)
        f.close()

                
def lookup(p):   
    if p.webhdfsEndpoint == None:
        if not os.path.isdir(p.hadoopConfDir):
            misc.ERROR("{0} must be an existing folder, or --hadoopConfDir  or --webhdfsEndpoint provided as parameter.".format(p.hadoopConfDir))
        candidates = []
        hspath = os.path.join(p.hadoopConfDir, "hdfs-site.xml")
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
                webHDFS= WebHDFS(endpoint, p.hdfsUser)
                (x, err) = webHDFS.test()
                if x:
                    p.webhdfsEndpoint = webHDFS.endpoint
                    return webHDFS
                else:
                    errors.append(err)
            misc.ERROR("Unable to find a valid 'webhdfs_endpoint' in hdfs-site.xml:" + str(errors))
        else:
            misc.ERROR("Unable to find file {0}. Provide 'webhdfs_endpoint' or 'hadoop_conf_dir' parameter", hspath)
    else:
        candidates = p.webhdfsEndpoint.split(",")
        errors = []
        for endpoint in candidates:
            webHDFS= WebHDFS(endpoint, p.hdfsUser)
            (x, err) = webHDFS.test()
            if x:
                p.webhdfsEndpoint = webHDFS.endpoint
                return webHDFS
            else:
                errors.append(err)
        misc.ERROR("Unable to find a valid 'webhdfs_endpoint' in: " + p.webhdfsEndpoint + " (" + str(errors) + ")")
    
    
    
    
    
    
    
    
    