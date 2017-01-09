# hdfsmirror

hdfsmirror is a small project aimed to copy files tree from local to HDFS and reverse. 

It is made of two python script: hdfsput.py and hdfsget.py, who shared the same logic and behavior

It behave by comparing source and target tree and perform the following.

* Files existing on the source, but not on the target will be copied. Modification time of the target is adjusted to the source value. owner, groups and permission will be adjusted if such parameters are provided.

* Files existing on the target but not on the source will not be affected.

* Files existing on the target and on the source will be compared by size and modification time. If they differ by one of theses values, target will be replaced if the `--force` switch is set. A backup will be performed before if the `--backup` switch is set. 

A key point is hdfsmirror can works using multi-threading, thus dramatically improve performances on local to HDFS transfer. 

## Installation

Currently, hdfsmirror is not packaged. So the only way to install it is to clone this project.

Then, you must install some dependencies:

By using PIP:

    pip install -r requirements.txt

Or, if you are on RHEL/Centos 7.X, you can install required packages:

    yum install -y python-requests PyYAML

And, last steps, make files executable:

	chmod +x src/hdfsput.py
	chmod +x src/hdfsget.py

## Usage

Simply launch hdfsput.py, or hdfsget.py

	usage: hdfsput.py [-h] --src SRC --dest DEST [--checkMode] [--report]
                  [--reportFiles] [--nbrThreads NBRTHREADS]
                  [--yamlLoggingConf YAMLLOGGINGCONF] [--force] [--backup]
                  [--owner OWNER] [--group GROUP] [--mode MODE]
                  [--directoryMode DIRECTORYMODE] [--forceExt]
                  [--hdfsUser HDFSUSER] [--hadoopConfDir HADOOPCONFDIR]
                  [--webhdfsEndpoint WEBHDFSENDPOINT]

	optional arguments:
	  -h, --help            show this help message and exit
	  --src SRC
	  --dest DEST
	  --checkMode
	  --report
	  --reportFiles
	  --nbrThreads NBRTHREADS
	  --yamlLoggingConf YAMLLOGGINGCONF
	                        Logging configuration as a yaml file
	  --force
	  --backup
	  --owner OWNER         owner for all files.
	  --group GROUP         group for all files.
	  --mode MODE           mode for all files.
	  --directoryMode DIRECTORYMODE
	  --forceExt
	  --hdfsUser HDFSUSER
	  --hadoopConfDir HADOOPCONFDIR
	  --webhdfsEndpoint WEBHDFSENDPOINT
	  
	  
Here is a short explanation of the options:

* `src:` Mandatory. The directory you want to copy from (local for hdfsput, HDFS for hdfsget). This must be an existing directory. See note on ending '/' below

* `dest:` Mandatory. The directory you want to copy into (HDFS for hdfsput, local for hdfsget). This must be an existing directory.

* `checkMode:` Boolean. Default: No. If set, the command will be perform any action. Just print report

* `report:` Boolean. Default: No. Produce a synthetic report.

* `reportFiles:` Boolean. Default: No. Produce a detailed report, listing all impacted files.

* `nbrThreads:` Allow mutithreading on --put. Value such as 10 or 20 can dramatically improve performance. Default to 1.

* `yamlLoggingConf:` Allow to specify an alternate logging configuration file. Default is to use the logging.yml file located in the same folder than hdfs[put/get].py

* `force:` Boolean. Default: No. Allow overwrite of target file if they differ from source.

* `forceExt:` Boolean. Default: No. If Yes, provided owner/group/mode value will be applied on existing files and directories on target. If no, only the newley created file and directories will be adjusted.  

* `backup:` Boolean. Default: No. In case of overwrite, perform a backup of original file.

* `owner:` If set, all files on target will belong to this user.

* `group:` If set, all files on target will belong to this group.

* `mode:` If set, all files on target will have this permission. Must be a string representing an octal value (i.e: "0644")

* `directoryMode:` permission for directories. (owner and group will the same as files). 

* `hdfsUser:` Default: "hdfs". This user on behalf all HDFS operation will be performed. Of course must be able to read an write on concerned folder.

* `hadoopConfDir:` Default: "/etc/hadoop/conf" Refer to 'Namenode lookup' below.

* `webHdfsEndpoint:` Refer to 'Namenode lookup' below.

## src ending with "/"

If `src` path ends with "/", only inside contents of that directory are copied to destination. Otherwise, if it does not end with "/", the directory itself with all contents is copied. This behavior is similar to Rsync.

## Namenode lookup

hdfsmirror performs all its operation through WebHDFS REST API. So it need to know the URL of the active Hadoop namenode. It will try to lookup such information by parsing the file `hdfs-site.xml` in the folder `/etc/hadoop/conf`. 

You can set an alternate configuration folder by using the `--hadoopConfDir` option. Or you can directly set this url by using the `--webHdfsEndpoint` option (i.e. `--webHdfsEndpoint amenode.mycluster.com:50070`).

Using this last method will allow you to use hdfsmirror.py from outside your cluster, without local HDFS client configuration. But, keep in mind the WebHDFS API need to be able to reach directly not only the namenode, but also all datanodes of the target cluster.

`--webHdfsEndpoint` value could also be a comma separated list of entry points, which will be checked up to a valid one. This will allow Namenode H.A. handling. 

## License

    Copyright (C) 2016 BROADSoftware

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.




