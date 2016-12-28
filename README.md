# hdfsmirror.py

hdfsmirror is a small project aimed to copy file tree from local to HDFS and reverse.

It behave by comparing source and target tree and perform the following.

* Files existing on the source, but not on the target will be copied. Modification time of the target is adjusted to the source value

* Files existing on the target but not on the source will not be affected.

* Files existing on the target and on the source will not be affected. If they differs by size or by modification time, this will be notified in the report.

A key point is hdfsmirror can works using multi-threading, thus dramatically improve performances on local to HDFS transfer. 

## Installation

Currently, hdfsmirror.py is not packaged. So the only way to install it is to clone this project.

Then, you must install some dependencies:

By using PIP:

    pip install -r requirements.txt

Or, if you are on RHEL/Centos 7.X, you can install required packages:

    yum install -y python-requests PyYAML

## Usage

Simply launch hdfsmirror.py

	src/hdfsmirror.py --help
	usage: hdfsmirror.py [-h] --local LOCAL --hdfs HDFS
	                     [--webHdfsEndpoint WEBHDFSENDPOINT]
	                     [--hadoopConfDir HADOOPCONFDIR] [--hdfsUser HDFSUSER]
	                     [--put] [--get] [--report] [--reportFiles]
	                     [--nbrThreads NBRTHREADS]
	                     [--yamlLoggingConf YAMLLOGGINGCONF]
	
	optional arguments:
	  -h, --help            show this help message and exit
	  --local LOCAL
	  --hdfs HDFS
	  --webHdfsEndpoint WEBHDFSENDPOINT
	  --hadoopConfDir HADOOPCONFDIR
	  --hdfsUser HDFSUSER
	  --put
	  --get
	  --report
	  --reportFiles
	  --nbrThreads NBRTHREADS
	  --yamlLoggingConf YAMLLOGGINGCONF
	                        Logging configuration as a yaml file



Here is a short explanation of the options:

* `local:` The local (Linux file system) path of the data you want to copy from/to. This must be an existing directory.

* `hdfs:` The HDFS path of the data you want to copy from/to. This must be an existing directory.

* `webHdfsEndpoint:` Refer to 'Namenode lookup' below.

* `hadoopConfDir:` Refer to 'Namenode lookup' below.

* `hdfsUser:` This user on behalf all HDFS operation will be performed. Of course must be able to read an write on concerned folder.

* `put:` Trigger the effective transfer from local to HDFS.

* `get:` Trigger the effective transfer from HDFS to local.

* `report:` Produce a synthetical report. If used without --put or --get, no operation will be performed.

* `reportFiles:` Produce a detailed report. If used without --put or --get, no operation will be performed.

* `nbrThreads:` Allow mutithreading on --put. Value such as 10 or 20 can dramatically improve performance. Default to 1.

* `yamlLoggingConf:` Allow to specify an alternate logging configuration file. Default is to use the logging.yml file located in the same folder than hdfsmirror.py


## Namenode lookup

hdfsmirror perform all its operation through WebHDFS REST API. So it need to know the url of the active namenode. It will try to lookup such information by parsing the file `hdfs-site.xml` in the folder `/etc/hadoop/conf`. 

You can set an alternate configuration folder by using the `--hadoopConfDir` option. Or you can directly set this url by using the `--webHdfsEndpoint` option (i.e. `--webHdfsEndpoint amenode.mycluster.com:50070`).

Using this last method will allow you to use hdfsmirror.py from outside your cluster, without local HDFS client configuration. But, keep in mind the WebHDFS API need to be able to reach directly not only the namenode, but also all datanodes of the target cluster.


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




