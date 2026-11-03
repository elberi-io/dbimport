# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import sys
import configparser
from common.Exceptions import *

def get(section, key, exitOnError=True):
	try:
		return config[section][key]
	except KeyError:
		if exitOnError == True:
			print("[%s] '%s' Key was not found in configuration file. Please check settings"%(section,key))
			sys.exit(1)
		else:
			raise invalidConfiguration("[%s] '%s' Key was not found in configuration file. Please check settings"%(section,key))

try:
	DBImport_Home = os.environ['DBIMPORT_HOME']
	configFile = DBImport_Home + '/conf/dbimport.cfg.dist'
except KeyError:
	print("Error: System Environment Variable DBIMPORT_HOME is not set")
	sys.exit(1)
 
try:
	configFile = os.environ['DBIMPORT_CONFIGFILE']
except KeyError:
	pass
 
if os.path.exists(configFile) == False:
	print("Error: Configuration file can't be found on '%s'"%(configFile))
	sys.exit(1)

config = configparser.ConfigParser()
config.read(configFile)
