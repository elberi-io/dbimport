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

import configparser
import os
import sys

from common.Exceptions import invalidConfiguration


def get(section: str, key: str, exit_on_error: bool = True) -> str:
    try:
        return config[section][key]
    except KeyError:
        if exit_on_error == True:
            print(
                f"[{section}] '{key}' Key was not found in configuration file. Please check settings"
            )
            sys.exit(1)
        else:
            raise invalidConfiguration(
                f"[{section}] '{key}' Key was not found in configuration file. Please check settings"
            )


try:
    dbimport_home = os.environ["DBIMPORT_HOME"]
    config_file = dbimport_home + "/conf/dbimport.cfg.dist"
except KeyError:
    print("Error: System Environment Variable DBIMPORT_HOME is not set")
    sys.exit(1)

try:
    config_file = os.environ["DBIMPORT_CONFIGFILE"]
except KeyError:
    pass

if os.path.exists(config_file) == False:
    print(f"Error: Configuration file can't be found on '{config_file}'")
    sys.exit(1)

config = configparser.ConfigParser()
config.read(config_file)
