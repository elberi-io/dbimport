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
import logging
import subprocess

# import jaydebeapi

# from urllib.parse import quote

# import mysql.connector
# from mysql.connector import errorcode

# import pandas as pd
from DBImportConfig import common_config
# import sqlalchemy as sa
# from sqlalchemy.orm import Session, sessionmaker
# from sqlalchemy.ext.automap import automap_base
# from sqlalchemy_utils import create_view
# from sqlalchemy_views import CreateView, DropView
# from sqlalchemy.sql import text
# from sqlalchemy.orm import aliased, sessionmaker, Query
# from alembic.config import Config
# from alembic import command as alembicCommand


class webServerOperations(object):
    def __init__(self):
        logging.debug("Executing webServerOperations.__init__()")

        self.debugLogLevel = False

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        try:
            self.DBImport_Home = os.environ["DBIMPORT_HOME"]
        except KeyError:
            logging.error("System Environment Variable DBIMPORT_HOME is not set")
            sys.exit(1)

        self.common_config = common_config.config()

        # 		# Fetch configuration about MySQL database and how to connect to it
        # 		self.databaseCredentials = self.common_config.getMysqlCredentials()
        # 		self.configHostname = self.databaseCredentials["mysql_hostname"]
        # 		self.configPort =     self.databaseCredentials["mysql_port"]
        # 		self.configDatabase = self.databaseCredentials["mysql_database"]
        # 		self.configUsername = self.databaseCredentials["mysql_username"]
        # 		self.configPassword = self.databaseCredentials["mysql_password"]
        # 		# self.configPassword = quote(self.databaseCredentials["mysql_password"], safe=" +")
        #
        # 		self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s"%(
        # 			self.configUsername,
        # 			# self.configPassword,
        # 			quote(self.configPassword, safe=" +"),
        # 			self.configHostname,
        # 			self.configPort,
        # 			self.configDatabase)
        #
        # 		# Esablish a SQLAlchemy connection to the DBImport database
        # 		try:
        # 			self.configDB = sa.create_engine(self.connectStr, echo = self.debugLogLevel)
        # 			self.configDB.connect()
        # 			self.configDBSession = sessionmaker(bind=self.configDB)
        #
        # 		except sa.exc.OperationalError as err:
        # 			logging.error("%s"%err)
        # 			sys.exit(1)
        # 		except:
        # 			print("Unexpected error: ")
        # 			print(sys.exc_info())
        # 			sys.exit(1)

        logging.debug("Executing webServerOperations.__init__() - Finished")

    def buildHTML(self):
        logging.debug("Executing webServerOperations.buildHTML()")

        self.runNPMinstall()
        self.runNPMbuild()

        print()
        logging.info("WebServer files built and installed successfully")
        logging.debug("Executing webServerOperations.buildHTML() - Finished")

    def runNPMinstall(self):
        logging.debug("Executing webServerOperations.runNPMinstall()")
        logging.info("Installing NPM Packages")

        prefixDir = self.DBImport_Home + "/bin/Server/html"

        result = subprocess.run(["npm", "config", "set", "fund", "false"])
        if result.returncode != 0:
            logging.error(
                "Unknown error when executing 'npm config' command. Please solve this and rerun setup command"
            )
            sys.exit(1)

        result = subprocess.run(["npm", "install", "--prefix", prefixDir])
        if result.returncode != 0:
            logging.error(
                "Unknown error when executing 'npm install' command. Please solve this and rerun setup command"
            )
            sys.exit(1)

        logging.debug("Executing webServerOperations.runNPMinstall() - Finished")

    def runNPMbuild(self):
        logging.debug("Executing webServerOperations.runNPMbuild()")
        logging.info("Building HTML files")

        prefixDir = self.DBImport_Home + "/bin/Server/html"
        envFile = prefixDir + "/.env"

        if not os.path.isfile(envFile):
            with open(envFile, "w") as file:
                file.write("VITE_PROXY_TARGET='https://127.0.0.1:5188'\n")

        result = subprocess.run(["npm", "run", "build", "--prefix", prefixDir])
        if result.returncode != 0:
            logging.error(
                "Unknown error when executing 'npm run build' command. Please solve this and rerun setup command"
            )
            sys.exit(1)

        logging.debug("Executing webServerOperations.runNPMbuild() - Finished")
