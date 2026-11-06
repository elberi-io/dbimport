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

import logging
import os
import subprocess
import sys

from DBImportConfig import common_config


class webServerOperations:
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
        # 		self.databaseCredentials = self.common_config.get_mysql_credentials()
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

    def build_html(self):
        logging.debug("Executing webServerOperations.build_html()")

        self.run_npm_install()
        self.run_npm_build()

        print()
        logging.info("WebServer files built and installed successfully")
        logging.debug("Executing webServerOperations.build_html() - Finished")

    def run_npm_install(self):
        logging.debug("Executing webServerOperations.run_npm_install()")
        logging.info("Installing NPM Packages")

        prefix_dir = self.DBImport_Home + "/bin/Server/html"

        result = subprocess.run(["npm", "config", "set", "fund", "false"])
        if result.returncode != 0:
            logging.error(
                "Unknown error when executing 'npm config' command. Please solve this and rerun setup command"
            )
            sys.exit(1)

        result = subprocess.run(["npm", "install", "--prefix", prefix_dir])
        if result.returncode != 0:
            logging.error(
                "Unknown error when executing 'npm install' command. Please solve this and rerun setup command"
            )
            sys.exit(1)

        logging.debug("Executing webServerOperations.run_npm_install() - Finished")

    def run_npm_build(self):
        logging.debug("Executing webServerOperations.run_npm_build()")
        logging.info("Building HTML files")

        prefix_dir = self.DBImport_Home + "/bin/Server/html"
        env_file = prefix_dir + "/.env"

        if not os.path.isfile(env_file):
            with open(env_file, "w") as file:
                file.write("VITE_PROXY_TARGET='https://127.0.0.1:5188'\n")

        result = subprocess.run(["npm", "run", "build", "--prefix", prefix_dir])
        if result.returncode != 0:
            logging.error(
                "Unknown error when executing 'npm run build' command. Please solve this and rerun setup command"
            )
            sys.exit(1)

        logging.debug("Executing webServerOperations.run_npm_build() - Finished")
