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

        self.debug_log_level = False

        if logging.root.level == 10:  # DEBUG
            self.debug_log_level = True

        try:
            self.dbimport_home = os.environ["DBIMPORT_HOME"]
        except KeyError:
            logging.error("System Environment Variable DBIMPORT_HOME is not set")
            sys.exit(1)

        self.common_config = common_config.config()

        # 		# Fetch configuration about MySQL database and how to connect to it
        # 		self.database_credentials = self.common_config.get_mysql_credentials()
        # 		self.config_hostname = self.database_credentials["mysql_hostname"]
        # 		self.config_port =     self.database_credentials["mysql_port"]
        # 		self.config_database = self.database_credentials["mysql_database"]
        # 		self.config_username = self.database_credentials["mysql_username"]
        # 		self.config_password = self.database_credentials["mysql_password"]
        # 		# self.config_password = quote(self.database_credentials["mysql_password"], safe=" +")
        #
        # 		self.connect_str = "mysql+pymysql://%s:%s@%s:%s/%s"%(
        # 			self.config_username,
        # 			# self.config_password,
        # 			quote(self.config_password, safe=" +"),
        # 			self.config_hostname,
        # 			self.config_port,
        # 			self.config_database)
        #
        # 		# Esablish a SQLAlchemy connection to the DBImport database
        # 		try:
        # 			self.config_db = sa.create_engine(self.connect_str, echo = self.debug_log_level)
        # 			self.config_db.connect()
        # 			self.config_db_session = sessionmaker(bind=self.config_db)
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

        prefix_dir = self.dbimport_home + "/bin/Server/html"

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

        prefix_dir = self.dbimport_home + "/bin/Server/html"
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
