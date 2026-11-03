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
import io
import sys
import logging
import subprocess 
import shutil
import jaydebeapi
import jpype
import base64
import re
import json
import ssl
import requests
import pendulum
from urllib.parse import quote
from itertools import zip_longest
from requests_kerberos import HTTPKerberosAuth
from requests.auth import HTTPBasicAuth
from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
from subprocess import Popen, PIPE
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, time, timedelta, timezone
from dateutil import *
from dateutil.tz import *
import pandas as pd
import numpy as np
from sourceSchemaReader import schemaReader
from common.Singleton import Singleton
from common import constants as constant
from DBImportConfig import decryption as decryption
from common.Exceptions import *
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select
from sqlalchemy.orm import aliased, sessionmaker, Query
import pymongo


class config(object, metaclass=Singleton):
	def __init__(self, Hive_DB=None, Hive_Table=None):
		logging.debug("Executing common_config.__init__()")

		self.Hive_DB = None
		self.Hive_Table = None
		if Hive_DB != None and self.Hive_DB == None:
			self.Hive_DB = Hive_DB
		if Hive_Table != None and self.Hive_Table == None:
			self.Hive_Table = Hive_Table

		self.mysql_conn = None
		self.mysql_cursor = None
		self.tempdir = None
		self.startDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') 
		self.operationType = None
		self.dbAlias = None
		self.targetSchema = None
		self.targetTable = None


		# Variables used in lookupConnectionAlias
		self.db_mssql = False
		self.db_oracle = False
		self.db_mysql = False
		self.db_postgresql = False
		self.db_progress = False
		self.db_db2udb = False
		self.db_db2as400 = False
		self.db_mongodb = False
		self.db_cachedb = False
		self.db_snowflake = False
		self.db_awss3 = False
		self.db_informix = False
		self.db_sqlanywhere = False
		self.jdbc_url = None
		self.jdbc_hostname = None
		self.jdbc_port = None
		self.jdbc_database = None
		self.jdbc_username = None
		self.jdbc_password = None
		self.jdbc_classpath = None
		self.jdbc_driver = None
		self.jdbc_driver_for_python = None
		self.jdbc_classpath_for_python = None
		self.jdbc_ad_domain = None
		self.jdbc_encrypt = None
		self.jdbc_encrypt_string = None
		self.jdbc_trustedservercert = None
		self.jdbc_trustedservercert_password = None
		self.jdbc_hostincert = None
		self.jdbc_logintimeout = None
		self.jdbc_servertype = None
		self.jdbc_oracle_sid = None
		self.jdbc_oracle_servicename = None
		self.jdbc_force_column_lowercase = None
		self.jdbc_environment = None
		self.seedFile = None
		self.mongoClient = None
		self.mongoDB = None
		self.mongoAuthSource = None
		self.awsS3assumeRole = None
		self.awsS3assumeRoleDuration = None
		self.awsS3proxyServer = None
		self.awsS3bucket = None
		self.awsS3region = None
		self.awsS3externalId = None
		self.awsS3fileFormat = None
		self.kerberosInitiated = False

		self.impalaDriver = None
		self.impalaClasspath = None
		self.impalaAddress = None
		self.impalaJDBCConn = None
		self.impalaJDBCCursor = None

		self.sparkPathAppend = None
		self.sparkPysparkSubmitArgs = None
		self.sparkJarFiles = None
		self.sparkPackages = None
		self.sparkMajorVersion = None
		self.sparkPyFiles = None
		self.sparkMaster = None
		self.sparkDeployMode = None
		self.sparkYarnQueue = None
		self.sparkDynamicAllocation = None
		self.sparkMinExecutors = None
		self.sparkMaxExecutors = None
		self.sparkExecutorMemory = None
		self.sparkHiveLibrary = None
		self.sparkLogLevel = None

		self.operationTimestamp = None
		self.yarnApplicationStart = None

		self.sourceSchema = None
		self.tableType = None

		self.custom_max_query = None

		self.source_columns_df = pd.DataFrame()
		self.source_keys_df = pd.DataFrame()

		self.JDBCConn = None
		self.JDBCCursor = None

		self.sourceSchema = schemaReader.source()

		# SQLAlchemy connection variables
		self.configDB = None
		self.configDBSession = None
		self.mysqlConnectionDetails = None

		self.debugLogLevel = False
		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

		self.kerberosPrincipal = configuration.get("Kerberos", "principal")
		self.kerberosKeytab = configuration.get("Kerberos", "keytab")

		try:
			self.sparkMajorVersion = int(configuration.get("Spark", "spark_major_version", exitOnError=False))
		except invalidConfiguration:
			logging.warning("Configuration option 'spark_major_version' under [Spark] in the configuration file is missing. Please update your config file")
			self.sparkMajorVersion = 3
		except ValueError:
			logging.warning("Configuration option 'spark_major_version' under [Spark] in the configuration file must be an integer. Please update your config file")
			self.sparkMajorVersion = 3

		if self.sparkMajorVersion < 2 or self.sparkMajorVersion > 4:
			logging.error("Only Spark version 2, 3 and 4 is supported.")
			sys.exit(1)

		try:
			self.sparkLogLevel = configuration.get("Spark", "spark_log_level", exitOnError=False)
		except invalidConfiguration:
			pass

		try:
			self.sparkPackages = configuration.get("Spark", "packages", exitOnError=False)
		except invalidConfiguration:
			logging.warning("Configuration option 'packages' under [Spark] in the configuration file is missing. Please update your config file")

		self.sparkPathAppend = configuration.get("Spark", "path_append")
		self.sparkJarFiles = configuration.get("Spark", "jar_files")
		self.sparkPyFiles = configuration.get("Spark", "py_files")
		self.sparkMaster = configuration.get("Spark", "master")
		self.sparkDeployMode = configuration.get("Spark", "deployMode")
		self.sparkYarnQueue = configuration.get("Spark", "yarnqueue")
		self.sparkExecutorMemory = configuration.get("Spark", "executor_memory")
		self.sparkHiveLibrary = configuration.get("Spark", "hive_library")

		try:
			if configuration.get("Spark", "s3_access_to_ozone", exitOnError=False).lower() == "true":
				self.sparkS3AccessToOzone = True
			else:
				self.sparkS3AccessToOzone = False
			self.sparkS3AccessKeyID = configuration.get("Spark", "s3_access_key_id")
			self.sparkS3SecretAccessKey = configuration.get("Spark", "s3_secret_access_key")

		except invalidConfiguration:
			self.sparkS3AccessToOzone = False
			self.sparkS3AccessKeyID = ""
			self.sparkS3SecretAccessKey = ""

		self.awsRegion = None
		try:
			self.awsRegion = configuration.get("AWS", "region", exitOnError=False)
		except invalidConfiguration:
			pass

		if configuration.get("Spark", "dynamic_allocation").lower() == "true":
			self.sparkDynamicAllocation = True
		else:
			self.sparkDynamicAllocation = False

		self.crypto = decryption.crypto()

		# Sets and create a temporary directory
		self.tempdir = "/tmp/dbimport." + str(os.getpid()) + ".tmp"

		# If the temp dir exists, we just remove it as it's a leftover from another DBImport execution
		try:
			shutil.rmtree(self.tempdir)
		except FileNotFoundError:
			pass
		except PermissionError:
			logging.error("The temporary directory (%s) already exists but cant be removed due to permission error"%(self.tempdir))
			sys.exit(1)

		try:
			os.mkdir(self.tempdir)
		except OSError: 
			logging.error("Creation of the temporary directory %s failed" % self.tempdir)
			sys.exit(1)

		try:
			os.chmod(self.tempdir, 0o700)
		except OSError:
			logging.error("Error while changing permission on %s to 700" % self.tempdir)
			self.remove_temporary_files()
			sys.exit(1)

		self.reconnectConfigDatabase(printReconnectMessage=False)

		logging.debug("Executing common_config.__init__() - Finished")

	def disconnectConfigDatabase(self):
		""" Closing connection against config database. Only used during debug and development """
		logging.debug("Connection against MySQL Config database have been closed")
		self.mysql_cursor.close()
		self.mysql_conn.close()


	def getMysqlCredentials(self):
		# Fetch configuration about MySQL database and how to connect to it

		if self.mysqlConnectionDetails != None:
			# This information is already fetched from the configuration. Just return the fetched values
			return self.mysqlConnectionDetails

		mysql_hostname = configuration.get("Database", "mysql_hostname")
		mysql_port =     configuration.get("Database", "mysql_port")
		mysql_database = configuration.get("Database", "mysql_database")
		mysql_username = configuration.get("Database", "mysql_username")
		mysql_password = configuration.get("Database", "mysql_password")

		if mysql_password == "" and mysql_username == "" and self.awsRegion != None:
			# No password is configured. Lets try the AWS get_secret function if configured
			aws_secret_name = None

			try:
				aws_secret_name = configuration.get("Database", "aws_secret_name", exitOnError=False)
			except invalidConfiguration:
				raise invalidConfiguration("Credentials configuration to configuration database in config file is incorrect. Either use username/password or a AWS Secret Manager name") 
				# pass

			import boto3
			from botocore.exceptions import ClientError

			# Create a Secrets Manager client
			session = boto3.session.Session()
			client = session.client(
					service_name='secretsmanager',
					region_name=self.awsRegion
					)

			try:
				get_secret_value_response = client.get_secret_value(
					SecretId=aws_secret_name
				)
			except ClientError as e:
				# For a list of exceptions thrown, see
				# https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
				raise e

			rds_credentials = json.loads(get_secret_value_response['SecretString'])
			mysql_username = rds_credentials["username"]
			mysql_password = rds_credentials["password"]

		elif mysql_password == "" or mysql_username == "":
			raise invalidConfiguration("Credentials configuration to configuration database in config file is incorrect. Either use username/password or a AWS Secret Manager name") 

		self.mysqlConnectionDetails = {}
		self.mysqlConnectionDetails["mysql_hostname"] = mysql_hostname
		self.mysqlConnectionDetails["mysql_port"] = mysql_port
		self.mysqlConnectionDetails["mysql_database"] = mysql_database
		self.mysqlConnectionDetails["mysql_username"] = mysql_username
		self.mysqlConnectionDetails["mysql_password"] = mysql_password

		# print(mysql_username)
		# print(mysql_password)
	
		return self.mysqlConnectionDetails


	def reconnectConfigDatabase(self, printReconnectMessage=True):
		if self.mysql_conn == None or self.mysql_conn.is_connected() == False:
			if printReconnectMessage == True:
				logging.warn("Connection to MySQL have been lost. Reconnecting...")

			mysqlCredentials = self.getMysqlCredentials()

			# Esablish a connection to the DBImport database in MySQL
			try:
				self.mysql_conn = mysql.connector.connect(host=mysqlCredentials["mysql_hostname"], 
													 port=mysqlCredentials["mysql_port"], 
													 database=mysqlCredentials["mysql_database"], 
													 user=mysqlCredentials["mysql_username"], 
													 password=mysqlCredentials["mysql_password"])
			except mysql.connector.Error as err:
				if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
					logging.error("Something is wrong with your user name or password")
				elif err.errno == errorcode.ER_BAD_DB_ERROR:
					logging.error("Database does not exist")
				else:
					logging.error("%s"%err)
				logging.error("Error: There was a problem connecting to the MySQL database. Please check configuration and serverstatus and try again")
				self.remove_temporary_files()
				sys.exit(1)
			else:
				self.mysql_cursor = self.mysql_conn.cursor(buffered=True)

	def getAtlasJdbcConnectionData(self, dbAlias=None):
		""" Reads the extended information in jdbc_connections needed by Atlas (contact_info, owner and more) """
		logging.debug("Executing common_config.getAtlasJdbcConnectionData()")
	
		if dbAlias == None:
			dbAlias = self.dbAlias

		# Fetch data from jdbc_connection table
		query = "select contact_info, description, owner, atlas_discovery, atlas_include_filter, atlas_exclude_filter, atlas_last_discovery from jdbc_connections where dbalias = %s "
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, (dbAlias, ))

		if self.mysql_cursor.rowcount != 1:
			raise invalidConfiguration("The requested connection alias cant be found in the 'jdbc_connections' table")

		row = self.mysql_cursor.fetchone()
		returnDict = {}
		returnDict["contact_info"] = row[0]
		returnDict["description"] = row[1]
		returnDict["owner"] = row[2]
		returnDict["atlas_discovery"] = row[3]
		returnDict["atlas_include_filter"] = row[4]
		returnDict["atlas_exclude_filter"] = row[5]
		returnDict["atlas_last_discovery"] = row[6]

		if returnDict["contact_info"] == None:	returnDict["contact_info"] = ""
		if returnDict["description"] == None:	returnDict["description"] = ""
		if returnDict["owner"] == None:			returnDict["owner"] = ""

		logging.debug("Executing common_config.getAtlasJdbcConnectionData() - Finished")
		return returnDict


	def disconnectSQLAlchemy(self, logger=""):
		""" Disconnects from the database and removes all sessions and engine """
		log = logging.getLogger(logger)

		if self.configDB != None:
			log.info("Disconnecting from DBImport database")
			self.configDB.dispose()
			self.configDB = None

		self.configDBSession = None


	def connectSQLAlchemy(self, exitIfFailure=True, logger=""):
		log = logging.getLogger(logger)
		""" Connects to the configuration database with SQLAlchemy """

		if self.configDBSession != None:
			# If we already have a connection, we just say that it's ok....
			return True

		mysqlCredentials = self.getMysqlCredentials()

		self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s"%(
			mysqlCredentials["mysql_username"], 
			quote(mysqlCredentials["mysql_password"], safe=" +"),
			mysqlCredentials["mysql_hostname"], 
			mysqlCredentials["mysql_port"], 
			mysqlCredentials["mysql_database"]) 

		try:
			self.configDB = sa.create_engine(self.connectStr, echo = self.debugLogLevel, pool_pre_ping=True)
			self.configDB.connect()
			self.configDBSession = sessionmaker(bind=self.configDB)

		except sa.exc.OperationalError as err:
			logging.error("%s"%err)
			if exitIfFailure == True:
				self.remove_temporary_files()
				sys.exit(1)
			else:
				self.configDBSession = None
				self.configDB= None
				return False
		except:
			print("Unexpected error: ")
			print(sys.exc_info())
			if exitIfFailure == True:
				self.remove_temporary_files()
				sys.exit(1)
			else:
				self.configDBSession = None
				self.configDB= None
				return False

		else:
			return True

	def setHiveTable(self, Hive_DB, Hive_Table):
		""" Sets the parameters to work against a new Hive database and table """
		self.Hive_DB = Hive_DB.lower()
		self.Hive_Table = Hive_Table.lower()

	def getMysqlCursor(self):
		return self.mysql_cursor

	def getMysqlConnector(self):
		return self.mysql_conn

	def remove_temporary_files(self):
		logging.debug("Executing common_config.remove_temporary_files()")

		# Remove the kerberos ticket file
		if self.kerberosInitiated == True:
			try:
				klistCommandList = ['kdestroy']
				klistProc = subprocess.Popen(klistCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				stdOut, stdErr = klistProc.communicate()
			except:
				pass

		# We add this check just to make sure that self.tempdir contains a valid path and that no exception forces us to remove example /
		if len(self.tempdir) > 13 and self.tempdir.startswith( '/tmp/' ):
			try:
				shutil.rmtree(self.tempdir)
			except FileNotFoundError:
				# This can happen as we might call this function multiple times during an error and we dont want an exception because of that
				pass

		logging.debug("Executing common_config.remove_temporary_files() - Finished")

	def checkTimeWindow(self, connection_alias, atlasDiscoveryMode=False):
		logging.debug("Executing common_config.checkTimeWindow()")
		if atlasDiscoveryMode == False:
			logging.info("Checking if we are allowed to use this jdbc connection at this time")

		self.timeZone = self.getConfigValue("timezone")

		query = "select timewindow_start, timewindow_stop, timewindow_timezone from jdbc_connections where dbalias = %s"
		self.mysql_cursor.execute(query, (connection_alias, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		row = self.mysql_cursor.fetchone()
		hour = row[0]
		minute = row[1]

		# If there is a local timezone set on the JDBC connection, we use that instead of the configured default timezone
		if row[2] is not None and row[2].strip() != "":
			self.timeZone = row[2]

		currentTime = pendulum.now(self.timeZone)	# Get time in configured timeZone
		timeWindowStart = None
		timeWindowStop = None

		if hour != None: 
			if hour == timedelta(days=1):
				hour = timedelta(days=0)

			timeWindowStart = currentTime.set(
				hour=int(str(hour).split(":")[0]), 
				minute=int(str(hour).split(":")[1]), 
				second=int(str(hour).split(":")[2]))

		if row[1] != None:
			timeWindowStop = currentTime.set(
				hour=int(str(minute).split(":")[0]), 
				minute=int(str(minute).split(":")[1]), 
				second=int(str(minute).split(":")[2]))

		passedMidnight = False
		if timeWindowStart != None and timeWindowStop != None and timeWindowStart > timeWindowStop:
			# This happens if we pass midnight
			timeWindowStop = timeWindowStop.add(days=1)
			passedMidnight = True

		logging.debug("timeWindowStart: %s"%(timeWindowStart))
		logging.debug("timeWindowStop: %s"%(timeWindowStop))

		if timeWindowStart == None and timeWindowStop == None:
			if atlasDiscoveryMode == False:
				logging.info("SUCCESSFUL: This import is allowed to run at any time during the day.")
			return True
		elif timeWindowStart == None or timeWindowStop == None: 
			if atlasDiscoveryMode == False:
				logging.error("Atleast one of the TimeWindow settings are NULL in the database. Only way to disable the Time Window")
				logging.error("function is to put NULL into both columns. Otherwise the configuration is marked as invalid and will exit")
				logging.error("as it's not running inside a correct Time Window.")
				logging.error("Invalid TimeWindow configuration")
				self.remove_temporary_files()
				sys.exit(1)
			else:
				return False
		elif timeWindowStart == timeWindowStop:
			if atlasDiscoveryMode == False:
				logging.error("The value in timewindow_start column is the same as the value in timewindow_stop.")
				logging.error("Invalid TimeWindow configuration")
				self.remove_temporary_files()
				sys.exit(1)
			else:
				return False
		else:
			allowedTime = False

			if currentTime > timeWindowStart and currentTime < timeWindowStop:
				allowedTime = True

			if passedMidnight == True:
			# If we passed midnight, it means that we added one day to the stop time. But depending on what time it is, and when the
			# window starts, we might be on the previous days window and should be allowed to run. So we need to test for that aswell
				timeWindowStart = timeWindowStart.add(days=-1)
				timeWindowStop = timeWindowStop.add(days=-1)
				logging.debug("timeWindowStart: %s"%(timeWindowStart))
				logging.debug("timeWindowStop: %s"%(timeWindowStop))

				if currentTime > timeWindowStart and currentTime < timeWindowStop:
					allowedTime = True

			if allowedTime == False:
				if atlasDiscoveryMode == False:
					logging.error("We are not allowed to run this import outside the configured Time Window")
					logging.info("    Current time:     %s (%s)"%(currentTime.to_time_string(), self.timeZone))
					logging.info("    TimeWindow Start: %s"%(timeWindowStart.to_time_string()))
					logging.info("    TimeWindow Stop:  %s"%(timeWindowStop.to_time_string()))
					self.remove_temporary_files()
					sys.exit(1)
				else:
					return False
			else:		
				if atlasDiscoveryMode == False:
					logging.info("SUCCESSFUL: There is a configured Time Window for this operation, and we are running inside that window.")

		logging.debug("    currentTime = %s"%(currentTime.to_time_string()))
		logging.debug("    timeWindowStart = %s"%(timeWindowStart.to_time_string()))
		logging.debug("    timeWindowStop = %s"%(timeWindowStop.to_time_string()))
		logging.debug("Executing common_config.checkTimeWindow() - Finished")
		return True

	def checkConnectionAlias(self, connection_alias):
		""" Will check if the connection alias exists in the jdbc_connection table """
		logging.debug("Executing common_config.checkConnectionAlias()")

		RC = False

		query = "select count(1) from jdbc_connections where dbalias = %s "
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, (connection_alias, ))

		row = self.mysql_cursor.fetchone()
		if row[0] == 1:
			RC = True

		logging.debug("Executing common_config.checkConnectionAlias() - Finished")
		return RC

	def checkKerberosTicket(self):
		""" Checks if there is a valid kerberos ticket or not. Runst 'klist -s' and checks the exitCode. Returns True or False """
		logging.debug("Executing common_config.checkKerberosTicket()")

		os.environ["KRB5CCNAME"] = "FILE:/tmp/krb5cc_%s_%s"%(os.getuid(), os.getpid())

		if self.kerberosPrincipal == "" and self.kerberosKeytab != "":
			# There is information about a keytab, but not the principal. So lets grab the first from the file
			kinitCommandList = ['klist', '-k', self.kerberosKeytab]
			try:
				kinitProc = subprocess.Popen(kinitCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				stdOut, stdErr = kinitProc.communicate()
				stdOut = stdOut.decode('utf-8').rstrip()
				stdErr = stdErr.decode('utf-8').rstrip()
			except FileNotFoundError:
				logging.error("kinit command was not found. Have you installed and configured Kerberos?")
				self.remove_temporary_files()
				sys.exit(1)

			for line in stdOut.splitlines():
				try:
					if "@" in line.strip().split(" ")[1]:
						# Get the first line with a @ in it. That should be the first principal in the keytab file
						self.kerberosPrincipal = line.strip().split(" ")[1]
						break
				except IndexError:
					pass

			if self.kerberosPrincipal == "":
				logging.error("No kerberos principal can be found in the keytab file.")
				self.remove_temporary_files()
				sys.exit(1)

		elif self.kerberosPrincipal == "" or self.kerberosKeytab == "":
			logging.error("The kerberos information is not correct in configuration file.")
			self.remove_temporary_files()
			sys.exit(1)

		logging.info("Initialize Kerberos ticket")
		self.kerberosInitiated = True

		kinitCommandList = ['kinit', '-kt', self.kerberosKeytab, self.kerberosPrincipal]
		kinitProc = subprocess.Popen(kinitCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdOut, stdErr = kinitProc.communicate()
		stdOut = stdOut.decode('utf-8').rstrip()
		stdErr = stdErr.decode('utf-8').rstrip()

		klistCommandList = ['klist', '-s']
		klistProc = subprocess.Popen(klistCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdOut, stdErr = klistProc.communicate()
		stdOut = stdOut.decode('utf-8').rstrip()
		stdErr = stdErr.decode('utf-8').rstrip()

		if klistProc.returncode == 0:
			return True
		else:
			return False

		logging.debug("Executing common_config.checkKerberosTicket() - Finished")


	def encryptString(self, strToEncrypt):
		self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
		self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))
		
		encryptedStr = self.crypto.encrypt(strToEncrypt)
		return(encryptedStr)

	def encryptUserPassword(self, connection_alias, username, password):

		# Fetch public/private key from jdbc_connection table if it exists
		query = "select private_key_path, public_key_path from jdbc_connections where dbalias = %s "
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, (connection_alias, ))

		if self.mysql_cursor.rowcount != 1:
			raise invalidConfiguration("The requested connection alias cant be found in the 'jdbc_connections' table")

		row = self.mysql_cursor.fetchone()

		privateKeyFile = row[0]
		publicKeyFile = row[1]

		if privateKeyFile != None and publicKeyFile != None and privateKeyFile.strip() != '' and publicKeyFile.strip() != '':
			self.crypto.setPrivateKeyFile(privateKeyFile)
			self.crypto.setPublicKeyFile(publicKeyFile)
		else:
			self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
			self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))

		strToEncrypt = "%s %s\n"%(username, password)

		encryptedStr = self.crypto.encrypt(strToEncrypt)

		if encryptedStr != None and encryptedStr != "":
			query = "update jdbc_connections set credentials = %s where dbalias = %s "
			logging.debug("Executing the following SQL: %s" % (query))
			self.mysql_cursor.execute(query, (encryptedStr, connection_alias))
			self.mysql_conn.commit()


	def getJDBCDriverConfig(self, databaseType, version):
		logging.debug("Executing common_config.getJDBCDriverConfig()")
		query = "select driver, classpath from jdbc_connections_drivers where database_type = %s and version = %s"
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, (databaseType, version ))

		if self.mysql_cursor.rowcount != 1:
			raise invalidConfiguration("Error: Cant find JDBC driver with database_type as '%s' and version as '%s'"%(databaseType, version))

		row = self.mysql_cursor.fetchone()
		driver = row[0]
		classPath = row[1]

		if classPath == "add path to JAR file" or not classPath.startswith("/"):
			raise invalidConfiguration("Error: You need to specify the full path to the JAR files in the table 'jdbc_connections_drivers'")

		logging.debug("Executing common_config.getJDBCDriverConfig() - Finished")
		return driver, classPath

	def printConnectionAliasDetails(self):
		logging.debug("Executing common_config.printConnectionAliasDetails()")

		query = "select dbalias from jdbc_connections"
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, [])

		counter = 0
		for row in self.mysql_cursor.fetchall():
			counter = counter + 1
			try:
				self.lookupConnectionAlias(connection_alias = row[0], exceptionIfFailureToDecrypt=False)
			except:
				pass
			else:
				if self.jdbc_username == None:
					self.jdbc_username = ""
				if self.jdbc_password == None:
					self.jdbc_password = ""

				print("£%s£¤£%s£¤£%s£¤£DBImport£"%(self.dbAlias, self.jdbc_username, self.jdbc_password))

				if 0 == 1:
					if "," in self.jdbc_username:
						print("self.jdbc_username contains ,")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))
					if "'" in self.jdbc_username:
						print("self.jdbc_username contains '")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))
					if "£" in self.jdbc_username:
						print("self.jdbc_username contains £")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))
					if "¤" in self.jdbc_username:
						print("self.jdbc_username contains ¤")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))
	
					if "'" in self.jdbc_password:
						print("self.jdbc_password contains '")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))
					if "£" in self.jdbc_password:
						print("self.jdbc_password contains £")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))
					if "¤" in self.jdbc_username:
						print("self.jdbc_username contains ¤")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))
	
					if "," in self.jdbc_url:
						print("self.jdbc_url contains ,")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))
					if "£" in self.jdbc_url:
						print("self.jdbc_url contains £")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))
					if "¤" in self.jdbc_url:
						print("self.jdbc_url contains ¤")
						print("'%s','%s','%s','%s'"%(self.dbAlias, self.jdbc_username, self.jdbc_password, self.jdbc_url))

		print()
	
		logging.debug("Executing common_config.printConnectionAliasDetails() - Finished")

	def lookupConnectionAlias(self, connection_alias, decryptCredentials=True, copySlave=False, exceptionIfFailureToDecrypt=True, jdbcURL=None, quiet=False):
		logging.debug("Executing common_config.lookupConnectionAlias()")
	
		exit_after_function = False
		self.dbAlias = connection_alias
		self.atlasJdbcSourceSupport = False

		self.db_mssql = False
		self.db_oracle = False
		self.db_mysql = False
		self.db_postgresql = False
		self.db_progress = False
		self.db_db2udb = False
		self.db_db2as400 = False
		self.db_mongodb = False
		self.db_cachedb = False
		self.db_snowflake = False
		self.db_awss3 = False
		self.jdbc_servertype = None

		# Fetch data from jdbc_connection table
		if jdbcURL != None and decryptCredentials == False:
			# This is required from RestAPI when looking up all connections and returning a full list including database types. No need to do the lookup here as well
			self.jdbc_url = jdbcURL
		else:
			query = "select jdbc_url, credentials, private_key_path, public_key_path, environment from jdbc_connections where dbalias = %s "
			logging.debug("Executing the following SQL: %s" % (query))
			self.mysql_cursor.execute(query, (connection_alias, ))

			if self.mysql_cursor.rowcount != 1:
				raise invalidConfiguration("The requested connection alias cant be found in the 'jdbc_connections' table")

			row = self.mysql_cursor.fetchone()

			self.jdbc_url = row[0]
			jdbcCredentials = row[1]
			privateKeyFile = row[2]
			publicKeyFile = row[3]
			self.jdbc_environment = row[4]

		if decryptCredentials == True and copySlave == False:
			if jdbcCredentials is not None and jdbcCredentials.startswith("arn:aws:secretsmanager:"):
				# This jdbc connection have its key stored in AWS Secrets Manager
				import boto3
				from botocore.exceptions import ClientError

				# Create a Secrets Manager client
				session = boto3.session.Session()
				client = session.client(
						service_name='secretsmanager',
						region_name=self.awsRegion
						)
	
				try:
					get_secret_value_response = client.get_secret_value(
						SecretId=jdbcCredentials
					)
				except ClientError as e:
					# For a list of exceptions thrown, see
					# https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
					raise e

				jdbcSecrets = json.loads(get_secret_value_response['SecretString'])
				credentials = "%s %s"%(jdbcSecrets["username"], jdbcSecrets["password"])
				# print(json.loads(get_secret_value_response['SecretString']))
				# mysql_username = rds_credentials["username"]
				# mysql_password = rds_credentials["password"]
				# self.remove_temporary_files()
				# sys.exit(1)

			else:
				if privateKeyFile != None and publicKeyFile != None and privateKeyFile.strip() != '' and publicKeyFile.strip() != '':
					self.crypto.setPrivateKeyFile(privateKeyFile)
					self.crypto.setPublicKeyFile(publicKeyFile)
				else:
					self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
					self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))
	
				credentials = self.crypto.decrypt(jdbcCredentials)
			
			if credentials == None and exceptionIfFailureToDecrypt == True:
				raise invalidConfiguration("Cant decrypt username and password. Check private/public key in config file")
		
			if credentials != None:
				try:
					self.jdbc_username = credentials.split(" ")[0]
					self.jdbc_password = credentials.split(" ")[1]
				except IndexError:
					raise invalidConfiguration("Cant decrypt username and password. Please encrypt new credentials with the 'manage --encryptCredentials' function")

				# Sets a creates the password file that is used by sqoop and other tools
				self.jdbc_password_file = self.tempdir + "/jdbc_passwd"
				f = open(self.jdbc_password_file, "w")
				f.write(self.jdbc_password)
				f.close()
				os.chmod(self.jdbc_password_file, 0o600)
			else:
				self.jdbc_username = None
				self.jdbc_password = None
				self.jdbc_password_file = None
		else:
			self.jdbc_username = None
			self.jdbc_password = None
			self.jdbc_password_file = None

		# Lookup Connection details based on JDBC STRING for all different types we support
		if self.jdbc_url.startswith( 'jdbc:sqlserver:'): 
			self.atlasJdbcSourceSupport = True
			self.db_mssql = True
			self.jdbc_servertype = constant.MSSQL
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("SQL Server", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath
			self.jdbc_hostname = self.jdbc_url[17:].split(':')[0].split(';')[0]

			try:
				self.jdbc_port = self.jdbc_url[17:].split(':')[1].split(';')[0]
			except:
				self.jdbc_port = "1433"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "1433"

			try:
				self.jdbc_database = self.jdbc_url.split("database=")[1].split(';')[0]
			except:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True

			try:
				self.jdbc_encrypt = self.jdbc_url.split("encrypt=")[1].split(';')[0].lower()
				if self.jdbc_encrypt == "true": self.jdbc_encrypt = True
			except:
				self.jdbc_encrypt = False

			try:
				self.jdbc_trustedservercert = self.jdbc_url.split("trustServerCertificate=")[1].split(';')[0]
			except:
				self.jdbc_trustedservercert = None

			try:
				self.jdbc_hostincert = self.jdbc_url.split("hostNameInCertificate=")[1].split(';')[0]
			except:
				self.jdbc_hostincert = None

			try:
				self.jdbc_logintimeout = self.jdbc_url.split("loginTimeout=")[1].split(';')[0]
			except:
				self.jdbc_logintimeout = None

			# If all encrypt settings are available, we create an encryption string that will be used by the Schema Python Program
			if self.jdbc_encrypt == True and self.jdbc_trustedservercert != None and self.jdbc_hostincert != None and self.jdbc_logintimeout != None:
				self.jdbc_encrypt_string = "encrypt=true;trustServerCertificate=" + self.jdbc_trustedservercert + ";hostNameInCertificate=" + self.jdbc_hostincert + ";loginTimeout=" + self.jdbc_logintimeout


		if self.jdbc_url.startswith( 'jdbc:jtds:sqlserver:'): 
			self.atlasJdbcSourceSupport = True
			self.db_mssql = True
			self.jdbc_servertype = constant.MSSQL
			self.jdbc_force_column_lowercase = True
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("SQL Server", "jTDS")
			self.jdbc_classpath_for_python = self.jdbc_classpath
			self.jdbc_hostname = self.jdbc_url[22:].split(':')[0].split(';')[0]
			try:
				self.jdbc_port = self.jdbc_url[22:].split(':')[1].split(';')[0]
			except:
				self.jdbc_port = "1433"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "1433"

			try:
				self.jdbc_database = self.jdbc_url.split("databaseName=")[1].split(';')[0]
			except:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:oracle:'): 
			self.atlasJdbcSourceSupport = True
			self.db_oracle = True
			self.jdbc_servertype = constant.ORACLE
			self.jdbc_force_column_lowercase = False
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("Oracle", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath
			self.jdbc_database = "-"

			if self.jdbc_url.startswith('jdbc:oracle:thin:@ldap'):
				pattern = r'@ldap://([^:/]+):(\d+)/([^,]+),'
				r = re.search(pattern, self.jdbc_url)
				if r is None:
					if quiet == False:
						logging.error("jdbc_string for ldap must be of the form jdbc:oracle:thin@ldap://<ldap_host>:<ldap_port>/<db>")
					exit_after_function = True
			else:
				try:
					self.jdbc_hostname = self.jdbc_url.split("(HOST=")[1].split(')')[0]
				except:
					if quiet == False:
						logging.error("Cant determine hostname based on jdbc_string")
					exit_after_function = True

				try:
					self.jdbc_port = self.jdbc_url.split("(PORT=")[1].split(')')[0]
				except:
					if quiet == False:
						logging.error("Cant determine port based on jdbc_string")
					exit_after_function = True

				try:
					self.jdbc_oracle_sid = self.jdbc_url.split("(SID=")[1].split(')')[0]
				except:
					self.jdbc_oracle_sid = None

				try:
					self.jdbc_oracle_servicename = self.jdbc_url.split("(SERVICE_NAME=")[1].split(')')[0]
				except:
					self.jdbc_oracle_servicename = None

				if self.jdbc_oracle_sid == None and self.jdbc_oracle_servicename == None:
					if quiet == False:
						logging.error("Cant find either SID or SERVICE_NAME in Oracle URL")
					exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:mysql:'): 
			self.atlasJdbcSourceSupport = True
			self.db_mysql = True
			self.jdbc_servertype = constant.MYSQL
			self.jdbc_force_column_lowercase = True
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("MySQL", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[13:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[13:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "3306"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "3306"

			try:
				self.jdbc_database = self.jdbc_url[13:].split('/')[1].split(';')[0].split('?')[0]
			except:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:postgresql:'): 
			self.atlasJdbcSourceSupport = True
			self.db_postgresql = True
			self.jdbc_servertype = constant.POSTGRESQL
			self.jdbc_force_column_lowercase = True
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("PostgreSQL", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[18:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[18:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "5432"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "5432"

			try:
				self.jdbc_database = self.jdbc_url[18:].split('/')[1].split(';')[0]
			except:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:datadirect:openedge:'): 
			self.atlasJdbcSourceSupport = True
			self.db_progress = True
			self.jdbc_servertype = constant.PROGRESS
			self.jdbc_force_column_lowercase = True
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("Progress DB", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[27:].split(':')[0].split(';')[0]
			try:
				self.jdbc_port = self.jdbc_url[27:].split(':')[1].split(';')[0]
			except:
				self.jdbc_port = "9999"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "9999"

			try:
				self.jdbc_database = self.jdbc_url.split("databaseName=")[1].split(';')[0]
			except:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:db2://'): 
			self.atlasJdbcSourceSupport = True
			self.db_db2udb = True
			self.jdbc_servertype = constant.DB2_UDB
			self.jdbc_force_column_lowercase = False
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("DB2 UDB", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[11:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[11:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "50000"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "50000"

			try:
				self.jdbc_database = self.jdbc_url[11:].split('/')[1].split(';')[0].split(':')[0]
			except:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True

			try:
				self.jdbc_encrypt = self.jdbc_url.split("sslConnection=")[1].split(';')[0].lower()
				if self.jdbc_encrypt == "true": self.jdbc_encrypt = True
			except:
				self.jdbc_encrypt = False

			try:
				self.jdbc_trustedservercert = self.jdbc_url.split("sslTrustStoreLocation=")[1].split(';')[0]
			except:
				self.jdbc_trustedservercert = False

			try:
				self.jdbc_trustedservercert_password = self.jdbc_url.split("sslTrustStorePassword=")[1].split(';')[0]
			except:
				self.jdbc_trustedservercert_password = False


		if self.jdbc_url.startswith( 'jdbc:as400://'): 
			self.atlasJdbcSourceSupport = True
			self.db_db2as400 = True
			self.jdbc_servertype = constant.DB2_AS400
			self.jdbc_force_column_lowercase = False
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("DB2 AS400", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[13:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[13:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "446"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "446"

			try:
				self.jdbc_database = self.jdbc_url[13:].split('/')[1].split(';')[0].split(':')[0]
			except:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True

		if self.jdbc_url.startswith( 'mongo://'): 
			self.atlasJdbcSourceSupport = False
			self.db_mongodb = True
			self.jdbc_servertype = constant.MONGO
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("MongoDB", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[8:].split(':')[0]
			self.jdbc_port = self.jdbc_url[8:].split(':')[1].split('/')[0]
			self.jdbc_database = self.jdbc_url[8:].split('/')[1].split('?')[0].strip()

			# Get all options in order to find authSource
			try:
				mongoOptions = dict(x.split("=") for x in self.jdbc_url.split('?')[1].split("&"))
				for k, v in mongoOptions.items():
					if ( k.lower() == "authsource" ):
						self.mongoAuthSource = v
			except IndexError:
				pass

		if self.jdbc_url.startswith( 'jdbc:Cache://'): 
			self.atlasJdbcSourceSupport = True
			self.db_cachedb = True
			self.jdbc_servertype = constant.CACHEDB
			self.jdbc_force_column_lowercase = False
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("CacheDB", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[13:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[13:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "1972"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "1972"

			try:
				self.jdbc_database = self.jdbc_url[13:].split('/')[1].split(';')[0].split(':')[0]
			except:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True

		if self.jdbc_url.startswith( 'jdbc:snowflake://'): 
			self.atlasJdbcSourceSupport = False
			self.db_snowflake = True
			self.jdbc_servertype = constant.SNOWFLAKE
			self.jdbc_force_column_lowercase = False
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("SnowFlake", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[17:].split('/')[0]
			self.jdbc_port = None

			database_pos = self.jdbc_url.split('?')[1].find('db')
			if database_pos == -1:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True
			else:
				try:
					self.jdbc_database = self.jdbc_url.split('?')[1][database_pos:].split('=')[1].split('&')[0]
				except:
					if quiet == False:
						logging.error("Cant determine database based on jdbc_string")
					exit_after_function = True

		if self.jdbc_url.startswith( 's3a://'): 
			self.atlasJdbcSourceSupport = False
			self.db_awss3 = True
			self.jdbc_servertype = constant.AWS_S3
			self.jdbc_force_column_lowercase = False
			self.jdbc_driver = ""
			self.jdbc_classpath = ""
			self.jdbc_database = "-"

			self.awsS3bucket = self.jdbc_url.split(';')[0]

			try:
				self.awsS3assumeRole = self.jdbc_url.split(';assumeRole=')[1].split(';')[0]
			except:
				self.awsS3assumeRole = None

			try:
				self.awsS3assumeRoleDuration = int(self.jdbc_url.split(';assumeRoleDuration=')[1].split(';')[0])
			except:
				self.awsS3assumeRoleDuration = None

			try:
				self.awsS3proxyServer = self.jdbc_url.split(';proxy=')[1].split(';')[0]
			except:
				self.awsS3proxyServer = None

			try:
				self.awsS3fileFormat = self.jdbc_url.split(';format=')[1].split(';')[0]
				if self.awsS3fileFormat != "parquet":
					if quiet == False:
						logging.error("Only parquet files are supported for AWS S3")
					exit_after_function = True
			except:
				if quiet == False:
					logging.error("For AWS S3, you need to specify what file format to use")
				exit_after_function = True

			try:
				self.awsS3region = self.jdbc_url.split(';region=')[1].split(';')[0]
			except:
				if quiet == False:
					logging.error("For AWS S3, you need to specify what AWS region to work with in the jdbc_url string")
				exit_after_function = True

			try:
				self.awsS3externalId = self.jdbc_url.split(';externalId=')[1].split(';')[0]
			except:
				self.awsS3externalId = None

		if self.jdbc_url.startswith( 'jdbc:informix-sqli://'): 
			self.atlasJdbcSourceSupport = True
			self.db_informix = True
			self.jdbc_servertype = constant.INFORMIX
			self.jdbc_force_column_lowercase = False
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("Informix", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[21:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[21:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "1526"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "1526"

			try:
				self.jdbc_database = self.jdbc_url[21:].split('/')[1].split(';')[0].split(':')[0]
			except:
				if quiet == False:
					logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True

			if not "delimident=" in self.jdbc_url.lower(): 
				if quiet == False:
					logging.error("Informix requires DELIMIDENT=Y setting on the JDBC connection string")
				exit_after_function = True

		if self.jdbc_url.startswith( 'jdbc:sybase:Tds:'): 
			self.atlasJdbcSourceSupport = True
			self.db_sqlanywhere = True
			self.jdbc_servertype = constant.SQLANYWHERE
			self.jdbc_force_column_lowercase = False
			if jdbcURL == None:
				# jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
				self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("SQL Anywhere", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[16:].split('?')[0]
			if ":" in self.jdbc_hostname:
				self.jdbc_port = self.jdbc_hostname.split(':')[1]
				self.jdbc_hostname = self.jdbc_hostname.split(':')[0]
			else:
				self.jdbc_port = "2638"

			if self.jdbc_port.isdigit() == False: self.jdbc_port = "2638"


		# Add Impala drivers to CP if configured in config file
		try:
			self.impalaDriver = configuration.get("Impala", "driver", exitOnError=False)
			self.impalaClasspath = configuration.get("Impala", "class", exitOnError=False)
			self.impalaAddress = configuration.get("Impala", "address", exitOnError=False)
			if self.impalaDriver != None and self.impalaDriver.startswith("/path/to/") == False:
				self.jdbc_classpath += ":%s"%(self.impalaDriver)
				self.jdbc_classpath_for_python = self.jdbc_classpath
		except:
			pass

		# Check to make sure that we have a supported JDBC string
		if self.jdbc_servertype == "":
			if quiet == False:
				logging.error("JDBC Connection '%s' is not supported."%(self.jdbc_url))
			exit_after_function = True

		if copySlave == True:
			self.jdbc_hostname = None
			self.jdbc_port = None
			self.jdbc_database = None

		logging.debug("    db_mssql = %s"%(self.db_mssql))
		logging.debug("    db_oracle = %s"%(self.db_oracle))
		logging.debug("    db_mysql = %s"%(self.db_mysql))
		logging.debug("    db_postgresql = %s"%(self.db_postgresql))
		logging.debug("    db_progress = %s"%(self.db_progress))
		logging.debug("    db_db2udb = %s"%(self.db_db2udb))
		logging.debug("    db_db2as400 = %s"%(self.db_db2as400))
		logging.debug("    db_mongodb = %s"%(self.db_mongodb))
		logging.debug("    db_cachedb = %s"%(self.db_cachedb))
		logging.debug("    db_snowflake = %s"%(self.db_snowflake))
		logging.debug("    db_informix = %s"%(self.db_informix))
		logging.debug("    jdbc_servertype = %s"%(self.jdbc_servertype))
		logging.debug("    jdbc_url = %s"%(self.jdbc_url))
		logging.debug("    jdbc_username = %s"%(self.jdbc_username))
		logging.debug("    jdbc_password = <ENCRYPTED>")
		logging.debug("    jdbc_hostname = %s"%(self.jdbc_hostname))
		logging.debug("    jdbc_port = %s"%(self.jdbc_port))
		logging.debug("    jdbc_database = %s"%(self.jdbc_database))
		logging.debug("    jdbc_classpath = %s"%(self.jdbc_classpath))
		logging.debug("    jdbc_classpath_for_python = %s"%(self.jdbc_classpath_for_python))
		logging.debug("    jdbc_driver = %s"%(self.jdbc_driver))
		logging.debug("    jdbc_driver_for_python = %s"%(self.jdbc_driver_for_python))
		logging.debug("    jdbc_ad_domain = %s"%(self.jdbc_ad_domain))
		logging.debug("    jdbc_encrypt = %s"%(self.jdbc_encrypt))
		logging.debug("    jdbc_encrypt_string = %s"%(self.jdbc_encrypt_string))
		logging.debug("    jdbc_trustedservercert = %s"%(self.jdbc_trustedservercert))
		logging.debug("    jdbc_trustedservercert_password = %s"%(self.jdbc_trustedservercert_password))
		logging.debug("    jdbc_hostincert = %s"%(self.jdbc_hostincert))
		logging.debug("    jdbc_logintimeout = %s"%(self.jdbc_logintimeout))
		logging.debug("    jdbc_oracle_sid = %s"%(self.jdbc_oracle_sid))
		logging.debug("    jdbc_oracle_servicename = %s"%(self.jdbc_oracle_servicename))
		logging.debug("    self.awsS3assumeRole = %s"%(self.awsS3assumeRole))
		logging.debug("    self.awsS3assumeRoleDuration = %s"%(self.awsS3assumeRoleDuration))
		logging.debug("    self.awsS3proxyServer = %s"%(self.awsS3proxyServer))
		logging.debug("    self.awsS3externalId = %s"%(self.awsS3externalId))
		logging.debug("    self.awsS3region = %s"%(self.awsS3region))
		logging.debug("Executing common_config.lookupConnectionAlias() - Finished")

		if exit_after_function == True:
			raise invalidConfiguration


	def getJDBCTableDefinition(self, source_schema, source_table, printInfo=True):
		logging.debug("Executing common_config.getJDBCTableDefinition()")
		if printInfo == True:
			logging.info("Reading SQL table definitions from source database")
		self.source_schema = source_schema
		self.source_table = source_table

		# Connect to the source database
		self.connectToJDBC()

		self.source_columns_df = self.sourceSchema.readTableColumns(  self.JDBCCursor, 
														serverType = self.jdbc_servertype, 
														database = self.jdbc_database,
														schema = self.source_schema,
														table = self.source_table)

		self.source_keys_df = self.sourceSchema.readTableKeys(	self.JDBCCursor,
														serverType = self.jdbc_servertype, 
														database = self.jdbc_database,
														schema = self.source_schema,
														table = self.source_table)


		self.source_index_df = self.sourceSchema.readTableIndex(	self.JDBCCursor,
														serverType = self.jdbc_servertype, 
														database = self.jdbc_database,
														schema = self.source_schema,
														table = self.source_table)

		try: 
			self.tableType = self.sourceSchema.getJdbcTableType(self.jdbc_servertype, self.source_columns_df.iloc[0]["TABLE_TYPE"])
		except IndexError:
			self.tableType = "unknown"

		logging.debug("Executing common_config.getSourceTableDefinition() - Finished")

	def reconnectJDBC(self, logger=""):
		self.disconnectFromJDBC(logger=logger)
		self.connectToJDBC(logger=logger)

	def disconnectFromJDBC(self, logger=""):
		log = logging.getLogger(logger)
		log.debug("Disconnect from JDBC database")

		if self.db_mongodb == True:
			# This is a MongoDB connection. Lets rediret to disconnectFromMongo instead
			return self.disconnectFromMongo()

		try:
			self.JDBCCursor.close()
			self.JDBCConn.close()

		except AttributeError:
			pass

		except Exception as exception:
			log.debug("Unknown error during disconnection from JDBC database:")
			log.debug(exception.message())
			pass


		self.JDBCCursor = None
		

	def connectToImpala(self, logger=""):
		log = logging.getLogger(logger)
		if self.impalaDriver == None:
			log.info("Impala is not configured")
			return

		if self.impalaJDBCCursor == None:
			log.info("Connecting to Impala")
			log.debug("Connecting to Impala over JDBC")
			log.debug("   impalaDriver = %s"%(self.impalaDriver))
			log.debug("   impalaClasspath = %s"%(self.impalaClasspath))
			log.debug("   impalaAddress = %s"%(self.impalaAddress))

			try:
				JDBCCredentials = [ self.jdbc_username, self.jdbc_password ]

				self.impalaJDBCConn = jaydebeapi.connect(self.impalaClasspath, self.impalaAddress, [], self.impalaDriver)
				self.impalaJDBCCursor = self.impalaJDBCConn.cursor()
				log.info("Connected to Impala")
			except Exception as exception:
				log.error("Connection to Impala failed with the following error:")
				log.error(exception.message())


	def disconnectFromImpala(self, logger=""):
		log = logging.getLogger(logger)
		log.info("Disconnecting from Impala")

		try:
			self.impalaJDBCCursor.close()
			self.impalaJDBCConn.close()

		except AttributeError:
			pass

		except Exception as exception:
			log.info("Unknown error during disconnection from Impala:")
			log.info(exception.message())
			pass


		self.impalaJDBCCursor = None
		

	def connectToJDBC(self, allJarFiles=False, exitIfFailure=True, logger="", printError=True):
		log = logging.getLogger(logger)

		if self.db_mongodb == True:
			# This is a MongoDB connection. Lets rediret to connectToMongo instead
			return self.connectToMongo(exitIfFailure=exitIfFailure, logger=logger)

		if allJarFiles == True:
			query = "select classpath from jdbc_connections_drivers"
			log.debug("Executing the following SQL: %s" % (query))
			self.mysql_cursor.execute(query, )

			self.jdbc_classpath_for_python = []
			for row in self.mysql_cursor.fetchall():
				if row[0] != "add path to JAR file":
					self.jdbc_classpath_for_python.append(row[0])

		if self.jdbc_driver == None:
			log.error("There is no support for the configured JDBC Connection on this db alias. Please check configuration")
			self.remove_temporary_files()
			sys.exit(1)

		if self.JDBCCursor == None:
			log.debug("Connecting to database over JDBC")
			log.debug("   jdbc_username = %s"%(self.jdbc_username))
			log.debug("   jdbc_password = <ENCRYPTED>")
			log.debug("   jdbc_driver = %s"%(self.jdbc_driver))
			log.debug("   jdbc_url = %s"%(self.jdbc_url))
			log.debug("   jdbc_classpath_for_python = %s"%(self.jdbc_classpath_for_python))

			JDBCCredentials = [ self.jdbc_username, self.jdbc_password ]
			try:
				self.JDBCConn = jaydebeapi.connect(self.jdbc_driver, self.jdbc_url, JDBCCredentials , self.jdbc_classpath_for_python)
				self.JDBCCursor = self.JDBCConn.cursor()
			except TypeError as exception:
				if printError == True:
					log.error("Connection to database over JDBC failed with the following error:")
					log.error(str(exception))
				if exitIfFailure == True:
					self.remove_temporary_files()
					sys.exit(1)
				else:
					return False
			except Exception as exception:
				if printError == True:
					log.error("Connection to database over JDBC failed with the following error:")
					log.error(exception.message())
				if exitIfFailure == True:
					self.remove_temporary_files()
					sys.exit(1)
				else:
					return False

		return True

	def getJDBCcolumnMaxValue(self, source_schema, source_table, column):
		logging.debug("Executing common_config.getJDBCcolumnMaxValue()")

		self.connectToJDBC()
		query = None
	
		if self.db_mssql == True:
			query = "select max(%s) from [%s].[%s].[%s]"%(column, self.jdbc_database, source_schema, source_table) 

		if self.db_oracle == True:
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema.upper(), source_table.upper())

		if self.db_mysql == True:
			query = "select max(%s) from %s"%(column, source_table)

		if self.db_postgresql == True:
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema.lower(), source_table.lower())

		if self.db_progress == True:
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema, source_table)

		if self.db_db2udb == True:
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema, source_table)

		if self.db_db2as400 == True:		
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema, source_table)

		if self.db_snowflake == True:		
			query = "select max(\"%s\") from \"%s\".\"%s\""%(column, source_schema, source_table)

		if self.db_informix == True:		
			query = "select max(\"%s\") from \"%s\".\"%s\""%(column, source_schema, source_table)

		if self.custom_max_query != None:
			# If a custom query is configured, we just use that instead and ignore the config above
			query = self.custom_max_query
			logging.info("Using a custom query to get Max value from source table (%s)"%(query))

		self.JDBCCursor.execute(query)
		logging.debug("SQL Statement executed: %s" % (query) )
		row = self.JDBCCursor.fetchone()

		logging.info("Max value that will be used in the incremental import is '%s'"%(row[0]))

		return row[0]

		logging.debug("Executing common_config.getJDBCcolumnMaxValue() - Finished")

	def truncateJDBCTable(self, schema, table):
		""" Truncates a table on the JDBC connection """
		logging.debug("Executing common_config.truncateJDBCTable()")
		self.connectToJDBC()
		query = None

		if self.db_oracle == True:
			query = "truncate table \"%s\".\"%s\""%(schema.upper(), table.upper())

		if self.db_mssql == True:
			query = "truncate table %s.%s"%(schema, table)

		if self.db_db2udb == True:
			query = "truncate table \"%s\".\"%s\" immediate"%(schema.upper(), table.upper())

		if self.db_mysql == True:
			query = "truncate table %s"%(table)

		if self.db_postgresql == True:
			query = "truncate table %s.%s"%(schema.lower(), table.lower())

		if self.db_snowflake == True:
			query = "truncate table \"%s\".\"%s\""%(schema, table)

		if query == None:
			raise undevelopedFeature("There is no support for this database type in common_config.truncateJDBCTable()") 
		
		logging.debug("SQL Statement executed: %s" % (query) )
		self.JDBCCursor.execute(query)

		logging.debug("Executing common_config.truncateJDBCTable() - Finished")

	def checkJDBCTable(self, schema, table):
		""" Checks if a table exists on the JDBC connections. Return True or False """ 
		logging.debug("Executing common_config.checkJDBCTable()")
		self.connectToJDBC()
		query = None
	
		if self.db_oracle == True:
			query = "select count(owner) from all_tables where owner = '%s' and table_name = '%s'"%(schema.upper(), table.upper())

		if self.db_mssql == True:
			query = "select count(table_name) from INFORMATION_SCHEMA.COLUMNS where table_schema = '%s' and table_name = '%s'"%(schema, table)

		if self.db_db2udb == True:
			query = "select count(name) from SYSIBM.SYSTABLES where upper(creator) = '%s' and upper(name) = '%s'"%(schema.upper(), table.upper())

		if self.db_mysql == True:
			query = "select count(table_name) from information_schema.tables where table_schema = '%s' and table_name = '%s'"%(self.jdbc_database, table)

		if self.db_postgresql == True:
			query = "select count(table_name) from information_schema.tables where table_catalog = '%s' and table_schema = '%s' and table_name = '%s'"%(self.jdbc_database, schema.lower(), table.lower())

		if self.db_snowflake == True:
			query = "select count(table_name) from information_schema.tables where table_catalog = '%s' and table_schema = '%s' and table_name = '%s'"%(self.jdbc_database, schema, table)

		if query == None:
			raise undevelopedFeature("There is no support for this database type in common_config.checkJDBCTable()") 


		logging.debug("SQL Statement executed: %s" % (query) )
		self.JDBCCursor.execute(query)
		row = self.JDBCCursor.fetchone()

		tableExists = False

		if int(row[0]) > 0:
			tableExists = True

		logging.debug("Executing common_config.checkJDBCTable() - Finished")
		return tableExists

	def executeJDBCquery(self, query):
		""" Executes a query against the JDBC database and return the values in a Pandas DF """
		logging.debug("Executing common_config.executeJDBCquery()")

		logging.debug("Query to execute: %s"%(query))
		try:
			self.connectToJDBC()
			self.JDBCCursor.execute(query)
		except jaydebeapi.DatabaseError as errMsg:
			raise SQLerror(errMsg)	

		result_df = pd.DataFrame()
		try:
			result_df = pd.DataFrame(self.JDBCCursor.fetchall())
			if result_df.empty == False:
				result_df_columns = []
				for columns in self.JDBCCursor.description:
					result_df_columns.append(columns[0])    # Name of the column is in the first position
				result_df.columns = result_df_columns
		except jaydebeapi.Error:
			logging.debug("An error was raised during JDBCCursor.fetchall(). This happens during SQL operations that dont return any rows like 'create table'")
			
		# Set the correct column namnes in the DataFrame

		logging.debug("Executing common_config.executeJDBCquery() - Finished")
		return result_df

	def executeImpalaQuery(self, query):
		""" Executes a query against Impala and return the values in a Pandas DF """
		logging.debug("Executing common_config.executeImpalaQuery()")

		logging.debug("Query to execute: %s"%(query))
		try:
			self.connectToImpala()
			self.impalaJDBCCursor.execute(query)
		except jaydebeapi.DatabaseError as errMsg:
			raise SQLerror(errMsg)	

		result_df = pd.DataFrame()
		try:
			result_df = pd.DataFrame(self.impalaJDBCCursor.fetchall())
			if result_df.empty == False:
				result_df_columns = []
				for columns in self.impalaJDBCCursor.description:
					result_df_columns.append(columns[0])    # Name of the column is in the first position
				result_df.columns = result_df_columns
		except jaydebeapi.Error:
			logging.debug("An error was raised during impalaJDBCCursor.fetchall(). This happens during SQL operations that dont return any rows like 'create table'")
			
		# Set the correct column namnes in the DataFrame

		logging.debug("Executing common_config.executeImpalaQuery() - Finished")
		return result_df


	def getJDBCTableRowCount(self, source_schema, source_table, whereStatement=None):
		logging.debug("Executing common_config.getJDBCTableRowCount()")

		self.connectToJDBC()
		query = None
	
		if self.db_mssql == True:
			query = "select count_big(1) from [%s].[%s].[%s]"%(self.jdbc_database, source_schema, source_table) 

		if self.db_oracle == True:
			query = "select count(1) from \"%s\".\"%s\""%(source_schema.upper(), source_table.upper())

		if self.db_mysql == True:
			query = "select count(1) from %s" % source_table

		if self.db_postgresql == True:
			query = "select count(1) from \"%s\".\"%s\""%(source_schema.lower(), source_table.lower())

		if self.db_progress == True:
			query = "select count(1) from \"%s\".\"%s\""%(source_schema, source_table)

		if self.db_db2udb == True:
			query = "select cast(count(1) as bigint) from \"%s\".\"%s\""%(source_schema, source_table)

		if self.db_db2as400 == True:		
			query = "select count(1) from \"%s\".\"%s\""%(source_schema, source_table)

		if self.db_snowflake == True:		
			query = "select count(1) from \"%s\".\"%s\""%(source_schema, source_table)

		if self.db_informix == True:		
			query = "select count(1) from %s.%s"%(source_schema, source_table)

		if self.db_sqlanywhere == True:		
			query = "select count(1) from %s.%s"%(source_schema, source_table)

		if whereStatement != None and whereStatement != "":
			query = query + " where " + whereStatement

		logging.debug("SQL Statement executed: %s" % (query) )
		self.JDBCCursor.execute(query)
		row = self.JDBCCursor.fetchone()

		return int(row[0])

		logging.debug("Executing common_config.getJDBCTableRowCount() - Finished")

	def dropJDBCTable(self, schema, table):
		logging.debug("Executing common_config.dropJDBCTable()")

		self.connectToJDBC()
		query = "drop table %s"%(self.getJDBCsqlFromTable(schema=schema, table=table))
		self.JDBCCursor.execute(query)

		logging.debug("Executing common_config.dropJDBCTable() - Finished")

	def logImportFailure(self, errorText, severity, importType=None, hiveDB=None, hiveTable=None):

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		query  = "insert into import_failure_log "
		query += "( hive_db, hive_table, eventtime, severity, import_type, error_text ) "
		query += "values "
		query += "( %s, %s, %s, %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), severity, importType, errorText))
		self.mysql_conn.commit()

	def logHiveColumnAdd(self, column, columnType=None, description=None, hiveDB=None, hiveTable=None):
		if description == None:
			description = "Column '%s' added to table with type '%s'"%(column, columnType)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		query  = "insert into table_change_history "
		query += "( hive_db, hive_table, column_name, eventtime, event, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, 'column_added', %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), columnType, description))
		self.mysql_conn.commit()

	def logHiveColumnTypeChange(self, column, columnType, previous_columnType=None, description=None, hiveDB=None, hiveTable=None):

		if description == None:
			if previous_columnType == None:
				description = "Column '%s' type changed to %s"%(column, columnType)
			else:
				description = "Column '%s' type changed from %s to %s"%(column, previous_columnType, columnType)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		query  = "insert into table_change_history "
		query += "( hive_db, hive_table, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, 'column_type_change', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnType, columnType, description))
		self.mysql_conn.commit()

	def logHiveColumnRename(self, columnName, previous_columnName, description=None, hiveDB=None, hiveTable=None):

		if description == None:
			description = "Column '%s' renamed to %s"%(previous_columnName, columnName)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		query  = "insert into table_change_history "
		query += "( hive_db, hive_table, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, 'column_rename', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, columnName, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnName, columnName, description))
		self.mysql_conn.commit()

	def logJDBCColumnAdd(self, column, columnType=None, description=None, dbAlias=None, database=None, schema=None, table=None):
		if description == None:
			description = "Column '%s' added to table with type '%s'"%(column, columnType)

		query  = "insert into jdbc_table_change_history "
		query += "( dbalias, db_name, schema_name, table_name, column_name, eventtime, event, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, %s, %s, 'column_added', %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (dbAlias, database, schema, table, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), columnType, description))
		self.mysql_conn.commit()

	def logJDBCColumnTypeChange(self, column, columnType, previous_columnType=None, description=None, dbAlias=None, database=None, schema=None, table=None):

		if description == None:
			if previous_columnType == None:
				description = "Column '%s' type changed to %s"%(column, columnType)
			else:
				description = "Column '%s' type changed from %s to %s"%(column, previous_columnType, columnType)

		query  = "insert into jdbc_table_change_history "
		query += "( dbalias, db_name, schema_name, table_name, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, %s, %s, 'column_type_change', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (dbAlias, database, schema, table, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnType, columnType, description))
		self.mysql_conn.commit()

	def logJDBCColumnRename(self, columnName, previous_columnName, description=None, dbAlias=None, database=None, schema=None, table=None):

		if description == None:
			description = "Column '%s' renamed to %s"%(previous_columnName, columnName)

		query  = "insert into jdbc_table_change_history "
		query += "( dbalias, db_name, schema_name, table_name, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, %s, %s, 'column_rename', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (dbAlias, database, schema, table, columnName, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnName, columnName, description))
		self.mysql_conn.commit()

	def getQuoteAroundColumn(self):
		quoteAroundColumn = ""
		if self.jdbc_servertype == constant.MSSQL:        quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.ORACLE:       quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.MYSQL:        quoteAroundColumn = "`"
		if self.jdbc_servertype == constant.POSTGRESQL:   quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.PROGRESS:     quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.DB2_UDB:      quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.DB2_AS400:    quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.SNOWFLAKE:    quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.INFORMIX:     quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.SQLANYWHERE:  quoteAroundColumn = "`"

		return quoteAroundColumn

	def getJDBCUpperCase(self):
		upperCase = False
		if self.jdbc_servertype == constant.ORACLE:       upperCase = True
		if self.jdbc_servertype == constant.DB2_UDB:      upperCase = True

		return upperCase

	def getJDBCsqlFromTable(self, schema, table):
		logging.debug("Executing common_config.getJDBCsqlFromTable()")

		fromTable = ""
		if self.jdbc_servertype == constant.MSSQL:        fromTable = "[%s].[%s].[%s]"%(self.jdbc_database, schema, table)
		if self.jdbc_servertype == constant.ORACLE:       fromTable = "\"%s\".\"%s\""%(schema.upper(), table.upper())
		if self.jdbc_servertype == constant.MYSQL:        fromTable = "%s"%(table)
		if self.jdbc_servertype == constant.POSTGRESQL:   fromTable = "\"%s\".\"%s\""%(schema.lower(), table.lower())
		if self.jdbc_servertype == constant.PROGRESS:     fromTable = "\"%s\".\"%s\""%(schema, table)
		if self.jdbc_servertype == constant.DB2_UDB:      fromTable = "\"%s\".\"%s\""%(schema, table)
		if self.jdbc_servertype == constant.DB2_AS400:    fromTable = "\"%s\".\"%s\""%(schema, table)
		if self.jdbc_servertype == constant.SNOWFLAKE:    fromTable = "\"%s\".\"%s\""%(schema, table)
		if self.jdbc_servertype == constant.INFORMIX:     fromTable = "%s.%s"%(schema, table)
		if self.jdbc_servertype == constant.SQLANYWHERE:  fromTable = "%s"%(table)

		logging.debug("Executing common_config.getJDBCsqlFromTable() - Finished")
		return fromTable

	def getConfigValueColumn(self, key, ignoreErrorOutput = False):
		""" Return the type of the column that have the value for the specified key """

		valueColumn = ""
		boolValue = False

		if key in ("hive_remove_locks_by_force", "airflow_disable", "import_start_disable", "import_stage_disable", "export_start_disable", "export_stage_disable", "hive_validate_before_execution", "hive_print_messages", "import_process_empty", "hive_major_compact_after_merge", "hive_insert_only_tables", "hive_acid_with_clusteredby", "post_data_to_kafka", "post_data_to_kafka_extended", "post_data_to_rest", "post_data_to_rest_extended", "post_data_to_awssns", "post_data_to_awssns_extended", "post_airflow_dag_operations", "rest_verifyssl", "impala_invalidate_metadata", "airflow_aws_pool_to_instanceid", "airflow_create_pool_with_task", "timestamp_with_timezone" ):
			valueColumn = "valueInt"
			boolValue = True
		elif key in ("spark_max_executors", "import_default_sessions", "import_max_sessions", "export_default_sessions", "export_max_sessions", "atlas_discovery_interval", "airflow_major_version", "rest_timeout", "airflow_default_pool_size", "restserver_token_ttl"):
			valueColumn = "valueInt"
		elif key in ("import_staging_table", "import_staging_database", "import_work_table", "import_work_database", "import_history_table", "import_history_database", "export_staging_database", "hive_validate_table", "airflow_aws_instanceids", "airflow_sudo_user", "airflow_dbimport_commandpath", "airflow_dag_directory", "airflow_dag_staging_directory", "timezone", "airflow_dag_file_group", "airflow_dag_file_permission", "airflow_dummy_task_queue", "airflow_url", "cluster_name", "hdfs_address", "hdfs_blocksize", "hdfs_basedir", "kafka_brokers", "kafka_saslmechanism", "kafka_securityprotocol", "kafka_topic", "kafka_trustcafile", "rest_url", "rest_trustcafile", "import_columnname_delete", "import_columnname_import", "import_columnname_insert", "import_columnname_iud", "import_columnname_update", "import_columnname_histtime", "import_columnname_source", "restserver_secret_key", "restserver_admin_user", "restserver_authentication_method", "post_data_to_awssns_topic"):
			valueColumn = "valueStr"
		else:
			if ignoreErrorOutput == False:
				# This is needed from the REST interface, as it's parsing everything in the database. And entries may exists that isnt a valid config anymore
				logging.error("There is no configuration with the name '%s'"%(key))
				self.remove_temporary_files()
				sys.exit(1)
			else:
				valueColumn = None
				boolValue = None

		return (valueColumn, boolValue)


	def getConfigValue(self, key):
		""" Returns a value from the configuration table based on the supplied key. Value returned can be Int, Str or DateTime""" 
		logging.debug("Executing common_config.getConfigValue()")
		returnValue = None
		boolValue = False
	
		valueColumn, boolValue = self.getConfigValueColumn(key)

		query = "select %s from configuration where configkey = '%s'"%(valueColumn, key)

		logging.debug("SQL Statement executed: %s" % (query) )
		self.mysql_cursor.execute(query)
		row = self.mysql_cursor.fetchone()
		self.mysql_conn.commit()

		try:
			if valueColumn == "valueInt":
				returnValue = int(row[0])
	
			if valueColumn == "valueStr":
				returnValue = row[0]
				if returnValue == None or returnValue.strip() == "":
					logging.error("Configuration Key '%s' must have a value in '%s'"%(key, valueColumn))
					self.remove_temporary_files()
					sys.exit(1)

			if boolValue == True:
				if returnValue == 1:
					returnValue = True
				elif returnValue == 0:
					returnValue = False
				else:
					logging.error("Configuration Key '%s' can only have 0 or 1 in column '%s'"%(key, valueColumn))
					self.remove_temporary_files()
					sys.exit(1)
		except TypeError:
			logging.error("Configuration key '%s' cant be found. Have you upgraded the database schema to the latest version?"%(key))
			self.remove_temporary_files()
			sys.exit(1)

		logging.debug("Fetched configuration '%s' as '%s'"%(key, returnValue))
		logging.debug("Executing common_config.getConfigValue() - Finished")
		return returnValue

	def connectToMongo(self, exitIfFailure=True, logger=""):
		log = logging.getLogger(logger)
		log.debug("Executing common_config.connectToMongo()")

		if self.mongoClient != None:
			return True

		try:
			self.mongoClient = pymongo.MongoClient("mongodb://%s:%s/"%(self.jdbc_hostname, self.jdbc_port))
			self.mongoDB = self.mongoClient[self.jdbc_database.strip()]
			if ( self.mongoAuthSource == None ):
				self.mongoDB.authenticate(self.jdbc_username, self.jdbc_password )
			else:
				self.mongoDB.authenticate(self.jdbc_username, self.jdbc_password, source=self.mongoAuthSource )

		except pymongo.errors.ServerSelectionTimeoutError:
			log.error("Timeout Error when connecting to Mongo at %s.%s"%(self.jdbc_hostname, self.jdbc_port))
			if exitIfFailure == True:
				self.remove_temporary_files()
				sys.exit(1)
			else:
				return False

		return True

		log.debug("Executing common_config.connectToMongo() - Finished")

	def disconnectFromMongo(self):
		logging.debug("Executing common_config.disconnectFromMongo()")

		self.mongoClient.close()
		self.mongoClient = None
		self.mongoDB = None

		logging.debug("Executing common_config.disconnectFromMongo() - Finished")

	def getMongoCollections(self, collectionFilter=None):
		logging.debug("Executing common_config.getMongoTables()")

		# Connect to MongoDB and get a list of all collections
		self.connectToMongo()
		mongoCollections = self.mongoDB.list_collection_names()
		self.disconnectFromMongo()

		if collectionFilter == None:
			collectionFilter = ""
		collectionFilter = collectionFilter.replace('*', '.*')

		collectionList = []
		for collection in mongoCollections:
			if re.search(collectionFilter, collection):
				collectionDict = {"schema": "-"}
				collectionDict["table"] = collection
				collectionList.append(collectionDict)

		result_df = pd.DataFrame(collectionList)

		logging.debug("Executing common_config.getMongoTables() - Finished")
		return result_df

	def getJDBCtablesAndViews(self, schemaFilter=None, tableFilter=None):
		logging.debug("Executing common_config.getJDBCtablesAndViews()")
		self.connectToJDBC()

		result_df = self.sourceSchema.getJDBCtablesAndViews(
			JDBCCursor=self.JDBCCursor,
			serverType=self.jdbc_servertype,
			database=self.jdbc_database,
			schemaFilter=schemaFilter,
			tableFilter=tableFilter)

		logging.debug("Executing common_config.getJDBCtablesAndViews() - Finished")
		return result_df

	def getAtlasDiscoverConfigObject(self, dbAlias=None, logger=""):
		""" Discover all RDBMS objects on the 'dbAlias' and populate Atlas with them """
		log = logging.getLogger(logger)
		log.debug("Executing common_config.getAtlasDiscoverConfigObject()")

		if dbAlias == None:
			dbAlias = self.dbAlias

		self.lookupConnectionAlias(dbAlias)
		jdbcConnectionData = self.getAtlasJdbcConnectionData(dbAlias)

		configObject = {}
		configObject["dbAlias"] = dbAlias
		configObject["contactInfo"] = jdbcConnectionData.get('contact_info')
		configObject["description"] = jdbcConnectionData.get('description')
		configObject["owner"] = jdbcConnectionData.get('owner')
		configObject["atlasIncludeFilter"] = jdbcConnectionData.get('atlas_include_filter')
		configObject["atlasExcludeFilter"] = jdbcConnectionData.get('atlas_exclude_filter')
		configObject["jdbc_hostname"] = self.jdbc_hostname
		configObject["jdbc_port"] = self.jdbc_port
		configObject["jdbc_servertype"] = self.jdbc_servertype
		configObject["jdbc_database"] = self.jdbc_database
		configObject["jdbc_oracle_sid"] = self.jdbc_oracle_sid
		configObject["jdbc_oracle_servicename"] = self.jdbc_oracle_servicename
		configObject["jdbc_username"] = self.jdbc_username
		configObject["jdbc_password"] = self.jdbc_password
		configObject["jdbc_driver"] = self.jdbc_driver
		configObject["jdbc_url"] = self.jdbc_url
		configObject["jdbc_classpath_for_python"] = self.jdbc_classpath_for_python
		configObject["jdbc_environment"] = self.jdbc_environment
		configObject["hdfs_address"] = self.getConfigValue(key = "hdfs_address")
		configObject["cluster_name"] = self.getConfigValue(key = "cluster_name")

		log.debug("Executing common_config.getAtlasDiscoverConfigObject() - Finished")
		return configObject

	def getAnonymizationSeed(self):
		logging.debug("Executing common_config.getAnonymizationSeed()")

		# Set the default seed from the configuration file
		seed = configuration.get("Anonymization", "seed")

		query = "select seed_file from jdbc_connections where dbalias = %s "
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, (self.dbAlias, ))

		row = self.mysql_cursor.fetchone()
		seedFile = row[0]

		if seedFile != None:
			if os.path.exists(seedFile) == False:
				logging.error("The connection have a seed file configured, but the tool is unable to find that file.")
				self.remove_temporary_files()
				sys.exit(1)

			seed = open(seedFile,"r").read()

		if len(seed) > 16:
			logging.warning("The seed is longer that 16 characters. Will truncate the seed to only include the first 16 characters")
			seed = seed[:16]

		logging.debug("Executing common_config.getAnonymizationSeed() - Finished")
		return seed

	def stripUnwantedCharComment(self, work_string):
		if work_string == None: return
		work_string = work_string.replace('`', '')
		work_string = work_string.replace('\'', '')
		work_string = work_string.replace(';', '')
		work_string = work_string.replace('\n', '')
		work_string = work_string.replace('\\', '')
		work_string = work_string.replace('’', '')
		work_string = work_string.replace('"', '')
		return work_string.strip()

	def stripUnwantedCharColumnName(self, work_string):
		if work_string == None: return
		work_string = work_string.replace('`', '')
		work_string = work_string.replace('\'', '')
		work_string = work_string.replace(';', '')
		work_string = work_string.replace('\n', '')
		work_string = work_string.replace('\\', '')
		work_string = work_string.replace('’', '')
		work_string = work_string.replace(':', '')
		work_string = work_string.replace(',', '')
		work_string = work_string.replace('.', '')
		work_string = work_string.replace('"', '')
		return work_string.strip()

	def saveJsonToDatabase(self, datatype, destination, json):
		""" Saves a json that was unable to be sent successfully to the 'json_to_send' table so it can be sent at a later time """
		logging.debug("Executing common_config.saveJsonToDatabase()")

		query  = "insert into json_to_send "
		query += "( type, status, destination, jsondata ) "
		query += "values "
		query += "( %s, 0, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (datatype, destination, json))
		self.mysql_conn.commit()

		logging.debug("Executing common_config.saveJsonToDatabase() - Finished")


	def updateYarnStatistics(self, yarnApplicationID, operationTool, yarnContainersTotal = None, yarnContainersFailed = None, yarnContainersKilled = None): 
		""" insert or update data to yarn_statistics """
		logging.debug("Executing common_config.updateYarnStatistics()")

		query = None
		startStopDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		if yarnContainersTotal == None:
			# If it's None, it means that it was called when query was started. At this stage, we need to insert the data

			if self.operationType == "import": 
				self.yarnApplicationStart = startStopDate

				query  = "insert into yarn_statistics "
				query += "( yarn_application_id, operation_timestamp, operation_tool, operation_type, hive_db, hive_table, application_start ) "
				query += "values "
				query += "( %s, %s, %s, 'import', %s, %s, %s )"

				logging.debug("SQL Statement executed: %s" % (query))
				self.mysql_cursor.execute(query, (yarnApplicationID, self.operationTimestamp, operationTool, self.Hive_DB, self.Hive_Table, startStopDate))
				self.mysql_conn.commit()

			if self.operationType == "export": 
				self.yarnApplicationStart = startStopDate

				query  = "insert into yarn_statistics "
				query += "( yarn_application_id, operation_timestamp, operation_tool, operation_type, dbalias, target_schema, target_table, application_start ) "
				query += "values "
				query += "( %s, %s, %s, 'export', %s, %s, %s, %s )"

				logging.debug("SQL Statement executed: %s" % (query))
				self.mysql_cursor.execute(query, (yarnApplicationID, self.operationTimestamp, operationTool, self.dbAlias, self.targetSchema, self.targetTable, startStopDate))
				self.mysql_conn.commit()


		else:
			if self.yarnApplicationStart == None:
				# This is just a protection-function and should under normal operations never happen as a query must start before we get any data up update about. 
				return

			query  = "update yarn_statistics "
			query += "set application_stop = %s, yarn_containers_total = %s, yarn_containers_failed = %s, yarn_containers_killed = %s "
			query += "where yarn_application_id = %s and application_start = %s"

			logging.debug("SQL Statement executed: %s" % (query))
			self.mysql_cursor.execute(query, (startStopDate, yarnContainersTotal, yarnContainersFailed, yarnContainersKilled, yarnApplicationID, self.yarnApplicationStart))
			self.mysql_conn.commit()

		logging.debug("Executing common_config.updateYarnStatistics() - Finished")
