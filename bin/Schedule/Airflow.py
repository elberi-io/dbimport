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
import re
import json
import boto3
import botocore
import random
from ConfigReader import configuration
import pendulum
from urllib.parse import quote
from datetime import date, datetime, time, timedelta
import pandas as pd
from common import constants as constant
from common.Exceptions import *
from Schedule import airflowSchema
from DBImportConfig import configSchema
from DBImportConfig import common_config
from DBImportConfig import sendStatistics
import sqlalchemy as sa
# from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select
from sqlalchemy.orm import aliased, sessionmaker, Query, scoped_session


class initialize(object):
	def __init__(self):
		logging.debug("Executing Airflow.__init__()")

		self.mysql_conn = None
		self.mysql_cursor = None
		self.debugLogLevel = False

		self.logger = "root"
		log = logging.getLogger(self.logger)

		self.configDBSession = None
		self.configDB = None

		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

		self.common_config = common_config.config()
		self.sendStatistics = sendStatistics.sendStatistics()

		self.dbimportCommandPath = self.getConfigValue("airflow_dbimport_commandpath")
		self.defaultSudoUser = self.getConfigValue("airflow_sudo_user")
		self.DAGdirectory = self.getConfigValue("airflow_dag_directory")
		self.DAGstagingDirectory = self.getConfigValue("airflow_dag_staging_directory")
		self.DAGfileGroup = self.getConfigValue("airflow_dag_file_group")
		self.DAGfilePermission = self.getConfigValue("airflow_dag_file_permission")
		self.TaskQueueForDummy = self.getConfigValue("airflow_dummy_task_queue")
		self.airflowMajorVersion = self.getConfigValue("airflow_major_version")
		self.defaultTimeZone = self.getConfigValue("timezone")
		self.airflowAWSinstanceIds = self.getConfigValue("airflow_aws_instanceids")
		self.airflowAWSpoolToInstanceId = self.getConfigValue("airflow_aws_pool_to_instanceid")
		self.airflowDefaultPoolSize = self.getConfigValue("airflow_default_pool_size")
		self.createPoolsWithTasks = self.getConfigValue("airflow_create_pool_with_task")
		
		self.DAGfile = None
		self.DAGfilename = None
		self.DAGfilenameInAirflow = None
		self.DAGfilenameInAwsMWAA = None
		self.writeDAG = None

		self.sensorStartTask = None
		self.sensorStopTask = None
		self.preStartTask = None
		self.preStopTask = None
		self.mainStartTask = None
		self.mainStopTask = None
		self.postStartTask = None
		self.postStopTask = None
		
		self.airflowMode = "default"

		self.createdPools = []
		self.AWSinstanceIdAssignedPools = {}

		try:
			self.airflowMode = configuration.get("Airflow", "airflow_mode", exitOnError=False)
			if self.airflowMode != "default" and self.airflowMode != "aws_mwaa":
				raise invalidConfiguration("Invalid option specified for [Airflow][airflow_mode] in the configuration file.")
#				log.error("Invalid option specified for [Airflow][airflow_mode] in the configuration file.")
#				self.common_config.remove_temporary_files()
#				sys.exit(1)

		except invalidConfiguration:
			pass


		if self.airflowMode == "default" and self.createPoolsWithTasks == False:
			# Establish a SQLAlchemy connection to the Airflow database
			airflowConnectStr = configuration.get("Airflow", "airflow_alchemy_conn")
			try:
				self.airflowDB = sa.create_engine(airflowConnectStr, echo = self.debugLogLevel)
				self.airflowDB.connect()
				self.airflowDBSession = sessionmaker(bind=self.airflowDB)

			except sa.exc.OperationalError as err:
				raise SQLerror(err)
#				log.error("%s"%err)
#				self.common_config.remove_temporary_files()
#				sys.exit(1)
			except:
				raise SQLerror(sys.exc_info())
#				print("Unexpected error: ")
#				print(sys.exc_info())
#				self.common_config.remove_temporary_files()
#				sys.exit(1)


		log.debug("Executing Airflow.__init__() - Finished")

	def disconnectDBImportDB(self):
		""" Disconnects from the database and removes all sessions and engine """
		log = logging.getLogger(self.logger)

		if self.configDB != None:
			log.info("Disconnecting from DBImport database")
			self.configDB.dispose()
			self.configDB = None

		self.configDBSession = None



	def getDBImportSession(self):
		""" Connects to the configuration database with SQLAlchemy and return a session """
		log = logging.getLogger(self.logger)

		if self.configDBSession != None:
			# If we already have a connection, we just return the session
			# return self.configDBSession()
			return scoped_session(self.configDBSession)

		mysqlCredentials = self.common_config.getMysqlCredentials()

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
			log.error("%s"%err)
			self.configDBSession = None
			self.configDB = None
			raise SQLerror("Can't connect to DBImport database")

		except:
			print("Unexpected error: ")
			print(sys.exc_info())
			self.configDBSession = None
			self.configDB = None
			raise SQLerror("Can't connect to DBImport database")

		else:
			# return self.configDBSession()
			return scoped_session(self.configDBSession)

	def getConfigValue(self, key):
		""" Returns a value from the configuration table based on the supplied key. Value returned can be Int, Str or DateTime""" 
		log = logging.getLogger(self.logger)

		returnValue = None
		boolValue = False
	
		valueColumn, boolValue = self.common_config.getConfigValueColumn(key)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		configurationTable = aliased(configSchema.configuration)

		configuration = (session.query(
					configurationTable.configKey,
					configurationTable.valueInt,
					configurationTable.valueStr,
					configurationTable.valueDate
				)
				.select_from(configurationTable)
				.filter(configurationTable.configKey == key)
				.one()
			)

		session.remove()
		returnValue = None

		try:
			if valueColumn == "valueInt":
				returnValue = int(configuration[1])
	
			if valueColumn == "valueStr":
				returnValue = configuration[2]
				if returnValue == None or returnValue.strip() == "":
					log.error("Configuration Key '%s' must have a value in '%s'"%(key, valueColumn))
					returnValue = None

			if boolValue == True:
				if returnValue == 1:
					returnValue = True
				elif returnValue == 0:
					returnValue = False
				else:
					log.error("Configuration Key '%s' can only have 0 or 1 in column '%s'"%(key, valueColumn))
					returnValue = None
		except TypeError:
			log.error("Configuration key '%s' cant be found. Have you upgraded the database schema to the latest version?"%(key))
			returnValue = None

		return returnValue


	def checkExecution(self):
		""" Checks the 'airflow_disable' settings and exit with 0 or 1 depending on that """

		airflowExecutionDisabled = self.getConfigValue("airflow_disable")
		if airflowExecutionDisabled == False:
			print("Airflow execution is enabled")
			self.common_config.remove_temporary_files()
			sys.exit(0)
		else:
			print("Airflow execution is disabled")
			self.common_config.remove_temporary_files()
			sys.exit(1)


	def getDBImportCommandPath(self, sudoUser=""):

		if sudoUser == None or sudoUser == "":
			sudoUser = self.defaultSudoUser

		return self.dbimportCommandPath.replace("${SUDO_USER}", sudoUser)

	def generateDAGfromREST(self, name, currentUser):
		log = logging.getLogger(self.logger)

		try:
			self.generateDAG(name=name, writeDAG=True)
		except (invalidConfiguration, SQLerror, RuntimeError) as errMsg:
			result = str(errMsg)
			returnCode = 500
		except:
			log.error(sys.exc_info())
			result = str(sys.exc_info())
			returnCode = 500
		else:
			result = "Airflow DAG generated"
			returnCode = 200

		return (result, returnCode)


	def generateDAG(self, name=None, writeDAG=False, autoDAGonly=False, DAGFolder=None):
		log = logging.getLogger(self.logger)

		self.writeDAG = writeDAG
		self.DAGFolder = DAGFolder

		# session = self.configDBSession()
		session = self.getDBImportSession()
		airflowCustomDags = aliased(configSchema.airflowCustomDags)
		airflowExportDags = aliased(configSchema.airflowExportDags)
		airflowImportDags = aliased(configSchema.airflowImportDags)
		airflowEtlDags = aliased(configSchema.airflowEtlDags)

		exportDAG = pd.DataFrame(session.query(
				airflowExportDags.dag_name,
				airflowExportDags.schedule_interval,
				airflowExportDags.filter_dbalias,
				airflowExportDags.filter_target_schema,
				airflowExportDags.filter_target_table,
				airflowExportDags.retries,
				airflowExportDags.auto_regenerate_dag,
				airflowExportDags.sudo_user,
				airflowExportDags.timezone,
				airflowExportDags.email,
				airflowExportDags.email_on_failure,
				airflowExportDags.email_on_retries,
				airflowExportDags.tags,
				airflowExportDags.sla_warning_time,
				airflowExportDags.retry_exponential_backoff,
				airflowExportDags.concurrency
			)
			.select_from(airflowExportDags)
			.all()).fillna('')

		importDAG = pd.DataFrame(session.query(
				airflowImportDags.dag_name,
				airflowImportDags.schedule_interval,
				airflowImportDags.filter_hive,
				airflowImportDags.retries,
				airflowImportDags.retries_stage1,
				airflowImportDags.retries_stage2,
				airflowImportDags.pool_stage1,
				airflowImportDags.pool_stage2,
				airflowImportDags.run_import_and_etl_separate,
				airflowImportDags.finish_all_stage1_first,
				airflowImportDags.auto_regenerate_dag,
				airflowImportDags.sudo_user,
				airflowImportDags.metadata_import,
				airflowImportDags.timezone,
				airflowImportDags.email,
				airflowImportDags.email_on_failure,
				airflowImportDags.email_on_retries,
				airflowImportDags.tags,
				airflowImportDags.sla_warning_time,
				airflowImportDags.retry_exponential_backoff,
				airflowImportDags.concurrency
			)
			.select_from(airflowImportDags)
			.all()).fillna('')

		etlDAG = pd.DataFrame(session.query(
				airflowEtlDags.dag_name,
				airflowEtlDags.schedule_interval,
				airflowEtlDags.filter_job,
				airflowEtlDags.filter_task,
				airflowEtlDags.filter_source_db,
				airflowEtlDags.filter_target_db,
				airflowEtlDags.retries,
				airflowEtlDags.auto_regenerate_dag,
				airflowEtlDags.sudo_user,
				airflowEtlDags.timezone,
				airflowEtlDags.email,
				airflowEtlDags.email_on_failure,
				airflowEtlDags.email_on_retries,
				airflowEtlDags.tags,
				airflowEtlDags.sla_warning_time,
				airflowEtlDags.retry_exponential_backoff,
				airflowEtlDags.concurrency
			)
			.select_from(airflowEtlDags)
			.all()).fillna('')

		customDAG = pd.DataFrame(session.query(
				airflowCustomDags.dag_name,
				airflowCustomDags.schedule_interval,
				airflowCustomDags.retries,
				airflowCustomDags.auto_regenerate_dag,
				airflowCustomDags.sudo_user,
				airflowCustomDags.timezone,
				airflowCustomDags.email,
				airflowCustomDags.email_on_failure,
				airflowCustomDags.email_on_retries,
				airflowCustomDags.tags,
				airflowCustomDags.sla_warning_time,
				airflowCustomDags.retry_exponential_backoff,
				airflowCustomDags.concurrency
			)
			.select_from(airflowCustomDags)
			.all()).fillna('')

		if name != None:
			if importDAG.empty == False:
				importDAG = importDAG.loc[importDAG['dag_name'] == name]
			if exportDAG.empty == False:
				exportDAG = exportDAG.loc[exportDAG['dag_name'] == name]
			if customDAG.empty == False:
				customDAG = customDAG.loc[customDAG['dag_name'] == name]

		if autoDAGonly == True:
			if importDAG.empty == False:
				importDAG = importDAG.loc[importDAG['auto_regenerate_dag'] == 1]
			if exportDAG.empty == False:
				exportDAG = exportDAG.loc[exportDAG['auto_regenerate_dag'] == 1]
			if customDAG.empty == False:
				customDAG = customDAG.loc[customDAG['auto_regenerate_dag'] == 1]

		dagFound = False

		if name == None or len(importDAG) > 0: 
			dagFound = True
			for index, row in importDAG.iterrows():
				self.generateImportDAG(DAG=row)

		if name == None or len(exportDAG) > 0: 
			dagFound = True
			for index, row in exportDAG.iterrows():
				self.generateExportDAG(DAG=row)

		if name == None or len(customDAG) > 0: 
			dagFound = True
			for index, row in customDAG.iterrows():
				self.generateCustomDAG(DAG=row)

		if dagFound == False and name != None:
			raise invalidConfiguration("Can't find DAG with that name")
#			log.error("Can't find DAG with that name")
#			self.common_config.remove_temporary_files()
#			sys.exit(1)

		session.close()

	def getAirflowHostPoolName(self):
		log = logging.getLogger(self.logger)

		if self.airflowMode == "aws_mwaa" and self.airflowAWSpoolToInstanceId == True:
			# We can choice to override the poolname and set it to the instance ID if we are running on AWS
			poolName = self.airflowAWSinstanceIds 
		else:
			try:
				hostname = self.common_config.jdbc_hostname.lower().split("/")[0].split("\\")[0] 
				poolName = "DBImport_server_%s"%(hostname)
			except (TypeError, AttributeError):
				log.warning("Cant find hostname in jdbc_connections table. Setting pool to 'default'")
				poolName = "default"	
			
		return poolName[0:50]

	def AWSappendInstanceIDasPools(self):
		# This will split the instanceID configration setting based on , and append them all as pool names
		log = logging.getLogger(self.logger)

		usedPools = []

		for AWSpool in self.airflowAWSinstanceIds.split(','):
			usedPools.append(AWSpool.strip())
		
		self.createAirflowPools(pools=usedPools)
		

	def getAWSInstanceID(self, configObject):
		# Return one instanceID from a comma seperated list. 
		log = logging.getLogger(self.logger)

		if configObject in self.AWSinstanceIdAssignedPools:
			return self.AWSinstanceIdAssignedPools[configObject]
		else:
			randomInstanceID = random.choice(self.airflowAWSinstanceIds.strip().split(','))
			self.AWSinstanceIdAssignedPools[configObject] = randomInstanceID
			return randomInstanceID

		# return random.choice(self.airflowAWSinstanceIds.strip().split(','))
		

	def generateExportDAG(self, DAG):
		""" Generates a Import DAG """
		log = logging.getLogger(self.logger)

		usedPools = []
		tableFilters = []
		defaultPool = DAG['dag_name']
		sudoUser = DAG['sudo_user']
		usedPools.append(defaultPool)

		cronSchedule = self.convertTimeToCron(DAG["schedule_interval"])
		self.createDAGfileWithHeader(dagName = DAG['dag_name'], cronSchedule = cronSchedule, defaultPool = defaultPool, sudoUser = sudoUser, dagTimeZone = DAG['timezone'], email = DAG['email'], email_on_failure = DAG['email_on_failure'], email_on_retries = DAG['email_on_retries'], tags = DAG['tags'], slaWarningTime = DAG['sla_warning_time'], retryExponentialBackoff = DAG['retry_exponential_backoff'], concurrency = DAG['concurrency'])

		# session = self.configDBSession()
		session = self.getDBImportSession()
		exportTables = aliased(configSchema.exportTables)

		exportTablesQuery = Query([exportTables.target_schema, exportTables.target_table, exportTables.dbalias, exportTables.airflow_priority, exportTables.export_type, exportTables.sqoop_last_mappers])
		exportTablesQuery = exportTablesQuery.filter(exportTables.include_in_airflow == 1)

		filterDBAlias = DAG['filter_dbalias'].strip().replace(r'*', '%')
		filterTargetSchema = DAG['filter_target_schema'].strip().replace(r'*', '%')
		filterTargetTable = DAG['filter_target_table'].strip().replace(r'*', '%')

#		if filterDBAlias == '':
#			log.error("'filter_dbalias' in airflow_export_dags cant be empty")
#			self.DAGfile.close()
#			self.common_config.remove_temporary_files()
#			sys.exit(1)

		if filterDBAlias != '':			exportTablesQuery = exportTablesQuery.filter(exportTables.dbalias.like(filterDBAlias))
		if filterTargetSchema != '':	exportTablesQuery = exportTablesQuery.filter(exportTables.target_schema.like(filterTargetSchema))
		if filterTargetTable  != '':	exportTablesQuery = exportTablesQuery.filter(exportTables.target_table.like(filterTargetTable))
		tables = pd.DataFrame(exportTablesQuery.with_session(session).all()).fillna('')

		if DAG['retries'] == None or DAG['retries'] == '':
			retries = 5
		else:
			retries = int(DAG['retries'])

		# in 'tables' we now have all the tables that will be part of the DAG
		previousConnectionAlias = ""

		for index, row in tables.iterrows():
			if row['dbalias'] != previousConnectionAlias:
				# We save the previousConnectionAlias just to avoid making lookups for every dbalias even if they are all the same
				try:
					self.common_config.lookupConnectionAlias(connection_alias=row['dbalias'], decryptCredentials=False)
					previousConnectionAlias = row['dbalias']

				except invalidConfiguration as errMsg:
					log.warning("The connection alias '%s' cant be found in the configuration database"%(row['dbalias']))
					previousConnectionAlias = None
					continue
		
			exportPool = self.getAirflowHostPoolName()

			# usedPools is later used to check if the pools that we just are available in Airflow
			if exportPool not in usedPools:
				usedPools.append(exportPool)
	
			taskID = row['target_table'].replace(r'/', '_').replace(r'.', '_')
			dbexportCMD = "%sbin/export"%(self.getDBImportCommandPath(sudoUser = sudoUser)) 
			dbexportClearStageCMD = "%sbin/manage --clearExportStage"%(self.getDBImportCommandPath(sudoUser = sudoUser)) 

			airflowPriority = 1		# Default Airflow Priority
			if row['airflow_priority'] != None and row['airflow_priority'] != '':
				airflowPriority = int(row['airflow_priority'])
			elif row['sqoop_last_mappers'] != None and row['sqoop_last_mappers'] != '':
				airflowPriority = int(row['sqoop_last_mappers'])

			clearStageRequired = False
			if row['export_type'] == "full":
				clearStageRequired = True

			if clearStageRequired == True:
				self.DAGfile.write("%s_clearStage = BashOperator(\n"%(taskID))
				self.DAGfile.write("    task_id='%s_clearStage',\n"%(taskID))
				self.DAGfile.write("    bash_command='%s -a %s -S %s -T %s ',\n"%(dbexportClearStageCMD, row['dbalias'], row['target_schema'], row['target_table']))
				self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
				self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    retries=%s,\n"%(retries))
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

			self.DAGfile.write("%s = BashOperator(\n"%(taskID))
			self.DAGfile.write("    task_id='%s',\n"%(taskID))
			self.DAGfile.write("    bash_command='%s -a %s -S %s -T %s ',\n"%(dbexportCMD, row['dbalias'], row['target_schema'], row['target_table']))
			self.DAGfile.write("    pool='%s',\n"%(exportPool))
			self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    retries=%s,\n"%(retries))
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")

			if clearStageRequired == True:
				self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
				self.DAGfile.write("%s_clearStage.set_downstream(%s)\n"%(taskID, taskID))
				self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
			else:
				self.DAGfile.write("%s.set_downstream(%s)\n"%(self.mainStartTask, taskID))
				self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
			self.DAGfile.write("\n")

		self.addTasksToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"], defaultRetries=retries, defaultSudoUser=sudoUser)
		self.addSensorsToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"])
		self.createAirflowPools(pools=usedPools)
		self.closeDAGfile()
		session.close()


	def generateImportDAG(self, DAG):
		""" Generates a Import DAG """
		log = logging.getLogger(self.logger)

		importPhaseFinishFirst = False
		if DAG['finish_all_stage1_first'] == 1:
			importPhaseFinishFirst = True

		runImportAndEtlSeparate = False
		if DAG['run_import_and_etl_separate'] == 1:
			runImportAndEtlSeparate = True

		usedPools = []
		tableFilters = []
		defaultPool = DAG['dag_name']
		sudoUser = DAG['sudo_user']
		usedPools.append(defaultPool)

		if DAG['metadata_import'] == 1:
			metaDataImportOption = "-m"
		else:
			metaDataImportOption = ""

		cronSchedule = self.convertTimeToCron(DAG["schedule_interval"])
		self.createDAGfileWithHeader(dagName = DAG['dag_name'], cronSchedule = cronSchedule, importPhaseFinishFirst = importPhaseFinishFirst, defaultPool = defaultPool, sudoUser = sudoUser, dagTimeZone = DAG['timezone'], email = DAG['email'], email_on_failure = DAG['email_on_failure'], email_on_retries = DAG['email_on_retries'], tags = DAG['tags'], slaWarningTime = DAG['sla_warning_time'], retryExponentialBackoff = DAG['retry_exponential_backoff'], concurrency = DAG['concurrency'])

		# session = self.configDBSession()
		session = self.getDBImportSession()
		importTables = aliased(configSchema.importTables)

		importTablesQuery = Query([importTables.hive_db, importTables.hive_table, importTables.dbalias, importTables.airflow_priority, importTables.import_type, importTables.import_phase_type, importTables.etl_phase_type, importTables.sqoop_last_mappers, importTables.copy_slave])
		importTablesQuery = importTablesQuery.filter(importTables.include_in_airflow == 1)

		for hiveTarget in DAG['filter_hive'].split(';'):
			try:
				hiveDB = hiveTarget.split(".")[0].strip().replace(r'*', '%')
				hiveTable = hiveTarget.split(".")[1].strip().replace(r'*', '%')
				if hiveDB == None or hiveTable == None or hiveDB == "" or hiveTable == "":
					self.DAGfile.close()
					raise invalidConfiguration("Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;.....")
			except IndexError:
				self.DAGfile.close()
				raise invalidConfiguration("Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;.....")
#				log.error("Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;.....")
#				self.DAGfile.close()
#				self.common_config.remove_temporary_files()
#				sys.exit(1)

			tableFilters.append((importTables.hive_db.like(hiveDB)) & (importTables.hive_table.like(hiveTable)))

		importTablesQuery = importTablesQuery.filter(sa.or_(*tableFilters))
		tables = pd.DataFrame(importTablesQuery.with_session(session).all()).fillna('')

		retries=int(DAG['retries'])
		try:
			retriesImportPhase = int(DAG['retries_stage1'])
		except ValueError:
			retriesImportPhase = retries

		try:
			retriesEtlPhase = int(DAG['retries_stage2'])
		except ValueError:
			retriesEtlPhase = retries

		# in 'tables' we now have all the tables that will be part of the DAG
		previousConnectionAlias = ""

		for index, row in tables.iterrows():
			try:
				if row['dbalias'] != previousConnectionAlias:
					# We save the previousConnectionAlias just to avoid making lookups for every dbalias even if they are all the same
					self.common_config.lookupConnectionAlias(connection_alias=row['dbalias'], decryptCredentials=False)
					previousConnectionAlias = row['dbalias']
			except:
				continue
		
			if row['copy_slave'] == 1:
				importPhaseAsSensor = True
			else:
				importPhaseAsSensor = False

			if self.airflowMode == "aws_mwaa" and self.airflowAWSpoolToInstanceId == True:
				# We can choice to override the poolname and set it to the instance ID if we are running on AWS
				instanceID = self.getAWSInstanceID(row['hive_table'])
				importPhasePool = instanceID
				etlPhasePool = instanceID
			else:
				importPhasePool = self.getAirflowHostPoolName()
				etlPhasePool = DAG['dag_name'][0:50]
	
				if DAG['pool_stage1'] != '':
					importPhasePool = DAG['pool_stage1']
	
				if DAG['pool_stage2'] != '':
					etlPhasePool = DAG['pool_stage2']
		
			# usedPools is later used to check if the pools that we just are available in Airflow
			if importPhasePool not in usedPools:
				usedPools.append(importPhasePool)

			if etlPhasePool not in usedPools:
				usedPools.append(etlPhasePool)

			dbimportCMD = "%sbin/import"%(self.getDBImportCommandPath(sudoUser = sudoUser)) 
			dbimportClearStageCMD = "%sbin/manage --clearImportStage"%(self.getDBImportCommandPath(sudoUser = sudoUser)) 

			taskID = row['hive_table'].replace(r'/', '_').replace(r'.', '_')

			airflowPriority = 1		# Default Airflow Priority
			if row['airflow_priority'] != None and row['airflow_priority'] != '':
				airflowPriority = int(row['airflow_priority'])
			elif row['sqoop_last_mappers'] != None and row['sqoop_last_mappers'] != '':
				airflowPriority = int(row['sqoop_last_mappers'])

			clearStageRequired = False
			if row['import_type'] in ("full_direct", "full", "oracle_flashback_merge", "full_history", "full_merge_direct_history", "full_merge_direct", "full_append"):
				clearStageRequired = True

			if row['import_phase_type'] in ("full", "oracle_flashback", "mssql_change_tracking"):
				clearStageRequired = True

			if clearStageRequired == True:
				if self.airflowMode == "default":
					self.DAGfile.write("%s_clearStage = BashOperator(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_clearStage',\n"%(taskID))
					self.DAGfile.write("    bash_command='%s -h %s -t %s ',\n"%(dbimportClearStageCMD, row['hive_db'], row['hive_table']))
					self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    retries=%s,\n"%(retries))
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")
				elif self.airflowMode == "aws_mwaa":
					instanceID = self.getAWSInstanceID(row['hive_table'])
					self.DAGfile.write("%s_clearStage = PythonOperator(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_clearStage',\n"%(taskID))
					self.DAGfile.write("    python_callable=startDBImportExecution,\n")
					# self.DAGfile.write("    op_kwargs={\"command\": \"manage --clearImportStage -h %s -t %s\"},\n"%(row['hive_db'], row['hive_table']))
					self.DAGfile.write("    op_kwargs={\"command\": \"manage --clearImportStage -h %s -t %s\", \"instanceID\": \"%s\"},\n"%(row['hive_db'], row['hive_table'], instanceID))
					self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
					# self.DAGfile.write("    pool='%s',\n"%(instanceID))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    retries=%s,\n"%(retries))
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

			if DAG['finish_all_stage1_first'] == 1 or runImportAndEtlSeparate == True:
				if importPhaseAsSensor == True:
					# Running Import phase as a sensor
					self.DAGfile.write("%s_sensor = SqlSensor(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_sensor',\n"%(taskID))
					self.DAGfile.write("    conn_id='DBImport',\n")
					self.DAGfile.write("    sql=\"\"\"select count(*) from import_tables where hive_db = '%s' and hive_table = '%s' and "%(row['hive_db'], row['hive_table']))
					self.DAGfile.write("copy_finished >= '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') }}'\"\"\",\n")
					self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    timeout=18000,\n")
					self.DAGfile.write("    poke_interval=300,\n")
					self.DAGfile.write("    mode='reschedule',\n")
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

				if self.airflowMode == "default":
					self.DAGfile.write("%s_import = BashOperator(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_import',\n"%(taskID))
					self.DAGfile.write("    bash_command='%s -h %s -t %s -I -C ',\n"%(dbimportCMD, row['hive_db'], row['hive_table']))
					self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    retries=%s,\n"%(retriesImportPhase))
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")
			
					self.DAGfile.write("%s_etl = BashOperator(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_etl',\n"%(taskID))
					self.DAGfile.write("    bash_command='%s -h %s -t %s -E ',\n"%(dbimportCMD, row['hive_db'], row['hive_table']))
					self.DAGfile.write("    pool='%s',\n"%(etlPhasePool))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    retries=%s,\n"%(retriesEtlPhase))
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

				elif self.airflowMode == "aws_mwaa":
					instanceID = self.getAWSInstanceID(row['hive_table'])
					self.DAGfile.write("%s_import = PythonOperator(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_import',\n"%(taskID))
					self.DAGfile.write("    python_callable=startDBImportExecution,\n")
					self.DAGfile.write("    op_kwargs={\"command\": \"import -d %s -t %s -I -C\", \"instanceID\": \"%s\"},\n"%(row['hive_db'], row['hive_table'], instanceID))
					self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    retries=%s,\n"%(retriesImportPhase))
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

					self.DAGfile.write("%s_etl = PythonOperator(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_etl',\n"%(taskID))
					self.DAGfile.write("    python_callable=startDBImportExecution,\n")
					self.DAGfile.write("    op_kwargs={\"command\": \"import -d %s -t %s -E\", \"instanceID\": \"%s\"},\n"%(row['hive_db'], row['hive_table'], instanceID))
					self.DAGfile.write("    pool='%s',\n"%(etlPhasePool))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    retries=%s,\n"%(retriesEtlPhase))
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

				if clearStageRequired == True and DAG['finish_all_stage1_first'] == 1:
					self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s_clearStage.set_downstream(%s_sensor)\n"%(taskID, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s_import)\n"%(taskID, taskID))
					else:
						self.DAGfile.write("%s_clearStage.set_downstream(%s_import)\n"%(taskID, taskID))
					self.DAGfile.write("%s_import.set_downstream(Import_Phase_Finished)\n"%(taskID))
					self.DAGfile.write("Import_Phase_Finished.set_downstream(%s_etl)\n"%(taskID))
					self.DAGfile.write("%s_etl.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				elif clearStageRequired == True and DAG['finish_all_stage1_first'] == 0:		# This means that runImportAndEtlSeparate == True
					self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s_clearStage.set_downstream(%s_sensor)\n"%(taskID, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s_import)\n"%(taskID, taskID))
					else:
						self.DAGfile.write("%s_clearStage.set_downstream(%s_import)\n"%(taskID, taskID))
					self.DAGfile.write("%s_import.set_downstream(%s_etl)\n"%(taskID, taskID))
					self.DAGfile.write("%s_etl.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				elif clearStageRequired == False and DAG['finish_all_stage1_first'] == 1:
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s.set_downstream(%s_sensor)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s_import)\n"%(taskID, taskID))
					else:
						self.DAGfile.write("%s.set_downstream(%s_import)\n"%(self.mainStartTask, taskID))
					self.DAGfile.write("%s_import.set_downstream(Import_Phase_Finished)\n"%(taskID))
					self.DAGfile.write("Import_Phase_Finished.set_downstream(%s_etl)\n"%(taskID))
					self.DAGfile.write("%s_etl.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				else:
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s.set_downstream(%s_sensor)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s_import)\n"%(taskID, taskID))
					else:
						self.DAGfile.write("%s.set_downstream(%s_import)\n"%(self.mainStartTask, taskID))
					self.DAGfile.write("%s_import.set_downstream(%s_etl)\n"%(taskID, taskID))
					self.DAGfile.write("%s_etl.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				self.DAGfile.write("\n")

			else:
				if importPhaseAsSensor == True:
					# Running Import phase as a sensor
					self.DAGfile.write("%s_sensor = SqlSensor(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_sensor',\n"%(taskID))
					self.DAGfile.write("    conn_id='DBImport',\n")
					self.DAGfile.write("    sql=\"\"\"select count(*) from import_tables where hive_db = '%s' and hive_table = '%s' and "%(row['hive_db'], row['hive_table']))
					self.DAGfile.write("copy_finished >= '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') }}'\"\"\",\n")
					self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    timeout=18000,\n")
					self.DAGfile.write("    poke_interval=300,\n")
					self.DAGfile.write("    mode='reschedule',\n")
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

				if self.airflowMode == "default":
					self.DAGfile.write("%s = BashOperator(\n"%(taskID))
					self.DAGfile.write("    task_id='%s',\n"%(taskID))
					self.DAGfile.write("    bash_command='%s -h %s -t %s %s ',\n"%(dbimportCMD, row['hive_db'], row['hive_table'], metaDataImportOption))
					self.DAGfile.write("    pool='%s',\n"%(etlPhasePool))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    retries=%s,\n"%(retries))
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")
				elif self.airflowMode == "aws_mwaa":
					instanceID = self.getAWSInstanceID(row['hive_table'])
					self.DAGfile.write("%s = PythonOperator(\n"%(taskID))
					self.DAGfile.write("    task_id='%s',\n"%(taskID))
					self.DAGfile.write("    python_callable=startDBImportExecution,\n")
					self.DAGfile.write("    op_kwargs={\"command\": \"import -d %s -t %s %s\", \"instanceID\": \"%s\"},\n"%(row['hive_db'], row['hive_table'], metaDataImportOption, instanceID))
					self.DAGfile.write("    pool='%s',\n"%(etlPhasePool))
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    retries=%s,\n"%(retries))
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")


				if clearStageRequired == True:
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_clearStage.set_downstream(%s_sensor)\n"%(taskID, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s)\n"%(taskID, taskID))
						self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
					else:
						self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_clearStage.set_downstream(%s)\n"%(taskID, taskID))
						self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				else:
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s.set_downstream(%s_sensor)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s)\n"%(taskID, taskID))
						self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
					else:
						self.DAGfile.write("%s.set_downstream(%s)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				self.DAGfile.write("\n")

		self.addTasksToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"], defaultRetries=retries, defaultSudoUser=sudoUser)
		self.addSensorsToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"])
		self.createAirflowPools(pools=usedPools)
		if self.airflowMode == "aws_mwaa" and self.airflowAWSpoolToInstanceId == True:
			self.AWSappendInstanceIDasPools()
		self.closeDAGfile()
		session.close()

	def createAirflowPools(self, pools): 
		""" Creates the pools in Airflow database """
		log = logging.getLogger(self.logger)

		if self.airflowMode == "default" and self.createPoolsWithTasks == False:
			session = self.airflowDBSession()
			slotPool = aliased(airflowSchema.slotPool)
		
			airflowPools = pd.DataFrame(session.query(
						slotPool.pool,
						slotPool.slots,
						slotPool.description)
					.select_from(slotPool)
					.all())
	
			for pool in pools:
				# Just to make sure we dont create the same pool twice. That will generate an error in Airflow
				if pool in self.createdPools:
					continue
				else:
					self.createdPools.append(pool)

				if len(airflowPools) == 0 or len(airflowPools.loc[airflowPools['pool'] == pool]) == 0:
					try:
						log.info("Creating the Airflow pool '%s' with %s slots"%(pool, self.airflowDefaultPoolSize))
						newPool = airflowSchema.slotPool(pool=pool, slots=self.airflowDefaultPoolSize, include_deferred=0)
						session.add(newPool)
						session.commit()
					except sa.exc.IntegrityError:
						log.warning("Cant create pool '%s' as there is a duplicate pool already in the Airflow database."%(pool))
						session.rollback()

			session.close()

		elif self.createPoolsWithTasks == True:
			for pool in pools:
				# Just to make sure we dont create the same pool twice. That will generate an error in Airflow
				if pool in self.createdPools:
					continue
				else:
					self.createdPools.append(pool)

				taskName = "createPool_%s"%(pool.replace('.', '_').replace('-', '_'))
				self.DAGfile.write("\n")
				self.DAGfile.write("%s = CreatePoolOperator(\n"%(taskName))
				self.DAGfile.write("    name='%s',\n"%(pool))
				self.DAGfile.write("    slots=%s,\n"%(self.airflowDefaultPoolSize))
				self.DAGfile.write("    task_id='%s',\n"%(taskName))
				self.DAGfile.write("    retries=5,\n")
				self.DAGfile.write("    priority_weight=1,\n")
				self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    pool=None,\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")
				self.DAGfile.write("start.set_downstream(%s)\n"%(taskName))
				self.DAGfile.write("%s.set_downstream(%s)\n"%(taskName, self.preStopTask))
				self.DAGfile.write("\n")


	def generateCustomDAG(self, DAG):
		""" Generates a Custom DAG """
		log = logging.getLogger(self.logger)

		# session = self.configDBSession()
		session = self.getDBImportSession()
		airflowTasks = aliased(configSchema.airflowTasks)

		tasks = (session.query(airflowTasks.task_name)
				.select_from(airflowTasks)
				.filter(airflowTasks.dag_name == DAG['dag_name'])
				.filter(airflowTasks.include_in_airflow == 1)
				.filter(airflowTasks.placement == 'in main')
				.count())

		if tasks == 0:
			log.warning("There are no tasks defined 'in main' for DAG '%s'. This is required for a custom DAG"%(DAG["dag_name"]))
			session.close()
			return

		usedPools = []
#		if self.airflowMode == "aws_mwaa" and self.airflowAWSpoolToInstanceId == True:
#			# We can choice to override the poolname and set it to the instance ID if we are running on AWS
#			defaultPool = self.airflowAWSinstanceIds
#		else:
#			defaultPool = DAG['dag_name']
		defaultPool = DAG['dag_name']
		sudoUser = DAG['sudo_user']
		usedPools.append(defaultPool)

		if DAG['retries'] == None or DAG['retries'] == '':
			retries = 0
		else:
			retries = int(DAG['retries'])

		cronSchedule = self.convertTimeToCron(DAG["schedule_interval"])
		self.createDAGfileWithHeader(dagName = DAG['dag_name'], cronSchedule = cronSchedule, defaultPool = defaultPool, sudoUser = sudoUser, dagTimeZone = DAG['timezone'], email = DAG['email'], email_on_failure = DAG['email_on_failure'], email_on_retries = DAG['email_on_retries'], tags = DAG['tags'], slaWarningTime = DAG['sla_warning_time'], retryExponentialBackoff = DAG['retry_exponential_backoff'], concurrency = DAG['concurrency'])
		self.addTasksToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"], defaultRetries=retries, defaultSudoUser=sudoUser)
		self.addSensorsToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"])
		self.createAirflowPools(pools=usedPools)
		self.closeDAGfile()
		session.close()

	def convertTimeToCron(self, time):
		""" Converts a string in format HH:MM to a CRON time string based on 'minute hour day month weekday' """
		log = logging.getLogger(self.logger)
		returnValue = time

		if re.search('^[0-2][0-9]:[0-5][0-9]$', time):
			hour = re.sub(':[0-5][0-9]$', '', time) 
			minute = re.sub('^[0-2][0-9]:', '', time) 
			returnValue = "'%s %s * * *'"%(int(minute), int(hour)) 

		return returnValue

	def validateTimeZone(self, timeZone):
		log = logging.getLogger(self.logger)
		try:
			pendulum.timezone(timeZone)
		except pendulum.tz.zoneinfo.exceptions.InvalidTimezone:
			return False
		except ValueError:
			return False
		else:
			return True


	def createDAGfileWithHeader(self, dagName, cronSchedule, defaultPool, importPhaseFinishFirst=False, sudoUser="", dagTimeZone=None, email=None, email_on_failure=None, email_on_retries=None, tags=None, slaWarningTime=None, retryExponentialBackoff=None, concurrency=None):
		log = logging.getLogger(self.logger)
		# session = self.configDBSession()
		session = self.getDBImportSession()

		self.sensorStartTask = "start"
		self.sensorStopTask = "dag_sensors_finished"
		self.preStartTask = "start"
		self.preStopTask = "before_tasks_finished"
		self.mainStartTask = "start"
		self.mainStopTask = "stop"
		self.postStartTask = "main_tasks_finished"
		self.postStopTask = "stop"

		if self.TaskQueueForDummy != None and self.TaskQueueForDummy.strip() != "" and self.TaskQueueForDummy.strip() != "default":
			self.TaskQueueForDummy = self.TaskQueueForDummy.strip()
		else:
			self.TaskQueueForDummy = None

		tasksBeforeMainExists = False
		tasksAfterMainExists = False
		tasksSensorsExists = False

		if dagTimeZone != None and dagTimeZone != "" and self.validateTimeZone(dagTimeZone) == False:
			log.warning("Time zone value specified for DAG is not valid, (%s)" %(dagTimeZone))
			log.warning("Using default time zone value instead")
			dagTimeZone=None

		if self.DAGFolder == None: 
			self.DAGfilename = "%s/%s.py"%(self.DAGstagingDirectory, dagName)
			if not os.path.exists(self.DAGstagingDirectory):
				try:
					log.info("Creating DAG staging directory (%s)"%(self.DAGstagingDirectory))
					os.makedirs(self.DAGstagingDirectory)
				except:
					pass
		else:
			self.DAGfilename = "%s/%s.py"%(self.DAGFolder, dagName)

		self.DAGfilenameInAwsMWAA = "dags/%s.py"%(dagName)
		self.DAGfilenameInAirflow = "%s/%s.py"%(self.DAGdirectory, dagName)

		try:
			self.DAGfile = open(self.DAGfilename, "w")
		except FileNotFoundError:
			log.error("Cant open file '%s'. Please verify that the directory exists and that the configuration 'airflow_dbimport_commandpath' is correct"%(self.DAGfilename))
			raise RuntimeError
			# self.common_config.remove_temporary_files()
			# sys.exit(1)
			
		self.DAGfile.write("# -*- coding: utf-8 -*-\n")
		self.DAGfile.write("import airflow\n")
		self.DAGfile.write("from airflow import DAG\n")
		self.DAGfile.write("from airflow.models import Variable\n")

		if self.airflowMajorVersion == 2:
			self.DAGfile.write("from airflow.sensors.external_task import ExternalTaskSensor\n")
		else:
			self.DAGfile.write("from airflow.operators.sensors import ExternalTaskSensor\n")

		self.DAGfile.write("from airflow.sensors.sql_sensor import SqlSensor\n")
		self.DAGfile.write("from airflow.operators.bash_operator import BashOperator\n")
		self.DAGfile.write("from airflow.operators.python_operator import BranchPythonOperator\n")
		self.DAGfile.write("from airflow.operators.dagrun_operator import TriggerDagRunOperator\n")
		self.DAGfile.write("from airflow.operators.dummy_operator import DummyOperator\n")
		self.DAGfile.write("from airflow.api.common.experimental.pool import get_pool, create_pool\n")
		self.DAGfile.write("from airflow.exceptions import PoolNotFound\n")
		self.DAGfile.write("from airflow.models import BaseOperator\n")

		if self.airflowMode == "aws_mwaa":
			self.DAGfile.write("from airflow.operators.dummy_operator import DummyOperator\n")
			self.DAGfile.write("from airflow.operators.python_operator import PythonOperator\n")
			self.DAGfile.write("import botocore\n")
			self.DAGfile.write("import boto3\n")
			self.DAGfile.write("import time\n")

		self.DAGfile.write("from datetime import datetime, timedelta, timezone\n")
		self.DAGfile.write("import pendulum\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("try:\n")
		self.DAGfile.write("    Email_receiver = Variable.get(\"Email_receiver\")\n")
		self.DAGfile.write("except KeyError:\n")
		self.DAGfile.write("    Email_receiver = \"not-set@domain.com\"\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("\n")

		self.DAGfile.write("class CreatePoolOperator(BaseOperator):\n")
		self.DAGfile.write("    ui_color = '#b8e9ee'\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("    def __init__(\n")
		self.DAGfile.write("            self,\n")
		self.DAGfile.write("            name,\n")
		self.DAGfile.write("            slots,\n")
		self.DAGfile.write("            description='',\n")
		self.DAGfile.write("            *args, **kwargs):\n")
		self.DAGfile.write("        super(CreatePoolOperator, self).__init__(*args, **kwargs)\n")
		self.DAGfile.write("        self.description = description\n")
		self.DAGfile.write("        self.slots = slots\n")
		self.DAGfile.write("        self.name = name\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("    def execute(self, context):\n")
		self.DAGfile.write("        try:\n")
		self.DAGfile.write("            pool = get_pool(name=self.name)\n")
		# self.DAGfile.write("            if pool:\n")
		self.DAGfile.write("            print('Pool exists: %s'%(pool))\n")
		self.DAGfile.write("        except PoolNotFound:\n")
		self.DAGfile.write("            # create the pool\n")
		self.DAGfile.write("            pool = create_pool(name=self.name, slots=self.slots, description=self.description)\n")
		self.DAGfile.write("            print('Pool exists: %s'%(pool))\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("\n")


		if dagTimeZone == None or dagTimeZone == "":
			self.DAGfile.write("local_tz = pendulum.timezone(\"%s\")\n"%(self.defaultTimeZone))
			self.DAGfile.write("\n")
		else:
			self.DAGfile.write("local_tz = pendulum.timezone(\"%s\")\n"%(dagTimeZone))
			self.DAGfile.write("\n")

		if self.airflowMode == "aws_mwaa":
			self.DAGfile.write("def printAndFindEventErrors(event, startWithNewline = False):\n")
			self.DAGfile.write("    errorFound = False\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    if startWithNewline == True:\n")
			self.DAGfile.write("        findPattern = \"\\nERROR\"\n")
			self.DAGfile.write("    else:\n")
			self.DAGfile.write("        findPattern = \"ERROR\"\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    try:\n")
			self.DAGfile.write("        print(event['message'], end=\"\")\n")
			self.DAGfile.write("        if event['message'].find(findPattern) != -1:\n")
			# self.DAGfile.write("        if event['message'].find(\"ERROR\") != -1:\n")
			self.DAGfile.write("            errorFound = True\n")
			# self.DAGfile.write("            print(\"Error Found (A) !!!!!\")\n")
			self.DAGfile.write("            print(event)\n")
			self.DAGfile.write("    except:\n")
			self.DAGfile.write("        print(event)\n")
			self.DAGfile.write("        if str(event).find(findPattern) != -1:\n")
			# self.DAGfile.write("        if str(event).find(\"ERROR\") != -1:\n")
			self.DAGfile.write("            errorFound = True\n")
			# self.DAGfile.write("            print(\"Error Found (B) !!!!!\")\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    return errorFound\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("def startDBImportExecution(command, instanceID):\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    ssm_client = boto3.client('ssm',region_name='eu-west-1')\n")
			self.DAGfile.write("    logs_client = boto3.client('logs',region_name='eu-west-1')\n")
			self.DAGfile.write("\n")
			# self.DAGfile.write("    instanceID = \"%s\"\n"%(self.airflowAWSinstanceIds))
			self.DAGfile.write("    commandPath = \"%s\"\n"%(self.getDBImportCommandPath()))
			self.DAGfile.write("    command = \"%sbin/%s || echo ERROR; sleep 1\"%(commandPath, command)\n")
			self.DAGfile.write("    cloudWatchLogGroupName = \"dbimport\"\n")
			self.DAGfile.write("    errorFound = False\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    # Start the import by the help of SSM Send Command\n")
			self.DAGfile.write("    sendCommand_response = ssm_client.send_command(\n")
			self.DAGfile.write("        InstanceIds=[instanceID],\n")
			self.DAGfile.write("        DocumentName=\"AWS-RunShellScript\",\n")
			self.DAGfile.write("        Parameters={'commands': [command], 'executionTimeout': ['172800']},\n")
			self.DAGfile.write("        CloudWatchOutputConfig={'CloudWatchLogGroupName': cloudWatchLogGroupName, 'CloudWatchOutputEnabled': True}\n")
			self.DAGfile.write("        )\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    # Get the commandID and set the logStreamName for CloudWatch\n")
			self.DAGfile.write("    commandID = sendCommand_response['Command']['CommandId']\n")
			self.DAGfile.write("    logStreamNameStdOut = \"%s/%s/aws-runShellScript/stdout\"%(commandID, instanceID)\n")
			self.DAGfile.write("    logStreamNameStdErr = \"%s/%s/aws-runShellScript/stderr\"%(commandID, instanceID)\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    print('')\n")
			self.DAGfile.write("    print('CloudWatch logs is available at:')\n")
			self.DAGfile.write("    print(logStreamNameStdOut)\n")
			self.DAGfile.write("    print(logStreamNameStdErr)\n")
			self.DAGfile.write("    print('')\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    # Run a loop until the command is actually running\n")
			self.DAGfile.write("    commandRunning = False\n")
			self.DAGfile.write("    while commandRunning == False:\n")
			self.DAGfile.write("        try:\n")
			self.DAGfile.write("            commandInvocation_response = ssm_client.get_command_invocation(\n")
			self.DAGfile.write("                CommandId=commandID,\n")
			self.DAGfile.write("                InstanceId=instanceID\n")
			self.DAGfile.write("                )\n")
			self.DAGfile.write("            commandStatus = commandInvocation_response['Status']\n")
			self.DAGfile.write("\n")
			# self.DAGfile.write("            print('commandStatus = %s'%(commandStatus))\n")
			# self.DAGfile.write("\n")
			self.DAGfile.write("            if commandStatus in ('InProgress', 'Success'):\n")
			self.DAGfile.write("                commandRunning = True\n")
			self.DAGfile.write("            elif commandStatus in ('Pending', 'Delayed'):\n")
			self.DAGfile.write("                commandRunning = False\n")
			self.DAGfile.write("            elif commandStatus in ('DeliveryTimedOut', 'ExecutionTimedOut', 'Failed', 'Cancelled', 'Undeliverable', 'Terminated', 'InvalidPlatform', 'AccessDenied'):\n")
			self.DAGfile.write("                raise\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        except botocore.exceptions.ClientError as err:\n")
			self.DAGfile.write("            if err.response['Error']['Code'] == 'InvocationDoesNotExist':\n")
			self.DAGfile.write("                pass\n")
			self.DAGfile.write("            else:\n")
			self.DAGfile.write("                raise err\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        time.sleep(5)\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    # Start fetching log from CloudWatch. Initial fetch will be from head and also fetch the token for upcomming messages\n")
			self.DAGfile.write("    nextTokenStdOut = None\n")
			self.DAGfile.write("    nextTokenStdErr = None\n")
			self.DAGfile.write("    logEventsRunning = False\n")
			self.DAGfile.write("    while logEventsRunning == False:\n")
			self.DAGfile.write("        try:\n")
			self.DAGfile.write("            responseStdOut = logs_client.get_log_events(\n")
			self.DAGfile.write("                logGroupName='dbimport',\n")
			self.DAGfile.write("                logStreamName=logStreamNameStdOut,\n")
			self.DAGfile.write("                startFromHead=True\n")
			self.DAGfile.write("                )\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            for event in responseStdOut['events']:\n")
			self.DAGfile.write("                if printAndFindEventErrors(event, startWithNewline = False) == True:\n")
			self.DAGfile.write("                    errorFound = True\n")
			# self.DAGfile.write("                try:\n")
			# self.DAGfile.write("                    print(event['message'], end=\"\")\n")
			# self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (1) !!!!!\")\n")
			# self.DAGfile.write("                        print(event)\n")
			# self.DAGfile.write("                except:\n")
			# self.DAGfile.write("                    print(event)\n")
			# self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (2) !!!!!\")\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            nextTokenStdOut = responseStdOut['nextForwardToken']\n")
			self.DAGfile.write("            logEventsRunning = True\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            responseStdErr = logs_client.get_log_events(\n")
			self.DAGfile.write("                logGroupName='dbimport',\n")
			self.DAGfile.write("                logStreamName=logStreamNameStdErr,\n")
			self.DAGfile.write("                startFromHead=True\n")
			self.DAGfile.write("                )\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            nextTokenStdErr = responseStdErr['nextForwardToken']\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            for event in responseStdErr['events']:\n")
			self.DAGfile.write("                if printAndFindEventErrors(event, startWithNewline = True) == True:\n")
			self.DAGfile.write("                    errorFound = True\n")
			# self.DAGfile.write("                try:\n")
			# self.DAGfile.write("                    print(event['message'], end=\"\")\n")
			# self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (1) !!!!!\")\n")
			# self.DAGfile.write("                        print(event)\n")
			# self.DAGfile.write("                except:\n")
			# self.DAGfile.write("                    print(event)\n")
			# self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (2) !!!!!\")\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        except botocore.exceptions.ClientError as err:\n")
			self.DAGfile.write("            if err.response['Error']['Code'] == 'ResourceNotFoundException':\n")
			self.DAGfile.write("                pass\n")
			self.DAGfile.write("            elif err.response['Error']['Code'] == 'ThrottlingException':\n")
			self.DAGfile.write("                pass\n")
			self.DAGfile.write("            else:\n")
			self.DAGfile.write("                print(\"DEBUG: %s\"%(err.response['Error']['Code']))\n")
			self.DAGfile.write("                raise err\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        time.sleep(1)\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    if commandStatus == 'TimedOut':\n")
			self.DAGfile.write("        print('AWS SSM Send-command timeout!')\n")
			self.DAGfile.write("        errorFound = True\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    # Loop while the command is running and print all messages from CloudWatch at the same time\n")
			self.DAGfile.write("    while commandStatus == 'InProgress':\n")
			self.DAGfile.write("        try:\n")
			self.DAGfile.write("            commandInvocation_response = ssm_client.get_command_invocation(\n")
			self.DAGfile.write("                CommandId=commandID,\n")
			self.DAGfile.write("                InstanceId=instanceID\n")
			self.DAGfile.write("                )\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            commandStatus = commandInvocation_response['Status']\n")
			# self.DAGfile.write("            print(commandInvocation_response)\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            if commandStatus == 'TimedOut':\n")
			self.DAGfile.write("                print('AWS SSM Send-command timeout!')\n")
			self.DAGfile.write("                errorFound = True\n")
			self.DAGfile.write("                break\n")
			self.DAGfile.write("            elif commandStatus != 'InProgress':\n")
			self.DAGfile.write("                break\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            responseStdOut = logs_client.get_log_events(\n")
			self.DAGfile.write("                logGroupName='dbimport',\n")
			self.DAGfile.write("                logStreamName=logStreamNameStdOut,\n")
			self.DAGfile.write("                nextToken=nextTokenStdOut\n")
			self.DAGfile.write("                )\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            nextTokenStdOut = responseStdOut['nextForwardToken']\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            for event in responseStdOut['events']:\n")
			self.DAGfile.write("                if printAndFindEventErrors(event, startWithNewline = False) == True:\n")
			self.DAGfile.write("                    errorFound = True\n")
			# self.DAGfile.write("                try:\n")
			# self.DAGfile.write("                    print(str(event['message']), end=\"\")\n")
			# self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (3) !!!!!\")\n")
			# self.DAGfile.write("                        print(event)\n")
			# self.DAGfile.write("                except:\n")
			# self.DAGfile.write("                    print(event)\n")
			# self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (4) !!!!!\")\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            if nextTokenStdErr == None:\n")
			self.DAGfile.write("                responseStdErr = logs_client.get_log_events(\n")
			self.DAGfile.write("                    logGroupName='dbimport',\n")
			self.DAGfile.write("                    logStreamName=logStreamNameStdErr,\n")
			self.DAGfile.write("                    startFromHead=True\n")
			self.DAGfile.write("                    )\n")
			self.DAGfile.write("            else:\n")
			self.DAGfile.write("                responseStdErr = logs_client.get_log_events(\n")
			self.DAGfile.write("                    logGroupName='dbimport',\n")
			self.DAGfile.write("                    logStreamName=logStreamNameStdErr,\n")
			self.DAGfile.write("                    nextToken=nextTokenStdErr\n")
			self.DAGfile.write("                    )\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            nextTokenStdErr = responseStdErr['nextForwardToken']\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            for event in responseStdErr['events']:\n")
			self.DAGfile.write("                if printAndFindEventErrors(event, startWithNewline = True) == True:\n")
			self.DAGfile.write("                    errorFound = True\n")
			# self.DAGfile.write("                try:\n")
			# self.DAGfile.write("                    print(str(event['message']), end=\"\")\n")
			# self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (3) !!!!!\")\n")
			# self.DAGfile.write("                        print(event)\n")
			# self.DAGfile.write("                except:\n")
			# self.DAGfile.write("                    print(event)\n")
			# self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (4) !!!!!\")\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        except botocore.exceptions.ClientError as err:\n")
			self.DAGfile.write("            if err.response['Error']['Code'] == 'ResourceNotFoundException':\n")
			self.DAGfile.write("                pass\n")
			self.DAGfile.write("            elif err.response['Error']['Code'] == 'ThrottlingException':\n")
			self.DAGfile.write("                pass\n")
			self.DAGfile.write("            else:\n")
			self.DAGfile.write("                print(\"DEBUG: %s\"%(err.response['Error']['Code']))\n")
			self.DAGfile.write("                raise err\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        except UnboundLocalError:\n")
			self.DAGfile.write("            pass\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        time.sleep(15)\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("    # Loop to get the last message and print in the output\n")
			self.DAGfile.write("    lastMessagePrinted = False\n")
			self.DAGfile.write("    while lastMessagePrinted == False:\n")
			self.DAGfile.write("        try:\n")
			self.DAGfile.write("            responseStdOut = logs_client.get_log_events(\n")
			self.DAGfile.write("                logGroupName='dbimport',\n")
			self.DAGfile.write("                logStreamName=logStreamNameStdOut,\n")
			self.DAGfile.write("                nextToken=nextTokenStdOut\n")
			self.DAGfile.write("                )\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            nextTokenStdOut = responseStdOut['nextForwardToken']\n")
			self.DAGfile.write("            for event in responseStdOut['events']:\n")
			self.DAGfile.write("                if printAndFindEventErrors(event, startWithNewline = False) == True:\n")
			self.DAGfile.write("                    errorFound = True\n")
			# self.DAGfile.write("                try:\n")
			# self.DAGfile.write("                    print(str(event['message']), end=\"\")\n")
			# self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (5) !!!!!\")\n")
			# self.DAGfile.write("                        print(event)\n")
			# self.DAGfile.write("                except:\n")
			# self.DAGfile.write("                    print(event)\n")
			# self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (6) !!!!!\")\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            lastMessagePrinted = True\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            if nextTokenStdErr == None:\n")
			self.DAGfile.write("                responseStdErr = logs_client.get_log_events(\n")
			self.DAGfile.write("                    logGroupName='dbimport',\n")
			self.DAGfile.write("                    logStreamName=logStreamNameStdErr,\n")
			self.DAGfile.write("                    startFromHead=True\n")
			self.DAGfile.write("                    )\n")
			self.DAGfile.write("            else:\n")
			self.DAGfile.write("                responseStdErr = logs_client.get_log_events(\n")
			self.DAGfile.write("                    logGroupName='dbimport',\n")
			self.DAGfile.write("                    logStreamName=logStreamNameStdErr,\n")
			self.DAGfile.write("                    nextToken=nextTokenStdErr\n")
			self.DAGfile.write("                    )\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            nextTokenStdErr = responseStdErr['nextForwardToken']\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("            for event in responseStdErr['events']:\n")
			self.DAGfile.write("                if printAndFindEventErrors(event, startWithNewline = True) == True:\n")
			self.DAGfile.write("                    errorFound = True\n")
			# self.DAGfile.write("                try:\n")
			# self.DAGfile.write("                    print(str(event['message']), end=\"\")\n")
			# self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (5) !!!!!\")\n")
			# self.DAGfile.write("                        print(event)\n")
			# self.DAGfile.write("                except:\n")
			# self.DAGfile.write("                    print(event)\n")
			# self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
			# # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
			# self.DAGfile.write("                        errorFound = True\n")
			# self.DAGfile.write("                        print(\"Error Found (6) !!!!!\")\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        except botocore.exceptions.ClientError as err:\n")
			self.DAGfile.write("            if err.response['Error']['Code'] == 'ResourceNotFoundException':\n")
			self.DAGfile.write("                pass\n")
			self.DAGfile.write("            elif err.response['Error']['Code'] == 'ThrottlingException':\n")
			self.DAGfile.write("                pass\n")
			self.DAGfile.write("            else:\n")
			self.DAGfile.write("                print(\"DEBUG: %s\"%(err.response['Error']['Code']))\n")
			self.DAGfile.write("                raise err\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        except UnboundLocalError:\n")
			self.DAGfile.write("            pass\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("        time.sleep(1)\n")
			self.DAGfile.write("\n")
			# self.DAGfile.write("    print(response)\n")
			# self.DAGfile.write("    print('Sequence - 008')\n")
			#self.DAGfile.write("    print(errorFound)\n")
			#self.DAGfile.write("    print(commandInvocation_response)\n")
			#self.DAGfile.write("    if commandStatus != 'Success':\n")
			self.DAGfile.write("    if errorFound == True:\n")
			#self.DAGfile.write("        print('Raising error as errorFound = True')\n")
			self.DAGfile.write("        raise\n")
			self.DAGfile.write("\n")
			#self.DAGfile.write("    print('AWS ssm send-command exit')\n")
			self.DAGfile.write("\n")

		self.DAGfile.write("\n")
		self.DAGfile.write("default_args = {\n")
		self.DAGfile.write("    'owner': 'airflow',\n")
		self.DAGfile.write("    'depends_on_past': False,\n")
		self.DAGfile.write("    'start_date': datetime(2017, 1, 1, 0, 0, tzinfo=local_tz),\n")
		self.DAGfile.write("    'max_active_runs': 1,\n")

		if email != None and email.strip() != '':
			email = email.split(',')
			self.DAGfile.write("    'email': %s,\n"%(email))
		else:
			self.DAGfile.write("    'email': Email_receiver,\n")

		if email_on_failure == 1:
			self.DAGfile.write("    'email_on_failure': True,\n")
		else:
			self.DAGfile.write("    'email_on_failure': False,\n")

		if email_on_retries == 1:
			self.DAGfile.write("    'email_on_retry': True,\n")
		else:
			self.DAGfile.write("    'email_on_retry': False,\n")

		if retryExponentialBackoff == 1:
			self.DAGfile.write("    'retry_exponential_backoff': True,\n")
		else:
			self.DAGfile.write("    'retry_exponential_backoff': False,\n")

		self.DAGfile.write("    'retries': 0,\n")
		self.DAGfile.write("    'pool': '%s',\n"%(defaultPool))
		self.DAGfile.write("    'retry_delay': timedelta(minutes=5),\n")

		if slaWarningTime != None and type(slaWarningTime) == time:
			self.DAGfile.write("    'sla': timedelta(hours=%s, minutes=%s, seconds=%s),\n"%(slaWarningTime.hour, slaWarningTime.minute, slaWarningTime.second))

		self.DAGfile.write("}\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("dag = DAG(\n")
		self.DAGfile.write("    '%s',\n"%(dagName))
		self.DAGfile.write("    default_args=default_args,\n")
		self.DAGfile.write("    description='%s',\n"%(dagName))
		self.DAGfile.write("    catchup=False,\n")

		if tags != None and tags.strip() != "":
			self.DAGfile.write("    tags=[%s],\n"%((', '.join(['"{}"'.format(value) for value in [x.strip() for x in tags.split(",")]]))))

		try:
			if int(concurrency) > 0: 
				self.DAGfile.write("    concurrency=%s,\n"%(int(concurrency)))
		except ValueError:
		 	pass

		self.DAGfile.write("    schedule_interval=%s)\n"%(cronSchedule))

		self.DAGfile.write("\n")
		if self.airflowMode == "default":
			self.DAGfile.write("start = BashOperator(\n")
			self.DAGfile.write("    task_id='start',\n")
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    bash_command='%sbin/manage --checkAirflowExecution --airflowDAG=%s',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), dagName))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    pool=None,\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")

			self.DAGfile.write("stop = BashOperator(\n")
			self.DAGfile.write("    task_id='stop',\n")
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    bash_command='%sbin/manage --sendAirflowStopMessage --airflowDAG=%s',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), dagName))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    pool=None,\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")

		elif self.airflowMode == "aws_mwaa":
			self.DAGfile.write("start = PythonOperator(\n")
			self.DAGfile.write("    task_id='start',\n")
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    python_callable=startDBImportExecution,\n")
			self.DAGfile.write("    op_kwargs={\"command\": \"manage --checkAirflowExecution --airflowDAG=%s\", \"instanceID\": \"%s\"},\n"%(dagName, self.getAWSInstanceID('start')))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    pool=None,\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")

			self.DAGfile.write("stop = PythonOperator(\n")
			self.DAGfile.write("    task_id='stop',\n")
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    python_callable=startDBImportExecution,\n")
			self.DAGfile.write("    op_kwargs={\"command\": \"manage --checkAirflowExecution --airflowDAG=%s\", \"instanceID\": \"%s\"},\n"%(dagName, self.getAWSInstanceID('stop')))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    pool=None,\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")

		if self.airflowMajorVersion == 1:
			self.DAGfile.write("def always_trigger(context, dag_run_obj):\n")
			self.DAGfile.write("    return dag_run_obj\n")
			self.DAGfile.write("\n")

		if importPhaseFinishFirst == True:
			self.DAGfile.write("Import_Phase_Finished = DummyOperator(\n")
			self.DAGfile.write("    task_id='Import_Phase_Finished',\n")
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")

		airflowDAGsensors = aliased(configSchema.airflowDagSensors, name="ads")
		sensors = (session.query(
					airflowDAGsensors.dag_name 
					)
				.select_from(airflowDAGsensors)
				.filter(airflowDAGsensors.dag_name == dagName)
				.count())

		if sensors > 0: tasksSensorsExists = True

		airflowTasks = aliased(configSchema.airflowTasks)
		tasks = pd.DataFrame(session.query(
					airflowTasks.dag_name,
					airflowTasks.placement)
				.select_from(airflowTasks)
				.filter(airflowTasks.dag_name == dagName)
				.filter(airflowTasks.include_in_airflow == 1)
				.all())

		if len(tasks) == 0:
			tasks = pd.DataFrame(columns=['dag_name', 'placement'])

		if len(tasks.loc[tasks['placement'] == 'before main']) > 0 or self.createPoolsWithTasks == True:
			tasksBeforeMainExists = True

		if len(tasks.loc[tasks['placement'] == 'after main']) > 0:
			tasksAfterMainExists = True


		if tasksBeforeMainExists == True:
			self.DAGfile.write("%s = DummyOperator(\n"%(self.preStopTask))
			self.DAGfile.write("    task_id='%s',\n"%(self.preStopTask))
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")
			
		if tasksAfterMainExists == True:
			self.DAGfile.write("%s = DummyOperator(\n"%(self.postStartTask))
			self.DAGfile.write("    task_id='%s',\n"%(self.postStartTask))
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")
			
		if tasksSensorsExists == True:
			self.DAGfile.write("%s = DummyOperator(\n"%(self.sensorStopTask))
			self.DAGfile.write("    task_id='%s',\n"%(self.sensorStopTask))
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")
			
		if tasksSensorsExists == False and tasksBeforeMainExists == True:
			self.mainStartTask = self.preStopTask

		if tasksSensorsExists == True and tasksBeforeMainExists == False:
			self.mainStartTask = self.sensorStopTask

		if tasksSensorsExists == True and tasksBeforeMainExists == True:
			self.preStartTask = self.sensorStopTask
			self.mainStartTask = self.preStopTask

		if tasksAfterMainExists == True:
			self.mainStopTask = self.postStartTask

		session.close()

		log.debug("sensorStartTask = %s"%(self.sensorStartTask))
		log.debug("sensorStopTask = %s"%(self.sensorStopTask))
		log.debug("preStartTask = %s"%(self.preStartTask))
		log.debug("preStopTask = %s"%(self.preStopTask))
		log.debug("mainStartTask = %s"%(self.mainStartTask))
		log.debug("mainStopTask = %s"%(self.mainStopTask))
		log.debug("postStartTask = %s"%(self.postStartTask))
		log.debug("postStopTask = %s"%(self.postStopTask))

	def closeDAGfile(self):
		log = logging.getLogger(self.logger)
		self.DAGfile.close()

		try:
			os.chmod(self.DAGfilename, int(self.DAGfilePermission, 8))	
		except PermissionError:
			log.warning("Could not change file mode to '%s'"%(self.DAGfilePermission))

		try:
			shutil.chown(self.DAGfilename, group=self.DAGfileGroup)
		except PermissionError:
			log.warning("Could not change group owner of file to '%s'. Permission Denied"%(self.DAGfileGroup))
		except LookupError:
			log.warning("Could not change group owner of file to '%s'. Group does not exists"%(self.DAGfileGroup))

		if self.writeDAG == True:
			if self.airflowMode == "default":
				try:
					shutil.copy(self.DAGfilename, self.DAGfilenameInAirflow)
					os.chmod(self.DAGfilenameInAirflow, int(self.DAGfilePermission, 8))	
					shutil.chown(self.DAGfilenameInAirflow, group=self.DAGfileGroup)
				except PermissionError:
					log.warning("Could not copy and change permission of file '%s'. Permission Denied"%(self.DAGfilenameInAirflow))
				except LookupError:
					pass
				else:
					print("DAG file written to %s"%(self.DAGfilenameInAirflow))

			elif self.airflowMode == "aws_mwaa":
				try:
					s3client = boto3.client('s3', region_name=self.common_config.awsRegion)
					# response = s3client.upload_file(self.DAGfilename, "dl-mwaa-artifacts-uat-eu-west-1-176686534647", self.DAGfilenameInAwsMWAA)
					response = s3client.upload_file(self.DAGfilename, self.DAGdirectory, self.DAGfilenameInAwsMWAA)
				except botocore.exceptions.ClientError as err:
					log.error(err)
				except boto3.exceptions.S3UploadFailedError as err:
					log.error(err)
				except botocore.exceptions.ParamValidationError as err:
					log.error(err)

		else:
			print("DAG file written to %s"%(self.DAGfilename))


	def addFailureTask(self, taskName):
		log = logging.getLogger(self.logger)
		self.DAGfile.write("%s = BashOperator(\n"%(taskName))
		self.DAGfile.write("    task_id='%s',\n"%(taskName))
		self.DAGfile.write("    bash_command='exit 1 ',\n")
		self.DAGfile.write("    priority_weight=1,\n")
		self.DAGfile.write("    weight_rule='absolute',\n")
		self.DAGfile.write("    dag=dag)\n")
		self.DAGfile.write("\n")

	def addTasksToDAGfile(self, dagName, mainDagSchedule, defaultRetries=0, defaultSudoUser=""):
		log = logging.getLogger(self.logger)

		# session = self.configDBSession()
		session = self.getDBImportSession()

		if defaultSudoUser == None: defaultSudoUser = ""

		airflowTasks = aliased(configSchema.airflowTasks)

		tasks = pd.DataFrame(session.query(
					airflowTasks.task_name,
					airflowTasks.task_type,
					airflowTasks.placement,
					airflowTasks.airflow_pool,
					airflowTasks.airflow_priority,
					airflowTasks.task_dependency_downstream,
					airflowTasks.task_dependency_upstream,
					airflowTasks.task_config,
					airflowTasks.jdbc_dbalias,
					airflowTasks.hive_db,
					airflowTasks.sensor_poke_interval,
					airflowTasks.sensor_timeout_minutes,
					airflowTasks.sensor_connection,
					airflowTasks.sensor_soft_fail,
					airflowTasks.sudo_user
					)
				.select_from(airflowTasks)
				.filter(airflowTasks.dag_name == dagName)
				.filter(airflowTasks.include_in_airflow == 1)
				.all()).fillna('')

		taskDependencies = ""
		allTaskDependencies = tasks.filter(['task_name', 'task_dependency_upstream', 'task_dependency_downstream', 'placement'])
		allTaskDependenciesUpstream = tasks.filter(['task_dependency_upstream'])
		allTaskDependenciesDownstream = tasks.filter(['task_dependency_downstream'])
		taskDependencyWarningPrinted = False

#		print(allTaskDependencies)
#		print(type(allTaskDependencies))

		for index, row in tasks.iterrows():
			sensorPokeInterval = row['sensor_poke_interval']
			taskName = row['task_name']

			sudoUser = row['sudo_user']
			if sudoUser == None or sudoUser == "":
				sudoUser = defaultSudoUser

			if sensorPokeInterval == '':
				# Default to 5 minutes
				sensorPokeInterval = 300

			sensorTimeoutSeconds = row['sensor_timeout_minutes']
			if sensorTimeoutSeconds == '':
				# Default to 5 hours
				sensorTimeoutSeconds = "18000"	
			else:
				sensorTimeoutSeconds = sensorTimeoutSeconds * 60

			if row['task_type'] == "DAG Sensor":
				addDagSensor = True
				airflowCustomDags = aliased(configSchema.airflowCustomDags)
				airflowEtlDags    = aliased(configSchema.airflowEtlDags)
				airflowExportDags = aliased(configSchema.airflowExportDags)
				airflowImportDags = aliased(configSchema.airflowImportDags)

				if "." in row['task_config']:
					waitForDag = row['task_config'].split(".")[0]
					waitForTask = row['task_config'].split(".")[1]
				else:
					waitForDag = row['task_config'].split(".")[0]
					waitForTask = "stop"
				
				importSensorSchedule = session.query(airflowImportDags.schedule_interval).filter(airflowImportDags.dag_name == waitForDag).one_or_none()		
				exportSensorSchedule = session.query(airflowExportDags.schedule_interval).filter(airflowExportDags.dag_name == waitForDag).one_or_none()		
				customSensorSchedule = session.query(airflowCustomDags.schedule_interval).filter(airflowCustomDags.dag_name == waitForDag).one_or_none()		
				etlSensorSchedule    = session.query(airflowEtlDags.schedule_interval).filter(airflowEtlDags.dag_name == waitForDag).one_or_none()		

				waitDagSchedule = ''
				if importSensorSchedule != None:
					waitDagSchedule = importSensorSchedule[0] 
				elif exportSensorSchedule != None:
					waitDagSchedule = exportSensorSchedule[0]
				elif customSensorSchedule != None:
					waitDagSchedule = customSensorSchedule[0]
				elif etlSensorSchedule != None:
					waitDagSchedule = etlSensorSchedule[0]

				if waitDagSchedule == '':
					log.warning("Issue on DAG '%s'"%(dagName))
					log.warning("There is no schedule interval for DAG to wait for")
					log.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")

					row['task_name'] = "%s_FAILURE"%(row['task_name'])
					self.addFailureTask(row['task_name'])
					addDagSensor = False
#					continue
#					log.error("Cant find schedule interval for DAG to wait for")
#					self.DAGfile.close()
#					self.common_config.remove_temporary_files()
#					sys.exit(1)

				mainDagMatchHourMin = re.search('^[0-2][0-9]:[0-5][0-9]$', mainDagSchedule)
				waitDagMatchHourMin = re.search('^[0-2][0-9]:[0-5][0-9]$', waitDagSchedule)

				timeDiff = "0"
	
				if ( mainDagMatchHourMin == None and waitDagMatchHourMin != None ) or (mainDagMatchHourMin != None and waitDagMatchHourMin == None):
					log.warning("Issue on DAG '%s'"%(dagName))
					log.warning("Both the current DAG and the DAG the sensor is waiting for must have the same scheduling format (HH:MM or cron)")
					log.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
					row['task_name'] = "%s_FAILURE"%(row['task_name'])
					self.addFailureTask(row['task_name'])
					addDagSensor = False

				if mainDagMatchHourMin != None and addDagSensor == True:
					# Time is in HH:MM format for both DAG's. So now we can calculate the diff in time between them
					noon = datetime.strptime('12:00', '%H:%M')
					mainDagScheduleDateTime = datetime.strptime(mainDagSchedule, '%H:%M')
					waitDagScheduleDateTime = datetime.strptime(waitDagSchedule, '%H:%M')

					# Add or remove half a day so we dont calculate over midnight.
					if mainDagScheduleDateTime > noon:
						mainDagScheduleDateTime = mainDagScheduleDateTime - timedelta(seconds=43200)
					else:
						mainDagScheduleDateTime = mainDagScheduleDateTime + timedelta(seconds=43200)

					if waitDagScheduleDateTime > noon:
						waitDagScheduleDateTime = waitDagScheduleDateTime - timedelta(seconds=43200)
					else:
						waitDagScheduleDateTime = waitDagScheduleDateTime + timedelta(seconds=43200)

					if mainDagScheduleDateTime > waitDagScheduleDateTime:
						timeDiff = mainDagScheduleDateTime - waitDagScheduleDateTime
						minusText = ''
					else:
						timeDiff = waitDagScheduleDateTime - mainDagScheduleDateTime
						minusText = '-'

					timeDiff = str(minusText + str(timeDiff.seconds))
				elif addDagSensor == True:
					if mainDagSchedule != waitDagSchedule:
						log.warning("Issue on DAG '%s'"%(dagName))
						log.warning("When using cron or cron alias schedules for DAG sensors, the schedule time in both DAG's must match")
						log.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
						row['task_name'] = "%s_FAILURE"%(row['task_name'])
						self.addFailureTask(row['task_name'])
						addDagSensor = False

				if addDagSensor == True:
					self.DAGfile.write("%s = ExternalTaskSensor(\n"%(row['task_name']))
					self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
					self.DAGfile.write("    external_dag_id='%s',\n"%(waitForDag))
					self.DAGfile.write("    external_task_id='%s',\n"%(waitForTask))
					self.DAGfile.write("    retries=0,\n")
					self.DAGfile.write("    execution_delta=timedelta(seconds=%s),\n"%(timeDiff))
					if row['airflow_pool'] != '':
						self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
					if row['airflow_priority'] != '':
						self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
						self.DAGfile.write("    weight_rule='absolute',\n")
					else:
						self.DAGfile.write("    priority_weight=100,\n")
						self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    timeout=%s,\n"%(int(sensorTimeoutSeconds)))
					self.DAGfile.write("    poke_interval=%s,\n"%(int(sensorPokeInterval)))
					self.DAGfile.write("    mode='reschedule',\n")
					if row['sensor_soft_fail'] is not None and row['sensor_soft_fail'] == 1:
						self.DAGfile.write("    soft_fail=True,\n")
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

			if row['task_type'] == "SQL Sensor":
				if row['sensor_connection'] == '':
					log.error("SQL Sensors requires a valid Airflow Connection ID in column 'sensor_connection'")
				else:
#					self.DAGfile.close()
#					self.common_config.remove_temporary_files()
#					sys.exit(1)

					sensorPokeInterval = row['sensor_poke_interval']
					if sensorPokeInterval == '':
						# Default to 5 minutes
						sensorPokeInterval = 300

					self.DAGfile.write("%s = SqlSensor(\n"%(row['task_name']))
					self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
					self.DAGfile.write("    conn_id='%s',\n"%(row['sensor_connection']))
					self.DAGfile.write("    sql=\"\"\"%s\"\"\",\n"%(row['task_config']))
					if row['airflow_pool'] != '':
						self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
					if row['airflow_priority'] != '':
						self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
						self.DAGfile.write("    weight_rule='absolute',\n")
					else:
						self.DAGfile.write("    priority_weight=100,\n")
						self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    timeout=%s,\n"%(int(sensorTimeoutSeconds)))
					self.DAGfile.write("    poke_interval=%s,\n"%(int(sensorPokeInterval)))
					self.DAGfile.write("    mode='reschedule',\n")
					if row['sensor_soft_fail'] is not None and row['sensor_soft_fail'] == 1:
						self.DAGfile.write("    soft_fail=True,\n")
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

			if row['task_type'] == "Trigger DAG":
				self.DAGfile.write("%s = TriggerDagRunOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    trigger_dag_id='%s',\n"%(row['task_config']))
				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=100,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				if self.airflowMajorVersion == 1:
					self.DAGfile.write("    python_callable=always_trigger,\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")
				
			if row['task_type'] == "shell script":
				self.DAGfile.write("%s = BashOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    retries=%s,\n"%(defaultRetries))
				self.DAGfile.write("    bash_command='%s ',\n"%(row['task_config']))
				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=1,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")
				
			if row['task_type'] == "Hive SQL Script":
				self.DAGfile.write("%s = BashOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    retries=%s,\n"%(defaultRetries))

				if row['hive_db'] != None and row['hive_db'].strip() != '':
					self.DAGfile.write("    bash_command='%sbin/manage --runHiveScript=%s --hiveDB=%s ',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), row['task_config'], row['hive_db']))
				else:
					self.DAGfile.write("    bash_command='%sbin/manage --runHiveScript=%s ',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), row['task_config']))

				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=1,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

			if row['task_type'] == "Hive SQL":
				jdbcSQL = row['task_config'].replace(r"'", "\\'")
				self.DAGfile.write("%s = BashOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    retries=%s,\n"%(defaultRetries))
				self.DAGfile.write("    bash_command='%sbin/manage --runHiveQuery=\"%s\" ',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), jdbcSQL))
				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=1,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")
				
			if row['task_type'] == "JDBC SQL":
				jdbcSQL = row['task_config'].replace(r"'", "\\'")
				self.DAGfile.write("%s = BashOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    retries=%s,\n"%(defaultRetries))
				self.DAGfile.write("    bash_command='%sbin/manage --dbAlias=%s --runJDBCQuery=\"%s\" ',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), row['jdbc_dbalias'], jdbcSQL))
				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=1,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

			if row['task_type'] == "DBImport command":
				dbimportCommand = row['task_config'].replace(r"'", "\\'")
				if self.airflowMode == "default":
					self.DAGfile.write("%s = BashOperator(\n"%(row['task_name']))
					self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
					self.DAGfile.write("    retries=%s,\n"%(defaultRetries))
					self.DAGfile.write("    bash_command='%sbin/%s',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), dbimportCommand))
					if row['airflow_pool'] != '':
						self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
					if row['airflow_priority'] != '':
						self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
						self.DAGfile.write("    weight_rule='absolute',\n")
					else:
						self.DAGfile.write("    priority_weight=1,\n")
						self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")
				elif self.airflowMode == "aws_mwaa":
					instanceID = self.getAWSInstanceID(row['task_name'])
					self.DAGfile.write("%s = PythonOperator(\n"%(row['task_name']))
					self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
					self.DAGfile.write("    retries=%s,\n"%(defaultRetries))
					self.DAGfile.write("    python_callable=startDBImportExecution,\n")
					self.DAGfile.write("    op_kwargs={\"command\": \"%s\", \"instanceID\": \"%s\"},\n"%(dbimportCommand, instanceID))
					if row['airflow_pool'] != '':
						self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
					else:
						if self.airflowAWSpoolToInstanceId == True:
							self.DAGfile.write("    pool='%s',\n"%(instanceID))
						else:
							self.DAGfile.write("    pool='%s',\n"%(dagName))
					if row['airflow_priority'] != '':
						self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
						self.DAGfile.write("    weight_rule='absolute',\n")
					else:
						self.DAGfile.write("    priority_weight=1,\n")
						self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

			if row['placement'] == "before main":
				tempStartTask = self.preStartTask
				tempStopTask = self.preStopTask

			if row['placement'] == "in main":
				tempStartTask = self.mainStartTask
				tempStopTask = self.mainStopTask

			if row['placement'] == "after main":
				tempStartTask = self.postStartTask
				tempStopTask = self.postStopTask

			for depIndex, depRow in allTaskDependencies.iterrows():
				if ( row['task_name'] == depRow['task_dependency_upstream'] or row['task_name'] == depRow['task_dependency_downstream'] ) and row['placement'] != depRow['placement']: 
					if taskDependencyWarningPrinted == False:
						log.warning("Issue on DAG '%s'"%(dagName))
						log.warning("There are custom tasks that have dependencies between 'in_main' and 'after_main'. This is not supported and the dependencies will be incorrect in Airflow")
						taskDependencyWarningPrinted = True
				
			# if row['placement'] == "in main" or row['placement'] == "after main":
			if 1 == 1:
#				if row['task_dependency_in_main_downstream'] == '':
#					# No dependency, set to "mainStart" task, what ever that might be
#					taskDependencies += "%s.set_downstream(%s)\n"%(self.mainStartTask, row['task_name'])
#				else:
#					# Check if there is any dependencies for this task
#					for dep in row['task_dependency_in_main_downstream'].split(','):	
#						dep = dep.strip()
#						if dep == "main_start":
#							dep = self.mainStartTask
#
#						taskDependencies += "%s.set_downstream(%s)\n"%(dep, row['task_name'])
				
				downStreamFound = False
				upStreamFound = False

				for depIndex, depRow in allTaskDependencies.iterrows():
					for depOnDep in depRow['task_dependency_downstream'].split(','):
						# depOnDep contains that tasks dependencies and we need to check if there is a dependency against the task we are working on
						depOnDep = depOnDep.strip()
						if depRow['placement'] == row['placement']:
							# Only do this if both placements are the same
							if row['task_name'] == depOnDep:
								# There is a match that means there is a downstrem dependency for another ask agains the current task
								upStreamFound = True

					for depOnDep in depRow['task_dependency_upstream'].split(','):
						# depOnDep contains that tasks dependencies and we need to check if there is a dependency against the task we are working on
						depOnDep = depOnDep.strip()
						if depRow['placement'] == row['placement']:
							# Only do this if both placements are the same
							if row['task_name'] == depOnDep:
								# There is a match that means there is a downstrem dependency for another ask agains the current task
								downStreamFound = True


				for dep in row['task_dependency_downstream'].split(','):
					dep = dep.strip()
					if dep == "":
						continue

					# Get the task that this have a depenceny against and check if it's in the same placement
					depRow = allTaskDependencies.loc[allTaskDependencies['task_name'] == dep, ['placement']]
					if depRow.empty == False:
						depRowValue = depRow.values[0][0]
						if depRow.values[0][0] == row['placement']:
							# Only do this if both placements are the same
							taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], dep)
							downStreamFound = True
						else:
							log.warning("Downstream dependency against '%s' wont work as it's not in the same placement as '%s'."%(dep, row['task_name']))
					else:
						# The depRow dataframe is empty. So the dependent task is not a customTask, but instead from import_tables or export_tables. So we just add it as a dependency
						taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], dep)
						downStreamFound = True

				for dep in row['task_dependency_upstream'].split(','):	
					dep = dep.strip()
					if dep == "":
						continue

					# Get the task that this have a depenceny against and check if it's in the same placement
					depRow = allTaskDependencies.loc[allTaskDependencies['task_name'] == dep, ['placement']]
					if depRow.empty == False:
						depRowValue = depRow.values[0][0]
						if depRow.values[0][0] == row['placement']:
							# Only do this if both placements are the same
							taskDependencies += "%s.set_upstream(%s)\n"%(row['task_name'], dep)
							upStreamFound = True
						else:
							log.warning("Upstream dependency against '%s' wont work as it's not in the same placement as '%s'."%(dep, row['task_name']))
					else:
						# The depRow dataframe is empty. So the dependent task is not a customTask, but instead from import_tables or export_tables. So we just add it as a dependency
						taskDependencies += "%s.set_upstream(%s)\n"%(row['task_name'], dep)
						upStreamFound = True

				
				if upStreamFound == False:
					taskDependencies += "%s.set_downstream(%s)\n"%(tempStartTask, row['task_name'])
				
				if downStreamFound == False:
					taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], tempStopTask)

				# Check if other tasks have this task as upstream or downstream
#				for depIndex, depRow in allTaskDependencies.iterrows():
#					pass

#				if row['task_dependency_in_main_upstream'] == '':
#					# No dependency, set to "mainStart" task, what ever that might be
#					# taskDependencies += "%s.set_upstream(%s)\n"%(row['task_name'], self.mainStartTask)
#					taskDependencies += "%s.set_upstream(%s)\n"%(row['task_name'], tempStartTask)
#				else:
#					# Check if there is any dependencies for this task
#					for dep in row['task_dependency_in_main_upstream'].split(','):	
#						dep = dep.strip()
#						taskDependencies += "%s.set_upstream(%s)\n"%(row['task_name'], dep)
#
#				if row['task_dependency_in_main_downstream'] != '':
#					for dep in row['task_dependency_in_main_downstream'].split(','):
#						dep = dep.strip()
#						taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], dep)

				# We also need to check if there is any dependencies on this task. If there is, we dont add a set_downstream to the stop taks as that will be handled by the other task
#				foundDependency = False
#				for dep in allTaskDependenciesUpstream.values:
#					for depTask in dep[0].split(','):
#						depTask = depTask.strip()
#						if depTask != '' and depTask == row['task_name']:
#							foundDependency = True
#
#				if foundDependency == False and row['task_dependency_downstream'] == '':
#					# taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], self.mainStopTask)
#					taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], tempStopTask)

				taskDependencies += "\n"


		self.DAGfile.write(taskDependencies)
		session.close()
			
	def addSensorsToDAGfile(self, dagName, mainDagSchedule):
		log = logging.getLogger(self.logger)
		# session = self.configDBSession()
		session = self.getDBImportSession()

		airflowDAGsensors = aliased(configSchema.airflowDagSensors, name="ads")
		airflowCustomDags = aliased(configSchema.airflowCustomDags, name="acd")
		airflowEtlDags    = aliased(configSchema.airflowEtlDags, name="aetld")
		airflowExportDags = aliased(configSchema.airflowExportDags, name="aed")
		airflowImportDags = aliased(configSchema.airflowImportDags, name="aid")

		sensors = pd.DataFrame(session.query(
					airflowDAGsensors.dag_name,
					airflowDAGsensors.sensor_name,
					airflowDAGsensors.wait_for_dag,
					airflowDAGsensors.wait_for_task,
					airflowDAGsensors.timeout_minutes,
					airflowDAGsensors.sensor_soft_fail,
					airflowImportDags.schedule_interval.label("import_schedule"),
					airflowExportDags.schedule_interval.label("export_schedule"),
					airflowCustomDags.schedule_interval.label("custom_schedule"),
					airflowEtlDags.schedule_interval.label("etl_schedule")
					)
				.select_from(airflowDAGsensors)
				.join(airflowImportDags, airflowDAGsensors.wait_for_dag == airflowImportDags.dag_name, isouter=True)
				.join(airflowExportDags, airflowDAGsensors.wait_for_dag == airflowExportDags.dag_name, isouter=True)
				.join(airflowCustomDags, airflowDAGsensors.wait_for_dag == airflowCustomDags.dag_name, isouter=True)
				.join(airflowEtlDags,    airflowDAGsensors.wait_for_dag == airflowEtlDags.dag_name, isouter=True)
				.filter(airflowDAGsensors.dag_name == dagName)
				.all()).fillna('')

		for index, row in sensors.iterrows():
			waitDagSchedule = ''
			if row["import_schedule"] != '':
				waitDagSchedule = row["import_schedule"]
			elif row["export_schedule"] != '':
				waitDagSchedule = row["export_schedule"]
			elif row["custom_schedule"] != '':
				waitDagSchedule = row["custom_schedule"]
			elif row["etl_schedule"] != '':
				waitDagSchedule = row["etl_schedule"]

			if waitDagSchedule == '':
				log.warning("Issue on DAG '%s'"%(dagName))
				log.warning("There is no schedule interval for DAG to wait for")
				log.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
				self.addFailureTask("%s_FAILURE"%(row['sensor_name']))
				self.DAGfile.write("%s.set_downstream(%s_FAILURE)\n"%(self.sensorStartTask, row['sensor_name']))
				self.DAGfile.write("%s_FAILURE.set_downstream(%s)\n"%(row['sensor_name'], self.sensorStopTask))
				self.DAGfile.write("\n")
				continue
#				log.error("Cant find schedule interval for DAG to wait for")
#				self.DAGfile.close()
#				self.common_config.remove_temporary_files()
#				sys.exit(1)

			waitForTask = row['wait_for_task']
			if waitForTask == '':
				waitForTask = "stop"

			timeoutSeconds = row['timeout_minutes']
			if timeoutSeconds == '':
				# Default to 5 hours
				timeoutSeconds = "18000"	
			else:
				timeoutSeconds = timeoutSeconds * 60

			mainDagMatchHourMin = re.search('^[0-2][0-9]:[0-5][0-9]$', mainDagSchedule)
			waitDagMatchHourMin = re.search('^[0-2][0-9]:[0-5][0-9]$', waitDagSchedule)

#			print(mainDagSchedule)
#			print(waitDagSchedule)
#			print(mainDagMatchHourMin)
#			print(waitDagMatchHourMin)

			timeDiff = "0"

			if ( mainDagMatchHourMin == None and waitDagMatchHourMin != None ) or (mainDagMatchHourMin != None and waitDagMatchHourMin == None):
				log.warning("Issue on DAG '%s'"%(dagName))
				log.warning("Both the current DAG and the DAG the sensor is waiting for must have the same scheduling format (HH:MM or cron)")
				log.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
				self.addFailureTask("%s_FAILURE"%(row['sensor_name']))
				self.DAGfile.write("%s.set_downstream(%s_FAILURE)\n"%(self.sensorStartTask, row['sensor_name']))
				self.DAGfile.write("%s_FAILURE.set_downstream(%s)\n"%(row['sensor_name'], self.sensorStopTask))
				self.DAGfile.write("\n")
				continue
#				self.DAGfile.close()
#				self.common_config.remove_temporary_files()
#				sys.exit(1)

			if mainDagMatchHourMin != None:
				# Time is in HH:MM format for both DAG's. So now we can calculate the diff in time between them
				noon = datetime.strptime('12:00', '%H:%M')
				mainDagScheduleDateTime = datetime.strptime(mainDagSchedule, '%H:%M')
				waitDagScheduleDateTime = datetime.strptime(waitDagSchedule, '%H:%M')

				# Add or remove half a day so we dont calculate over midnight.
				if mainDagScheduleDateTime > noon:
					mainDagScheduleDateTime = mainDagScheduleDateTime - timedelta(seconds=43200)
				else:
					mainDagScheduleDateTime = mainDagScheduleDateTime + timedelta(seconds=43200)

				if waitDagScheduleDateTime > noon:
					waitDagScheduleDateTime = waitDagScheduleDateTime - timedelta(seconds=43200)
				else:
					waitDagScheduleDateTime = waitDagScheduleDateTime + timedelta(seconds=43200)

#				print (mainDagScheduleDateTime)
#				print (waitDagScheduleDateTime)

				if mainDagScheduleDateTime > waitDagScheduleDateTime:
					timeDiff = mainDagScheduleDateTime - waitDagScheduleDateTime
					minusText = ''
				else:
					timeDiff = waitDagScheduleDateTime - mainDagScheduleDateTime
					minusText = '-'

				timeDiff = str(minusText + str(timeDiff.seconds))
#				print (timeDiff)
			else:
				if mainDagSchedule != waitDagSchedule:
					log.warning("Issue on DAG '%s'"%(dagName))
					log.warning("When using cron or cron alias schedules for DAG sensors, the schedule time in both DAG's must match")
					log.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
					self.addFailureTask("%s_FAILURE"%(row['sensor_name']))
					self.DAGfile.write("%s.set_downstream(%s_FAILURE)\n"%(self.sensorStartTask, row['sensor_name']))
					self.DAGfile.write("%s_FAILURE.set_downstream(%s)\n"%(row['sensor_name'], self.sensorStopTask))
					self.DAGfile.write("\n")
					continue
#					self.DAGfile.close()
#					self.common_config.remove_temporary_files()
#					sys.exit(1)

			self.DAGfile.write("%s = ExternalTaskSensor(\n"%(row['sensor_name']))
			self.DAGfile.write("    task_id='%s',\n"%(row['sensor_name']))
			self.DAGfile.write("    external_dag_id='%s',\n"%(row['wait_for_dag']))
			self.DAGfile.write("    external_task_id='%s',\n"%(waitForTask))
			self.DAGfile.write("    retries=0,\n")
#			self.DAGfile.write("    execution_timeout=timedelta(seconds=%s),\n"%(timeoutSeconds))
			self.DAGfile.write("    execution_delta=timedelta(seconds=%s),\n"%(timeDiff))
			self.DAGfile.write("    timeout=%s,\n"%(int(timeoutSeconds)))
			self.DAGfile.write("    poke_interval=300,\n")
			self.DAGfile.write("    mode='reschedule',\n")
			if row['sensor_soft_fail'] is not None and row['sensor_soft_fail'] == 1:
				self.DAGfile.write("    soft_fail=True,\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("%s.set_downstream(%s)\n"%(self.sensorStartTask, row['sensor_name']))
			self.DAGfile.write("%s.set_downstream(%s)\n"%(row['sensor_name'], self.sensorStopTask))
			self.DAGfile.write("\n")

		session.close()

	def sendJSON(self, airflowDAG, status): 
		""" Sends a start JSON document to REST and/or Kafka """
		log = logging.getLogger(self.logger)
		log.debug("Executing Airflow.sendStartJSON()")

		self.postAirflowDAGoperations = self.common_config.getConfigValue(key = "post_airflow_dag_operations")
		self.postDataToREST = self.common_config.getConfigValue(key = "post_data_to_rest")
		self.postDataToKafka = self.common_config.getConfigValue(key = "post_data_to_kafka")

#		self.postDataToREST = True

		if airflowDAG == None:
			return

		if self.postAirflowDAGoperations == False:
			log.info("Airflow DAG post to Kafka and/or Rest have been disabled. No message will be sent")
			return

		jsonData = {}
		jsonData["type"] = "airflow_dag"
		jsonData["status"] = status
		jsonData["dag"] = airflowDAG
		jsonData["airflow_timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

		if self.postDataToKafka == True:
			result = self.sendStatistics.publishKafkaData(json.dumps(jsonData))
			if result == False:
				log.warning("Kafka publish failed! No start message posted")

		if self.postDataToREST == True:
			response = self.sendStatistics.sendRESTdata(json.dumps(jsonData))
			if response != 200:
				log.warn("REST call failed! No start message posted")

		log.debug("Executing Airflow.sendStartJSON() - Finished")
	


