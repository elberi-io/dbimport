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

import sys
import re
import logging
import subprocess
import errno, os, pty
import shlex
from subprocess import Popen, PIPE
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from common.Singleton import Singleton
from common.Exceptions import *
from common import constants as constant
from common import sparkUDF as sparkUDF 
from DBImportConfig import import_config
from DBImportOperation import common_operations
from DBImportOperation import atlas_operations
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
import base64
import importlib
import json
import requests
from requests_kerberos import HTTPKerberosAuth, REQUIRED, OPTIONAL

class operation(object, metaclass=Singleton):
	def __init__(self, Hive_DB=None, Hive_Table=None):
		logging.debug("Executing import_operations.__init__()")
		self.Hive_DB = None
		self.Hive_Table = None
		self.mysql_conn = None
		self.mysql_cursor = None
		self.startDate = None
		self.import_config = None
		self.sqlGeneratedHiveColumnDefinition = None
		self.sqlGeneratedSqoopQuery = None
		self.sqoopStartTimestamp = None
		self.sqoopStartUTS = None
		self.sqoopSize = None
		self.sqoopRows = None
		self.sqoopIncrMaxValuePending = None
		self.sqoopIncrNoNewRows = None
		self.sqoopLaunchedMapTasks = None
		self.sqoopFailedMapTasks = None
		self.sqoopKilledMapTasks = None
		self.sqoopLaunchedReduceTasks = None
		self.sqoopFailedReduceTasks = None
		self.sqoopKilledReduceTasks = None
		self.sparkContext = None
		self.spark = None
		self.sparkUDF = None
		self.udfHashColumn = None
		self.udfReplaceCharWithStar = None
		self.udfShowFirstFourCharacters = None
		self.yarnApplicationID = None

		self.globalHiveConfigurationSet = False

		self.import_config = import_config.config(Hive_DB, Hive_Table)
		self.common_operations = common_operations.operation(Hive_DB, Hive_Table)
		self.atlasOperation = atlas_operations.atlasOperation()

		# self.import_config.sparkCatalogName = self.common_operations.sparkCatalogName

		if Hive_DB != None and Hive_Table != None:
			self.setHiveTable(Hive_DB, Hive_Table)

		logging.debug("Executing import_operations.__init__() - Finished")

	def setHiveTable(self, Hive_DB, Hive_Table):
		""" Sets the parameters to work against a new Hive database and table """
		self.Hive_DB = Hive_DB.lower()
		self.Hive_Table = Hive_Table.lower()

		self.common_operations.setHiveTable(self.Hive_DB, self.Hive_Table)
		self.import_config.setHiveTable(self.Hive_DB, self.Hive_Table)

		self.import_config.getImportConfig()
		self.startDate = self.import_config.startDate

		self.Hive_Import_DB = self.import_config.Hive_Import_DB 
		self.Hive_Import_Table = self.import_config.Hive_Import_Table 
		self.Hive_Import_View = self.import_config.Hive_Import_View 
		self.Hive_History_DB = self.import_config.Hive_History_DB 
		self.Hive_History_Table = self.import_config.Hive_History_Table 
		self.Hive_HistoryTemp_DB = self.import_config.Hive_HistoryTemp_DB 
		self.Hive_HistoryTemp_Table = self.import_config.Hive_HistoryTemp_Table 
		self.Hive_Import_PKonly_DB = self.import_config.Hive_Import_PKonly_DB
		self.Hive_Import_PKonly_Table = self.import_config.Hive_Import_PKonly_Table 
		self.Hive_Delete_DB = self.import_config.Hive_Delete_DB 
		self.Hive_Delete_Table = self.import_config.Hive_Delete_Table 
		self.Hive_ColumnName_Import = self.import_config.Hive_ColumnName_Import 
		self.Hive_ColumnName_Insert = self.import_config.Hive_ColumnName_Insert 
		self.Hive_ColumnName_Update = self.import_config.Hive_ColumnName_Update
		self.Hive_ColumnName_Delete = self.import_config.Hive_ColumnName_Delete
		self.Hive_ColumnName_IUD = self.import_config.Hive_ColumnName_IUD
		self.Hive_ColumnName_HistoryTimestamp = self.import_config.Hive_ColumnName_HistoryTimestamp
		self.Hive_ColumnName_Source = self.import_config.Hive_ColumnName_Source

		self.common_operations.setTableAndColumnNames(
			Hive_Import_DB = self.Hive_Import_DB, 
			Hive_Import_Table = self.Hive_Import_Table, 
			Hive_Import_View = self.Hive_Import_View, 
			Hive_History_DB = self.Hive_History_DB, 
			Hive_History_Table = self.Hive_History_Table, 
			Hive_HistoryTemp_DB = self.Hive_HistoryTemp_DB, 
			Hive_HistoryTemp_Table = self.Hive_HistoryTemp_Table, 
			Hive_Import_PKonly_DB = self.Hive_Import_PKonly_DB, 
			Hive_Import_PKonly_Table = self.Hive_Import_PKonly_Table, 
			Hive_Delete_DB = self.Hive_Delete_DB, 
			Hive_Delete_Table = self.Hive_Delete_Table, 
			Hive_ColumnName_Import = self.Hive_ColumnName_Import, 
			Hive_ColumnName_Insert = self.Hive_ColumnName_Insert, 
			Hive_ColumnName_Update = self.Hive_ColumnName_Update,
			Hive_ColumnName_Delete = self.Hive_ColumnName_Delete, 
			Hive_ColumnName_IUD = self.Hive_ColumnName_IUD,
			Hive_ColumnName_HistoryTimestamp = self.Hive_ColumnName_HistoryTimestamp,
			Hive_ColumnName_Source = self.Hive_ColumnName_Source)

		self.import_config.lookupConnectionAlias()

	def remove_temporary_files(self):
		self.import_config.remove_temporary_files()

	def checkTimeWindow(self):
		self.import_config.checkTimeWindow()
	
	def runStage(self, stage, stageMax = None):
		self.import_config.setStage(stage)

		if self.import_config.common_config.getConfigValue(key = "import_stage_disable") == True:
			logging.error("Stage execution disabled from DBImport configuration")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		tempStage = self.import_config.getStage()
		if stageMax == None:
			if tempStage == stage:
				return True
			else:
				return False
		else:
			if tempStage >= stage and tempStage <= stageMax:
				return True
			else:
				return False
	
	def setStage(self, stage, force=False):
		self.import_config.setStage(stage, force=force)
	
	def getStage(self):
		return self.import_config.getStage()
	
	def clearStage(self):
		self.import_config.clearStage()
	
	def saveRetryAttempt(self, stage):
		self.import_config.saveRetryAttempt(stage)
	
	def setStageOnlyInMemory(self):
		self.import_config.setStageOnlyInMemory()
	
	def saveIncrPendingValues(self):
		self.import_config.saveIncrPendingValues()

	def saveIncrMinValue(self):
		self.import_config.saveIncrMinValue()

	def convertStageStatisticsToJSON(self):
		self.import_config.convertStageStatisticsToJSON()
	
	def saveStageStatistics(self):
		self.import_config.saveStageStatistics()

	def sendStartJSON(self):
		self.import_config.sendStartJSON()
	
	def invalidateImpala(self):
		self.import_config.invalidateImpala()
	
	def updateAtlasWithImportData(self):
		if self.atlasOperation.checkAtlasSchema() == True:
			configObject = self.import_config.common_config.getAtlasDiscoverConfigObject()
			self.atlasOperation.setConfiguration(configObject)

			if self.import_config.common_config.source_columns_df.empty == True:
				self.import_config.getJDBCTableDefinition()

			self.atlasOperation.source_columns_df = self.import_config.common_config.source_columns_df
			self.atlasOperation.source_keys_df = self.import_config.common_config.source_keys_df

			startStopDict = self.import_config.stage.getStageStartStop(stage = self.import_config.importTool)

			try:
				self.atlasOperation.updateAtlasWithRDBMSdata(schemaName=self.import_config.source_schema, tableName=self.import_config.source_table)

				self.atlasOperation.updateAtlasWithImportLineage(
					Hive_DB=self.Hive_DB, 
					sourceSchema=self.import_config.source_schema, 
					sourceTable=self.import_config.source_table, 
					sqoopHdfsLocation=self.import_config.sqoop_hdfs_location,
					Hive_Table=self.Hive_Table, 
					startStopDict=startStopDict,
					fullExecutedCommand=self.import_config.fullExecutedCommand, 
					importTool=self.import_config.importTool)
			except:
				pass

			logging.info("")	# Just to get a blank row before connecting to Hive

	def checkDB(self, hiveDB):
		try:
			self.common_operations.checkDB(hiveDB)
		except databaseNotFound as errMsg:
			logging.error(errMsg)
			self.import_config.remove_temporary_files()
			sys.exit(1)
		except:
			self.import_config.remove_temporary_files()
			raise

	def getJDBCTableValidationData(self):
		if self.import_config.validationMethod == "customQuery":
			self.import_config.runCustomValidationQueryOnJDBCTable()
		else:
			self.getJDBCTableRowCount()

	def validateImportTool(self):
#		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
		if self.import_config.validationMethod == "customQuery":
			logging.info("No validation of spark/sqoop import when running with a customQuery validation")
			pass
		else:
			self.validateImportRowCount()

	def getImportTableValidationData(self):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			if self.import_config.validationMethod == "customQuery":
				if self.import_config.validationCustomQueryValidateImportTable == True:
					self.runCustomValidationQueryOnImportTable()
				else:
					logging.info("No validation of import table as that is disabled in configuration")
			else:
				self.getImportTableRowCount()

	def getTargetTableValidationData(self, incrementalMax=None):
		if self.import_config.validationMethod == "customQuery":
			if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				raise invalidConfiguration("Custom Query validation is not supported with Spark as the ETL engine (yet)")
			else:
				self.runCustomValidationQueryOnTargetTable()
		else:
			self.getTargetTableRowCount(incrementalMax=incrementalMax)


	def runCustomValidationQueryOnImportTable(self):
		logging.debug("Executing import_operations.runCustomValidationQueryOnImportTable()")

		logging.info("Executing custom validation query on Import table.")

		query = self.import_config.validationCustomQueryHiveSQL
		query = query.replace("${HIVE_DB}", self.import_config.Hive_Import_DB)
		query = query.replace("${HIVE_TABLE}", self.import_config.Hive_Import_Table)

		logging.debug("Validation Query on Import table: %s" % (query) )

		resultDF = self.common_operations.executeHiveQuery(query)
		resultJSON = resultDF.to_json(orient="values")
		self.import_config.validationCustomQueryHiveValue = resultJSON

		if len(self.import_config.validationCustomQueryHiveValue) > 1024:
			logging.warning("'%s' is to large." % (self.import_config.validationCustomQueryHiveValue))
			raise invalidConfiguration("The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes")

		self.import_config.saveCustomSQLValidationHiveValue(jsonValue = resultJSON)

		logging.debug("resultDF:")
		logging.debug(resultDF)
		logging.debug("resultJSON: %s" % (resultJSON))

		logging.debug("Executing import_operations.runCustomValidationQueryOnImportTable() - Finished")

	def runCustomValidationQueryOnTargetTable(self):
		logging.debug("Executing import_operations.runCustomValidationQueryOnTargetTable()")

		logging.info("Executing custom validation query on Target table.")

		query = self.import_config.validationCustomQueryHiveSQL
		query = query.replace("${HIVE_DB}", self.import_config.Hive_DB)
		query = query.replace("${HIVE_TABLE}", self.import_config.Hive_Table)

		logging.debug("Validation Query on Target table: %s" % (query) )

		resultDF = self.common_operations.executeHiveQuery(query)
		resultJSON = resultDF.to_json(orient="values")
		self.import_config.validationCustomQueryHiveValue = resultJSON

		if len(self.import_config.validationCustomQueryHiveValue) > 1024:
			logging.warning("'%s' is to large." % (self.import_config.validationCustomQueryHiveValue))
			raise invalidConfiguration("The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes")

		self.import_config.saveCustomSQLValidationHiveValue(jsonValue = resultJSON)

		logging.debug("resultDF:")
		logging.debug(resultDF)
		logging.debug("resultJSON: %s" % (resultJSON))

		logging.debug("Executing import_operations.runCustomValidationQueryOnTargetTable() - Finished")

	def getJDBCTableRowCount(self):
		try:
			self.import_config.getJDBCTableRowCount()
		except:
			logging.exception("Fatal error when reading source table row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getMongoRowCount(self):
		try:
			self.import_config.getMongoRowCount()
		except:
			logging.exception("Fatal error when reading Mongo row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getImportViewRowCountAsSource(self, incr=False):
		whereStatement = None
		if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
			# Needed otherwise we will count rows that will be deleted and that will cause the validation to fail
			whereStatement = "datalake_flashback_operation != 'D' or datalake_flashback_operation is null"

		try:
			importViewRowCount = self.common_operations.getHiveTableRowCount(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table, whereStatement = whereStatement)
			self.import_config.saveSourceTableRowCount(importViewRowCount, incr=incr)
		except:
			logging.exception("Fatal error when reading Hive view row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getImportTableRowCount(self, excludeOracleFlashbackDeletes=False):

		whereStatement = None

		if excludeOracleFlashbackDeletes == True:
			# Needed otherwise we will count rows that will be deleted and that will cause the validation to fail
			whereStatement = "datalake_flashback_operation != 'D' or datalake_flashback_operation is null"

		try:
			importTableRowCount = self.common_operations.getHiveTableRowCount(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table, whereStatement = whereStatement)
			self.import_config.saveHiveTableRowCount(importTableRowCount)
		except:
			logging.exception("Fatal error when reading Hive table row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getSparkTableRowCount(self, hiveDB, hiveTable, whereStatement=None):
			logging.debug("Executing import_operations.getSparkTableRowCount()")
			logging.info("Reading the number of rows from %s.%s"%(hiveDB, hiveTable))
			rowCount = 0

			query = "select count(1) as rowcount from %s.`%s`.`%s` "%(self.common_operations.sparkCatalogName, hiveDB, hiveTable)
			if whereStatement != None:
				query += "where " + whereStatement
			else:
				query += "limit 1"

			self.startSpark()
			logging.info(query)
			tableSizeResult = self.spark.sql(query)
			rowCount = int(tableSizeResult.first()['rowcount'])

			logging.debug("Rowcount from %s.%s: %s"%(hiveDB, hiveTable, rowCount))
			logging.debug("Executing import_operations.getSparkTableRowCount() - Finished")
			return rowCount


	def getTargetTableRowCount(self, incrementalMax=None):
		try:
			whereStatement = self.import_config.getIncrWhereStatement(ignoreIfOnlyIncrMax=True, ignoreSQLwhereAddition=True, incrementalMax=incrementalMax)

			if whereStatement == None and self.import_config.import_with_merge == True and self.import_config.soft_delete_during_merge == True:
				# whereStatement = "datalake_iud != 'D' or datalake_iud is null"
				whereStatement = "%s != 'D' or %s is null"%(self.Hive_ColumnName_IUD, self.Hive_ColumnName_IUD)

			if whereStatement == None and self.import_config.importPhase == constant.IMPORT_PHASE_FULL and self.import_config.etlPhase == constant.ETL_PHASE_INSERT:
				# whereStatement = "datalake_import == '%s'"%(self.import_config.sqoop_last_execution_timestamp)
				whereStatement = "%s == '%s'"%(self.Hive_ColumnName_Import, self.import_config.sqoop_last_execution_timestamp)

			logging.debug("whereStatement: %s"%(whereStatement))

			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				targetTableRowCount = self.common_operations.getHiveTableRowCount(self.import_config.Hive_DB, self.import_config.Hive_Table, whereStatement=whereStatement)

			if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				targetTableRowCount = self.getSparkTableRowCount(self.import_config.Hive_DB, self.import_config.Hive_Table, whereStatement=whereStatement)

			self.import_config.saveHiveTableRowCount(targetTableRowCount)

		except:
			logging.exception("Fatal error when reading Hive table row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def clearValidationData(self):
		try:
			self.import_config.clearValidationData()
		except:
			logging.exception("Fatal error when clearing validation data from previous imports")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def validateImportTable(self):
		# This is executed as the first step in the ETL stage. If it's not Hive, then we dont need to execute this as the table is already verified by the import tool
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			if self.import_config.validationMethod == "customQuery":
				if self.import_config.validationCustomQueryValidateImportTable == True:
					if self.import_config.validateCustomQuery() == True:
						logging.info("Import table validation successful!")
			else:
				if self.import_config.import_is_incremental == False:
					self.validateRowCount()
				else:
					self.validateIncrRowCount()

	def validateTargetTable(self):
		if self.import_config.validationMethod == "customQuery":
			if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				raise invalidConfiguration("Custom Query validation is not supported with Spark as the ETL engine (yet)")
				
			if self.import_config.validateCustomQuery() == True:
				logging.info("Target table validation successful!")
		else:
			self.validateRowCount()

	def validateRowCount(self):
		logging.debug("Executing import_operations.validateRowCount()")
		try:
			validateResult = self.import_config.validateRowCount(validateSqoop=False, incremental=False) 
		except:
			logging.exception("Fatal error when validating imported rows")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if validateResult == False:
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def validateImportRowCount(self):
		logging.debug("Executing import_operations.validateImportRowCount()")
		try:
			validateResult = self.import_config.validateRowCount(validateSqoop=True, incremental=False) 
		except:
			logging.exception("Fatal error when validating imported rows by sqoop")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if validateResult == False:
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def validateIncrRowCount(self):
		logging.debug("Executing import_operations.validateIncrRowCount()")
		try:
			validateResult = self.import_config.validateRowCount(validateSqoop=False, incremental=True) 
		except:
			logging.exception("Fatal error when validating imported incremental rows")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if validateResult == False:
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getSourceTableSchema(self):
		try:
			if self.import_config.mongoImport == False:
				# Mongo gets is TableDefinition from the spark code. We cant use the getJDBCTableDefinition() for Mongo
				self.import_config.getJDBCTableDefinition()

			self.import_config.updateLastUpdateFromSource()
			self.import_config.removeFKforTable()
			self.import_config.saveColumnData()
			self.import_config.setPrimaryKeyColumn()
			self.import_config.saveKeyData()
			self.import_config.saveIndexData()
			self.import_config.saveGeneratedData()
		except invalidConfiguration as errMsg:
			logging.error(errMsg)
			self.import_config.remove_temporary_files()
			sys.exit(1)
		except SystemExit:
			self.import_config.remove_temporary_files()
			sys.exit(1)
		except:
			logging.exception("Fatal error when reading and/or processing source table schema")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def discoverAndAddTablesFromSource(self, dbalias, hiveDB, schemaFilter=None, tableFilter=None, addSchemaToTable=False, addCustomText=None, addCounterToTable=False, counterStart=None, onlyJsonOutput=False):
		""" This is the main function to search for tables/view on source database and add them to import_tables """
		logging.debug("Executing import_operations.discoverAndAddTablesFromSource()")
		errorDuringAdd = False
		self.import_config.lookupConnectionAlias(connection_alias=dbalias)

		if self.import_config.mongoImport == False:
			sourceDF = self.import_config.common_config.getJDBCtablesAndViews(schemaFilter=schemaFilter, tableFilter=tableFilter )
		else:
			sourceDF = self.import_config.common_config.getMongoCollections(collectionFilter=tableFilter )

		if len(sourceDF) == 0:
			print("There are no tables in the source database that we dont already have in DBImport")
			self.import_config.remove_temporary_files()
			sys.exit(0)

		importDF = self.import_config.getImportTables(hiveDB = hiveDB)
		importDF = importDF.loc[importDF['dbalias'] == dbalias].filter(['source_schema', 'source_table'])
		importDF.rename(columns={'source_schema':'schema', 'source_table':'table'}, inplace=True)

		mergeDF = pd.merge(sourceDF, importDF, on=None, how='outer', indicator='Exist')
		mergeDF['hiveTable'] = mergeDF['table']
		discoveredTables = len(mergeDF.loc[mergeDF['Exist'] == 'left_only'])

		if addCounterToTable == True or addSchemaToTable == True or addCustomText != None:
			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					mergeDF.loc[index, 'hiveTable'] = "_%s"%(mergeDF.loc[index, 'hiveTable'].strip())

		if addCounterToTable == True:
			if counterStart == None:
				counterStart = "1"
			numberLength=len(counterStart)
			try:
				startValue = int(counterStart)
			except ValueError:
				logging.error("The value specified for --counterStart must be a number")
				self.import_config.remove_temporary_files()
				sys.exit(1)

			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					zeroToAdd = ""
					while len(zeroToAdd) < (numberLength - len(str(startValue))):
						zeroToAdd += "0"

					mergeDF.loc[index, 'hiveTable'] = "%s%s%s"%(zeroToAdd, startValue, mergeDF.loc[index, 'hiveTable'])
					startValue += 1

		if addSchemaToTable == True:
			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					mergeDF.loc[index, 'hiveTable'] = "%s%s"%(mergeDF.loc[index, 'schema'].lower().strip(), mergeDF.loc[index, 'hiveTable'])

		if addCustomText != None:
			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					mergeDF.loc[index, 'hiveTable'] = "%s%s"%(addCustomText.lower().strip(), mergeDF.loc[index, 'hiveTable'])


		if discoveredTables == 0:
			print("There are no tables in the source database that we dont already have in DBImport")
			self.import_config.remove_temporary_files()
			sys.exit(0)

		# At this stage, we have discovered tables in the source system that we dont know about in DBImport

		if onlyJsonOutput == False:
			print("The following tables and/or views have been discovered in the source database and not found in DBImport")
			print("")
			print("%-20s%-40s%-30s%-20s%s"%("Hive DB", "Hive Table", "Connection Alias", "Schema", "Table/View"))
			print("=============================================================================================================================")

			for index, row in mergeDF.loc[mergeDF['Exist'] == 'left_only'].iterrows():
				print("%-20s%-40s%-30s%-20s%s"%(hiveDB, row['hiveTable'].lower(), dbalias, row['schema'], row['table']))

			answer = input("Do you want to add these imports to DBImport? (y/N): ")
#			answer = "N"
			if answer == "y":
				print("")
				for index, row in mergeDF.loc[mergeDF['Exist'] == 'left_only'].iterrows():
					if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
						addResult = self.import_config.addImportTable(
				    		hiveDB=hiveDB, 
				    		hiveTable=row['hiveTable'].lower(),
				    		dbalias=dbalias,
				    		schema=row['schema'].strip(),
				    		table=row['table'].strip(),
							catalog=constant.CATALOG_HIVE_DIRECT)
	
					elif self.common_operations.metastore_type == constant.CATALOG_GLUE:
						addResult = self.import_config.addImportTable(
				    		hiveDB=hiveDB, 
				    		hiveTable=row['hiveTable'].lower(),
				    		dbalias=dbalias,
				    		schema=row['schema'].strip(),
				    		table=row['table'].strip(),
							catalog=constant.CATALOG_GLUE)
	
					if addResult == False:
						errorDuringAdd = True
	
				if errorDuringAdd == False:
					print("All tables saved successfully in DBImport")
				else:
					print("")
					print("Not all tables was saved to DBImport. Please check log and output")
			else:
				print("")
				print("Aborting")

		else:
			# Only output the json file
			# This is used by the rest interface to return a list of tables to the webUI

			tableList = []
			for index, row in mergeDF.loc[mergeDF['Exist'] == 'left_only'].iterrows():
				resultDict = {}
				resultDict["database"] = hiveDB
				resultDict["table"] = row['hiveTable'].lower()
				resultDict["connection"] = dbalias
				resultDict["sourceSchema"] = row['schema']
				resultDict["sourceTable"] = row['table']
				tableList.append(resultDict)

			jsonResult = json.dumps(tableList)
			print(jsonResult)


		logging.debug("Executing import_operations.discoverAndAddTablesFromSource() - Finished")

	def checkOracleFlashbackSCNnumber(self):
		""" Checks if the Oracle Flashback SCN number used in the previous import is valid. If not, we force a full import """

		incrWhereStatement = self.import_config.getIncrWhereStatement(whereForSqoop=True).replace('"', '\'')
		query = "%s %s AND 1 = 0"%(self.import_config.sqlGeneratedSqoopQuery, incrWhereStatement)

		try:
			self.import_config.common_config.executeJDBCquery(query)
		except SQLerror as e:
			errorMsg = str(e)
			if "java.sql.SQLException: ORA-08181:" in errorMsg or "java.sql.SQLException: ORA-30052" in errorMsg :
				logging.warning("The last import with Oracle Flashback is to far into the past. In order to continue, we will be forced to do a new initial load")
				logging.warning("Reseting Oracle Flashback versions")

				self.resetIncrMinMaxValues(maxValue=None)
				self.truncateTargetTable()
				self.import_config.sqoopIncrMinvaluePending = None
				self.import_config.sqoopIncrMaxvaluePending = None
				self.import_config.incr_maxvalue = None

			elif "java.sql.SQLException: ORA-01466:" in errorMsg:
				logging.warning("Table definition have changed. One reason when this happens is when the source table is loaded or truncated. In order to continue, we will be forced to do a new initial load")
				logging.warning("Reseting Oracle Flashback versions")

				self.resetIncrMinMaxValues(maxValue=None)
				self.truncateTargetTable()
				self.import_config.sqoopIncrMinvaluePending = None
				self.import_config.sqoopIncrMaxvaluePending = None
				self.import_config.incr_maxvalue = None

			else:
				logging.error("Unknown Oracle Flashback error when checking for valid SCN number")
				print(errorMsg)
				self.import_config.remove_temporary_files()
				sys.exit(1)


	def convertSparkTypeToBinary(self, schema):
		import pyspark.sql

		if schema.__class__ == pyspark.sql.types.StructType:
			return pyspark.sql.types.StructType([self.convertSparkTypeToBinary(f) for f in schema.fields])
		if schema.__class__ == pyspark.sql.types.StructField:
			return pyspark.sql.types.StructField(schema.name, self.convertSparkTypeToBinary(schema.dataType), schema.nullable)
		if schema.__class__ == pyspark.sql.types.ArrayType:
			return pyspark.sql.types.ArrayType(self.convertSparkTypeToBinary(schema.elementType))
		if schema.__class__ == pyspark.sql.types.BinaryType:
			return pyspark.sql.types.StringType()
		return schema
	
	def convertSparkSchema(self, schema, forceDateAsString=False):
		import pyspark.sql

		if schema.__class__ == pyspark.sql.types.StructType:
			return pyspark.sql.types.StructType([self.convertSparkSchema(f, forceDateAsString) for f in schema.fields])
		if schema.__class__ == pyspark.sql.types.StructField:
			return pyspark.sql.types.StructField(schema.name, self.convertSparkSchema(schema.dataType, forceDateAsString), schema.nullable)
		if schema.__class__ == pyspark.sql.types.ArrayType:
			return pyspark.sql.types.ArrayType(self.convertSparkSchema(schema.elementType, forceDateAsString))
		if schema.__class__ == pyspark.sql.types.NullType:
			return pyspark.sql.types.StringType()
		if forceDateAsString:
			if schema.__class__ in [pyspark.sql.types.DateType, pyspark.sql.types.TimestampType]:
				return pyspark.sql.types.StringType()
		return schema

	def runSparkImportForMongo(self):
		logging.debug("Executing import_operations.runSparkImportForMongo()")

		self.sparkStartTimestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
		self.sparkStartUTS = int(time.time())
		sparkQuery = ""

		# Fetch the number of executors that should be used. 
		# This is already handled in self.import_config.calculateJobMappers, so we use that 
		# function for this even if we dont need the JobMappers
		self.import_config.calculateJobMappers()

		# Override Spark Executor memory if it's set on the import table configuration
		if self.import_config.spark_executor_memory != None and self.import_config.spark_executor_memory.strip() != "":
			self.import_config.common_config.sparkExecutorMemory = self.import_config.spark_executor_memory

		logging.debug("")
		logging.debug("=======================================================================")
		logging.debug("Target HDFS: %s"%(self.import_config.sqoop_hdfs_location))
		logging.debug("Spark Executor Memory: %s"%(self.import_config.common_config.sparkExecutorMemory))
		logging.debug("Spark Max Executors: %s"%(self.import_config.sparkMaxExecutors))
		logging.debug("Spark Dynamic Executors: %s"%(self.import_config.common_config.sparkDynamicAllocation))
		logging.debug("=======================================================================")


		# Create a valid PYSPARK_SUBMIT_ARGS string
		sparkPysparkSubmitArgs = "--jars "
		sparkJars = ""

		firstLoop = True
		if self.import_config.common_config.sparkJarFiles.strip() != "":
			for jarFile in self.import_config.common_config.sparkJarFiles.split(","):
				if firstLoop == False:
					sparkPysparkSubmitArgs += ","
					sparkJars += ","
				sparkPysparkSubmitArgs += jarFile.strip()
				sparkJars += jarFile.strip()
				firstLoop = False

		for jarFile in self.import_config.common_config.jdbc_classpath.split(":"):
			if firstLoop == False:
				sparkPysparkSubmitArgs += ","
				sparkJars += ","
			sparkPysparkSubmitArgs += jarFile.strip()
			sparkJars += jarFile.strip()
			firstLoop = False

		if self.import_config.common_config.sparkPyFiles.strip() != "":
			sparkPysparkSubmitArgs += " --py-files "
			firstLoop = True
			for pyFile in self.import_config.common_config.sparkPyFiles.split(","):
				if firstLoop == False:
					sparkPysparkSubmitArgs += ","
				sparkPysparkSubmitArgs += pyFile.strip()
				firstLoop = False

		sparkPysparkSubmitArgs += " pyspark-shell"

		# Setup the additional path required to find the libraries/modules
		for path in self.import_config.common_config.sparkPathAppend.split(","):
			sys.path.append(path.strip())

		# Set required OS parameters
		# os.environ['HDP_VERSION'] = self.import_config.common_config.sparkHDPversion
		os.environ['PYSPARK_SUBMIT_ARGS'] = sparkPysparkSubmitArgs

		print(" _____________________ ")
		print("|                     |")
		print("| Spark Import starts |")
		print("|_____________________|")
		print("")

		mongoUri = "mongodb://%s:%s@%s:%s/%s.%s"%(
			self.import_config.common_config.jdbc_username,
			self.import_config.common_config.jdbc_password,
			self.import_config.common_config.jdbc_hostname,
			self.import_config.common_config.jdbc_port,
			self.import_config.common_config.jdbc_database,
			self.import_config.source_table)

		if self.import_config.common_config.mongoAuthSource != None: 
			mongoUri = mongoUri + "?authSource=" + self.import_config.common_config.mongoAuthSource
		
		# import all packages after the environment is set
		from pyspark.context import SparkContext, SparkConf
		import pyspark.sql
		from pyspark.sql import SparkSession
		from pyspark import HiveContext
		from pyspark.context import SparkContext
		import pyspark.sql.types 
		from pyspark.sql.functions import udf

		conf = SparkConf()
		conf.setMaster(self.import_config.common_config.sparkMaster)
		conf.set('spark.mongodb.input.uri', mongoUri)
		conf.set('spark.submit.deployMode', self.import_config.common_config.sparkDeployMode )
		conf.setAppName('DBImport Import - %s.%s'%(self.Hive_DB, self.Hive_Table))
		conf.set('spark.jars', sparkJars)
		conf.set('spark.executor.memory', self.import_config.common_config.sparkExecutorMemory)
		conf.set('spark.yarn.queue', self.import_config.common_config.sparkYarnQueue)
		conf.set('spark.hadoop.yarn.timeline-service.enabled', 'false')
		if self.import_config.common_config.sparkDynamicAllocation == True:
			conf.set('spark.shuffle.service.enabled', 'true')
			conf.set('spark.dynamicAllocation.enabled', 'true')
			conf.set('spark.dynamicAllocation.minExecutors', '0')
			conf.set('spark.dynamicAllocation.maxExecutors', str(self.import_config.sparkMaxExecutors))
			logging.info("Number of executors is dynamic with a max value of %s executors"%(self.import_config.sparkMaxExecutors))
		else:
			conf.set('spark.dynamicAllocation.enabled', 'false')
			conf.set('spark.shuffle.service.enabled', 'false')
			if self.import_config.sqlSessions < self.import_config.sparkMaxExecutors:
				conf.set('spark.executor.instances', str(self.import_config.sqlSessions))
				logging.info("Number of executors is fixed at %s"%(self.import_config.sqlSessions))
			else:
				conf.set('spark.executor.instances', str(self.import_config.sparkMaxExecutors))
				logging.info("Number of executors is fixed at %s"%(self.import_config.sparkMaxExecutors))

		sys.stdout.flush()

		sc = SparkContext(conf=conf)
		sc.addPyFile("%s/bin/common/sparkUDF.py"%(os.environ['DBIMPORT_HOME']))
		from sparkUDF import base64EncodeArray
		from sparkUDF import parseStructDate
		sys.stdout.flush()

		spark = SparkSession(sc)
		spark.sql("set spark.sql.orc.impl=native")
		sys.stdout.flush()

		self.yarnApplicationID = sc.applicationId
		logging.info("Yarn application started with id %s"%(self.yarnApplicationID))
		sys.stdout.flush()
		
		print("Loading data from Mongo")
		sys.stdout.flush()
		df = spark.read.format("mongo").load()
		sys.stdout.flush()


		# The logic here is the following. 
		# Read 2 schemas but one of the forces all data and timestamp columns into strings. In the DF where we are not forcing them to string
		# we search for timestamp and date columns and saves them. Once we have them, we try to run a Spark SQL against that column to determine
		# if the data in the column can be a timestamp or not.
		# If we find out that the column is infact a timestamp, we convert the column first to a string based on unixtimestamp and later
		# cast it to a timestamp. The result of this is that the DF with strings gets it's columns replace to timestamp where the data actually
		# is a timestamp.
		# Without this logic, the data would be strings for ever or would generate errors when the syntax is not a timestamp.

		print("Loading Schema from Mongo")
		sys.stdout.flush()
		sparkSchema = self.convertSparkSchema(df.schema, forceDateAsString=False)
		sparkSchemaString = self.convertSparkSchema(df.schema, forceDateAsString=True)
		df = spark.read.format("mongo").schema(sparkSchema).load()
		dfString = spark.read.format("mongo").schema(sparkSchemaString).load()
		sys.stdout.flush()

		# Save the datatypes. Will later be used to update import_columns table
		print("Getting column types")
		sys.stdout.flush()
		dataTypes = dict(df.dtypes)
		dataTypesString = dict(dfString.dtypes)

		# Find what columns are timestamp or date columns
		dateColumns = {}
		for column, columnType in dataTypes.items():
			if columnType.lower() in ["timestamp", "date"]:
				dateColumns[column] = columnType

		# Test the columns we just found to verify if it actually is a timestamp or not
		dfString.createOrReplaceTempView("find_timestamp")
		sys.stdout.flush()

		incorrectDateColumns = []
		correctDateColumns = {}
		for column, columnType in dateColumns.items():
			print("Checking if column '%s' really is a timestamp column and compatible with Hive"%(column))
			sys.stdout.flush()

			dfDate = spark.sql("SELECT %s FROM find_timestamp where %s is not null and CAST(%s as timestamp) is not null" % (column, column, column))
			if len(dfDate.head(1)) != 0:
				incorrectDateColumns.append(column)
			else:
				correctDateColumns[column] = columnType

		# Convert the column in the DF based on the fact that it can be converted or not to a timestamp
		udfParseStructDate = udf(parseStructDate, pyspark.sql.types.StringType())

		for column in incorrectDateColumns:
			print("Converting date/timestamp column '%s'"%(column))
			sys.stdout.flush()
			dfString = dfString.withColumn(column, udfParseStructDate(column))

		for column, columnType in correctDateColumns.items():
			print("Converting date/timestamp column '%s'"%(column))
			sys.stdout.flush()
			dfString = dfString.withColumn(column, udfParseStructDate(column))
			dfString = dfString.withColumn(column, dfString[column].cast(columnType))

		sys.stdout.flush()

		# Find and convert binary fields into a Base64 encoded binary field
		print("Finding binary columns")
		sys.stdout.flush()
		columns = dfString.columns
		types = [f.dataType for f in dfString.schema.fields]
		columnsAndTypes = list(zip(columns, types))

		columnTypesBinary = {}
		for column, columnType in columnsAndTypes:
			if "struct<subType:tinyint,data:binary>" in dataTypes[column]:
				columnTypesBinary[column] = self.convertSparkTypeToBinary(columnType)

		for column, columnType in columnTypesBinary.items():
			print("Converting binary column '%s'"%(column))
			sys.stdout.flush()
			udfBase64EncodeArray = udf(base64EncodeArray, columnType)
			dfString = dfString.withColumn(column, udfBase64EncodeArray(column))

		sys.stdout.flush()

		# Get new dataTypes as we have updated and converted from binary to string
		print("Getting updated column types after column changes")
		sys.stdout.flush()
		dataTypes = dict(dfString.dtypes)
		sys.stdout.flush()

		print("Writing data to HDFS as ORC")
		sys.stdout.flush()
		dfString.write.mode('overwrite').format("orc").save(self.import_config.sqoop_hdfs_location)
		sys.stdout.flush()

		# Get the number of rows from the ORC files
		print("Reading ORC files from HDFS to verify size and rows")
		sys.stdout.flush()
		hdfsDataDf = spark.read.format("orc").load(self.import_config.sqoop_hdfs_location)
		rowsWrittenBySpark = hdfsDataDf.count()
		sys.stdout.flush()
		logging.info("Number of rows written by spark = %s"%(rowsWrittenBySpark))
		sys.stdout.flush()

		# Get size of all files on HDFS that spark wrote
		hdfsCommandList = ['hdfs', 'dfs', '-du', '-s', self.import_config.sqoop_hdfs_location]
		hdfsProc = subprocess.Popen(hdfsCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdOut, stdErr = hdfsProc.communicate()
		stdOut = stdOut.decode('utf-8').rstrip()
		stdErr = stdErr.decode('utf-8').rstrip()

		sizeWrittenBySpark = stdOut.split()[0]
		logging.info("Size of data written by spark = %s bytes"%(sizeWrittenBySpark))
		sys.stdout.flush()

		sc.stop()

		print(" ________________________ ")
		print("|                        |")
		print("| Spark Import completed |")
		print("|________________________|")
		print("")
		sys.stdout.flush()

		# As the import can take a very long time on a large table, we need to verify that the MySQL connection against the configuration database is still valid
		self.import_config.reconnectConfigDatabase()	

		columnDict = {}
		for column in dataTypes:
			columnType = dataTypes[column]

			for list1 in columnType.split('<'):
				for list2 in list1.split('>'):
					for list3 in list2.split(','):
						if len(list3.split(':')) == 2:
							columnType = re.sub(r'([<>,])' + list3 + '([<>,])', r'\1`%s`:%s\2'%(list3.split(':')[0], list3.split(':')[1]), columnType) 
			columnDict[column] = columnType


		dataTypes = columnDict

		# Create the colum pandasDF as we cant get that from the source as we do on normal JDBC sources
		rows_list = []
		for column in dataTypes:
			line_dict = {}
			line_dict["TABLE_COMMENT"] = None
			line_dict["SOURCE_COLUMN_NAME"] = column
			line_dict["SOURCE_COLUMN_TYPE"] = dataTypes[column]
			line_dict["SOURCE_COLUMN_LENGTH"] = None
			line_dict["SOURCE_COLUMN_COMMENT"] = None
			line_dict["IS_NULLABLE"] = None
			line_dict["TABLE_TYPE"] = None
			line_dict["TABLE_CREATE_TIME"] = None
			line_dict["DEFAULT_VALUE"] = None
			rows_list.append(line_dict)
		self.import_config.common_config.source_columns_df = pd.DataFrame(rows_list)
		self.import_config.common_config.source_keys_df = result_df = pd.DataFrame()

		# Update import_columns with column information
		self.getSourceTableSchema()

		try:
			self.import_config.saveImportStatistics(self.sparkStartUTS, sqoopSize=sizeWrittenBySpark, sqoopRows=rowsWrittenBySpark, sqoopMappers=self.import_config.sqlSessions)
		except:
			logging.exception("Fatal error when saving spark statistics")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		logging.debug("Executing import_operations.runSparkImportForMongo() - Finished")

	def startSpark(self):
		logging.debug("Executing import_operations.startSpark()")

		if self.spark != None:
			# Spark is already running. Skip this
			return

		# Override Spark Executor memory if it's set on the import table configuration
		if self.import_config.spark_executor_memory != None and self.import_config.spark_executor_memory.strip() != "":
			self.import_config.common_config.sparkExecutorMemory = self.import_config.spark_executor_memory

		logging.debug("")
		logging.debug("=======================================================================")
		logging.debug("splitByColumn: %s"%(self.import_config.splitByColumn))
		logging.debug("sqoopBoundaryQuery: %s"%(self.import_config.sqoopBoundaryQuery))
		logging.debug("parallell sessions: %s"%(self.import_config.sqlSessions))
		logging.debug("Target HDFS: %s"%(self.import_config.sqoop_hdfs_location))
		logging.debug("Spark Executor Memory: %s"%(self.import_config.common_config.sparkExecutorMemory))
		logging.debug("Spark Max Executors: %s"%(self.import_config.sparkMaxExecutors))
		logging.debug("Spark Dynamic Executors: %s"%(self.import_config.common_config.sparkDynamicAllocation))
		logging.debug("=======================================================================")

		# Create a valid PYSPARK_SUBMIT_ARGS string
		sparkPysparkSubmitArgs = "--jars "
		sparkJars = ""

		firstLoop = True
		if self.import_config.common_config.sparkJarFiles.strip() != "":
			for jarFile in self.import_config.common_config.sparkJarFiles.split(","):
				if firstLoop == False:
					sparkPysparkSubmitArgs += ","
					sparkJars += ","
				sparkPysparkSubmitArgs += jarFile.strip()
				sparkJars += jarFile.strip()
				firstLoop = False

		for jarFile in self.import_config.common_config.jdbc_classpath.split(":"):
			if firstLoop == False:
				sparkPysparkSubmitArgs += ","
				sparkJars += ","
			sparkPysparkSubmitArgs += jarFile.strip()
			sparkJars += jarFile.strip()
			firstLoop = False

		if self.import_config.common_config.sparkPyFiles.strip() != "":
			sparkPysparkSubmitArgs += " --py-files "
			firstLoop = True
			for pyFile in self.import_config.common_config.sparkPyFiles.split(","):
				if firstLoop == False:
					sparkPysparkSubmitArgs += ","
				sparkPysparkSubmitArgs += pyFile.strip()
				firstLoop = False

		sparkPysparkSubmitArgs += " pyspark-shell"

		# Setup the additional path required to find the libraries/modules
		for path in self.import_config.common_config.sparkPathAppend.split(","):
			sys.path.append(path.strip())

		# Set required OS parameters
		# os.environ['HDP_VERSION'] = self.import_config.common_config.sparkHDPversion
		os.environ['PYSPARK_SUBMIT_ARGS'] = sparkPysparkSubmitArgs
		# print(sparkPysparkSubmitArgs)

		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			print(" _____________________ ")
			print("|                     |")
			print("| Spark Import starts |")
			print("|_____________________|")
			print("")

		import pyspark

		conf = pyspark.context.SparkConf()
		conf.setMaster(self.import_config.common_config.sparkMaster)
		conf.set('spark.submit.deployMode', self.import_config.common_config.sparkDeployMode )
		conf.setAppName('DBImport Import - %s.%s'%(self.Hive_DB, self.Hive_Table))
		conf.set('spark.jars', sparkJars)
		if self.import_config.common_config.sparkPackages != None and self.import_config.common_config.sparkPackages != "":
			conf.set('spark.jars.packages', self.import_config.common_config.sparkPackages)
		conf.set('spark.executor.memory', self.import_config.common_config.sparkExecutorMemory)
		conf.set('spark.yarn.queue', self.import_config.common_config.sparkYarnQueue)
		conf.set('spark.port.maxRetries', '128')
		if self.import_config.common_config.sparkLogLevel != None:
			conf.set('spark.log.level', self.import_config.common_config.sparkLogLevel)
		conf.set('spark.hadoop.yarn.timeline-service.enabled', 'false')
		conf.set('spark.driver.log.persistToDfs.enabled', 'false')

		if self.import_config.common_config.sparkMajorVersion == 2:
			conf.set('spark.yarn.keytab', self.import_config.common_config.kerberosKeytab)
			conf.set('spark.yarn.principal', self.import_config.common_config.kerberosPrincipal)
		else:
			conf.set('spark.kerberos.keytab', self.import_config.common_config.kerberosKeytab)
			conf.set('spark.kerberos.principal', self.import_config.common_config.kerberosPrincipal)
			conf.set('spark.sql.debug.maxToStringFields', 1000)

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			if self.common_operations.metastore_type == constant.CATALOG_GLUE:
				conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
				conf.set('spark.sql.catalog.glue', 'org.apache.iceberg.spark.SparkCatalog')
				conf.set('spark.sql.catalog.glue.warehouse', self.import_config.hdfsBaseDir)
				conf.set('spark.sql.catalog.glue.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
				conf.set('spark.sql.catalog.glue.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
			else:
				if self.import_config.common_config.sparkS3AccessToOzone == False:
#					conf.set('spark.sql.extensions', 'com.hortonworks.spark.sql.rule.Extensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
					conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
					conf.set('spark.sql.catalog.hive', 'org.apache.iceberg.spark.SparkCatalog')
					conf.set('spark.sql.catalog.hive.type', 'hive')
					conf.set('spark.sql.catalog.hive.cache-enabled', 'false')
					conf.set('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog')
					conf.set('spark.sql.catalog.local.type', 'hadoop')
					conf.set('spark.sql.catalog.local.warehouse', self.import_config.hdfsBaseDir)
				else:
					conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
					conf.set('spark.sql.catalog.hive', 'org.apache.iceberg.spark.SparkCatalog')
					conf.set('spark.sql.catalog.hive.type', 'hive')
					conf.set('spark.sql.catalog.hive.cache-enabled', 'false')
					conf.set('spark.sql.catalog.hive.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
					conf.set('spark.sql.catalog.hive.http-client.type', 'urlconnection')
					conf.set('spark.sql.catalog.hive.s3.path-style-access', 'true')
					conf.set('spark.sql.catalog.hive.client.assume-role.region', 'us-east-1')
					conf.set('spark.sql.catalog.hive.s3.access-key-id', self.import_config.common_config.sparkS3AccessKeyID)
					conf.set('spark.sql.catalog.hive.s3.secret-access-key', self.import_config.common_config.sparkS3SecretAccessKey)
					conf.set('spark.hadoop.fs.s3a.access.key', self.import_config.common_config.sparkS3AccessKeyID)
					conf.set('spark.hadoop.fs.s3a.secret.key', self.import_config.common_config.sparkS3SecretAccessKey)


		if self.import_config.common_config.sparkDynamicAllocation == True:
			conf.set('spark.shuffle.service.enabled', 'true')
			conf.set('spark.dynamicAllocation.enabled', 'true')
			conf.set('spark.dynamicAllocation.minExecutors', '0')
			conf.set('spark.dynamicAllocation.maxExecutors', str(self.import_config.sparkMaxExecutors))
			logging.info("Number of executors is dynamic with a max value of %s executors"%(self.import_config.sparkMaxExecutors))
		else:
			conf.set('spark.dynamicAllocation.enabled', 'false')
			conf.set('spark.shuffle.service.enabled', 'false')
			if self.import_config.sqlSessions < self.import_config.sparkMaxExecutors:
				conf.set('spark.executor.instances', str(self.import_config.sqlSessions))
				logging.info("Number of executors is fixed at %s"%(self.import_config.sqlSessions))
			else:
				conf.set('spark.executor.instances', str(self.import_config.sparkMaxExecutors))
				logging.info("Number of executors is fixed at %s"%(self.import_config.sparkMaxExecutors))

		if self.import_config.common_config.getConfigValue(key = "timestamp_with_timezone") == False:
			# conf.set('spark.sql.iceberg.handle-timestamp-without-timezone', 'true')
			# conf.set('spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables', 'true')
			conf.set('spark.sql.timestampType', 'TIMESTAMP_NTZ')

		sys.stdout.flush()
		self.sparkContext = pyspark.context.SparkContext(conf=conf)
		sys.stdout.flush()
		self.spark = pyspark.sql.SparkSession(self.sparkContext)
		sys.stdout.flush()

		sparkVersionSplit = self.spark.version.split(".")
		sparkMajorVersion = int(sparkVersionSplit[0])
		sparkMinorVersion = int(sparkVersionSplit[1])
		sparkVersion = "%s.%s.%s"%(sparkVersionSplit[0], sparkVersionSplit[1], sparkVersionSplit[2])
		logging.info("Running with Spark Version %s"%(sparkVersion))

		if sparkMajorVersion == 2 and self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			self.stopSpark()
			raise invalidConfiguration("Using Spark as the ETL engine is only supported in Spark3.")
		elif sparkMajorVersion == 3 and sparkMinorVersion != 4:
			if self.import_config.common_config.getConfigValue(key = "timestamp_with_timezone") == False:
				self.spark.conf.set('spark.sql.iceberg.handle-timestamp-without-timezone', 'true')
				self.spark.conf.set('spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables', 'true')

		self.sparkContext.addPyFile("%s/bin/common/sparkUDF2.py"%(os.environ['DBIMPORT_HOME']))
		import sparkUDF2
		sparkUDF = sparkUDF2.sparkUDFClass()

		# Anonymization of data in columns
		sparkUDF.setSeedString(self.import_config.common_config.getAnonymizationSeed())

		if sparkMajorVersion == 2:
			from pyspark.sql.functions import udf
			self.udfHashColumn = udf(sparkUDF.hashColumn, pyspark.sql.types.StringType())
			self.udfReplaceCharWithStar = udf(sparkUDF.replaceCharWithStar, pyspark.sql.types.StringType())
			self.udfShowFirstFourCharacters = udf(sparkUDF.showFirstFourCharacters, pyspark.sql.types.StringType())
		else:
			self.udfHashColumn = pyspark.sql.functions.udf(sparkUDF.hashColumn, pyspark.sql.types.StringType())
			self.udfReplaceCharWithStar = pyspark.sql.functions.udf(sparkUDF.replaceCharWithStar, pyspark.sql.types.StringType())
			self.udfShowFirstFourCharacters = pyspark.sql.functions.udf(sparkUDF.showFirstFourCharacters, pyspark.sql.types.StringType())

		# get Yarn info
		self.yarnApplicationID = self.sparkContext.applicationId
		logging.info("Yarn application started with id %s"%(self.yarnApplicationID))
		self.import_config.common_config.updateYarnStatistics(self.yarnApplicationID, "spark") 

		sys.stdout.flush()
		
		self.spark.sql("set spark.sql.orc.impl=native")

		logging.debug("Executing import_operations.startSpark()")


	def stopSpark(self):

		# This might be called multiple times. Just return in case it's already closed
		if self.sparkContext == None:
			return 

		# Connect to the Spark UI and request the API to get the total number of Executors used. This will be saved in the 'yarn_statistics' table
		sparkURL = "%s/api/v1/applications/%s/allexecutors"%(self.sparkContext.uiWebUrl, self.yarnApplicationID)
		self.kerberosPrincipal = configuration.get("Kerberos", "principal")
		sparkAuth = HTTPKerberosAuth(force_preemptive=True, principal=self.kerberosPrincipal, mutual_authentication=OPTIONAL)

		requests.packages.urllib3.disable_warnings()		# Disable SSL/Cert warnings for Spark application API call
		response = requests.get(sparkURL, auth=sparkAuth, verify=False)
		responseJson = response.json()

		sparkExecutorCount = 0
		for i in responseJson:
			if i["id"] != "driver":
				sparkExecutorCount += 1

		self.import_config.common_config.updateYarnStatistics(self.yarnApplicationID, "spark", 
			yarnContainersTotal = sparkExecutorCount) 

		logging.info("Stopping Spark context")
		self.sparkContext.stop()
		self.sparkContext = None
		self.spark = None

	def runSparkImport(self, PKOnlyImport, incrementalMax=None):
		logging.debug("Executing import_operations.runSparkImport()")

		self.sparkStartTimestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
		self.sparkStartUTS = int(time.time())
		sparkQuery = ""

		# Fetch the number of executors and sql splits that should be used
		self.import_config.calculateJobMappers()

		rowCount = 0
		incrWhereStatement = ""
		if self.import_config.import_is_incremental == True and PKOnlyImport == False and self.import_config.importPhase != constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING:

			if self.import_config.common_config.jdbc_servertype == constant.SNOWFLAKE:
				incrWhereStatement = self.import_config.getIncrWhereStatement(whereForSqoop=True, incrementalMax=incrementalMax)
			else:
				incrWhereStatement = self.import_config.getIncrWhereStatement(whereForSqoop=True, incrementalMax=incrementalMax).replace('"', '\'')

			if incrWhereStatement == "":
				# DBImport is unable to find the max value for the configured incremental column. Is the table empty?
				rowCount = self.import_config.common_config.getJDBCTableRowCount(self.import_config.source_schema, self.import_config.source_table)
				if rowCount == 0:
					self.sqoopIncrNoNewRows = True
					try:
						logging.warning("There are no rows in the source table. As this is an incremental import, spark will not run")
						self.import_config.saveImportStatistics(self.sqoopStartUTS, sqoopSize=0, sqoopRows=0, sqoopMappers=0)
						self.import_config.saveSourceTableRowCount(rowCount=0, incr=False)
						self.import_config.saveSourceTableRowCount(rowCount=0, incr=True, printInfo=False)
						self.deleteSqoopHdfsLocation()
					except:
						logging.exception("Fatal error when saving sqoop statistics")
						self.import_config.remove_temporary_files()
						sys.exit(1)
					return
				else:
					logging.error("DBImport is unable to find the max value for the configured incremental column.")
					self.import_config.remove_temporary_files()
					sys.exit(1)


		# Fetch Min and Max values for Boundary query
		if self.import_config.sqlSessions > 1:
			self.import_config.generateSqoopSplitBy()
			if self.import_config.import_is_incremental == True and self.import_config.splitByColumn == self.import_config.sqoop_incr_column:
				# If it is an incremental import and both splitBy and incr_columns are the same, we can just use the min/max values for the BoundaryValues
				if self.import_config.sqoopIncrMinvaluePending != None and self.import_config.sqoopIncrMaxvaluePending != None:
					minMaxDict = {}
					minMaxDict['min'] = self.import_config.sqoopIncrMinvaluePending
					minMaxDict['max'] = self.import_config.sqoopIncrMaxvaluePending
				elif incrementalMax != None:
					minMaxDict = {}
					minMaxDict['min'] = self.import_config.sqoopIncrMinvaluePending
					minMaxDict['max'] = incrementalMax
				else:
					# Generate the SQL that fetch the min and max values from the column that is defined in self.import_config.splitByColumn
					minMaxDict = self.import_config.getMinMaxBoundaryValues()
			else:
				# Generate the SQL that fetch the min and max values from the column that is defined in self.import_config.splitByColumn
				minMaxDict = self.import_config.getMinMaxBoundaryValues()
		else:
			minMaxDict = {}
			minMaxDict['min'] = "Unknown"
			minMaxDict['max'] = "Unknown"


		# Create the SQL that will be used to fetch the data. This will be used on a "from" statement
		if self.import_config.importPhase == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING: 
			sparkQuery = "(%s) %s"%(self.import_config.getSQLtoReadFromSourceWithMSSQLChangeTracking(), self.Hive_Table)
		elif incrWhereStatement != "":
			sparkQuery = "(%s where %s) %s"%(self.import_config.getSQLtoReadFromSource(), incrWhereStatement, self.Hive_Table)
		elif self.import_config.getSQLWhereAddition() != None:
			sparkQuery = "(%s where %s) %s"%(self.import_config.getSQLtoReadFromSource(), self.import_config.getSQLWhereAddition(), self.Hive_Table)
		else:
			sparkQuery = "(%s) %s"%(self.import_config.getSQLtoReadFromSource(), self.Hive_Table)

#		print("import_is_incremental: %s"%self.import_config.import_is_incremental)
#		print("incrWhereStatement: %s"%incrWhereStatement)
#		print("Rowcount: %s"%rowCount)
#		print("sparkQuery: %s"%sparkQuery)
#		self.import_config.remove_temporary_files()
#		sys.exit(1)

		# Start the spark session
		self.startSpark()
		import pyspark

		quoteAroundColumn = self.import_config.common_config.getQuoteAroundColumn()
		partitionColumn = "%s%s%s"%(quoteAroundColumn, str(self.import_config.splitByColumn).lower(), quoteAroundColumn)

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:

			icebergStagingTable = "%s.%s.%s"%(self.common_operations.sparkCatalogName, self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
			icebergPKOnlyDeletedTable = "%s.%s.%s__pkonly__deleted"%(self.common_operations.sparkCatalogName, self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
			icebergTargetTable = "%s.%s.%s"%(self.common_operations.sparkCatalogName, self.import_config.Hive_DB, self.import_config.Hive_Table)
			icebergHistoryTable = "%s.%s.%s_history"%(self.common_operations.sparkCatalogName, self.import_config.Hive_DB, self.import_config.Hive_Table)

#			print("----------------------------------------------------------------------------")
#			print("icebergStagingTable       = %s"%(icebergStagingTable))
#			print("icebergPKOnlyDeletedTable = %s"%(icebergPKOnlyDeletedTable))
#			print("icebergTargetTable        = %s"%(icebergTargetTable))
#			print("icebergHistoryTable       = %s"%(icebergHistoryTable))
#			print("----------------------------------------------------------------------------")

		# Only run the import if the stage have a shortname of "spark". This way, we dont have to bother about individual stage numbers
		if self.import_config.getStageShortName(self.import_config.getStage()) == "spark":
			print("")
			logging.info("Ingesting data from Source table to Import table")
			sys.stdout.flush()

			if self.import_config.sqlSessions < 2:
				df = (self.spark.read.format("jdbc")
					.option("driver", self.import_config.common_config.jdbc_driver)
					.option("url", self.import_config.common_config.jdbc_url)
					.option("dbtable", sparkQuery)
					.option("user", self.import_config.common_config.jdbc_username)
					.option("password", self.import_config.common_config.jdbc_password)
					.option("fetchsize", "10000")
					.load())
			else:
				df = (self.spark.read.format("jdbc")
					.option("driver", self.import_config.common_config.jdbc_driver)
					.option("url", self.import_config.common_config.jdbc_url)
					.option("dbtable", sparkQuery)
					.option("user", self.import_config.common_config.jdbc_username)
					.option("password", self.import_config.common_config.jdbc_password)
					.option("fetchsize", "10000")
					.option("partitionColumn", partitionColumn)
					.option("lowerBound", minMaxDict["min"])
					.option("upperBound", minMaxDict["max"])
					.option("numPartitions", self.import_config.sqlSessions)
					.load())
	
			# Get a Pandas DF with columnames and anonymization function
			anonymizationFunction = self.import_config.getAnonymizationFunction()

			for index, row in anonymizationFunction.iterrows():
				cName = row['source_column_name']
				cFunction = row['anonymization_function']

				logging.info("Anonymizing data in column '%s' with function '%s'"%(cName, cFunction))
				sys.stdout.flush()

				if cFunction == 'Hash':
					df = df.withColumn(cName, udfHashColumn(cName))
				elif cFunction == 'Replace with star':
					df = df.withColumn(cName, udfReplaceCharWithStar(cName))
				elif cFunction == 'Show first 4 chars':
					df = df.withColumn(cName, udfShowFirstFourCharacters(cName))


			# Write ORC files
			sys.stdout.flush()
			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				df.write.mode('overwrite').format("orc").save(self.import_config.sqoop_hdfs_location)
				sys.stdout.flush()

				logging.info("Import data saved to Import tables")
				sys.stdout.flush()

				hdfsDataDf = self.spark.read.format("orc").load(self.import_config.sqoop_hdfs_location)
				rowsWrittenBySpark = hdfsDataDf.count()

			if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				# ETL Engine is Spark, that means IceBerg tables

				if self.common_operations.checkTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == True:
					if self.common_operations.isTableExternalIcebergFormat(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == False:
						# Staging table exists and is not an Iceberg table. We need to drop it in Hive
						logging.warning("Staging table exists and is not an Iceberg table. Dropping the staging table")
						self.common_operations.connectToHive(forceSkipTest=True)
						self.common_operations.dropHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)

				# Drop the current Staging table
				logging.info("Dropping old Iceberg Import table")
				sys.stdout.flush()
				try:
					query = "drop table if exists %s purge"%(icebergStagingTable)
					logging.info(query)
					self.spark.sql(query)
				except pyspark.sql.utils.AnalysisException as e: 
					if "Table or view not found" in str(e):
						pass
					else:
						raise

				# Create the table
				logging.info("Creating Iceberg Import table and writing data from source database into it. This is the main loading step of data from source database")
				sys.stdout.flush()
				df.writeTo(icebergStagingTable).create()

				logging.info("Data saved to Import table")
				logging.info("Fetching number of rows written from Import table")
				sys.stdout.flush()

				hdfsDataDf = self.spark.table(icebergStagingTable)
				rowsWrittenBySpark = hdfsDataDf.count()

			sys.stdout.flush()
			if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				logging.info("Number of rows written by spark to import location = %s"%(rowsWrittenBySpark))
			else:
				logging.info("Number of rows written by spark = %s"%(rowsWrittenBySpark))
			sys.stdout.flush()

			if self.import_config.importPhase == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING:
				if rowsWrittenBySpark == 0:
					# If there is no new rows, we cant get the MaxValue from the output. 
					# So we need to set it to the previous value
					incrMaxvaluePending = self.import_config.incr_maxvalue 
					logging.warning("There are no new rows in the source table.")
				else:
					# We need to read the max version number and use that as the min value on next import
					incrMaxvaluePending = hdfsDataDf.groupBy().max("datalake_mssql_changetrack_version").first()[0]
					logging.info("Max Version number for MSSQL Change Tracking that was fetched = %s"%(incrMaxvaluePending))
			else:
				incrMaxvaluePending = None

			# Get the size of the table on disk
			if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				stagingTableSize = self.spark.sql("select sum(file_size_in_bytes) as size from %s.all_data_files"%(icebergStagingTable))
				sizeWrittenBySpark = stagingTableSize.first()['size']
			else:
				# Get size of all files on HDFS that spark wrote
				hdfsCommandList = ['hdfs', 'dfs', '-du', '-s', self.import_config.sqoop_hdfs_location]
				hdfsProc = subprocess.Popen(hdfsCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				stdOut, stdErr = hdfsProc.communicate()
				stdOut = stdOut.decode('utf-8').rstrip()
				stdErr = stdErr.decode('utf-8').rstrip()

				try:
					sizeWrittenBySpark = stdOut.split()[0]
				except IndexError:
					sizeWrittenBySpark = -1 

			logging.info("Size of data written by spark = %s bytes"%(sizeWrittenBySpark))
			sys.stdout.flush()

			# As the import can take a very long time on a large table, we need to verify that the MySQL connection against the configuration database is still valid
			self.import_config.reconnectConfigDatabase()	

			if PKOnlyImport == False:
				try:
					self.import_config.saveImportStatistics(self.sparkStartUTS, sqoopSize=sizeWrittenBySpark, sqoopRows=rowsWrittenBySpark, sqoopMappers=self.import_config.sqlSessions, sqoopIncrMaxvaluePending=incrMaxvaluePending)
				except:
					logging.exception("Fatal error when saving spark statistics")
					self.import_config.remove_temporary_files()
					sys.exit(1)

				if self.import_config.importPhase == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING:
					self.import_config.saveSourceTableRowCount(rowCount=rowsWrittenBySpark, incr=True, printInfo=True)


		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			# If it's the Spark ETL engine, we need the spark context later in the import
			self.stopSpark()

			print(" ________________________ ")
			print("|                        |")
			print("| Spark Import completed |")
			print("|________________________|")
			print("")
			sys.stdout.flush()

		logging.debug("Executing import_operations.runSparkImport() - Finished")

	def purgeIcebergVersionsOnTargetTable(self):
		logging.debug("Executing import_operations.purgeIcebergVersionsOnTargetTable()")
		# Removes old Iceberg snapshot version on the target table  

		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			return

		icebergTargetTable = "%s.%s"%(self.import_config.Hive_DB, self.import_config.Hive_Table)

		deleteOlderThanRef = datetime.utcnow() - timedelta(hours=24)
		deleteOlderThan = deleteOlderThanRef.strftime('%Y-%m-%d %H:%M:%S.000')

		self.startSpark()
		logging.info("Expire older Iceberg snapshots on Target table")
		# logging.warning("LINE REMARKED - MUST BE FIXED")
		self.spark.sql("CALL %s.system.expire_snapshots(table => '%s', older_than => TIMESTAMP '%s', retain_last => 1, stream_results => true)"%(self.common_operations.sparkCatalogName, icebergTargetTable, deleteOlderThan))

		logging.info("Removing orphan Iceberg files on Target table")
		# logging.warning("LINE REMARKED - MUST BE FIXED")
		self.spark.sql("CALL %s.system.remove_orphan_files(table => '%s', older_than => TIMESTAMP '%s')"%(self.common_operations.sparkCatalogName, icebergTargetTable, deleteOlderThan))


		logging.debug("Executing import_operations.purgeIcebergVersionsOnTargetTable() - Finsihed")


	def deleteSqoopHdfsLocation(self):
		logging.debug("Executing import_operations.deleteSqoopHdfsLocation()")
		logging.info("Deleting old imported data from previous DBImport executions on HDFS")

		hdfsCommandList = ['hdfs', 'dfs', '-rm', '-r', '-skipTrash', self.import_config.sqoop_hdfs_location]
		hdfsProc = subprocess.Popen(hdfsCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdOut, stdErr = hdfsProc.communicate()

		hdfsCommandList = ['hdfs', 'dfs', '-mkdir', self.import_config.sqoop_hdfs_location]
		hdfsProc = subprocess.Popen(hdfsCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdOut, stdErr = hdfsProc.communicate()

		logging.debug("Executing import_operations.deleteSqoopHdfsLocation() - Finished")

	def runSqoopImport(self, PKOnlyImport, incrementalMax=None):
		logging.debug("Executing import_operations.runSqoopImport()")

		self.sqoopStartTimestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
		self.sqoopStartUTS = int(time.time())

		# Check if any column is anonymized. If it is, we need to exit as that is only supported with Spark
		if self.import_config.isAnyColumnAnonymized() == True:
			logging.error("There are column in this table that is configured for anonymization. Those functions are only supported by spark and not by sqoop.")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		# Fetch the number of mappers that should be used
		self.import_config.calculateJobMappers()

		self.sqoopLaunchedMapTasks = None
		self.sqoopFailedMapTasks = None
		self.sqoopKilledMapTasks = None
		self.sqoopLaunchedReduceTasks = None
		self.sqoopFailedReduceTasks = None
		self.sqoopKilledReduceTasks = None

		# Sets the correct sqoop table and schema that will be used if a custom SQL is not used
		sqoopQuery = ""
		sqoopSourceSchema = [] 
		sqoopSourceTable = ""
		sqoopDirectOption = ""
		if self.import_config.common_config.db_mssql == True: 
			sqoopSourceSchema = ["--", "--schema", self.import_config.source_schema]
			sqoopSourceTable = self.import_config.source_table
		if self.import_config.common_config.db_oracle == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema.upper(), self.import_config.source_table.upper())
			# sqoopDirectOption = "--direct"
		if self.import_config.common_config.db_postgresql == True: 
			sqoopSourceSchema = ["--", "--schema", self.import_config.source_schema]
			sqoopSourceTable = self.import_config.source_table
		if self.import_config.common_config.db_progress == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table)
			# sqoopDirectOption = "--direct"
		if self.import_config.common_config.db_mysql == True: 
			sqoopSourceTable = self.import_config.source_table
		if self.import_config.common_config.db_db2udb == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table.upper())
		if self.import_config.common_config.db_db2as400 == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table.upper())
			# sqoopDirectOption = "--direct"
		if self.import_config.common_config.db_informix == True: 
			sqoopSourceTable = self.import_config.source_table
			# sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table)

		if self.import_config.sqoop_use_generated_sql == True and self.import_config.sqoop_query == None:
			sqoopQuery = self.import_config.sqlGeneratedSqoopQuery

		if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
			# Add the specific Oracle FlashBack columns that we need to import
			sqoopQuery = re.sub('^select', 'select \"VERSIONS_OPERATION\" as \"datalake_flashback_operation\", \"VERSIONS_STARTSCN\" as \"datalake_flashback_startscn\",', self.import_config.sqlGeneratedSqoopQuery)

		if self.import_config.sqoop_query != None:
			sqoopQuery = self.import_config.sqoop_query

		# Handle mappers, split-by with custom query
		if sqoopQuery != "":
			if "split-by" not in self.import_config.sqoop_options.lower():
				self.import_config.generateSqoopSplitBy()

		self.import_config.generateSqoopBoundaryQuery()

		# Handle the situation where the source dont have a PK and there is no split-by (force mappers to 1)
		if self.import_config.generatedPKcolumns == None and "split-by" not in self.import_config.sqoop_options.lower():
			logging.warning("There is no PrimaryKey in source system and no --split-by in the sqoop_options columns. This will force the import to use only 1 mapper")
			self.import_config.sqlSessions = 1	
	

		# From here and forward we are building the sqoop command with all options
		sqoopCommand = []
		sqoopCommand.extend(["sqoop", "import", "-D", "mapreduce.job.user.classpath.first=true"])
		sqoopCommand.extend(["-D", "mapreduce.job.queuename=%s"%(configuration.get("Sqoop", "yarnqueue"))])
		sqoopCommand.extend(["-D", "oraoop.disabled=true"]) 
		sqoopCommand.extend(["-D", "yarn.timeline-service.enabled=false"]) 
		sqoopCommand.extend(["-D", "org.apache.sqoop.splitter.allow_text_splitter=%s"%(self.import_config.sqoop_allow_text_splitter)])
		sqoopCommand.extend(["-D", "sqoop.parquet.logical_types.decimal.enable=false"])
		sqoopCommand.extend(["-D", "sqoop.avro.logical_types.decimal.enable=false"])


		if "split-by" not in self.import_config.sqoop_options.lower():
			sqoopCommand.append("--autoreset-to-one-mapper")

		# Progress and DB2 AS400 imports needs to know the class name for the JDBC driver. 
		if self.import_config.common_config.db_progress == True or self.import_config.common_config.db_db2as400 == True or self.import_config.common_config.db_informix == True or self.import_config.common_config.db_sqlanywhere == True:
			sqoopCommand.extend(["--driver", self.import_config.common_config.jdbc_driver])

		sqoopCommand.extend(["--class-name", "dbimport"]) 
		sqoopCommand.append("--fetch-size=10000") 

		sqoopCommand.append("--as-parquetfile") 
		sqoopCommand.append("--compress")
		sqoopCommand.extend(["--compression-codec", "snappy"])

		if PKOnlyImport == True:
			sqoopCommand.extend(["--target-dir", self.import_config.sqoop_hdfs_location_pkonly])
		else:
			sqoopCommand.extend(["--target-dir", self.import_config.sqoop_hdfs_location])

		sqoopCommand.extend(["--outdir", self.import_config.common_config.tempdir])
		sqoopCommand.extend(["--connect", "\"%s\""%(self.import_config.common_config.jdbc_url)])
		sqoopCommand.extend(["--username", self.import_config.common_config.jdbc_username])
		sqoopCommand.extend(["--password-file", "file://%s"%(self.import_config.common_config.jdbc_password_file)])
		if sqoopDirectOption != "":
			sqoopCommand.append(sqoopDirectOption)

		sqoopCommand.extend(["--num-mappers", str(self.import_config.sqlSessions)])

		# If we dont have a SQL query to use for sqoop, then we need to specify the table instead
		if sqoopQuery == "" and self.import_config.importPhase != constant.IMPORT_PHASE_ORACLE_FLASHBACK:
			sqoopCommand.extend(["--table", sqoopSourceTable])

		sqoopCommand.append("--delete-target-dir")

		if self.import_config.generatedSqoopOptions != "" and self.import_config.generatedSqoopOptions != None:
			sqoopCommand.extend(shlex.split(self.import_config.generatedSqoopOptions))

		if self.import_config.sqoop_options.strip() != "" and self.import_config.sqoop_options != None:
			if self.import_config.common_config.jdbc_force_column_lowercase == True:
				sqoopCommand.extend(shlex.split(self.import_config.sqoop_options.lower()))
			else:
				sqoopCommand.extend(shlex.split(self.import_config.sqoop_options))

		if self.import_config.sqoopBoundaryQuery.strip() != "" and self.import_config.sqlSessions > 1:
			sqoopCommand.extend(["--boundary-query", self.import_config.sqoopBoundaryQuery])

		if self.import_config.import_is_incremental == True and PKOnlyImport == False:
			incrWhereStatement = self.import_config.getIncrWhereStatement(whereForSqoop=True, incrementalMax=incrementalMax).replace('"', '\'')
			if incrWhereStatement == "":
				# DBImport is unable to find the max value for the configured incremental column. Is the table empty?
				rowCount = self.import_config.common_config.getJDBCTableRowCount(self.import_config.source_schema, self.import_config.source_table)
				if rowCount == 0:
					self.sqoopIncrNoNewRows = True
					try:
						logging.warning("There are no rows in the source table. As this is an incremental import, sqoop will not run")
						self.import_config.saveImportStatistics(self.sqoopStartUTS, sqoopSize=0, sqoopRows=0, sqoopMappers=0)
						self.import_config.saveSourceTableRowCount(rowCount=0, incr=False)
						self.import_config.saveSourceTableRowCount(rowCount=0, incr=True, printInfo=False)
						self.deleteSqoopHdfsLocation()
					except:
						logging.exception("Fatal error when saving sqoop statistics")
						self.import_config.remove_temporary_files()
						sys.exit(1)
					return
				else:
					logging.error("DBImport is unable to find the max value for the configured incremental column.")
					self.import_config.remove_temporary_files()
					sys.exit(1)

		if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
			sqoopCommand.extend(["--query", "%s %s AND $CONDITIONS "%(sqoopQuery, incrWhereStatement)])
		elif sqoopQuery == "":
			if self.import_config.import_is_incremental == True and PKOnlyImport == False:
				if self.import_config.getSQLWhereAddition() != None:
					sqoopCommand.extend(["--where", "%s and %s"%(incrWhereStatement, self.import_config.getSQLWhereAddition())])
				else:
					sqoopCommand.extend(["--where", incrWhereStatement])
			else:
				if self.import_config.getSQLWhereAddition() != None:
					sqoopCommand.extend(["--where", self.import_config.getSQLWhereAddition()])
			sqoopCommand.extend(sqoopSourceSchema)
		else:
			if self.import_config.import_is_incremental == True and PKOnlyImport == False:
				sqoopCommand.extend(["--query", "%s where $CONDITIONS and %s"%(sqoopQuery, incrWhereStatement)])
			else:
				if self.import_config.getSQLWhereAddition() != None:
					sqoopCommand.extend(["--query", "%s where $CONDITIONS and %s"%(sqoopQuery, self.import_config.getSQLWhereAddition())])
				else:
					sqoopCommand.extend(["--query", "%s where $CONDITIONS"%(sqoopQuery)])

		logging.info("Starting sqoop with the following command: %s"%(sqoopCommand))
		sqoopOutput = ""

		print(" _____________________ ")
		print("|                     |")
		print("| Sqoop Import starts |")
		print("|_____________________|")
		print("")

		# Start Sqoop
		sh_session = subprocess.Popen(sqoopCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

		# Print Stdout and stderr while sqoop is running
		while sh_session.poll() == None:
			row = sh_session.stdout.readline().decode('utf-8').rstrip()
			if row != "": 
				print(row)
				sys.stdout.flush()
				sqoopOutput += row + "\n"
				self.parseSqoopOutput(row)

		# Print what is left in output after sqoop finished
		for row in sh_session.stdout.readlines():
			row = row.decode('utf-8').rstrip()
			if row != "": 
				print(row)
				sys.stdout.flush()
				sqoopOutput += row + "\n"
				self.parseSqoopOutput(row)

		print(" ________________________ ")
		print("|                        |")
		print("| Sqoop Import completed |")
		print("|________________________|")
		print("")

		# As the import can take a very long time on a large table, we need to verify that the MySQL connection against the configuration database is still valid
		self.import_config.reconnectConfigDatabase()	

		# At this stage, the entire sqoop job is run and we fetched all data from the output. Lets parse and store it
		if self.sqoopSize == None: self.sqoopSize = 0
		if self.sqoopRows == None: self.sqoopRows = 0
		
		# Update table 'yarn_statistics'
		yarnContainersTotal = 0
		yarnContainersFailed = 0
		yarnContainersKilled = 0
		if self.sqoopLaunchedMapTasks != None:		yarnContainersTotal  += self.sqoopLaunchedMapTasks 
		if self.sqoopLaunchedReduceTasks != None:	yarnContainersTotal  += self.sqoopLaunchedReduceTasks 
		if self.sqoopFailedMapTasks != None:		yarnContainersFailed += self.sqoopFailedMapTasks 
		if self.sqoopFailedReduceTasks != None:		yarnContainersFailed += self.sqoopFailedReduceTasks 
		if self.sqoopKilledMapTasks != None:		yarnContainersKilled += self.sqoopKilledMapTasks 
		if self.sqoopKilledReduceTasks != None:		yarnContainersKilled += self.sqoopKilledReduceTasks 

		self.import_config.common_config.updateYarnStatistics(self.yarnApplicationID, "sqoop", 
			yarnContainersTotal  = yarnContainersTotal, 
			yarnContainersFailed = yarnContainersFailed, 
			yarnContainersKilled = yarnContainersKilled)

		# Check for errors in output
		sqoopWarning = False

		if " ERROR " in sqoopOutput:
			for row in sqoopOutput.split("\n"):
				if "Arithmetic overflow error converting expression to data type int" in row:
					# This is not really an error, but we cant verify the table.
					logging.warning("Unable to verify the import due to errors from MsSQL server. Will continue regardless of this")
					sqoopWarning = True
		
				if "Not authorized to access topics: [" in row:
					# This is not really an error, but we cant verify the table.
					logging.warning("Problem sending data to Atlas")
					sqoopWarning = True
		
				if "ERROR hook.AtlasHook" in row:
					# This is not really an error, but we cant send to Atlas
					logging.warning("Problem sending data to Atlas")
					sqoopWarning = True
		
				if "clients.NetworkClient: [Producer clientId" in row:
					# Unknown Kafka error
					sqoopWarning = True

				if "java.sql.SQLException: ORA-30052:" in row:
					# java.sql.SQLException: ORA-30052: invalid lower limit snapshot expression 
					logging.error("The Oracle Flashback import has passed the valid timeframe for undo logging in Oracle. To recover from this failure, you need to do an initial load.")
					logging.error("This is best done with './manage --resetCDCImport -h <Hive_DB> -t <Hive_Table>'")
					self.remove_temporary_files()
					sys.exit(1)

				# The following two tests is just to ensure that we dont mark the import as failed just because there is a column called error
				if " ERROR " in row and "Precision" in row and "Scale" in row:
					sqoopWarning = True

				if " ERROR " in row and "The HCatalog field" in row:
					sqoopWarning = True

			if sqoopWarning == False:
				# We detected the string ERROR in the output but we havent found a vaild reason for it in the IF statements above.
				# We need to mark the sqoop command as error and exit the program
				logging.error("Unknown error in sqoop import. Please check the output for errors and try again")
				self.remove_temporary_files()
				sys.exit(1)

		# No errors detected in the output and we are ready to store the result
		logging.info("Sqoop executed successfully")			
		
		if self.sqoopIncrNoNewRows == True:
			# If there is no new rows, we cant get the MaxValue from the output. 
			# So we need to set it to what we used to launch sqoop
			self.sqoopIncrMaxValuePending = self.import_config.sqoop_incr_lastvalue 
			self.sqoopSize = 0
			self.sqoopRows = 0
			logging.warning("There are no new rows in the source table.")

		if PKOnlyImport == False:
			try:
				self.import_config.saveImportStatistics(self.sqoopStartUTS, sqoopSize=self.sqoopSize, sqoopRows=self.sqoopRows, sqoopMappers=self.import_config.sqlSessions)
			except:
				logging.exception("Fatal error when saving sqoop statistics")
				self.import_config.remove_temporary_files()
				sys.exit(1)

		logging.debug("Executing import_operations.runSqoopImport() - Finished")


	def parseSqoopOutput(self, row ):
		# 19/03/28 05:15:44 HDFS: Number of bytes written=17
		if "HDFS: Number of bytes written" in row:
			self.sqoopSize = int(row.split("=")[1])
		
		# 19/03/28 05:15:44 INFO mapreduce.ImportJobBase: Retrieved 2 records.
		if "mapreduce.ImportJobBase: Retrieved" in row:
			self.sqoopRows = int(row.split(" ")[5])

		# 19/04/23 17:19:58 INFO tool.ImportTool:   --last-value 34
		if "tool.ImportTool:   --last-value" in row:
			self.sqoopIncrMaxValuePending = row.split("last-value ")[1].strip()

		# 19/04/24 07:13:00 INFO tool.ImportTool: No new rows detected since last import.
		if "No new rows detected since last import" in row:
			self.sqoopIncrNoNewRows = True

		# 23/10/04 04:52:07 INFO impl.YarnClientImpl: Submitted application application_1695989285495_0371
		if "Submitted application application_" in row:
			self.yarnApplicationID = row.split("Submitted application ")[1].strip()
			self.import_config.common_config.updateYarnStatistics(self.yarnApplicationID, "sqoop")

		# Used for updating table 'yarn_statistics' after Sqoop is finished
		if "Launched map tasks=" in row:	self.sqoopLaunchedMapTasks = int(row.split("=")[1].strip())
		if "Failed map tasks=" in row:		self.sqoopFailedMapTasks = int(row.split("=")[1].strip())
		if "Killed map tasks=" in row:		self.sqoopKilledMapTasks = int(row.split("=")[1].strip())
		if "Launched reduce tasks=" in row:	self.sqoopLaunchedReduceTasks = int(row.split("=")[1].strip())
		if "Failed reduce tasks=" in row:		self.sqoopFailedReduceTasks = int(row.split("=")[1].strip())
		if "Killed reduce tasks=" in row:		self.sqoopKilledReduceTasks = int(row.split("=")[1].strip())

	def connectToHive(self, forceSkipTest=False):
		logging.debug("Executing import_operations.connectToHive()")

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			# No need to connect to Hive when ETL engine is Spark
			return

		try:
			self.common_operations.connectToHive(forceSkipTest=forceSkipTest)
		except Exception as ex:
			logging.error(ex)
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if self.globalHiveConfigurationSet == False:
			self.globalHiveConfigurationSet = True
			if self.import_config.hiveJavaHeap != None:
				query = "set hive.tez.container.size=%s"%(self.import_config.hiveJavaHeap)
				self.common_operations.executeHiveQuery(query)
			else:
				query = "set hive.tez.container.size=3072"
				self.common_operations.executeHiveQuery(query)

			if self.import_config.hiveSplitCount != None and self.import_config.hiveSplitCount > 0:
				query = "set tez.grouping.split-count=%s"%(self.import_config.hiveSplitCount)
				self.common_operations.executeHiveQuery(query)

		logging.debug("Executing import_operations.connectToHive() - Finished")

	def createExternalImportView(self):
		logging.debug("Executing import_operations.createExternalImportView()")

		if self.import_config.isExternalViewRequired() == True:
			if self.common_operations.checkTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_View) == False:
				logging.info("Creating External Import View")

				query = "create view `%s`.`%s` as %s "%(
					self.import_config.Hive_Import_DB, 
					self.import_config.Hive_Import_View,
					self.import_config.getSelectForImportView())

				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()


		logging.debug("Executing import_operations.createExternalImportView() - Finished")

	def updateExternalImportView(self):
		logging.debug("Executing import_operations.updateExternalImportView()")

		if self.import_config.isExternalViewRequired() == True:
			columnsConfig = self.import_config.getColumnsFromConfigDatabase(sourceIsParquetFile=True, includeAllColumns=False) 

			hiveDB = self.import_config.Hive_Import_DB
			hiveView = self.import_config.Hive_Import_View
			columnsHive = self.common_operations.getColumns(hiveDB, hiveView, includeType=True, excludeDataLakeColumns=True) 
			columnsMerge = pd.merge(columnsConfig, columnsHive, on=None, how='outer', indicator='Exist')
			columnsDiffCount  = len(columnsMerge.loc[columnsMerge['Exist'] != 'both'])

			if columnsDiffCount > 0:
				# There is a diff between the configuration and the view. Lets update it
				logging.info("Updating Import View as columns in Hive is not the same as in the configuration")
				query = "alter view `%s`.`%s` as %s "%(
					self.import_config.Hive_Import_DB, 
					self.import_config.Hive_Import_View,
					self.import_config.getSelectForImportView())

				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.updateExternalImportView() - Finished")

	def createExternalImportTable(self):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			self.createExternalTable(tableType="import")

	def createExternalTargetTable(self):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			self.createExternalTable(tableType="target")

	def createExternalTable(self, tableType=""):
		logging.debug("Executing import_operations.createExternalImportTable()")

		if tableType == "import":
			hiveDB = self.import_config.Hive_Import_DB
			hiveTable = self.import_config.Hive_Import_Table
			hdfsLocation = self.import_config.sqoop_hdfs_location

		if tableType == "target":
			hiveDB = self.Hive_DB
			hiveTable = self.Hive_Table
			hdfsLocation = self.import_config.sqoop_hdfs_location_sparkETL

		if self.common_operations.checkTable(hiveDB, hiveTable) == True:
			# Table exist, so we need to make sure that it's a managed table and not an external table
			if self.common_operations.isTableExternalParquetFormat(hiveDB, hiveTable) == False and self.import_config.importTool == "sqoop":
				logging.info("Dropping %s table as it's not an external table based on parquet"%(tableType))
				self.common_operations.dropHiveTable(hiveDB, hiveTable)
				self.common_operations.reconnectHiveMetaStore()

			if self.common_operations.isTableExternalOrcFormat(hiveDB, hiveTable) == False and self.import_config.importTool in ("spark", "local"):
				logging.info("Dropping %s table as it's not an external table based on ORC"%(tableType))
				self.common_operations.dropHiveTable(hiveDB, hiveTable)
				self.common_operations.reconnectHiveMetaStore()

		if self.common_operations.checkTable(hiveDB, hiveTable) == False:
			# Import table does not exist. We just create it in that case
			logging.info("Creating External %s Table"%(tableType))

			query  = "create external table `%s`.`%s` ("%(hiveDB, hiveTable)
			columnsDF = self.import_config.getColumnsFromConfigDatabase(includeAllColumns=False)

			if self.import_config.importTool == "sqoop":
				columnsDF = self.updateColumnsForImportTable(columnsDF)
#			elif self.import_config.importTool == "spark":
#				columnsDF = self.updateColumnsForImportTable(columnsDF, replaceNameOnly=True)

			firstLoop = True

			if self.import_config.importPhase == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING:
				# Add the specific MSSQL Change Tracking columns that we need to import
				firstLoop = False
				query += "`datalake_mssql_changetrack_version` bigint"
				query += ", `datalake_mssql_changetrack_operation` varchar(2)"

			for index, row in columnsDF.iterrows():
				if firstLoop == False: query += ", "

				query += "`%s` %s"%(row['name'], row['type'])

				if row['comment'] != None:
					query += " COMMENT \"%s\""%(row['comment'])
				firstLoop = False

			if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
				# Add the specific Oracle FlashBack columns that we need to import
				query += ", `datalake_flashback_operation` varchar(2)"
				query += ", `datalake_flashback_startscn` string"

			query += ") "

			tableComment = self.import_config.getHiveTableComment()
			if tableComment != None:
				query += "COMMENT \"%s\" "%(tableComment)

			if self.import_config.importTool == "sqoop":
				query += "stored as parquet "
				query += "LOCATION '%s%s/' "%(self.common_operations.hdfs_address, hdfsLocation)
				query += "TBLPROPERTIES ('parquet.compress' = 'SNAPPY') "
			elif self.import_config.importTool in ("spark", "local"):
				query += "stored as ORC "
				query += "LOCATION '%s%s/' "%(self.common_operations.hdfs_address, hdfsLocation)

			self.common_operations.executeHiveQuery(query)
			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createExternalImportTable() - Finished")

	def convertHiveTableToACID(self):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			if self.common_operations.isTableTransactional(self.Hive_DB, self.Hive_Table) == False:
				self.common_operations.convertHiveTableToACID(self.Hive_DB, self.Hive_Table, createDeleteColumn=self.import_config.soft_delete_during_merge, createMergeColumns=True)

	def addHiveDBImportColumnsToHistoryTable(self):
		""" Will add the required DBImport columns in the Hive table """
		logging.debug("Executing import_operations.createHiveMergeColumns()")

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			self.startSpark()
			import pyspark

		# self.Hive_ColumnName_IUD = self.import_config.Hive_ColumnName_IUD
		# self.Hive_ColumnName_HistoryTimestamp = self.import_config.Hive_ColumnName_HistoryTimestamp

		Hive_History_DB = self.import_config.Hive_History_DB
		Hive_History_Table = self.import_config.Hive_History_Table

		columns = self.common_operations.getColumns(hiveDB=Hive_History_DB, hiveTable=Hive_History_Table, includeType=False, includeComment=False)

		if columns[columns['name'] == self.Hive_ColumnName_Source].empty == True and self.import_config.datalake_source != None:
			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				query = "alter table `%s`.`%s` add columns ( %s varchar(256) )"%(Hive_History_DB, Hive_History_Table, self.Hive_ColumnName_Source)
				self.common_operations.executeHiveQuery(query)

			elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				query = "alter table `%s`.`%s`.`%s` add columns ( %s string )"%(self.common_operations.sparkCatalogName, Hive_History_DB, Hive_History_Table, self.Hive_ColumnName_Source)
				logging.info(query)
				self.spark.sql(query)

			if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
				self.common_operations.reconnectHiveMetaStore()

		if columns[columns['name'] == self.Hive_ColumnName_IUD].empty == True:
			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				query = "alter table `%s`.`%s` add columns ( %s char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\")" \
					%(Hive_History_DB, Hive_History_Table, self.Hive_ColumnName_IUD)
				self.common_operations.executeHiveQuery(query)

			elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				query = "alter table `%s`.`%s`.`%s` add columns ( %s string COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\")" \
					%(self.common_operations.sparkCatalogName, Hive_History_DB, Hive_History_Table, self.Hive_ColumnName_IUD)
				logging.info(query)
				self.spark.sql(query)

			if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
				self.common_operations.reconnectHiveMetaStore()

		if columns[columns['name'] == self.Hive_ColumnName_HistoryTimestamp].empty == True:
			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				query = "alter table `%s`.`%s` add columns ( %s timestamp COMMENT \"Timestamp for insert in Datalake\")" \
					%(Hive_History_DB, Hive_History_Table, self.Hive_ColumnName_HistoryTimestamp)
				self.common_operations.executeHiveQuery(query)

			elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				query = "alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT \"Timestamp for insert in Datalake\" )" \
					%(self.common_operations.sparkCatalogName, Hive_History_DB, Hive_History_Table, self.Hive_ColumnName_HistoryTimestamp)
				logging.info(query)
				self.spark.sql(query)

			if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
				self.common_operations.reconnectHiveMetaStore()


	def addHiveDBImportColumns(self, mergeOperation):
		""" Will add the required DBImport columns in the Hive table """
		logging.debug("Executing import_operations.createHiveMergeColumns()")

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			self.startSpark()
			import pyspark

		columns = self.common_operations.getColumns(hiveDB=self.Hive_DB, hiveTable=self.Hive_Table, includeType=False, includeComment=False)

		if columns[columns['name'] == self.Hive_ColumnName_Source].empty == True and self.import_config.datalake_source != None:
			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				query = "alter table `%s`.`%s` add columns ( %s varchar(256) )"%(self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Source)
				self.common_operations.executeHiveQuery(query)

			elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				query = "alter table `%s`.`%s`.`%s` add columns ( %s string )"%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Source)
				logging.info(query)
				self.spark.sql(query)

			if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
				self.common_operations.reconnectHiveMetaStore()


		if mergeOperation == False:
			if self.import_config.create_datalake_import_column == True:
				# if columns[columns['name'] == 'datalake_insert'].empty == False and columns[columns['name'] == 'datalake_import'].empty == True:
				if columns[columns['name'] == self.Hive_ColumnName_Insert].empty == False and columns[columns['name'] == self.Hive_ColumnName_Import].empty == True:
					if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
						# query = "alter table `%s`.`%s` change column datalake_insert datalake_import timestamp"%(self.Hive_DB, self.Hive_Table)
						query = "alter table `%s`.`%s` change column %s %s timestamp" \
							%(self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Insert, self.Hive_ColumnName_Import)
						self.common_operations.executeHiveQuery(query)

					elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
						query = "alter table `%s`.`%s`.`%s` rename column %s to %s" \
							%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Insert, self.Hive_ColumnName_Import)
#						print(query)
						logging.info(query)
						self.spark.sql(query)

					if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
						self.common_operations.reconnectHiveMetaStore()

				# elif columns[columns['name'] == 'datalake_import'].empty == True:
				elif columns[columns['name'] == self.Hive_ColumnName_Import].empty == True:
					if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
						# query = "alter table `%s`.`%s` add columns ( datalake_import timestamp COMMENT \"Import time from source database\")"%(self.Hive_DB, self.Hive_Table)
						query = "alter table `%s`.`%s` add columns ( %s timestamp COMMENT \"Import time from source database\")" \
							%(self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Import)
						self.common_operations.executeHiveQuery(query)

					elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
						#query = "alter table `%s`.`%s`.`%s` add columns ( datalake_import timestamp COMMENT \"Import time from source database\" )" \
						#		%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table)
						query = "alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT \"Import time from source database\" )" \
								%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Import)
#						print(query)
						logging.info(query)
						self.spark.sql(query)

					if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
						self.common_operations.reconnectHiveMetaStore()

		else:
			# if columns[columns['name'] == 'datalake_import'].empty == False:
			# if columns[columns['name'] == self.Hive_ColumnName_Import].empty == False:
			if columns[columns['name'] == self.Hive_ColumnName_Import].empty == False and columns[columns['name'] == self.Hive_ColumnName_Insert].empty == True:
				if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
					# query = "alter table `%s`.`%s` change column datalake_import datalake_insert timestamp"%(self.Hive_DB, self.Hive_Table)
					query = "alter table `%s`.`%s` change column %s %s timestamp" \
						%(self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Import, self.Hive_ColumnName_Insert)
					self.common_operations.executeHiveQuery(query)

				elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
					#query = "alter table `%s`.`%s`.`%s` rename column datalake_import to datalake_insert" \
					#		%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table)
					query = "alter table `%s`.`%s`.`%s` rename column %s to %s" \
							%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Import, self.Hive_ColumnName_Insert)
#					print(query)
					logging.info(query)
					self.spark.sql(query)

				if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
					self.common_operations.reconnectHiveMetaStore()

			# elif columns[columns['name'] == 'datalake_insert'].empty == True:
			elif columns[columns['name'] == self.Hive_ColumnName_Insert].empty == True:
				if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
					# query = "alter table `%s`.`%s` add columns ( datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\")"%(self.Hive_DB, self.Hive_Table)
					query = "alter table `%s`.`%s` add columns ( %s timestamp COMMENT \"Timestamp for insert in Datalake\")" \
						%(self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Insert)
					self.common_operations.executeHiveQuery(query)

				elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
					query = "alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT \"Timestamp for insert in Datalake\" )" \
						%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Insert)
					logging.info(query)
					self.spark.sql(query)

				if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
					self.common_operations.reconnectHiveMetaStore()

#			if columns[columns['name'] == 'datalake_iud'].empty == True:
			if columns[columns['name'] == self.Hive_ColumnName_IUD].empty == True:
				if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
					query = "alter table `%s`.`%s` add columns ( %s char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\")" \
						%(self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_IUD)
					self.common_operations.executeHiveQuery(query)

				elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
					query = "alter table `%s`.`%s`.`%s` add columns ( %s string COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\")" \
						%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_IUD)
					logging.info(query)
					self.spark.sql(query)

				if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
					self.common_operations.reconnectHiveMetaStore()

			# if columns[columns['name'] == 'datalake_update'].empty == True:
			if columns[columns['name'] == self.Hive_ColumnName_Update].empty == True:
				if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
					# query = "alter table `%s`.`%s` add columns ( datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\")"%(self.Hive_DB, self.Hive_Table)
					query = "alter table `%s`.`%s` add columns ( %s timestamp COMMENT \"Timestamp for last update in Datalake\")" \
						%(self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Update)
					self.common_operations.executeHiveQuery(query)

				elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
					# query = "alter table `%s`.`%s`.`%s` add columns ( datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\")" \
					query = "alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT \"Timestamp for last update in Datalake\")" \
						%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Update)
					# print(query)
					logging.info(query)
					self.spark.sql(query)

				if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
					self.common_operations.reconnectHiveMetaStore()

			# if columns[columns['name'] == 'datalake_delete'].empty == True and self.import_config.soft_delete_during_merge == True:
			if columns[columns['name'] == self.Hive_ColumnName_Delete].empty == True and self.import_config.soft_delete_during_merge == True:
				if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
					# query = "alter table `%s`.`%s` add columns ( datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\")" \
					query = "alter table `%s`.`%s` add columns ( %s timestamp COMMENT \"Timestamp for soft delete in Datalake\")" \
						%(self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Delete)
					self.common_operations.executeHiveQuery(query)

				elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
					# query = "alter table `%s`.`%s`.`%s` add columns ( datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\")" \
					query = "alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT \"Timestamp for soft delete in Datalake\")" \
						%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Delete)
					logging.info(query)
					self.spark.sql(query)

				if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
					self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createHiveMergeColumns() - Finished")

	def generateCreateTargetTableSQL(self, hiveDB, hiveTable, acidTable=False, buckets=4, restrictColumns=None, acidInsertOnly=False, icebergTable=False):
		""" Will generate the common create table for the target table as a list. """
		logging.debug("Executing import_operations.generateCreateTargetTableSQL()")

		queryList = []
		if icebergTable == True:
			query  = "create table %s.`%s`.`%s` ("%(self.common_operations.sparkCatalogName, hiveDB, hiveTable)
		else:
			query  = "create table `%s`.`%s` ("%(hiveDB, hiveTable)
		columnsDF = self.import_config.getColumnsFromConfigDatabase(includeAllColumns=False) 

		restrictColumnsList = []
		if restrictColumns != None:
			restrictColumnsList = restrictColumns.split(",")

		firstLoop = True
		for index, row in columnsDF.iterrows():
			if restrictColumnsList != []:
				if row['name'] not in restrictColumnsList:
					continue
			if firstLoop == False: query += ", "
			query += "`%s` %s"%(row['name'], row['type'])
			if row['comment'] != None:
				query += " COMMENT \"%s\""%(row['comment'])
			firstLoop = False
		queryList.append(query)

		query = ") "
		tableComment = self.import_config.getHiveTableComment()

		if icebergTable == True:
			# Generate a create table statment that will be executed through Spark
			query += "USING iceberg "

			if tableComment != None:
				query += "COMMENT \"%s\" "%(tableComment)
		else:
			if tableComment != None:
				query += "COMMENT \"%s\" "%(tableComment)

			if acidTable == False:
				query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB') "
			else:
				if self.import_config.common_config.getConfigValue(key = "hive_acid_with_clusteredby") == True:
					query += "CLUSTERED BY ("
					firstColumn = True
		
					if self.import_config.getPKcolumns(PKforMerge = True) == None:		# To look at both pk_column_override and pk_override_merge_only 
						logging.error("There is no Primary Key for this table. Please add one in source system or in 'pk_column_override'")
						self.import_config.remove_temporary_files()
						sys.exit(1)
	
					for column in self.import_config.getPKcolumns(PKforMerge = True).split(","):
						if firstColumn == False:
							query += ", " 
						query += "`" + column + "`" 
						firstColumn = False
					#TODO: Smarter calculation of the number of buckets
					query += ") into %s buckets "%(buckets)

				if acidInsertOnly == False:
					query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true') "
				else:
					query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true', 'transactional_properties'='insert_only') "
	
		queryList.append(query)

		logging.debug("Executing import_operations.generateCreateTargetTableSQL() - Finished")
		return queryList

	def createDeleteTable(self):
		logging.debug("Executing import_operations.createDeleteTable()")

		Hive_Delete_DB = self.import_config.Hive_Delete_DB
		Hive_Delete_Table = self.import_config.Hive_Delete_Table

		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			if self.common_operations.checkTable(Hive_Delete_DB, Hive_Delete_Table) == False:
				# Target table does not exist. We just create it in that case
				logging.info("Creating Delete table %s.%s in Hive"%(Hive_Delete_DB, Hive_Delete_Table))
				if self.import_config.common_config.getConfigValue(key = "hive_insert_only_tables") == True:
					queryList = self.generateCreateTargetTableSQL( 
						hiveDB = Hive_Delete_DB, 
						hiveTable = Hive_Delete_Table, 
						acidTable = True, 
						acidInsertOnly = True, 
						restrictColumns = self.import_config.getPKcolumns(PKforMerge=True))
				else:
					queryList = self.generateCreateTargetTableSQL( 
						hiveDB = Hive_Delete_DB, 
						hiveTable = Hive_Delete_Table, 
						acidTable = False, 
						acidInsertOnly = False, 
						restrictColumns = self.import_config.getPKcolumns(PKforMerge=True))

				query = "".join(queryList)
	
				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			if self.common_operations.checkTable(Hive_Delete_DB, Hive_Delete_Table) == True:
				# Table exist, so we need to make sure that it's an Iceberg table and not something else

				if self.common_operations.isTableExternalIcebergFormat(Hive_Delete_DB, Hive_Delete_Table) == False:
					logging.info("Delete table is not a Iceberg table. Dropping and recreating the Delete table")
					self.common_operations.dropHiveTable(Hive_Delete_DB, Hive_Delete_Table)

			# We need to check again as the table might just been droped because it was an external table to begin with
			if self.common_operations.checkTable(Hive_Delete_DB, Hive_Delete_Table) == False:

				logging.info("Creating Delete table %s.%s in Spark"%(Hive_Delete_DB, Hive_Delete_Table))

				# Create the Iceberg table
				queryList = self.generateCreateTargetTableSQL( 
					hiveDB=Hive_Delete_DB,
					hiveTable=Hive_Delete_Table,
					restrictColumns = self.import_config.getPKcolumns(PKforMerge=True),
					icebergTable=True)

				query = "".join(queryList)

				self.startSpark()
				logging.info(query)
				self.spark.sql(query)
				self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createDeleteTable() - Finished")


	def createHistoryTable(self):
		logging.debug("Executing import_operations.createHistoryTable()")

		Hive_History_DB = self.import_config.Hive_History_DB
		Hive_History_Table = self.import_config.Hive_History_Table

		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			if self.common_operations.checkTable(Hive_History_DB, Hive_History_Table) == False:
				# Target table does not exist. We just create it in that case
				logging.info("Creating History table %s.%s in Hive"%(Hive_History_DB, Hive_History_Table))
				if self.import_config.common_config.getConfigValue(key = "hive_insert_only_tables") == True:
					queryList = self.generateCreateTargetTableSQL( hiveDB=Hive_History_DB, hiveTable=Hive_History_Table, acidTable=True, acidInsertOnly=True)
				else:
					queryList = self.generateCreateTargetTableSQL( hiveDB=Hive_History_DB, hiveTable=Hive_History_Table, acidTable=False, acidInsertOnly=False)

				query = queryList[0]

				if self.import_config.datalake_source != None:
					# query += ", datalake_source varchar(256)"
					query += ", %s varchar(256)"%(self.Hive_ColumnName_Source)

				# query += ", datalake_iud char(1) COMMENT \"SQL operation of this record was I=Insert, U=Update or D=Delete\""
				# query += ", datalake_timestamp timestamp COMMENT \"Timestamp for SQL operation in Datalake\""
				query += ", %s char(1) COMMENT \"SQL operation of this record was I=Insert, U=Update or D=Delete\""%(self.Hive_ColumnName_IUD)
				query += ", %s timestamp COMMENT \"Timestamp for SQL operation in Datalake\""%(self.Hive_ColumnName_HistoryTimestamp)
				query += queryList[1]

				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			if self.common_operations.checkTable(Hive_History_DB, Hive_History_Table) == True:
				# Table exist, so we need to make sure that it's an Iceberg table and not something else

				if self.common_operations.isTableExternalIcebergFormat(Hive_History_DB, Hive_History_Table) == False:
					# Table exists and is not an Iceberg table
					raise undevelopedFeature("The history table already exists, but it's not an Iceberg table. Convertion between manage table and external Iceberg table is not handled automatically. Please save the data and drop the table manually to continue with the import")
				else:
					return
			else:
				logging.info("Creating History table %s.%s in Spark"%(Hive_History_DB, Hive_History_Table))

				# Create the Iceberg table
				queryList = self.generateCreateTargetTableSQL( 
					hiveDB=Hive_History_DB,
					hiveTable=Hive_History_Table,
					icebergTable=True)

				query = queryList[0]

				if self.import_config.datalake_source != None:
					# query += ", datalake_source varchar(256)"
					query += ", %s varchar(256)"%(self.Hive_ColumnName_Source)

				# query += ", datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""
				# query += ", datalake_timestamp timestamp COMMENT \"Timestamp for SQL operation in Datalake\""
				query += ", %s char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""%(self.Hive_ColumnName_IUD)
				query += ", %s timestamp COMMENT \"Timestamp for SQL operation in Datalake\""%(self.Hive_ColumnName_HistoryTimestamp)
				query += queryList[1]

				self.startSpark()
				logging.info(query)
				self.spark.sql(query)
				self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createHistoryTable() - Finished")

	def createTargetTable(self):
		logging.debug("Executing import_operations.createTargetTable()")

		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			if self.common_operations.checkTable(self.Hive_DB, self.Hive_Table) == True:
				# Table exist, so we need to make sure that it's a managed table and not an external table
				if self.common_operations.isTableExternal(self.Hive_DB, self.Hive_Table) == True:
					if self.import_config.importPhase == constant.IMPORT_PHASE_FULL:
						# If the import stage is full, it's safe to drop the table and recreate it as the import have all the data anyway
						self.common_operations.dropHiveTable(self.Hive_DB, self.Hive_Table)

			# We need to check again as the table might just been droped because it was an external table to begin with
			if self.common_operations.checkTable(self.Hive_DB, self.Hive_Table) == False:
				# Target table does not exist. We just create it in that case
				logging.info("Creating Target table %s.%s in Hive"%(self.Hive_DB, self.Hive_Table))

				queryList = self.generateCreateTargetTableSQL( 
					hiveDB=self.Hive_DB, 
					hiveTable=self.Hive_Table, 
					acidTable=self.import_config.create_table_with_acid,
					acidInsertOnly=self.import_config.create_table_with_acid_insert_only)		

				query = queryList[0]

				if self.import_config.datalake_source != None:
					# query += ", datalake_source varchar(256)"
					query += ", %s varchar(256)"%(self.Hive_ColumnName_Source)

				if self.import_config.import_with_merge == False:
					if self.import_config.create_datalake_import_column == True:
						# query += ", datalake_import timestamp COMMENT \"Import time from source database\""
						query += ", %s timestamp COMMENT \"Import time from source database\""%(self.Hive_ColumnName_Import)
				else:
					# query += ", datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""
					# query += ", datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\""
					# query += ", datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\""
					query += ", %s char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""%(self.Hive_ColumnName_IUD)
					query += ", %s timestamp COMMENT \"Timestamp for insert in Datalake\""%(self.Hive_ColumnName_Insert)
					query += ", %s timestamp COMMENT \"Timestamp for last update in Datalake\""%(self.Hive_ColumnName_Update)

					if self.import_config.soft_delete_during_merge == True:
						# query += ", datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\""
						query += ", %s timestamp COMMENT \"Timestamp for soft delete in Datalake\""%(self.Hive_ColumnName_Delete)

				query += queryList[1]

				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			if self.common_operations.checkTable(self.Hive_DB, self.Hive_Table) == True:
				# Table exist, so we need to make sure that it's an Iceberg table and not something else

				if self.common_operations.isTableExternalIcebergFormat(self.Hive_DB, self.Hive_Table) == False:
					# Table exists and is not an Iceberg table
					logging.warning("Target table exists and is not an Iceberg table")
					if self.import_config.importPhase == constant.IMPORT_PHASE_FULL:
						# If the import stage is full, it's safe to drop the table and recreate it as the import have all the data anyway
						self.common_operations.dropHiveTable(self.Hive_DB, self.Hive_Table)
					else:
						raise undevelopedFeature("This is an incremental import and the target table already exists, but it's not an Iceberg table. Convertion between manage table and external Iceberg table is not handled automatically. Please save the data and drop the table manually to continue with the import")

			# We need to check again as the table might just been droped because it was an external table to begin with
			if self.common_operations.checkTable(self.Hive_DB, self.Hive_Table) == False:
				# Target table does not exist. We just create it in that case
				logging.info("Creating Target table %s.%s in Spark"%(self.Hive_DB, self.Hive_Table))

				# Create the Iceberg table
				queryList = self.generateCreateTargetTableSQL( 
					hiveDB=self.Hive_DB, 
					hiveTable=self.Hive_Table, 
					icebergTable=True)

				query = queryList[0]

				if self.import_config.datalake_source != None:
					# query += ", datalake_source varchar(256)"
					query += ", %s varchar(256)"%(self.Hive_ColumnName_Source)

				if self.import_config.import_with_merge == False:
					if self.import_config.create_datalake_import_column == True:
						# query += ", datalake_import timestamp COMMENT \"Import time from source database\""
						query += ", %s timestamp COMMENT \"Import time from source database\""%(self.Hive_ColumnName_Import)
				else:
					# query += ", datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""
					# query += ", datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\""
					# query += ", datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\""
					query += ", %s char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""%(self.Hive_ColumnName_IUD)
					query += ", %s timestamp COMMENT \"Timestamp for insert in Datalake\""%(self.Hive_ColumnName_Insert)
					query += ", %s timestamp COMMENT \"Timestamp for last update in Datalake\""%(self.Hive_ColumnName_Update)

					if self.import_config.soft_delete_during_merge == True:
						# query += ", datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\""
						query += ", %s timestamp COMMENT \"Timestamp for soft delete in Datalake\""%(self.Hive_ColumnName_Delete)

				query += queryList[1]

				self.startSpark()
				logging.info(query)
				self.spark.sql(query)
				self.common_operations.reconnectHiveMetaStore()
						
		logging.debug("Executing import_operations.createTargetTable() - Finished")
		
	def dropTargetTable(self):
		logging.debug("Executing import_operations.dropTargetTable()")
		if self.common_operations.checkTable(self.Hive_DB, self.Hive_Table) == True:
			logging.info("Dropping Target table %s.%s"%(self.Hive_DB, self.Hive_Table))

			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				query = "drop table if exists `%s`.`%s`"%(self.Hive_DB, self.Hive_Table)

				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()
	
			if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				self.startSpark()
				import pyspark

				query = "drop table if exists %s.`%s`.`%s`"%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table)
				logging.info(query)
				self.spark.sql(query)

		logging.debug("Executing import_operations.dropTargetTable() - Finished")

	def updateTargetTable(self):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			logging.info("Updating Target table columns based on source system schema")
			self.updateHiveTable(self.Hive_DB, self.Hive_Table)

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			logging.info("Updating Target table columns based on source system schema")
			self.updateHiveTable(self.Hive_DB, self.Hive_Table)

	def updateHistoryTable(self):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			logging.info("Updating History table columns based on source system schema")
			self.updateHiveTable(self.import_config.Hive_History_DB, self.import_config.Hive_History_Table)

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			logging.info("Updating History table columns based on source system schema")
			self.updateHiveTable(self.import_config.Hive_History_DB, self.import_config.Hive_History_Table)
			# self.updateHiveTable(self.Hive_DB, self.Hive_Table)

	def updateDeleteTable(self):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			logging.info("Updating Delete table columns based on source system schema")
			self.updateHiveTable(self.import_config.Hive_Delete_DB, self.import_config.Hive_Delete_Table, restrictColumns = self.import_config.getPKcolumns(PKforMerge=True))

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			logging.info("Updating Delete table columns based on source system schema")
			self.updateHiveTable(self.import_config.Hive_Delete_DB, self.import_config.Hive_Delete_Table, restrictColumns = self.import_config.getPKcolumns(PKforMerge=True))

	def updateExternalImportTable(self):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			logging.info("Updating Import table columns based on source system schema")
			if self.import_config.importTool == "sqoop":
				self.updateHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table, sourceIsParquetFile=True)
			elif self.import_config.importTool in ("spark", "local"):
				self.updateHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table, sourceIsParquetFile=False)
	
			tableLocation = self.common_operations.getTableLocation(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
			configuredLocation = "%s%s"%(self.common_operations.hdfs_address, self.import_config.sqoop_hdfs_location)

			if tableLocation != configuredLocation:
				logging.info("The configured location for the external table have changed. Updating the external table")
				logging.debug("tableLocation:      %s"%(tableLocation))
				logging.debug("configuredLocation: %s"%(configuredLocation))
	
				query = "alter table `%s`.`%s` set location \"%s\""%(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table, self.import_config.sqoop_hdfs_location)
				self.common_operations.executeHiveQuery(query)
		

	def updateColumnsForImportTable(self, columnsDF, replaceNameOnly = False):
		""" Parquet import format from sqoop cant handle all datatypes correctly. So for the column definition, we need to change some. We also replace <SPACE> with underscore in the column name """
		if replaceNameOnly == False:
			columnsDF["type"].replace(["date"], "string", inplace=True)
			columnsDF["type"].replace(["timestamp"], "string", inplace=True)
			columnsDF["type"].replace(["decimal(.*)"], "string", regex=True, inplace=True)
			columnsDF["type"].replace(["bigint"], "string", regex=True, inplace=True)

		# If you change any of the name replace rows, you also need to change the same data in function self.copyHiveTable() and import_definitions.saveColumnData()
		columnsDF["name"].replace([" "], "_", regex=True, inplace=True)
		columnsDF["name"].replace(["%"], "pct", regex=True, inplace=True)
		columnsDF["name"].replace(["\("], "_", regex=True, inplace=True)
		columnsDF["name"].replace(["\)"], "_", regex=True, inplace=True)
		columnsDF["name"].replace(["ü"], "u", regex=True, inplace=True)
		columnsDF["name"].replace(["å"], "a", regex=True, inplace=True)
		columnsDF["name"].replace(["ä"], "a", regex=True, inplace=True)
		columnsDF["name"].replace(["ö"], "o", regex=True, inplace=True)

		return columnsDF

	def updateColumnsForMongoTables(self, columnsDF):
		""" Mongo Imports have both structs and arrays as columntypes. For this, the columnames needs a ` around them inside
		the struct or array. If this is included in the columnType when doing a compare with Hive, there will be an alter table
		at every import as the columntype is never the same """

		columnsDF["type"].replace(["`"], "", regex=True, inplace=True)
		return columnsDF

	def updateHiveTable(self, hiveDB, hiveTable, restrictColumns=None, sourceIsParquetFile=False):
		""" Update the target table based on the column information in the configuration database """
		logging.debug("Executing import_operations.updateHiveTable()")
		columnsConfig = self.import_config.getColumnsFromConfigDatabase(restrictColumns=restrictColumns, sourceIsParquetFile=sourceIsParquetFile, includeAllColumns=False) 
		columnsHive   = self.common_operations.getColumns(hiveDB, hiveTable, includeType=True, excludeDataLakeColumns=True) 

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			self.startSpark()
			import pyspark

#		pd.set_option('display.max_rows', None)
#		print(columnsConfig)
#		print("==============================================")
#		print(columnsHive)

#		raise undevelopedFeature("Stopping here to debug")

		# If we are working on the import table, we need to change some column types to handle Parquet files
		if self.import_config.importTool == "sqoop":
			if hiveDB == self.import_config.Hive_Import_DB and hiveTable == self.import_config.Hive_Import_Table:
				columnsConfig = self.updateColumnsForImportTable(columnsConfig)

		# There is a difference between comparing column types and then updating Hive with that columntype. For example. Mongo import
		# requires that ` is around the columns inside the structs and arrays. But Hive doesnt store that. So doing a compare will
		# always fail due to that. So we need to compare without the ` around the column names. But  if there is a difference, we need to
		# run the alter command WITH the `, otherwise the Hive query will fail. columnTypesChanged is used to identify that
		columnTypesChanged = False
		columnsConfigSaved = columnsConfig.copy()

		# Update the column types if the import is a Mongo import
		if self.import_config.mongoImport == True:
			columnsConfig = self.updateColumnsForMongoTables(columnsConfig)
			columnTypesChanged = True

		# Check for missing columns
		columnsConfigOnlyName = columnsConfig.filter(['name'])
		columnsHiveOnlyName = columnsHive.filter(['name'])
		columnsMergeOnlyName = pd.merge(columnsConfigOnlyName, columnsHiveOnlyName, on=None, how='outer', indicator='Exist')

		columnsConfigCount = len(columnsConfigOnlyName)
		columnsHiveCount   = len(columnsHiveOnlyName)
		columnsMergeLeftOnlyCount  = len(columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'])
		columnsMergeRightOnlyCount = len(columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'right_only'])

		if self.import_config.importTool in ("spark", "local"):
			# Orc files needs the column to be in the right order. This can only be fixed with a drop/create
			# And it's only needed for the external tables
			if hiveDB == self.import_config.Hive_Import_DB and hiveTable == self.import_config.Hive_Import_Table:
				foundColumnError = False
				for hiveIndex, hiveRow in columnsHiveOnlyName.iterrows():
					hiveName = hiveRow['name']
					indexInConfig = -1
					if len(columnsConfigOnlyName.loc[columnsConfigOnlyName['name'] == hiveName]) > 0:
						# Row in Hive exists in Config aswell. Need to check position
						indexInConfig = columnsConfigOnlyName.loc[columnsConfigOnlyName['name'] == hiveName].index.tolist()[0]
						if hiveIndex != indexInConfig:
							foundColumnError = True
	
				if foundColumnError == True:
					# There was an error with the column order. We need to drop and recreate
					logging.warning("The column order was not correct in the External Import Table. As this is a ORC table, we will have to drop and recreate it. This is needed because Hive tables based on ORC files does not support reordering of columns")
					self.common_operations.dropHiveTable(hiveDB, hiveTable)
					self.createExternalImportTable()
					self.common_operations.reconnectHiveMetaStore()

		# pd.set_option('display.max_rows', None)
		logging.debug("columnsConfigOnlyName")
		logging.debug(columnsConfigOnlyName)
		logging.debug("================================================================")
		logging.debug("columnsHiveOnlyName")
		logging.debug(columnsHiveOnlyName)
		logging.debug("================================================================")
		logging.debug("columnsMergeOnlyName")
		logging.debug(columnsMergeOnlyName)
		logging.debug("")

		if columnsConfigCount == columnsHiveCount and columnsMergeLeftOnlyCount > 0:
			# The number of columns in config and Hive is the same, but there is a difference in name. This is most likely because of a rename of one or more of the columns
			# To handle this, we try a rename. This might fail if the column types are also changed to an incompatable type
			# The logic here is to 
			# 1. get all columns from mergeDF that exists in Left_Only
			# 2. Get the position in configDF with that column name
			# 3. Get the column in the same position from HiveDF
			# 4. Check if that column name exists in the mergeDF with Right_Only. If it does, the column was just renamed
			for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
				rowInConfig = columnsConfig.loc[columnsConfig['name'] == row['name']].iloc[0]
				indexInConfig = columnsConfig.loc[columnsConfig['name'] == row['name']].index.item()
				rowInHive = columnsHive.iloc[indexInConfig]
				
				if len(columnsMergeOnlyName.loc[(columnsMergeOnlyName['Exist'] == 'right_only') & (columnsMergeOnlyName['name'] == rowInHive["name"])]) > 0: 
					# This is executed if the column is renamed and exists in the same position
					logging.debug("Name in config:  %s"%(rowInConfig["name"]))
					logging.debug("Type in config:  %s"%(rowInConfig["type"]))
					logging.debug("Index in config: %s"%(indexInConfig))
					logging.debug("--------------------")
					logging.debug("Name in Hive: %s"%(rowInHive["name"]))
					logging.debug("Type in Hive: %s"%(rowInHive["type"]))
					logging.debug("======================================")
					logging.debug("")
	
					if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
						query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, rowInHive['name'], rowInConfig['name'], rowInConfig['type'])
						self.common_operations.executeHiveQuery(query)

					elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
						query = "alter table `%s`.`%s`.`%s` rename column `%s` to %s" \
								%(self.common_operations.sparkCatalogName, hiveDB, hiveTable, rowInHive['name'], rowInConfig['name'])
						logging.info(query)
						self.spark.sql(query)

					self.import_config.logHiveColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)
					if rowInConfig["type"] != rowInHive["type"]:
						self.import_config.logHiveColumnTypeChange(rowInConfig['name'], rowInConfig['type'], previous_columnType=rowInHive["type"], hiveDB=hiveDB, hiveTable=hiveTable) 
				else:
					if columnsMergeLeftOnlyCount == 1 and columnsMergeRightOnlyCount == 1:
						# So the columns are not in the same position, but it's only one column that changed. In that case, we just rename that one column
						rowInMergeLeft  = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iloc[0]
						rowInMergeRight = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'right_only'].iloc[0]
						rowInConfig = columnsConfig.loc[columnsConfig['name'] == rowInMergeLeft['name']].iloc[0]
						rowInHive = columnsHive.loc[columnsHive['name'] == rowInMergeRight['name']].iloc[0]
						logging.debug(rowInConfig["name"])
						logging.debug(rowInConfig["type"])
						logging.debug("--------------------")
						logging.debug(rowInHive["name"])
						logging.debug(rowInHive["type"])

						if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
							query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, rowInHive['name'], rowInConfig['name'], rowInHive['type'])
							self.common_operations.executeHiveQuery(query)

						elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
							query = "alter table `%s`.`%s`.`%s` rename column `%s` to %s" \
									%(self.common_operations.sparkCatalogName, hiveDB, hiveTable, rowInHive['name'], rowInConfig['name'])
							logging.info(query)
							self.spark.sql(query)

						self.import_config.logHiveColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)
						if rowInConfig["type"] != rowInHive["type"]:
							self.import_config.logHiveColumnTypeChange(rowInConfig['name'], rowInConfig['type'], previous_columnType=rowInHive["type"], hiveDB=hiveDB, hiveTable=hiveTable) 

			if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
				self.common_operations.reconnectHiveMetaStore()

			columnsHive   = self.common_operations.getColumns(hiveDB, hiveTable, includeType=True, includeComment=True, excludeDataLakeColumns=True) 
			columnsHiveOnlyName = columnsHive.filter(['name'])
			columnsMergeOnlyName = pd.merge(columnsConfigOnlyName, columnsHiveOnlyName, on=None, how='outer', indicator='Exist')

		for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
			# This will iterate over columns that only exists in the config and not in Hive. We add these to Hive
			fullRow = columnsConfig.loc[columnsConfig['name'] == row['name']].iloc[0]

			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				query = "alter table `%s`.`%s` add columns (`%s` %s"%(hiveDB, hiveTable, fullRow['name'], fullRow['type'])
				if fullRow['comment'] != None:
					query += " COMMENT \"%s\""%(fullRow['comment'])
				query += ")"

				self.common_operations.executeHiveQuery(query)

			elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				query = "alter table `%s`.`%s`.`%s` add columns (`%s` %s"%(self.common_operations.sparkCatalogName, hiveDB, hiveTable, fullRow['name'], fullRow['type'])
				if fullRow['comment'] != None:
					query += " comment \"%s\""%(fullRow['comment'])
				query += ")"

				logging.info(query)
				self.spark.sql(query)

			self.import_config.logHiveColumnAdd(fullRow['name'], columnType=fullRow['type'], hiveDB=hiveDB, hiveTable=hiveTable) 

		# Check for changed column types
		if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
			self.common_operations.reconnectHiveMetaStore()

		columnsHive = self.common_operations.getColumns(hiveDB, hiveTable, includeType=True, excludeDataLakeColumns=True) 

		columnsConfigOnlyNameType = columnsConfig.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
		columnsHiveOnlyNameType = columnsHive.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
		columnsMergeOnlyNameType = pd.merge(columnsConfigOnlyNameType, columnsHiveOnlyNameType, on=None, how='outer', indicator='Exist')

		# pd.set_option('display.max_rows', None)
		logging.debug("columnsConfigOnlyNameType")
		logging.debug(columnsConfigOnlyNameType)
		logging.debug("================================================================")
		logging.debug("columnsHiveOnlyNameType")
		logging.debug(columnsHiveOnlyNameType)
		logging.debug("================================================================")
		logging.debug("columnsMergeOnlyNameType")
		logging.debug(columnsMergeOnlyNameType)
		logging.debug("")

#		for index, row in columnsMergeOnlyNameType.loc[columnsMergeOnlyNameType['Exist'] == 'right_only'].iterrows():
#			# This will iterate over columns that only exists in Target table and not on the import table
#			if self.import_config.import_is_incremental == False and self.import_config.import_with_merge == False:
#				# As the aim is to drop the column, we must make sure to only do that when we are sure to not loose data
#				columnName = row['name']
#
#				# Drop column is not supported in Hive. Thats why the Hive code is not here
#				if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
#					logging.info("Dropping column '%s'"%(columnName))
#					try:
#						query = "alter table `%s`.`%s`.`%s` drop column `%s` " \
#								%(self.common_operations.sparkCatalogName, hiveDB, hiveTable, columnName)
#						logging.info(query)
#						self.spark.sql(query)
#
#					except pyspark.sql.utils.AnalysisException as e:
#						raise SQLerror(str(e))

		for index, row in columnsMergeOnlyNameType.loc[columnsMergeOnlyNameType['Exist'] == 'left_only'].iterrows():
			# This will iterate over columns that had the type changed from the source
			columnName = row['name']
			columnType = row['type']

			if columnTypesChanged == True:
				# This means that we compare with another value than will be used in the alter. We need to get the correct alter column type
				columnType = columnsConfigSaved.loc[columnsConfigSaved['name'] == columnName]['type'].iloc[0]

			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, columnName, columnName, columnType)
				self.common_operations.executeHiveQuery(query)

			elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				query = "alter table `%s`.`%s`.`%s` alter column `%s` type %s" \
						%(self.common_operations.sparkCatalogName, hiveDB, hiveTable, columnName, columnType)
				try:
					logging.info(query)
					self.spark.sql(query)
				except pyspark.sql.utils.AnalysisException as e:
					if "Cannot update" in str(e) and "cannot be cast to" in str(e):
						if self.import_config.import_is_incremental == False and self.import_config.import_with_merge == False:
							# This is a full import without merge. So it's safe to drop the column and recreate it
							logging.info("Alter type of column '%s' failed due to incompatible column types. But as this is a full none-merge import, we will drop and recreate the column instead")
							try:
								query = "alter table `%s`.`%s`.`%s` drop column `%s` " \
										%(self.common_operations.sparkCatalogName, hiveDB, hiveTable, columnName)
								logging.info(query)
								self.spark.sql(query)

								query = "alter table `%s`.`%s`.`%s` add columns (`%s` %s) " \
										%(self.common_operations.sparkCatalogName, hiveDB, hiveTable, columnName, columnType)
								logging.info(query)
								self.spark.sql(query)

							except pyspark.sql.utils.AnalysisException as e:
								raise SQLerror(str(e))
						else:
							logging.error("The source table schema have changed. During alter of column to change the type in the Iceberg table, Spark returned an error.")
							logging.error("This mostly means that the column types change is not supported and cant be handled by DBImport automatically. ")
							logging.error("To get out of this problem, the table needs to be recreated manually before a successful import can be executed again")
							raise SQLerror(str(e))
					else:
						raise SQLerror(str(e))

			# Get the previous column type from the Pandas DF with right_only in Exist column
			previous_columnType = (columnsMergeOnlyNameType.loc[
				(columnsMergeOnlyNameType['name'] == row['name']) &
				(columnsMergeOnlyNameType['Exist'] == 'right_only')]
				).reset_index().at[0, 'type']

			self.import_config.logHiveColumnTypeChange(row['name'], columnType=row['type'], previous_columnType=previous_columnType, hiveDB=hiveDB, hiveTable=hiveTable) 

		# Check for change column comments
		if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
			self.common_operations.reconnectHiveMetaStore()

		columnsHive = self.common_operations.getColumns(hiveDB, hiveTable, includeType=True, includeComment=True, excludeDataLakeColumns=True) 

		try:
			columnsHive['comment'] = columnsHive['comment'].replace(['^$'], [None], regex=True)	# Replace blank column comments with None as it would otherwise trigger an alter table on every run
		except ValueError:
			# This exception will happen if all columns are None.
			pass

		columnsMerge = pd.merge(columnsConfig, columnsHive, on=None, how='outer', indicator='Exist')

		for index, row in columnsMerge.loc[columnsMerge['Exist'] == 'left_only'].iterrows():
			if row['comment'] == None: row['comment'] = ""

			columnName = row['name']
			columnType = row['type']
			columnComment = row['comment']

			if columnTypesChanged == True:
				# This means that we compare with another value than will be used in the alter. We need to get the correct alter column type
				columnType = columnsConfigSaved.loc[columnsConfigSaved['name'] == columnName]['type'].iloc[0]
	
			if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
				query = "alter table `%s`.`%s` change column `%s` `%s` %s comment \"%s\""%(hiveDB, hiveTable, columnName, columnName, columnType, columnComment)
				self.common_operations.executeHiveQuery(query)

			elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
				query = "alter table `%s`.`%s`.`%s` alter column `%s` comment \"%s\"" \
						%(self.common_operations.sparkCatalogName, hiveDB, hiveTable, columnName, columnComment)
				logging.info(query)
				self.spark.sql(query)

		logging.debug("Executing import_operations.updateHiveTable() - Finished")

	def updatePKonTargetTable(self,):
		""" Update the PrimaryKey definition on the Target Hive Table """
		logging.debug("Executing import_operations.updatePKonTargetTable()")
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			# Only required if using Hive as the ETL Engine. Spark Iceberg tables does not support primary keys

			self.updatePKonTable(self.import_config.Hive_DB, self.import_config.Hive_Table)

		logging.debug("Executing import_operations.updatePKonTargetTable() - Finished")

	def updatePKonTable(self, hiveDB, hiveTable):
		""" Update the PrimaryKey definition on a Hive Table """
		logging.debug("Executing import_operations.updatePKonTable()")
		if self.import_config.getPKcolumns() == None:
			logging.info("No Primary Key informaton exists for this table.")
		else:
			logging.info("Creating Primary Key on table.")
			PKonHiveTable = self.common_operations.getPK(hiveDB, hiveTable)
			PKinConfigDB = self.import_config.getPKcolumns()
			self.common_operations.reconnectHiveMetaStore()

			if PKonHiveTable != PKinConfigDB:
				# The PK information is not correct. We need to figure out what the problem is and alter the table to fix it
				logging.debug("PK in Hive:   %s"%(PKonHiveTable))
				logging.debug("PK in Config: %s"%(PKinConfigDB))
				logging.info("PrimaryKey definition is not correct. If it exists, we will drop it and recreate")
				logging.info("PrimaryKey on Hive table:     %s"%(PKonHiveTable))
				logging.info("PrimaryKey from source table: %s"%(PKinConfigDB))

				currentPKname = self.common_operations.getPKname(hiveDB, hiveTable)
				if currentPKname != None:
					query  = "alter table `%s`.`%s` "%(self.import_config.Hive_DB, self.import_config.Hive_Table)
					query += "drop constraint %s"%(currentPKname)
					self.common_operations.executeHiveQuery(query)

				# There are now no PK on the table. So it's safe to create one
				PKname = "%s__%s__PK"%(self.import_config.Hive_DB, self.import_config.Hive_Table)
				query  = "alter table `%s`.`%s` "%(self.import_config.Hive_DB, self.import_config.Hive_Table)
				query += "add constraint %s primary key ("%(PKname)

				firstColumn = True
				for column in self.import_config.getPKcolumns().split(","):
					if firstColumn == False:
						query += ", " 
					query += "`" + column + "`" 
					firstColumn = False

				query += ") DISABLE NOVALIDATE"
				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

			else:
				logging.info("PrimaryKey definition is correct. No change required.")


		logging.debug("Executing import_operations.updatePKonTable() - Finished")

	def updateFKonTargetTable(self,):
		""" Update the ForeignKeys definition on the Target Hive Table """
		logging.debug("Executing import_operations.updatePKonTargetTable()")
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			# Only required if using Hive as the ETL Engine. Spark Iceberg tables does not support foreign keys

			self.updateFKonTable(self.import_config.Hive_DB, self.import_config.Hive_Table)

		logging.debug("Executing import_operations.updatePKonTargetTable() - Finished")

	def updateFKonTable(self, hiveDB, hiveTable):
		""" Update the ForeignKeys definition on a Hive Table """
		logging.debug("Executing import_operations.updateFKonTable()")

		if self.import_config.create_foreign_keys == 0:
			logging.info("No Foreign Keys will be created for this table as it's disabled in the configuration")
			return

		foreignKeysFromConfig = self.import_config.getForeignKeysFromConfig()
		foreignKeysFromHive   = self.common_operations.getFKs(hiveDB, hiveTable)
	
		if foreignKeysFromConfig.empty == True and foreignKeysFromHive.empty == True:
			logging.info("No Foreign Keys informaton exists for this table")
			return

		if foreignKeysFromConfig.equals(foreignKeysFromHive) == True:
			logging.info("ForeignKey definitions is correct. No change required.")
		else:
			logging.info("ForeignKey definitions is not correct. Will check what exists and create/drop required foreign keys.")

			foreignKeysDiff = pd.merge(foreignKeysFromConfig, foreignKeysFromHive, on=None, how='outer', indicator='Exist')

			for index, row in foreignKeysDiff.loc[foreignKeysDiff['Exist'] == 'right_only'].iterrows():
				# This will iterate over ForeignKeys that only exists in Hive
				# If it only exists in Hive, we delete it!

				logging.info("Dropping FK in Hive as it doesnt match the FK's in DBImport config database")
				query = "alter table `%s`.`%s` drop constraint %s"%(hiveDB, hiveTable, row['fk_name'])
				try:
					self.common_operations.executeHiveQuery(query)
				except Exception as ex:
					logging.error(ex)
					logging.error("There was a problem when dropping FK. Will stop trying to drop the rest of the FK's on this table")
					break
					
							
			for index, row in foreignKeysDiff.loc[foreignKeysDiff['Exist'] == 'left_only'].iterrows():
				# This will iterate over ForeignKeys that only exists in the DBImport configuration database
				# If it doesnt exist in Hive, we create it

				if self.common_operations.checkTable(row['ref_hive_db'], row['ref_hive_table'], ) == True:
					query = "alter table `%s`.`%s` add constraint %s foreign key ("%(hiveDB, hiveTable, row['fk_name'])
					firstLoop = True
					for column in row['source_column_name'].split(','):
						if firstLoop == False: query += ", "
						query += "`%s`"%(column)
						firstLoop = False
	
					query += ") REFERENCES `%s`.`%s` ("%(row['ref_hive_db'], row['ref_hive_table'])
					firstLoop = True
					for column in row['ref_column_name'].split(','):
						if firstLoop == False: query += ", "
						query += "`%s`"%(column)
						firstLoop = False
					query += ") DISABLE NOVALIDATE RELY"
	
					logging.info("Creating FK in Hive as it doesnt exist")
					
					try:
						self.common_operations.executeHiveQuery(query)
					except Exception as ex:
						logging.error(ex)
						logging.error("There was a problem when creating FK. Will stop trying to create the rest of the FK's on this table")
						break

			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.updateFKonTable()  - Finished")

	def removeHiveLocks(self,):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			if self.import_config.common_config.getConfigValue(key = "hive_remove_locks_by_force") == True:
				self.common_operations.removeLocksByForce(self.Hive_DB, self.Hive_Table)
				self.common_operations.removeLocksByForce(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
				self.common_operations.removeLocksByForce(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_View)
				self.common_operations.removeLocksByForce(self.import_config.Hive_Import_PKonly_DB, self.import_config.Hive_Import_PKonly_Table)
				self.common_operations.removeLocksByForce(self.import_config.Hive_Delete_DB, self.import_config.Hive_Delete_Table)
				self.common_operations.removeLocksByForce(self.import_config.Hive_HistoryTemp_DB, self.import_config.Hive_HistoryTemp_Table)

	def runHiveMajorCompaction(self,):
		self.common_operations.connectToHive(forceSkipTest=True)
		self.common_operations.runHiveMajorCompaction(self.Hive_DB, self.Hive_Table)


	def runHiveCompaction(self, ):
		""" Force a compaction on a Hive table. The type of compaction is configured per table or a major as default """
		logging.debug("Executing import_operations.runHiveCompaction()")

		compactionMethod = "none"
		andWait = False
		compactionDescription = ""

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			return

		if self.import_config.mergeCompactionMethod == "default":
			if self.import_config.common_config.getConfigValue(key = "hive_major_compact_after_merge") == True:
				compactionMethod = "major"
				compactionDescription = compactionMethod
		else:
			if self.import_config.mergeCompactionMethod == "minor_and_wait":
				compactionMethod = "minor"
				andWait = True
				compactionDescription = "minor and wait"
			elif self.import_config.mergeCompactionMethod == "major_and_wait":
				compactionMethod = "major"
				andWait = True
				compactionDescription = "major and wait"
			else:
				compactionMethod = self.import_config.mergeCompactionMethod
				compactionDescription = compactionMethod

		if compactionMethod != "none":
			logging.info("Running a '%s' compaction on Hive table"%(compactionDescription))

			try:
				if andWait == False:
					self.common_operations.executeHiveQuery("alter table `%s`.`%s` compact '%s' "%(self.Hive_DB, self.Hive_Table, compactionMethod))
				else:
					self.common_operations.executeHiveQuery("alter table `%s`.`%s` compact '%s' and wait "%(self.Hive_DB, self.Hive_Table, compactionMethod))
			except:
				logging.error("Major compaction failed with the following error message:")
				logging.warning(sys.exc_info())
			pass
		else:
			logging.info("Compaction is disabled for this import")

		logging.debug("Executing import_operations.runHiveCompaction() - Finished")

	def truncateTargetTable(self,):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			logging.info("Truncating Target table in Hive")
			self.common_operations.connectToHive(forceSkipTest=True)
			self.common_operations.truncateHiveTable(self.Hive_DB, self.Hive_Table)

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			logging.info("Truncating Target table in Spark")
			self.startSpark()

			query = "truncate table %s.`%s`.`%s`"%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table)
			logging.info(query)
			self.spark.sql(query)

	
	def updateStatisticsOnTargetTable(self,):
		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			logging.info("Updating the Hive statistics on the target table")
			self.common_operations.updateHiveTableStatistics(self.Hive_DB, self.Hive_Table)

	def loadDataFromImportToTargetTable(self,):
		logging.info("Loading data from import table to target table")
		importTableExists = self.common_operations.checkTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
		targetTableExists = self.common_operations.checkTable(self.Hive_DB, self.Hive_Table)
	
		if importTableExists == False:
			logging.error("The import table %s.%s does not exist"%(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table))
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if targetTableExists == False:
			logging.error("The target table %s.%s does not exist"%(self.Hive_DB, self.Hive_Table))
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
			self.copyHiveTable(	self.import_config.Hive_Import_DB, 
								self.import_config.Hive_Import_Table, 
								self.Hive_DB, 
								self.Hive_Table)

		if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
			self.copySparkTable( self.import_config.Hive_Import_DB,
								self.import_config.Hive_Import_Table, 
								self.Hive_DB, 
								self.Hive_Table)


	def generateCopyTableStatement(self, sourceDB, sourceTable, targetDB, targetTable, icebergTable=False):
		""" Generate the SQL statement that is required for inserting all rows from one table to another were column match """
		logging.debug("Executing import_operations.generateCopyTableStatement()")
		columnMerge = self.common_operations.getHiveColumnNameDiff(sourceDB=sourceDB, sourceTable=sourceTable, targetDB=targetDB, targetTable=targetTable, importTool = self.import_config.importTool, sourceIsImportTable=True)

		firstLoop = True
		columnDefinitionSource = ""
		columnDefinitionTarget = ""
		for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
			if firstLoop == False: columnDefinitionSource += ", "
			if firstLoop == False: columnDefinitionTarget += ", "
			columnDefinitionSource += "`%s`"%(row['sourceName'])
			columnDefinitionTarget += "`%s`"%(row['targetName'])
			firstLoop = False

		if icebergTable == False:
			query = "insert into `%s`.`%s` ("%(targetDB, targetTable)
		else:
			query = "insert into %s.`%s`.`%s` ("%(self.common_operations.sparkCatalogName, targetDB, targetTable)

		query += columnDefinitionTarget
		if self.import_config.datalake_source != None:
			# query += ", datalake_source"
			query += ", %s"%(self.Hive_ColumnName_Source)
		if self.import_config.create_datalake_import_column == True:
			# query += ", datalake_import"
			query += ", %s"%(self.Hive_ColumnName_Import)

		query += ") select "
		query += columnDefinitionSource
		if self.import_config.datalake_source != None:
			query += ", '%s'"%(self.import_config.datalake_source)
		if self.import_config.create_datalake_import_column == True:
			if icebergTable == False:
				query += ", '%s'"%(self.import_config.sqoop_last_execution_timestamp)
			else:
				query += ", TIMESTAMP('%s')"%(self.import_config.sqoop_last_execution_timestamp)

		if icebergTable == False:
			query += " from `%s`.`%s` "%(sourceDB, sourceTable)
		else:
			query += " from %s.`%s`.`%s` "%(self.common_operations.sparkCatalogName, sourceDB, sourceTable)

		if self.import_config.nomerge_ingestion_sql_addition != None:
			query += self.import_config.nomerge_ingestion_sql_addition

		logging.debug("Executing import_operations.generateCopyTableStatement() - Finished")
		return query

	def copyHiveTable(self, sourceDB, sourceTable, targetDB, targetTable):
		""" Copy one Hive table into another for the columns that have the same name """
		logging.debug("Executing import_operations.copyHiveTable()")

		query = self.generateCopyTableStatement(sourceDB, sourceTable, targetDB, targetTable, icebergTable=False)
		self.common_operations.executeHiveQuery(query)

		logging.debug("Executing import_operations.copyHiveTable() - Finished")

	def copySparkTable(self, sourceDB, sourceTable, targetDB, targetTable):
		""" Copy one Hive table into another for the columns that have the same name """
		logging.debug("Executing import_operations.copyHiveTable()")

		query = self.generateCopyTableStatement(sourceDB, sourceTable, targetDB, targetTable, icebergTable=True)
		self.startSpark()
		logging.info(query)
		self.spark.sql(query)

		logging.debug("Executing import_operations.copyHiveTable() - Finished")

	def resetIncrMinMaxValues(self, maxValue):
		self.import_config.resetIncrMinMaxValues(maxValue=maxValue)

	def resetIncrMaxValue(self, hiveDB=None, hiveTable=None):
		""" Will read the Max value from the Hive table and save that into the incr_maxvalue column """
		logging.debug("Executing import_operations.resetIncrMaxValue()")

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
			logging.error("Oracle Flashback imports does not support repairing the table.")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if self.import_config.import_is_incremental == True:
			self.sqoopIncrNoNewRows = False
			query = "select max(`%s`) from `%s`.`%s` "%(self.import_config.sqoop_incr_column, hiveDB, hiveTable)

			self.common_operations.connectToHive(forceSkipTest=True)
			result_df = self.common_operations.executeHiveQuery(query)
			maxValue = result_df.loc[0][0]

			if type(maxValue) == pd._libs.tslibs.timestamps.Timestamp:
				(dt, micro) = maxValue.strftime('%Y-%m-%d %H:%M:%S.%f').split(".")
				maxValue = "%s.%03d" % (dt, int(micro) / 1000)
			else:
				if maxValue != None:
					maxValue = int(maxValue)

			if maxValue != None:
				self.import_config.resetSqoopStatistics(maxValue)
				self.import_config.clearStage()
		else:
			logging.error("This is not an incremental import. Nothing to repair.")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		logging.debug("Executing import_operations.resetIncrMaxValue() - Finished")

	def repairAllIncrementalImports(self):
		logging.debug("Executing import_operations.repairAllIncrementalImports()")
		result_df = self.import_config.getAllActiveIncrImports()

		tablesRepaired = False
		for index, row in result_df.iterrows():
			tablesRepaired = True
			hiveDB = row[0]
			hiveTable = row[1]
			logging.info("Repairing table \u001b[33m%s.%s\u001b[0m"%(hiveDB, hiveTable))
			self.setHiveTable(hiveDB, hiveTable)
			self.resetIncrMaxValue(hiveDB, hiveTable)
			logging.info("")

		if tablesRepaired == False:
			logging.info("\n\u001b[32mNo incremental tables found that could be repaired\u001b[0m\n")

		logging.debug("Executing import_operations.repairAllIncrementalImports() - Finished")


	def concatenateTable(self, hiveDB=None, hiveTable=None):
		""" Execute a concatenate table on the specified table, or what is already set in the class """
		logging.debug("Executing import_operations.concatenateTable()")

		self.connectToHive(forceSkipTest=True)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		hdfsBlocksize = int(self.import_config.common_config.getConfigValue(key = "hdfs_blocksize"))
		query = "set mapreduce.input.fileinputformat.split.minsize=%s"%(hdfsBlocksize)
		self.common_operations.executeHiveQuery(query)

		query = "alter table `%s`.`%s` concatenate"%(hiveDB, hiveTable)
		self.common_operations.executeHiveQuery(query)

		logging.debug("Executing import_operations.concatenateTable() - Finished")

