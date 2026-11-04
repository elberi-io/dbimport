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
import json
from common.Singleton import Singleton
from common.Exceptions import catalogError, databaseNotFound, undevelopedFeature
import pandas as pd

# import sqlalchemy as sa
from DBImportConfig import common_config
# from sqlalchemy.sql import alias, select
# from sqlalchemy.orm import aliased, sessionmaker
# from sqlalchemy.pool import QueuePool

import botocore
import boto3


class glueCatalog(object, metaclass=Singleton):
    def __init__(self):
        logging.debug("Executing glueCatalog.__init__()")

        self.debugLogLevel = False
        # self.debugLogLevel = True

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.common_config = common_config.config()

        self.glueClient = boto3.client("glue", region_name=self.common_config.awsRegion)

        # self.checkDB("amwsktst_history_uat")
        # self.checkTable("amwsktst_history_uat", "braks01")
        # self.getTableLocation("amwsktst_history_uat", "braks01")
        # self.isTableExternalIcebergFormat("amwsktst_history_uat", "braks01")
        # self.isTableExternal("amwsktst_history_uat", "braks01")
        # self.getColumns("amwsktst_history_uat", "braks01")
        # self.getColumns("etl_import_staging", "dbimport__import_tables_full_truncate_insert__staging")
        # self.getTables()

        logging.debug("Executing glueCatalog.__init__() - Finished")

    def checkDB(self, hiveDB):
        logging.debug("Executing glueCatalog.checkDB()")

        try:
            response = self.glueClient.get_database(Name=hiveDB)
        # 			print(response)
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise databaseNotFound(
                "Can't find database '%s' in Glue Catalog" % (hiveDB)
            )
            # raise catalogError("Can't find database '%s' in Glue Catalog"%(hiveDB))

        logging.debug("Executing glueCatalog.checkDB() - Finished")

    def checkTable(self, hiveDB, hiveTable):
        logging.debug("Executing glueCatalog.checkTable()")

        result = False
        try:
            response = self.glueClient.get_table(DatabaseName=hiveDB, Name=hiveTable)
            # 			print(response)
            result = True
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] != "EntityNotFoundException":
                logging.error(error)

        logging.debug("Executing glueCatalog.checkTable() - Finished")
        return result

    def getTableLocation(self, hiveDB, hiveTable):
        logging.debug("Executing glueCatalog.getTableLocation()")

        try:
            response = self.glueClient.get_table(DatabaseName=hiveDB, Name=hiveTable)
            location = response["Table"]["StorageDescriptor"]["Location"]
        # 			print(location)
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise catalogError("Error when reading table location from Glue Catalog")

        logging.debug("Executing glueCatalog.getTableLocation() - Finished")
        return location

    def isTableExternalOrcFormat(self, hiveDB, hiveTable):
        logging.debug("Executing glueCatalog.isTableExternalOrcFormat()")
        logging.warning(
            "glueCatalog.isTableExternalOrcFormat() is undeveloped. Please contact developer if this is needed"
        )
        logging.debug("Executing glueCatalog.isTableExternalOrcFormat() - Finished")

    def isTableExternalParquetFormat(self, hiveDB, hiveTable):
        logging.debug("Executing glueCatalog.isTableExternalParquetFormat()")
        logging.warning(
            "glueCatalog.isTableExternalParquetFormat() is undeveloped. Please contact developer if this is needed"
        )
        logging.debug("Executing glueCatalog.isTableExternalParquetFormat() - Finished")

    def isTableExternalIcebergFormat(self, hiveDB, hiveTable):
        logging.debug("Executing glueCatalog.isTableExternalIcebergFormat()")

        returnValue = False
        try:
            response = self.glueClient.get_table(DatabaseName=hiveDB, Name=hiveTable)
            tableType = response["Table"]["TableType"]
            parameterTableType = response["Table"]["Parameters"]["table_type"]

            if tableType == "EXTERNAL_TABLE" and parameterTableType == "ICEBERG":
                returnValue = True

        # 			print(response)
        # 			print("========================================")
        # 			print(parameterTableType)
        # 			print("========================================")
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise catalogError("Error when checking table type in Glue Catalog")

        logging.debug("Executing glueCatalog.isTableExternalIcebergFormat() - Finished")
        return returnValue

    def isTableExternal(self, hiveDB, hiveTable):
        logging.debug("Executing glueCatalog.isTableExternal()")

        returnValue = False
        try:
            response = self.glueClient.get_table(DatabaseName=hiveDB, Name=hiveTable)
            tableType = response["Table"]["TableType"]

            if tableType == "EXTERNAL_TABLE":
                returnValue = True

        # 			print(response)
        # 			print("========================================")
        # 			print(tableType)
        # 			print("========================================")
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise catalogError("Error when checking table type in Glue Catalog")

        logging.debug("Executing glueCatalog.isTableExternal() - Finished")
        return returnValue

    def getPK(self, hiveDB, hiveTable, quotedColumns=False):
        """Reads the PK from the Hive Metadatabase and return a comma separated string with the information"""
        logging.debug("Executing glueCatalog.getPK()")

        logging.warning(
            "No support for Primary Key with Glue Catalog in current version of DBImport"
        )
        # 		if self.hiveMetaSession == None:
        # 			self.connectToHiveMetastoreDB()
        #
        result = ""
        #
        # 		PKerrorFound = False
        #
        # 		session = self.hiveMetaSession()
        # 		KEY_CONSTRAINTS = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
        # 		TBLS = aliased(hiveSchema.TBLS, name="T")
        # 		COLUMNS_V2 = aliased(hiveSchema.COLUMNS_V2, name="C")
        # 		DBS = aliased(hiveSchema.DBS, name="D")
        #
        # 		for row in (session.query(COLUMNS_V2.COLUMN_NAME)
        # 				.select_from(KEY_CONSTRAINTS)
        # 				.join(TBLS, KEY_CONSTRAINTS.TBLS_PARENT)
        # 				.join(COLUMNS_V2, (COLUMNS_V2.CD_ID == KEY_CONSTRAINTS.PARENT_CD_ID) & (COLUMNS_V2.INTEGER_IDX == KEY_CONSTRAINTS.PARENT_INTEGER_IDX))
        # 				.join(DBS)
        # 				.filter(KEY_CONSTRAINTS.CONSTRAINT_TYPE == 0)
        # 				.filter(TBLS.TBL_NAME == hiveTable.lower())
        # 				.filter(DBS.NAME == hiveDB.lower())
        # 				.order_by(KEY_CONSTRAINTS.POSITION)
        # 				.all()):
        #
        # 			if result != "":
        # 				result += ","
        # 			if row[0] == None:
        # 				PKerrorFound = True
        # 				break
        # 			if quotedColumns == False:
        # 				result += row[0]
        # 			else:
        # 				result += "`%s`"%(row[0])
        #
        # 		if PKerrorFound == True: result = ""
        #
        # 		logging.debug("Primary Key columns: %s"%(result))
        logging.debug("Executing glueCatalog.getPK() - Finished")
        return result

    def getPKname(self, hiveDB, hiveTable):
        """Returns the name of the Primary Key that exists on the table. Returns None if it doesnt exist"""
        logging.debug("Executing glueCatalog.getPKname()")

        logging.warning(
            "No support for Primary Key with Glue Catalog in current version of DBImport"
        )
        #
        # 		if self.hiveMetaSession == None:
        # 			self.connectToHiveMetastoreDB()
        #
        # 		session = self.hiveMetaSession()
        # 		KEY = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
        # 		TBLS = aliased(hiveSchema.TBLS, name="T")
        # 		DBS = aliased(hiveSchema.DBS, name="D")
        #
        # 		try:
        # 			row = (session.query(KEY.CONSTRAINT_NAME)
        # 				.select_from(KEY)
        # 				.join(TBLS, KEY.TBLS_PARENT)
        # 				.join(DBS)
        # 				.filter(KEY.CONSTRAINT_TYPE == 0)
        # 				.filter(TBLS.TBL_NAME == hiveTable.lower())
        # 				.filter(DBS.NAME == hiveDB.lower())
        # 				.group_by(KEY.CONSTRAINT_NAME)
        # 				.one())
        # 		except sa.orm.exc.NoResultFound:
        # 			logging.debug("PK Name: <None>")
        # 			logging.debug("Executing glueCatalog.getPKname() - Finished")
        # 			return None
        #
        # 		logging.debug("PK Name: %s"%(row[0]))
        logging.debug("Executing glueCatalog.getPKname() - Finished")
        # 		return row[0]
        return None

    def getFKs(self, hiveDB, hiveTable):
        """Reads the ForeignKeys from the Hive Metastore tables and return the result in a Pandas DF"""
        logging.debug("Executing glueCatalog.getFKs()")

        logging.warning(
            "No support for Foreign Key with Glue Catalog in current version of DBImport"
        )
        # 		if self.hiveMetaSession == None:
        # 			self.connectToHiveMetastoreDB()
        #
        # 		result_df = None
        #
        # 		session = self.hiveMetaSession()
        # 		KEY = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
        # 		TBLS_TP = aliased(hiveSchema.TBLS, name="TP")
        # 		TBLS_TC = aliased(hiveSchema.TBLS, name="TC")
        # 		COLUMNS_CP = aliased(hiveSchema.COLUMNS_V2, name="CP")
        # 		COLUMNS_CC = aliased(hiveSchema.COLUMNS_V2, name="CC")
        # 		DBS_DP = aliased(hiveSchema.DBS, name="DP")
        # 		DBS_DC = aliased(hiveSchema.DBS, name="DC")
        #
        # 		result_df = pd.DataFrame(session.query(
        # 					KEY.POSITION.label("fk_index"),
        # 					DBS_DC.NAME.label("source_hive_db"),
        # 					TBLS_TC.TBL_NAME.label("source_hive_table"),
        # 					DBS_DP.NAME.label("ref_hive_db"),
        # 					TBLS_TP.TBL_NAME.label("ref_hive_table"),
        # 					COLUMNS_CC.COLUMN_NAME.label("source_column_name"),
        # 					COLUMNS_CP.COLUMN_NAME.label("ref_column_name"),
        # 					KEY.CONSTRAINT_NAME.label("fk_name")
        # 				)
        # 				.select_from(KEY)
        # 				.join(TBLS_TC, KEY.TBLS_CHILD, isouter=True)
        # 				.join(TBLS_TP, KEY.TBLS_PARENT, isouter=True)
        # 				.join(COLUMNS_CP, (COLUMNS_CP.CD_ID == KEY.PARENT_CD_ID) & (COLUMNS_CP.INTEGER_IDX == KEY.PARENT_INTEGER_IDX), isouter=True)
        # 				.join(COLUMNS_CC, (COLUMNS_CC.CD_ID == KEY.CHILD_CD_ID) & (COLUMNS_CC.INTEGER_IDX == KEY.CHILD_INTEGER_IDX), isouter=True)
        # 				.join(DBS_DC, isouter=True)
        # 				.join(DBS_DP, isouter=True)
        # 				.filter(KEY.CONSTRAINT_TYPE == 1)
        # 				.filter(DBS_DC.NAME == hiveDB)
        # 				.filter(TBLS_TC.TBL_NAME == hiveTable)
        # 				.order_by(KEY.POSITION)
        # 				.all()).fillna('')
        #
        # 		if len(result_df) == 0:
        # 			return pd.DataFrame(columns=['fk_name', 'source_hive_db', 'source_hive_table', 'ref_hive_db', 'ref_hive_table', 'source_column_name', 'ref_column_name'])
        #
        # 		result_df = (result_df.groupby(by=['fk_name', 'source_hive_db', 'source_hive_table', 'ref_hive_db', 'ref_hive_table'] )
        # 			.aggregate({'source_column_name': lambda a: ",".join(a),
        # 						'ref_column_name': lambda a: ",".join(a)})
        # 			.reset_index())

        logging.debug("Executing glueCatalog.getFKs() - Finished")
        # 		return result_df.sort_values(by=['fk_name'], ascending=True)
        return pd.DataFrame(
            columns=[
                "fk_name",
                "source_hive_db",
                "source_hive_table",
                "ref_hive_db",
                "ref_hive_table",
                "source_column_name",
                "ref_column_name",
            ]
        )

    def removeLocksByForce(self, hiveDB, hiveTable):
        """Removes the locks on the Hive table from the Hive Metadatabase"""
        logging.debug("Executing glueCatalog.removeLocksByForce()")
        # This is not needed with Glue Catalog, but code must be here anyway
        logging.debug("Executing glueCatalog.removeLocksByForce() - Finished")

    def getColumns(
        self,
        hiveDB,
        hiveTable,
        includeType=False,
        includeComment=False,
        includeIdx=False,
        forceColumnUppercase=False,
        excludeDataLakeColumns=False,
    ):
        """Returns a pandas DataFrame with all columns in the specified table"""
        logging.debug("Executing glueCatalog.getColumns()")

        # 		print(hiveDB)
        # 		print(hiveTable)
        # 		print(includeType)
        # 		print(includeComment)
        # 		print(includeIdx)
        # 		print(forceColumnUppercase)
        # 		print(excludeDataLakeColumns)

        try:
            response = self.glueClient.get_table(DatabaseName=hiveDB, Name=hiveTable)
            columns = response["Table"]["StorageDescriptor"]["Columns"]
            # 			print(columns)

            normalizedColumnList = []
            normalizedColumn = {}
            for column in columns:
                normalizedColumn = {}
                normalizedColumn["name"] = column["Name"]
                normalizedColumn["type"] = column["Type"]

                try:
                    normalizedColumn["comment"] = json.loads(column["Comment"])[
                        "name"
                    ].replace("`", "")
                # 					normalizedColumn["is_pk"] = json.loads(column["Comment"])["is_pk"]
                except json.decoder.JSONDecodeError:
                    normalizedColumn["comment"] = column["Comment"]
                except KeyError:
                    normalizedColumn["comment"] = None
                # 					normalizedColumn["is_pk"] = False
                # 				normalizedColumn["Parameters"] = column["Parameters"]

                normalizedColumnList.append(normalizedColumn)

            # 			print(normalizedColumnList)

            result_df = pd.json_normalize(normalizedColumnList)
        # 			print(result_df)

        # 			print(response)
        # 			print("========================================")
        # 			print(columns)
        # 			print("========================================")
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise catalogError("Error when checking table type in Glue Catalog")

        if includeType == False:
            result_df.drop("type", axis=1, inplace=True)
        #
        if includeComment == False:
            result_df.drop("comment", axis=1, inplace=True)
        #
        # 		This is needed for exports
        # 		if includeIdx == False:
        # 			result_df.drop('idx', axis=1, inplace=True)
        #
        if forceColumnUppercase == True:
            result_df["name"] = result_df["name"].str.upper()
        #
        # 		if excludeDataLakeColumns == True:
        # 			result_df = result_df[~result_df['name'].astype(str).str.startswith('datalake_')]
        #
        # 		# Index needs to be reset if anything was droped in the DF.
        # 		# The merge will otherwise indicate that there is a difference and table will be recreated
        result_df.reset_index(drop=True, inplace=True)

        logging.debug("Executing glueCatalog.getColumns() - Finished")
        return result_df

    #
    def isTableTransactional(self, hiveDB, hiveTable):
        logging.debug("Executing glueCatalog.isTableTransactional()")
        # Always True for Glue Catalog
        logging.debug("Executing glueCatalog.isTableTransactional() - Finished")
        return True

    def isTableView(self, hiveDB, hiveTable):
        logging.debug("Executing glueCatalog.isTableView()")

        raise undevelopedFeature(
            "glueCatalog.isTableView() is undeveloped. Please contact the developers of DBImport"
        )

        returnValue = False
        logging.debug("Executing glueCatalog.isTableView() - Finished")
        return returnValue

    def getTables(self, dbFilter=None, tableFilter=None):
        """Return all tables from specific Hive DB"""
        logging.debug("Executing glueCatalog.getTables()")

        raise undevelopedFeature(
            "glueCatalog.getTables() is undeveloped. Please contact the developers of DBImport"
        )

        # 		if self.hiveMetaSession == None:
        # 			self.connectToHiveMetastoreDB()
        #
        # 		result_df = None
        #
        # 		if dbFilter != None:
        # 			dbFilter = dbFilter.replace('*', '%')
        # 		else:
        # 			dbFilter = "%"
        #
        # 		if tableFilter != None:
        # 			tableFilter = tableFilter.replace('*', '%')
        # 		else:
        # 			tableFilter = "%"
        #
        # 		session = self.hiveMetaSession()
        # 		TBLS = aliased(hiveSchema.TBLS, name="T")
        # 		DBS = aliased(hiveSchema.DBS, name="D")
        #
        # 		result_df = pd.DataFrame(session.query(
        # 				DBS.NAME.label('hiveDB'),
        # 				TBLS.TBL_NAME.label('hiveTable')
        # 			)
        # 			.select_from(TBLS)
        # 			.join(DBS)
        # 			.filter(TBLS.TBL_NAME.like(tableFilter))
        # 			.filter(DBS.NAME.like(dbFilter))
        # 			.order_by(DBS.NAME, TBLS.TBL_NAME)
        # 			.all())
        #
        # 		if len(result_df) == 0:
        # 			return pd.DataFrame(columns=['hiveDB', 'hiveTable'])
        #
        logging.debug("Executing glueCatalog.getTables() - Finished")


# 		return result_df
