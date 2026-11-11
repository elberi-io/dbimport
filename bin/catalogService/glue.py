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

import json
import logging

import boto3
import botocore
import pandas as pd

from common.Exceptions import catalogError, databaseNotFound, undevelopedFeature
from common.Singleton import Singleton
from DBImportConfig import common_config


class glueCatalog(metaclass=Singleton):
    def __init__(self):
        logging.debug("Executing glueCatalog.__init__()")

        self.debugLogLevel = False
        # self.debugLogLevel = True

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.common_config = common_config.config()

        self.glue_client = boto3.client(
            "glue", region_name=self.common_config.awsRegion
        )

        # self.check_db("amwsktst_history_uat")
        # self.check_table("amwsktst_history_uat", "braks01")
        # self.get_table_location("amwsktst_history_uat", "braks01")
        # self.is_table_external_iceberg_format("amwsktst_history_uat", "braks01")
        # self.is_table_external("amwsktst_history_uat", "braks01")
        # self.get_columns("amwsktst_history_uat", "braks01")
        # self.get_columns("etl_import_staging", "dbimport__import_tables_full_truncate_insert__staging")
        # self.get_tables()

        logging.debug("Executing glueCatalog.__init__() - Finished")

    def check_db(self, hive_db):
        logging.debug("Executing glueCatalog.check_db()")

        try:
            response = self.glue_client.get_database(Name=hive_db)
        # 			print(response)
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise databaseNotFound(
                "Can't find database '%s' in Glue Catalog" % (hive_db)
            )
            # raise catalogError("Can't find database '%s' in Glue Catalog"%(hiveDB))

        logging.debug("Executing glueCatalog.check_db() - Finished")

    def check_table(self, hive_db, hive_table):
        logging.debug("Executing glueCatalog.check_table()")

        result = False
        try:
            response = self.glue_client.get_table(DatabaseName=hive_db, Name=hive_table)
            # 			print(response)
            result = True
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] != "EntityNotFoundException":
                logging.error(error)

        logging.debug("Executing glueCatalog.check_table() - Finished")
        return result

    def get_table_location(self, hive_db, hive_table):
        logging.debug("Executing glueCatalog.get_table_location()")

        try:
            response = self.glue_client.get_table(DatabaseName=hive_db, Name=hive_table)
            location = response["Table"]["StorageDescriptor"]["Location"]
        # 			print(location)
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise catalogError("Error when reading table location from Glue Catalog")

        logging.debug("Executing glueCatalog.get_table_location() - Finished")
        return location

    def is_table_external_orc_format(self, hive_db, hive_table):
        logging.debug("Executing glueCatalog.is_table_external_orc_format()")
        logging.warning(
            "glueCatalog.is_table_external_orc_format() is undeveloped. Please contact developer if this is needed"
        )
        logging.debug("Executing glueCatalog.is_table_external_orc_format() - Finished")

    def is_table_external_parquet_format(self, hive_db, hive_table):
        logging.debug("Executing glueCatalog.is_table_external_parquet_format()")
        logging.warning(
            "glueCatalog.is_table_external_parquet_format() is undeveloped. Please contact developer if this is needed"
        )
        logging.debug(
            "Executing glueCatalog.is_table_external_parquet_format() - Finished"
        )

    def is_table_external_iceberg_format(self, hive_db, hive_table):
        logging.debug("Executing glueCatalog.is_table_external_iceberg_format()")

        return_value = False
        try:
            response = self.glue_client.get_table(DatabaseName=hive_db, Name=hive_table)
            table_type = response["Table"]["TableType"]
            parameter_table_type = response["Table"]["Parameters"]["table_type"]

            if table_type == "EXTERNAL_TABLE" and parameter_table_type == "ICEBERG":
                return_value = True

        # 			print(response)
        # 			print("========================================")
        # 			print(parameterTableType)
        # 			print("========================================")
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise catalogError("Error when checking table type in Glue Catalog")

        logging.debug(
            "Executing glueCatalog.is_table_external_iceberg_format() - Finished"
        )
        return return_value

    def is_table_external(self, hive_db, hive_table):
        logging.debug("Executing glueCatalog.is_table_external()")

        return_value = False
        try:
            response = self.glue_client.get_table(DatabaseName=hive_db, Name=hive_table)
            table_type = response["Table"]["TableType"]

            if table_type == "EXTERNAL_TABLE":
                return_value = True

        # 			print(response)
        # 			print("========================================")
        # 			print(tableType)
        # 			print("========================================")
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise catalogError("Error when checking table type in Glue Catalog")

        logging.debug("Executing glueCatalog.is_table_external() - Finished")
        return return_value

    def get_pk(self, hive_db, hive_table, quoted_columns=False):
        """Reads the PK from the Hive Metadatabase and return a comma separated string with the information"""
        logging.debug("Executing glueCatalog.get_pk()")

        logging.warning(
            "No support for Primary Key with Glue Catalog in current version of DBImport"
        )
        # 		if self.hive_meta_session == None:
        # 			self.connect_to_hive_metastore_db()
        #
        result = ""
        #
        # 		PKerrorFound = False
        #
        # 		session = self.hive_meta_session()
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
        logging.debug("Executing glueCatalog.get_pk() - Finished")
        return result

    def get_pk_name(self, hive_db, hive_table):
        """Returns the name of the Primary Key that exists on the table. Returns None if it doesnt exist"""
        logging.debug("Executing glueCatalog.get_pk_name()")

        logging.warning(
            "No support for Primary Key with Glue Catalog in current version of DBImport"
        )
        #
        # 		if self.hive_meta_session == None:
        # 			self.connect_to_hive_metastore_db()
        #
        # 		session = self.hive_meta_session()
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
        # 			logging.debug("Executing glueCatalog.get_pk_name() - Finished")
        # 			return None
        #
        # 		logging.debug("PK Name: %s"%(row[0]))
        logging.debug("Executing glueCatalog.get_pk_name() - Finished")
        # 		return row[0]
        return None

    def get_fks(self, hive_db, hive_table):
        """Reads the ForeignKeys from the Hive Metastore tables and return the result in a Pandas DF"""
        logging.debug("Executing glueCatalog.get_fks()")

        logging.warning(
            "No support for Foreign Key with Glue Catalog in current version of DBImport"
        )
        # 		if self.hive_meta_session == None:
        # 			self.connect_to_hive_metastore_db()
        #
        # 		result_df = None
        #
        # 		session = self.hive_meta_session()
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

        logging.debug("Executing glueCatalog.get_fks() - Finished")
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

    def remove_locks_by_force(self, hive_db, hive_table):
        """Removes the locks on the Hive table from the Hive Metadatabase"""
        logging.debug("Executing glueCatalog.remove_locks_by_force()")
        # This is not needed with Glue Catalog, but code must be here anyway
        logging.debug("Executing glueCatalog.remove_locks_by_force() - Finished")

    def get_columns(
        self,
        hive_db,
        hive_table,
        include_type=False,
        include_comment=False,
        include_idx=False,
        force_column_uppercase=False,
        exclude_data_lake_columns=False,
    ):
        """Returns a pandas DataFrame with all columns in the specified table"""
        logging.debug("Executing glueCatalog.get_columns()")

        # 		print(hiveDB)
        # 		print(hiveTable)
        # 		print(includeType)
        # 		print(includeComment)
        # 		print(includeIdx)
        # 		print(forceColumnUppercase)
        # 		print(excludeDataLakeColumns)

        try:
            response = self.glue_client.get_table(DatabaseName=hive_db, Name=hive_table)
            columns = response["Table"]["StorageDescriptor"]["Columns"]
            # 			print(columns)

            normalized_column_list = []
            normalized_column = {}
            for column in columns:
                normalized_column = {}
                normalized_column["name"] = column["Name"]
                normalized_column["type"] = column["Type"]

                try:
                    normalized_column["comment"] = json.loads(column["Comment"])[
                        "name"
                    ].replace("`", "")
                # 					normalizedColumn["is_pk"] = json.loads(column["Comment"])["is_pk"]
                except json.decoder.JSONDecodeError:
                    normalized_column["comment"] = column["Comment"]
                except KeyError:
                    normalized_column["comment"] = None
                # 					normalizedColumn["is_pk"] = False
                # 				normalizedColumn["Parameters"] = column["Parameters"]

                normalized_column_list.append(normalized_column)

            # 			print(normalizedColumnList)

            result_df = pd.json_normalize(normalized_column_list)
        # 			print(result_df)

        # 			print(response)
        # 			print("========================================")
        # 			print(columns)
        # 			print("========================================")
        except botocore.exceptions.ClientError as error:
            logging.error(error)
            raise catalogError("Error when checking table type in Glue Catalog")

        if include_type == False:
            result_df.drop("type", axis=1, inplace=True)
        #
        if include_comment == False:
            result_df.drop("comment", axis=1, inplace=True)
        #
        # 		This is needed for exports
        # 		if includeIdx == False:
        # 			result_df.drop('idx', axis=1, inplace=True)
        #
        if force_column_uppercase == True:
            result_df["name"] = result_df["name"].str.upper()
        #
        # 		if excludeDataLakeColumns == True:
        # 			result_df = result_df[~result_df['name'].astype(str).str.startswith('datalake_')]
        #
        # 		# Index needs to be reset if anything was droped in the DF.
        # 		# The merge will otherwise indicate that there is a difference and table will be recreated
        result_df.reset_index(drop=True, inplace=True)

        logging.debug("Executing glueCatalog.get_columns() - Finished")
        return result_df

    #
    def is_table_transactional(self, hive_db, hive_table):
        logging.debug("Executing glueCatalog.is_table_transactional()")
        # Always True for Glue Catalog
        logging.debug("Executing glueCatalog.is_table_transactional() - Finished")
        return True

    def is_table_view(self, hive_db, hive_table):
        logging.debug("Executing glueCatalog.is_table_view()")

        raise undevelopedFeature(
            "glueCatalog.is_table_view() is undeveloped. Please contact the developers of DBImport"
        )

        return_value = False
        logging.debug("Executing glueCatalog.is_table_view() - Finished")
        return return_value

    def get_tables(self, db_filter=None, table_filter=None):
        """Return all tables from specific Hive DB"""
        logging.debug("Executing glueCatalog.get_tables()")

        raise undevelopedFeature(
            "glueCatalog.get_tables() is undeveloped. Please contact the developers of DBImport"
        )

        # 		if self.hive_meta_session == None:
        # 			self.connect_to_hive_metastore_db()
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
        # 		session = self.hive_meta_session()
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
        logging.debug("Executing glueCatalog.get_tables() - Finished")


# 		return result_df
