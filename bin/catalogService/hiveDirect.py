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
import time
from common.Singleton import Singleton
from common.Exceptions import *
import common.Exceptions
from ConfigReader import configuration
from datetime import date, datetime, timedelta
import pandas as pd
import sqlalchemy as sa
from DBImportConfig import common_config
from DBImportOperation import hiveSchema
from sqlalchemy.sql import alias, select
from sqlalchemy.orm import aliased, sessionmaker
from sqlalchemy.pool import QueuePool


class hiveMetastoreDirect(object, metaclass=Singleton):
    def __init__(self):
        logging.debug("Executing hiveMetastoreDirect.__init__()")

        self.debugLogLevel = False
        # self.debugLogLevel = True

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.common_config = common_config.config()

        self.hiveConnectStr = configuration.get(
            "Metastore", "hive_metastore_alchemy_conn"
        )
        self.hiveMetaDB = None
        self.hiveMetaSession = None

    def connectToHiveMetastoreDB(self):

        logging.info("Connecting to Hive Metastore with Direct DB connection")

        try:
            self.hiveMetaDB = sa.create_engine(
                self.hiveConnectStr, echo=self.debugLogLevel, pool_pre_ping=True
            )
            self.hiveMetaDB.connect()
            self.hiveMetaSession = sessionmaker(bind=self.hiveMetaDB)
        except sa.exc.OperationalError as err:
            logging.error("%s" % err)
            self.common_config.remove_temporary_files()
            sys.exit(1)
        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.common_config.remove_temporary_files()
            sys.exit(1)

        logging.debug("Executing hiveMetastoreDirect.__init__() - Finished")

    def checkDB(self, hiveDB):
        logging.debug("Executing hiveMetastoreDirect.checkDB()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        DBS = hiveSchema.DBS

        try:
            row = session.query(DBS.NAME).filter(DBS.NAME == hiveDB).one()
        except sa.orm.exc.NoResultFound:
            raise databaseNotFound("Can't find database '%s' in Hive" % (hiveDB))

        logging.debug("Executing hiveMetastoreDirect.checkDB() - Finished")

    def checkTable(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.checkTable()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        TBLS = aliased(hiveSchema.TBLS, name="T")
        DBS = aliased(hiveSchema.DBS, name="D")

        result = True
        try:
            row = (
                session.query(TBLS.TBL_NAME, TBLS.TBL_TYPE, DBS.NAME)
                .select_from(TBLS)
                .join(DBS, TBLS.DBS)
                .filter(TBLS.TBL_NAME == hiveTable.lower())
                .filter(DBS.NAME == hiveDB.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            result = False

        logging.debug("Executing hiveMetastoreDirect.checkTable() - Finished")
        return result

    def getTableLocation(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.getExternalTableLocation()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        TBLS = aliased(hiveSchema.TBLS, name="T")
        DBS = aliased(hiveSchema.DBS, name="D")
        SDS = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(SDS.LOCATION)
                .select_from(TBLS)
                .join(SDS)
                .join(DBS)
                .filter(TBLS.TBL_NAME == hiveTable.lower())
                .filter(DBS.NAME == hiveDB.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise rowNotFound(
                "Cant get SDS.LOCATION from Hive Meta Database" % (hiveDB)
            )

        logging.debug(
            "Executing hiveMetastoreDirect.getExternalTableLocation() - Finished"
        )
        return row[0]

    def isTableExternalOrcFormat(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.isTableExternalOrcFormat()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        if self.isTableExternal(hiveDB, hiveTable) == False:
            return False

        session = self.hiveMetaSession()
        TBLS = aliased(hiveSchema.TBLS, name="T")
        DBS = aliased(hiveSchema.DBS, name="D")
        SDS = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(SDS.INPUT_FORMAT)
                .select_from(TBLS)
                .join(SDS)
                .join(DBS)
                .filter(TBLS.TBL_NAME == hiveTable.lower())
                .filter(DBS.NAME == hiveDB.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise rowNotFound(
                "Cant get SDS.INPUT_FORMAT from Hive Meta Database" % (hiveDB)
            )

        logging.debug(
            "Executing hiveMetastoreDirect.isTableExternalOrcFormat() - Finished"
        )
        if row[0] == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat":
            return True
        else:
            return False

    def isTableExternalParquetFormat(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.isTableExternalParquetFormat()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        if self.isTableExternal(hiveDB, hiveTable) == False:
            return False

        session = self.hiveMetaSession()
        TBLS = aliased(hiveSchema.TBLS, name="T")
        DBS = aliased(hiveSchema.DBS, name="D")
        SDS = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(SDS.INPUT_FORMAT)
                .select_from(TBLS)
                .join(SDS)
                .join(DBS)
                .filter(TBLS.TBL_NAME == hiveTable.lower())
                .filter(DBS.NAME == hiveDB.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise rowNotFound(
                "Cant get SDS.INPUT_FORMAT from Hive Meta Database" % (hiveDB)
            )

        logging.debug(
            "Executing hiveMetastoreDirect.isTableExternalParquetFormat() - Finished"
        )
        if row[0] == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat":
            return True
        else:
            return False

    def isTableExternalIcebergFormat(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.isTableExternalIcebergFormat()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        if self.isTableExternal(hiveDB, hiveTable) == False:
            return False

        session = self.hiveMetaSession()
        TBLS = aliased(hiveSchema.TBLS, name="T")
        TABLE_PARAMS = aliased(hiveSchema.TABLE_PARAMS, name="TP")
        DBS = aliased(hiveSchema.DBS, name="D")
        SDS = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(TABLE_PARAMS.PARAM_VALUE)
                .select_from(TBLS)
                .join(DBS)
                .join(TABLE_PARAMS)
                .filter(TBLS.TBL_NAME == hiveTable.lower())
                .filter(DBS.NAME == hiveDB.lower())
                .filter(TABLE_PARAMS.PARAM_KEY == "table_type")
                .one()
            )
        except sa.orm.exc.NoResultFound:
            return False

        logging.debug(
            "Executing hiveMetastoreDirect.isTableExternalIcebergFormat() - Finished"
        )
        if row[0] == "ICEBERG":
            return True
        else:
            return False

    def isTableExternal(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.isTableExternal()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        TBLS = aliased(hiveSchema.TBLS, name="T")
        DBS = aliased(hiveSchema.DBS, name="D")

        try:
            row = (
                session.query(TBLS.TBL_TYPE)
                .select_from(TBLS)
                .join(DBS)
                .filter(TBLS.TBL_NAME == hiveTable.lower())
                .filter(DBS.NAME == hiveDB.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise rowNotFound("Cant get TBLS.TBL_TYPE from Hive Meta Database")

        if row[0] == "EXTERNAL_TABLE":
            return True
        else:
            return False

    def getPK(self, hiveDB, hiveTable, quotedColumns=False):
        """Reads the PK from the Hive Metadatabase and return a comma separated string with the information"""
        logging.debug("Executing hiveMetastoreDirect.getPK()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        result = ""

        PKerrorFound = False

        session = self.hiveMetaSession()
        KEY_CONSTRAINTS = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
        TBLS = aliased(hiveSchema.TBLS, name="T")
        COLUMNS_V2 = aliased(hiveSchema.COLUMNS_V2, name="C")
        DBS = aliased(hiveSchema.DBS, name="D")

        for row in (
            session.query(COLUMNS_V2.COLUMN_NAME)
            .select_from(KEY_CONSTRAINTS)
            .join(TBLS, KEY_CONSTRAINTS.TBLS_PARENT)
            .join(
                COLUMNS_V2,
                (COLUMNS_V2.CD_ID == KEY_CONSTRAINTS.PARENT_CD_ID)
                & (COLUMNS_V2.INTEGER_IDX == KEY_CONSTRAINTS.PARENT_INTEGER_IDX),
            )
            .join(DBS)
            .filter(KEY_CONSTRAINTS.CONSTRAINT_TYPE == 0)
            .filter(TBLS.TBL_NAME == hiveTable.lower())
            .filter(DBS.NAME == hiveDB.lower())
            .order_by(KEY_CONSTRAINTS.POSITION)
            .all()
        ):
            if result != "":
                result += ","
            if row[0] == None:
                PKerrorFound = True
                break
            if quotedColumns == False:
                result += row[0]
            else:
                result += "`%s`" % (row[0])

        if PKerrorFound == True:
            result = ""

        logging.debug("Primary Key columns: %s" % (result))
        logging.debug("Executing hiveMetastoreDirect.getPK() - Finished")
        return result

    def getPKname(self, hiveDB, hiveTable):
        """Returns the name of the Primary Key that exists on the table. Returns None if it doesnt exist"""
        logging.debug("Executing hiveMetastoreDirect.getPKname()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        KEY = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
        TBLS = aliased(hiveSchema.TBLS, name="T")
        DBS = aliased(hiveSchema.DBS, name="D")

        try:
            row = (
                session.query(KEY.CONSTRAINT_NAME)
                .select_from(KEY)
                .join(TBLS, KEY.TBLS_PARENT)
                .join(DBS)
                .filter(KEY.CONSTRAINT_TYPE == 0)
                .filter(TBLS.TBL_NAME == hiveTable.lower())
                .filter(DBS.NAME == hiveDB.lower())
                .group_by(KEY.CONSTRAINT_NAME)
                .one()
            )
        except sa.orm.exc.NoResultFound:
            logging.debug("PK Name: <None>")
            logging.debug("Executing hiveMetastoreDirect.getPKname() - Finished")
            return None

        logging.debug("PK Name: %s" % (row[0]))
        logging.debug("Executing hiveMetastoreDirect.getPKname() - Finished")
        return row[0]

    def getFKs(self, hiveDB, hiveTable):
        """Reads the ForeignKeys from the Hive Metastore tables and return the result in a Pandas DF"""
        logging.debug("Executing hiveMetastoreDirect.getFKs()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        result_df = None

        session = self.hiveMetaSession()
        KEY = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
        TBLS_TP = aliased(hiveSchema.TBLS, name="TP")
        TBLS_TC = aliased(hiveSchema.TBLS, name="TC")
        COLUMNS_CP = aliased(hiveSchema.COLUMNS_V2, name="CP")
        COLUMNS_CC = aliased(hiveSchema.COLUMNS_V2, name="CC")
        DBS_DP = aliased(hiveSchema.DBS, name="DP")
        DBS_DC = aliased(hiveSchema.DBS, name="DC")

        result_df = pd.DataFrame(
            session.query(
                KEY.POSITION.label("fk_index"),
                DBS_DC.NAME.label("source_hive_db"),
                TBLS_TC.TBL_NAME.label("source_hive_table"),
                DBS_DP.NAME.label("ref_hive_db"),
                TBLS_TP.TBL_NAME.label("ref_hive_table"),
                COLUMNS_CC.COLUMN_NAME.label("source_column_name"),
                COLUMNS_CP.COLUMN_NAME.label("ref_column_name"),
                KEY.CONSTRAINT_NAME.label("fk_name"),
            )
            .select_from(KEY)
            .join(TBLS_TC, KEY.TBLS_CHILD, isouter=True)
            .join(TBLS_TP, KEY.TBLS_PARENT, isouter=True)
            .join(
                COLUMNS_CP,
                (COLUMNS_CP.CD_ID == KEY.PARENT_CD_ID)
                & (COLUMNS_CP.INTEGER_IDX == KEY.PARENT_INTEGER_IDX),
                isouter=True,
            )
            .join(
                COLUMNS_CC,
                (COLUMNS_CC.CD_ID == KEY.CHILD_CD_ID)
                & (COLUMNS_CC.INTEGER_IDX == KEY.CHILD_INTEGER_IDX),
                isouter=True,
            )
            .join(DBS_DC, isouter=True)
            .join(DBS_DP, isouter=True)
            .filter(KEY.CONSTRAINT_TYPE == 1)
            .filter(DBS_DC.NAME == hiveDB)
            .filter(TBLS_TC.TBL_NAME == hiveTable)
            .order_by(KEY.POSITION)
            .all()
        ).fillna("")

        if len(result_df) == 0:
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

        result_df = (
            result_df.groupby(
                by=[
                    "fk_name",
                    "source_hive_db",
                    "source_hive_table",
                    "ref_hive_db",
                    "ref_hive_table",
                ]
            )
            .aggregate(
                {
                    "source_column_name": lambda a: ",".join(a),
                    "ref_column_name": lambda a: ",".join(a),
                }
            )
            .reset_index()
        )

        logging.debug("Executing hiveMetastoreDirect.getFKs() - Finished")
        return result_df.sort_values(by=["fk_name"], ascending=True)

    def removeLocksByForce(self, hiveDB, hiveTable):
        """Removes the locks on the Hive table from the Hive Metadatabase"""
        logging.debug("Executing hiveMetastoreDirect.removeLocksByForce()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()

        lockCount = (
            session.query(hiveSchema.HIVE_LOCKS)
            .filter(hiveSchema.HIVE_LOCKS.HL_DB == hiveDB)
            .filter(hiveSchema.HIVE_LOCKS.HL_TABLE == hiveTable)
            .count()
        )

        if lockCount > 0:
            logging.warning(
                "Removing %s locks from table %s.%s by force"
                % (lockCount, hiveDB, hiveTable)
            )
            self.common_config.logImportFailure(
                hiveDB=hiveDB,
                hiveTable=hiveTable,
                severity="Warning",
                errorText="%s locks removed by force" % (lockCount),
            )

        (
            session.query(hiveSchema.HIVE_LOCKS)
            .filter(hiveSchema.HIVE_LOCKS.HL_DB == hiveDB)
            .filter(hiveSchema.HIVE_LOCKS.HL_TABLE == hiveTable)
            .delete()
        )
        session.commit()

        logging.debug("Executing hiveMetastoreDirect.removeLocksByForce() - Finished")

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
        """Returns a pandas DataFrame with all columns in the specified Hive table"""
        logging.debug("Executing hiveMetastoreDirect.getColumns()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        TBLS = aliased(hiveSchema.TBLS, name="T")
        COLUMNS = aliased(hiveSchema.COLUMNS_V2, name="C")
        DBS = aliased(hiveSchema.DBS, name="D")
        SDS = aliased(hiveSchema.SDS, name="S")
        CDS = aliased(hiveSchema.CDS)

        result_df = pd.DataFrame(
            session.query(
                COLUMNS.COLUMN_NAME.label("name"),
                COLUMNS.TYPE_NAME.label("type"),
                COLUMNS.COMMENT.label("comment"),
                COLUMNS.INTEGER_IDX.label("idx"),
            )
            .select_from(CDS)
            .join(SDS)
            .join(COLUMNS)
            .join(TBLS)
            .join(DBS)
            .filter(TBLS.TBL_NAME == hiveTable)
            .filter(DBS.NAME == hiveDB)
            .order_by(COLUMNS.INTEGER_IDX)
            .all()
        )

        if includeType == False:
            result_df.drop("type", axis=1, inplace=True)
        else:
            result_df.loc[
                result_df["type"] == "timestamp with local time zone", "type"
            ] = "timestamp"

        if includeComment == False:
            result_df.drop("comment", axis=1, inplace=True)

        if includeIdx == False:
            result_df.drop("idx", axis=1, inplace=True)

        if forceColumnUppercase == True:
            result_df["name"] = result_df["name"].str.upper()

        # 		if excludeDataLakeColumns == True:
        # 			result_df = result_df[~result_df['name'].astype(str).str.startswith('datalake_')]

        # Index needs to be reset if anything was droped in the DF.
        # The merge will otherwise indicate that there is a difference and table will be recreated
        result_df.reset_index(drop=True, inplace=True)

        logging.debug("Executing hiveMetastoreDirect.getColumns() - Finished")
        return result_df

    #
    def isTableTransactional(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.isTableTransactional()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        TABLE_PARAMS = aliased(hiveSchema.TABLE_PARAMS, name="TP")
        TBLS = aliased(hiveSchema.TBLS, name="T")
        DBS = aliased(hiveSchema.DBS, name="D")

        row = (
            session.query(TABLE_PARAMS.PARAM_VALUE.label("param_value"))
            .select_from(TABLE_PARAMS)
            .join(TBLS)
            .join(DBS)
            .filter(TBLS.TBL_NAME == hiveTable)
            .filter(DBS.NAME == hiveDB)
            .filter(TABLE_PARAMS.PARAM_KEY == "transactional")
            .one_or_none()
        )

        returnValue = False
        if row != None:
            if row[0].lower() == "true":
                returnValue = True

        logging.debug("isTableTransactional = %s" % (returnValue))
        logging.debug("Executing hiveMetastoreDirect.isTableTransactional() - Finished")
        return returnValue

    def isTableView(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.isTableView()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        TBLS = aliased(hiveSchema.TBLS, name="T")
        DBS = aliased(hiveSchema.DBS, name="D")

        row = (
            session.query(TBLS.TBL_TYPE)
            .select_from(TBLS)
            .join(DBS)
            .filter(TBLS.TBL_NAME == hiveTable)
            .filter(DBS.NAME == hiveDB)
            .one_or_none()
        )

        returnValue = False

        if row[0] == "VIRTUAL_VIEW":
            returnValue = True

        logging.debug("isTableView = %s" % (returnValue))
        logging.debug("Executing hiveMetastoreDirect.isTableView() - Finished")
        return returnValue

    def getTables(self, dbFilter=None, tableFilter=None):
        """Return all tables from specific Hive DB"""
        logging.debug("Executing hiveMetastoreDirect.getTables()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        result_df = None

        if dbFilter != None:
            dbFilter = dbFilter.replace("*", "%")
        else:
            dbFilter = "%"

        if tableFilter != None:
            tableFilter = tableFilter.replace("*", "%")
        else:
            tableFilter = "%"

        session = self.hiveMetaSession()
        TBLS = aliased(hiveSchema.TBLS, name="T")
        DBS = aliased(hiveSchema.DBS, name="D")

        result_df = pd.DataFrame(
            session.query(DBS.NAME.label("hiveDB"), TBLS.TBL_NAME.label("hiveTable"))
            .select_from(TBLS)
            .join(DBS)
            .filter(TBLS.TBL_NAME.like(tableFilter))
            .filter(DBS.NAME.like(dbFilter))
            .order_by(DBS.NAME, TBLS.TBL_NAME)
            .all()
        )

        if len(result_df) == 0:
            return pd.DataFrame(columns=["hiveDB", "hiveTable"])

        logging.debug("Executing hiveMetastoreDirect.getTables() - Finished")
        return result_df
