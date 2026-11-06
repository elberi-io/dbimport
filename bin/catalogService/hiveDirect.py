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
import sys

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import aliased, sessionmaker

from common.Exceptions import databaseNotFound, rowNotFound
from common.Singleton import Singleton
from ConfigReader import configuration
from DBImportConfig import common_config
from DBImportOperation import hiveSchema


class hiveMetastoreDirect(metaclass=Singleton):
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
        dbs = hiveSchema.DBS

        try:
            row = session.query(dbs.NAME).filter(dbs.NAME == hiveDB).one()
        except sa.orm.exc.NoResultFound:
            raise databaseNotFound("Can't find database '%s' in Hive" % (hiveDB))

        logging.debug("Executing hiveMetastoreDirect.checkDB() - Finished")

    def checkTable(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.checkTable()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        result = True
        try:
            row = (
                session.query(tbls.TBL_NAME, tbls.TBL_TYPE, dbs.NAME)
                .select_from(tbls)
                .join(dbs, tbls.DBS)
                .filter(tbls.TBL_NAME == hiveTable.lower())
                .filter(dbs.NAME == hiveDB.lower())
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
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")
        sds = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(sds.LOCATION)
                .select_from(tbls)
                .join(sds)
                .join(dbs)
                .filter(tbls.TBL_NAME == hiveTable.lower())
                .filter(dbs.NAME == hiveDB.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise rowNotFound(
                f"Cant get SDS.LOCATION from Hive Meta Database for {hiveDB}.{hiveTable}"
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
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")
        sds = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(sds.INPUT_FORMAT)
                .select_from(tbls)
                .join(sds)
                .join(dbs)
                .filter(tbls.TBL_NAME == hiveTable.lower())
                .filter(dbs.NAME == hiveDB.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise rowNotFound(
                f"Cant get SDS.INPUT_FORMAT from Hive Meta Database for {hiveDB}.{hiveTable}"
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
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")
        sds = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(sds.INPUT_FORMAT)
                .select_from(tbls)
                .join(sds)
                .join(dbs)
                .filter(tbls.TBL_NAME == hiveTable.lower())
                .filter(dbs.NAME == hiveDB.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise rowNotFound(
                f"Cant get SDS.INPUT_FORMAT from Hive Meta Database for {hiveDB}.{hiveTable}"
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
        tbls = aliased(hiveSchema.TBLS, name="T")
        table_params = aliased(hiveSchema.TABLE_PARAMS, name="TP")
        dbs = aliased(hiveSchema.DBS, name="D")
        sds = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(table_params.PARAM_VALUE)
                .select_from(tbls)
                .join(dbs)
                .join(table_params)
                .filter(tbls.TBL_NAME == hiveTable.lower())
                .filter(dbs.NAME == hiveDB.lower())
                .filter(table_params.PARAM_KEY == "table_type")
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
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        try:
            row = (
                session.query(tbls.TBL_TYPE)
                .select_from(tbls)
                .join(dbs)
                .filter(tbls.TBL_NAME == hiveTable.lower())
                .filter(dbs.NAME == hiveDB.lower())
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

        pk_error_found = False

        session = self.hiveMetaSession()
        key_constraints = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
        tbls = aliased(hiveSchema.TBLS, name="T")
        columns_v2 = aliased(hiveSchema.COLUMNS_V2, name="C")
        dbs = aliased(hiveSchema.DBS, name="D")

        for row in (
            session.query(columns_v2.COLUMN_NAME)
            .select_from(key_constraints)
            .join(tbls, key_constraints.TBLS_PARENT)
            .join(
                columns_v2,
                (columns_v2.CD_ID == key_constraints.PARENT_CD_ID)
                & (columns_v2.INTEGER_IDX == key_constraints.PARENT_INTEGER_IDX),
            )
            .join(dbs)
            .filter(key_constraints.CONSTRAINT_TYPE == 0)
            .filter(tbls.TBL_NAME == hiveTable.lower())
            .filter(dbs.NAME == hiveDB.lower())
            .order_by(key_constraints.POSITION)
            .all()
        ):
            if result != "":
                result += ","
            if row[0] == None:
                pk_error_found = True
                break
            if quotedColumns == False:
                result += row[0]
            else:
                result += "`%s`" % (row[0])

        if pk_error_found == True:
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
        key = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        try:
            row = (
                session.query(key.CONSTRAINT_NAME)
                .select_from(key)
                .join(tbls, key.TBLS_PARENT)
                .join(dbs)
                .filter(key.CONSTRAINT_TYPE == 0)
                .filter(tbls.TBL_NAME == hiveTable.lower())
                .filter(dbs.NAME == hiveDB.lower())
                .group_by(key.CONSTRAINT_NAME)
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
        key = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
        tbls_tp = aliased(hiveSchema.TBLS, name="TP")
        tbls_tc = aliased(hiveSchema.TBLS, name="TC")
        columns_cp = aliased(hiveSchema.COLUMNS_V2, name="CP")
        columns_cc = aliased(hiveSchema.COLUMNS_V2, name="CC")
        dbs_dp = aliased(hiveSchema.DBS, name="DP")
        dbs_dc = aliased(hiveSchema.DBS, name="DC")

        result_df = pd.DataFrame(
            session.query(
                key.POSITION.label("fk_index"),
                dbs_dc.NAME.label("source_hive_db"),
                tbls_tc.TBL_NAME.label("source_hive_table"),
                dbs_dp.NAME.label("ref_hive_db"),
                tbls_tp.TBL_NAME.label("ref_hive_table"),
                columns_cc.COLUMN_NAME.label("source_column_name"),
                columns_cp.COLUMN_NAME.label("ref_column_name"),
                key.CONSTRAINT_NAME.label("fk_name"),
            )
            .select_from(key)
            .join(tbls_tc, key.TBLS_CHILD, isouter=True)
            .join(tbls_tp, key.TBLS_PARENT, isouter=True)
            .join(
                columns_cp,
                (columns_cp.CD_ID == key.PARENT_CD_ID)
                & (columns_cp.INTEGER_IDX == key.PARENT_INTEGER_IDX),
                isouter=True,
            )
            .join(
                columns_cc,
                (columns_cc.CD_ID == key.CHILD_CD_ID)
                & (columns_cc.INTEGER_IDX == key.CHILD_INTEGER_IDX),
                isouter=True,
            )
            .join(dbs_dc, isouter=True)
            .join(dbs_dp, isouter=True)
            .filter(key.CONSTRAINT_TYPE == 1)
            .filter(dbs_dc.NAME == hiveDB)
            .filter(tbls_tc.TBL_NAME == hiveTable)
            .order_by(key.POSITION)
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

        lock_count = (
            session.query(hiveSchema.HIVE_LOCKS)
            .filter(hiveSchema.HIVE_LOCKS.HL_DB == hiveDB)
            .filter(hiveSchema.HIVE_LOCKS.HL_TABLE == hiveTable)
            .count()
        )

        if lock_count > 0:
            logging.warning(
                "Removing %s locks from table %s.%s by force"
                % (lock_count, hiveDB, hiveTable)
            )
            self.common_config.logImportFailure(
                hiveDB=hiveDB,
                hiveTable=hiveTable,
                severity="Warning",
                errorText="%s locks removed by force" % (lock_count),
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
        tbls = aliased(hiveSchema.TBLS, name="T")
        columns = aliased(hiveSchema.COLUMNS_V2, name="C")
        dbs = aliased(hiveSchema.DBS, name="D")
        sds = aliased(hiveSchema.SDS, name="S")
        cds = aliased(hiveSchema.CDS)

        result_df = pd.DataFrame(
            session.query(
                columns.COLUMN_NAME.label("name"),
                columns.TYPE_NAME.label("type"),
                columns.COMMENT.label("comment"),
                columns.INTEGER_IDX.label("idx"),
            )
            .select_from(cds)
            .join(sds)
            .join(columns)
            .join(tbls)
            .join(dbs)
            .filter(tbls.TBL_NAME == hiveTable)
            .filter(dbs.NAME == hiveDB)
            .order_by(columns.INTEGER_IDX)
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
        table_params = aliased(hiveSchema.TABLE_PARAMS, name="TP")
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        row = (
            session.query(table_params.PARAM_VALUE.label("param_value"))
            .select_from(table_params)
            .join(tbls)
            .join(dbs)
            .filter(tbls.TBL_NAME == hiveTable)
            .filter(dbs.NAME == hiveDB)
            .filter(table_params.PARAM_KEY == "transactional")
            .one_or_none()
        )

        return_value = False
        if row != None:
            if row[0].lower() == "true":
                return_value = True

        logging.debug("isTableTransactional = %s" % (return_value))
        logging.debug("Executing hiveMetastoreDirect.isTableTransactional() - Finished")
        return return_value

    def isTableView(self, hiveDB, hiveTable):
        logging.debug("Executing hiveMetastoreDirect.isTableView()")

        if self.hiveMetaSession == None:
            self.connectToHiveMetastoreDB()

        session = self.hiveMetaSession()
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        row = (
            session.query(tbls.TBL_TYPE)
            .select_from(tbls)
            .join(dbs)
            .filter(tbls.TBL_NAME == hiveTable)
            .filter(dbs.NAME == hiveDB)
            .one_or_none()
        )

        return_value = False

        if row[0] == "VIRTUAL_VIEW":
            return_value = True

        logging.debug("isTableView = %s" % (return_value))
        logging.debug("Executing hiveMetastoreDirect.isTableView() - Finished")
        return return_value

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
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        result_df = pd.DataFrame(
            session.query(dbs.NAME.label("hiveDB"), tbls.TBL_NAME.label("hiveTable"))
            .select_from(tbls)
            .join(dbs)
            .filter(tbls.TBL_NAME.like(tableFilter))
            .filter(dbs.NAME.like(dbFilter))
            .order_by(dbs.NAME, tbls.TBL_NAME)
            .all()
        )

        if len(result_df) == 0:
            return pd.DataFrame(columns=["hiveDB", "hiveTable"])

        logging.debug("Executing hiveMetastoreDirect.getTables() - Finished")
        return result_df
