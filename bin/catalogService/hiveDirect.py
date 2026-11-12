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

from common.Exceptions import DatabaseNotFoundError, RowNotFoundError
from common.Singleton import Singleton
from ConfigReader import configuration
from DBImportConfig import common_config
from DBImportOperation import hiveSchema


class HiveMetastoreDirect(metaclass=Singleton):
    def __init__(self):
        logging.debug("Executing HiveMetastoreDirect.__init__()")

        self.debug_log_level = False
        # self.debug_log_level = True

        if logging.root.level == 10:  # DEBUG
            self.debug_log_level = True

        self.common_config = common_config.Config()

        self.hive_connect_str = configuration.get(
            "Metastore", "hive_metastore_alchemy_conn"
        )
        self.hive_meta_db = None
        self.hive_meta_session = None

    def connect_to_hive_metastore_db(self):

        logging.info("Connecting to Hive Metastore with Direct DB connection")

        try:
            self.hive_meta_db = sa.create_engine(
                self.hive_connect_str, echo=self.debug_log_level, pool_pre_ping=True
            )
            self.hive_meta_db.connect()
            self.hive_meta_session = sessionmaker(bind=self.hive_meta_db)
        except sa.exc.OperationalError as err:
            logging.error("%s", err)
            self.common_config.remove_temporary_files()
            sys.exit(1)
        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.common_config.remove_temporary_files()
            sys.exit(1)

        logging.debug("Executing HiveMetastoreDirect.__init__() - Finished")

    def check_db(self, hive_db):
        logging.debug("Executing HiveMetastoreDirect.check_db()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        session = self.hive_meta_session()
        dbs = hiveSchema.DBS

        try:
            row = session.query(dbs.NAME).filter(dbs.NAME == hive_db).one()
        except sa.orm.exc.NoResultFound:
            raise DatabaseNotFoundError("Can't find database '%s' in Hive" % (hive_db))

        logging.debug("Executing HiveMetastoreDirect.check_db() - Finished")

    def check_table(self, hive_db, hive_table):
        logging.debug("Executing HiveMetastoreDirect.check_table()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        session = self.hive_meta_session()
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        result = True
        try:
            row = (
                session.query(tbls.TBL_NAME, tbls.TBL_TYPE, dbs.NAME)
                .select_from(tbls)
                .join(dbs, tbls.DBS)
                .filter(tbls.TBL_NAME == hive_table.lower())
                .filter(dbs.NAME == hive_db.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            result = False

        logging.debug("Executing HiveMetastoreDirect.check_table() - Finished")
        return result

    def get_table_location(self, hive_db, hive_table):
        logging.debug("Executing HiveMetastoreDirect.getExternalTableLocation()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        session = self.hive_meta_session()
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")
        sds = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(sds.LOCATION)
                .select_from(tbls)
                .join(sds)
                .join(dbs)
                .filter(tbls.TBL_NAME == hive_table.lower())
                .filter(dbs.NAME == hive_db.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise RowNotFoundError(
                f"Cant get SDS.LOCATION from Hive Meta Database for {hive_db}.{hive_table}"
            )

        logging.debug(
            "Executing HiveMetastoreDirect.getExternalTableLocation() - Finished"
        )
        return row[0]

    def is_table_external_orc_format(self, hive_db, hive_table):
        logging.debug("Executing HiveMetastoreDirect.is_table_external_orc_format()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        if self.is_table_external(hive_db, hive_table) == False:
            return False

        session = self.hive_meta_session()
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")
        sds = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(sds.INPUT_FORMAT)
                .select_from(tbls)
                .join(sds)
                .join(dbs)
                .filter(tbls.TBL_NAME == hive_table.lower())
                .filter(dbs.NAME == hive_db.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise RowNotFoundError(
                f"Cant get SDS.INPUT_FORMAT from Hive Meta Database for {hive_db}.{hive_table}"
            )

        logging.debug(
            "Executing HiveMetastoreDirect.is_table_external_orc_format() - Finished"
        )
        if row[0] == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat":
            return True
        else:
            return False

    def is_table_external_parquet_format(self, hive_db, hive_table):
        logging.debug(
            "Executing HiveMetastoreDirect.is_table_external_parquet_format()"
        )

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        if self.is_table_external(hive_db, hive_table) == False:
            return False

        session = self.hive_meta_session()
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")
        sds = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(sds.INPUT_FORMAT)
                .select_from(tbls)
                .join(sds)
                .join(dbs)
                .filter(tbls.TBL_NAME == hive_table.lower())
                .filter(dbs.NAME == hive_db.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise RowNotFoundError(
                f"Cant get SDS.INPUT_FORMAT from Hive Meta Database for {hive_db}.{hive_table}"
            )

        logging.debug(
            "Executing HiveMetastoreDirect.is_table_external_parquet_format() - Finished"
        )
        if row[0] == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat":
            return True
        else:
            return False

    def is_table_external_iceberg_format(self, hive_db, hive_table):
        logging.debug(
            "Executing HiveMetastoreDirect.is_table_external_iceberg_format()"
        )

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        if self.is_table_external(hive_db, hive_table) == False:
            return False

        session = self.hive_meta_session()
        tbls = aliased(hiveSchema.TBLS, name="T")
        table_params = aliased(hiveSchema.TableParams, name="TP")
        dbs = aliased(hiveSchema.DBS, name="D")
        sds = aliased(hiveSchema.SDS, name="S")

        try:
            row = (
                session.query(table_params.PARAM_VALUE)
                .select_from(tbls)
                .join(dbs)
                .join(table_params)
                .filter(tbls.TBL_NAME == hive_table.lower())
                .filter(dbs.NAME == hive_db.lower())
                .filter(table_params.PARAM_KEY == "table_type")
                .one()
            )
        except sa.orm.exc.NoResultFound:
            return False

        logging.debug(
            "Executing HiveMetastoreDirect.is_table_external_iceberg_format() - Finished"
        )
        if row[0] == "ICEBERG":
            return True
        else:
            return False

    def is_table_external(self, hive_db, hive_table):
        logging.debug("Executing HiveMetastoreDirect.is_table_external()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        session = self.hive_meta_session()
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        try:
            row = (
                session.query(tbls.TBL_TYPE)
                .select_from(tbls)
                .join(dbs)
                .filter(tbls.TBL_NAME == hive_table.lower())
                .filter(dbs.NAME == hive_db.lower())
                .one()
            )
        except sa.orm.exc.NoResultFound:
            raise RowNotFoundError("Cant get TBLS.TBL_TYPE from Hive Meta Database")

        if row[0] == "EXTERNAL_TABLE":
            return True
        else:
            return False

    def get_pk(self, hive_db, hive_table, quoted_columns=False):
        """Reads the PK from the Hive Metadatabase and return a comma separated string with the information"""
        logging.debug("Executing HiveMetastoreDirect.get_pk()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        result = ""

        pk_error_found = False

        session = self.hive_meta_session()
        key_constraints = aliased(hiveSchema.KeyConstraints, name="K")
        tbls = aliased(hiveSchema.TBLS, name="T")
        columns_v2 = aliased(hiveSchema.ColumnsV2, name="C")
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
            .filter(tbls.TBL_NAME == hive_table.lower())
            .filter(dbs.NAME == hive_db.lower())
            .order_by(key_constraints.POSITION)
            .all()
        ):
            if result != "":
                result += ","
            if row[0] == None:
                pk_error_found = True
                break
            if quoted_columns == False:
                result += row[0]
            else:
                result += "`%s`" % (row[0])

        if pk_error_found:
            result = ""

        logging.debug("Primary Key columns: %s", result)
        logging.debug("Executing HiveMetastoreDirect.get_pk() - Finished")
        return result

    def get_pk_name(self, hive_db, hive_table):
        """Returns the name of the Primary Key that exists on the table. Returns None if it doesnt exist"""
        logging.debug("Executing HiveMetastoreDirect.get_pk_name()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        session = self.hive_meta_session()
        key = aliased(hiveSchema.KeyConstraints, name="K")
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        try:
            row = (
                session.query(key.CONSTRAINT_NAME)
                .select_from(key)
                .join(tbls, key.TBLS_PARENT)
                .join(dbs)
                .filter(key.CONSTRAINT_TYPE == 0)
                .filter(tbls.TBL_NAME == hive_table.lower())
                .filter(dbs.NAME == hive_db.lower())
                .group_by(key.CONSTRAINT_NAME)
                .one()
            )
        except sa.orm.exc.NoResultFound:
            logging.debug("PK Name: <None>")
            logging.debug("Executing HiveMetastoreDirect.get_pk_name() - Finished")
            return None

        logging.debug("PK Name: %s" % (row[0]))
        logging.debug("Executing HiveMetastoreDirect.get_pk_name() - Finished")
        return row[0]

    def get_fks(self, hive_db, hive_table):
        """Reads the ForeignKeys from the Hive Metastore tables and return the result in a Pandas DF"""
        logging.debug("Executing HiveMetastoreDirect.get_fks()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        result_df = None

        session = self.hive_meta_session()
        key = aliased(hiveSchema.KeyConstraints, name="K")
        tbls_tp = aliased(hiveSchema.TBLS, name="TP")
        tbls_tc = aliased(hiveSchema.TBLS, name="TC")
        columns_cp = aliased(hiveSchema.ColumnsV2, name="CP")
        columns_cc = aliased(hiveSchema.ColumnsV2, name="CC")
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
            .filter(dbs_dc.NAME == hive_db)
            .filter(tbls_tc.TBL_NAME == hive_table)
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

        logging.debug("Executing HiveMetastoreDirect.get_fks() - Finished")
        return result_df.sort_values(by=["fk_name"], ascending=True)

    def remove_locks_by_force(self, hive_db, hive_table):
        """Removes the locks on the Hive table from the Hive Metadatabase"""
        logging.debug("Executing HiveMetastoreDirect.remove_locks_by_force()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        session = self.hive_meta_session()

        lock_count = (
            session.query(hiveSchema.HiveLocks)
            .filter(hiveSchema.HiveLocks.HL_DB == hive_db)
            .filter(hiveSchema.HiveLocks.HL_TABLE == hive_table)
            .count()
        )

        if lock_count > 0:
            logging.warning(
                "Removing %s locks from table %s.%s by force",
                lock_count,
                hive_db,
                hive_table,
            )
            self.common_config.log_import_failure(
                hive_db=hive_db,
                hive_table=hive_table,
                severity="Warning",
                error_text="%s locks removed by force" % (lock_count),
            )

        (
            session.query(hiveSchema.HiveLocks)
            .filter(hiveSchema.HiveLocks.HL_DB == hive_db)
            .filter(hiveSchema.HiveLocks.HL_TABLE == hive_table)
            .delete()
        )
        session.commit()

        logging.debug(
            "Executing HiveMetastoreDirect.remove_locks_by_force() - Finished"
        )

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
        """Returns a pandas DataFrame with all columns in the specified Hive table"""
        logging.debug("Executing HiveMetastoreDirect.get_columns()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        session = self.hive_meta_session()
        tbls = aliased(hiveSchema.TBLS, name="T")
        columns = aliased(hiveSchema.ColumnsV2, name="C")
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
            .filter(tbls.TBL_NAME == hive_table)
            .filter(dbs.NAME == hive_db)
            .order_by(columns.INTEGER_IDX)
            .all()
        )

        if include_type == False:
            result_df.drop("type", axis=1, inplace=True)
        else:
            result_df.loc[
                result_df["type"] == "timestamp with local time zone", "type"
            ] = "timestamp"

        if include_comment == False:
            result_df.drop("comment", axis=1, inplace=True)

        if include_idx == False:
            result_df.drop("idx", axis=1, inplace=True)

        if force_column_uppercase == True:
            result_df["name"] = result_df["name"].str.upper()

        # 		if excludeDataLakeColumns == True:
        # 			result_df = result_df[~result_df['name'].astype(str).str.startswith('datalake_')]

        # Index needs to be reset if anything was droped in the DF.
        # The merge will otherwise indicate that there is a difference and table will be recreated
        result_df.reset_index(drop=True, inplace=True)

        logging.debug("Executing HiveMetastoreDirect.get_columns() - Finished")
        return result_df

    #
    def is_table_transactional(self, hive_db, hive_table):
        logging.debug("Executing HiveMetastoreDirect.is_table_transactional()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        session = self.hive_meta_session()
        table_params = aliased(hiveSchema.TableParams, name="TP")
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        row = (
            session.query(table_params.PARAM_VALUE.label("param_value"))
            .select_from(table_params)
            .join(tbls)
            .join(dbs)
            .filter(tbls.TBL_NAME == hive_table)
            .filter(dbs.NAME == hive_db)
            .filter(table_params.PARAM_KEY == "transactional")
            .one_or_none()
        )

        return_value = False
        if row != None:
            if row[0].lower() == "true":
                return_value = True

        logging.debug("isTableTransactional = %s", return_value)
        logging.debug(
            "Executing HiveMetastoreDirect.is_table_transactional() - Finished"
        )
        return return_value

    def is_table_view(self, hive_db, hive_table):
        logging.debug("Executing HiveMetastoreDirect.is_table_view()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        session = self.hive_meta_session()
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        row = (
            session.query(tbls.TBL_TYPE)
            .select_from(tbls)
            .join(dbs)
            .filter(tbls.TBL_NAME == hive_table)
            .filter(dbs.NAME == hive_db)
            .one_or_none()
        )

        return_value = False

        if row[0] == "VIRTUAL_VIEW":
            return_value = True

        logging.debug("isTableView = %s", return_value)
        logging.debug("Executing HiveMetastoreDirect.is_table_view() - Finished")
        return return_value

    def get_tables(self, db_filter=None, table_filter=None):
        """Return all tables from specific Hive DB"""
        logging.debug("Executing HiveMetastoreDirect.get_tables()")

        if self.hive_meta_session == None:
            self.connect_to_hive_metastore_db()

        result_df = None

        if db_filter != None:
            db_filter = db_filter.replace("*", "%")
        else:
            db_filter = "%"

        if table_filter != None:
            table_filter = table_filter.replace("*", "%")
        else:
            table_filter = "%"

        session = self.hive_meta_session()
        tbls = aliased(hiveSchema.TBLS, name="T")
        dbs = aliased(hiveSchema.DBS, name="D")

        result_df = pd.DataFrame(
            session.query(dbs.NAME.label("hiveDB"), tbls.TBL_NAME.label("hiveTable"))
            .select_from(tbls)
            .join(dbs)
            .filter(tbls.TBL_NAME.like(table_filter))
            .filter(dbs.NAME.like(db_filter))
            .order_by(dbs.NAME, tbls.TBL_NAME)
            .all()
        )

        if len(result_df) == 0:
            return pd.DataFrame(columns=["hiveDB", "hiveTable"])

        logging.debug("Executing HiveMetastoreDirect.get_tables() - Finished")
        return result_df
