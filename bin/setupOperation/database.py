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
import random
import string
import sys
from urllib.parse import quote

import mysql.connector
import pandas as pd
import sqlalchemy as sa
from alembic import command as alembicCommand
from alembic.config import Config
from mysql.connector import errorcode
from sqlalchemy.orm import sessionmaker

from DBImportConfig import common_config, configSchema


class initialize:
    def __init__(self):
        logging.debug("Executing database.__init__()")

        self.mysql_conn = None
        self.mysql_cursor = None
        self.debugLogLevel = False

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        try:
            dbimport_home = os.environ["DBIMPORT_HOME"]
        except KeyError:
            print("Error: System Environment Variable DBIMPORT_HOME is not set")
            # 			self.remove_temporary_files()
            sys.exit(1)

        self.common_config = common_config.config()

        # Fetch configuration about MySQL database and how to connect to it
        self.database_credentials = self.common_config.get_mysql_credentials()
        self.config_hostname = self.database_credentials["mysql_hostname"]
        self.config_port = self.database_credentials["mysql_port"]
        self.config_database = self.database_credentials["mysql_database"]
        self.config_username = self.database_credentials["mysql_username"]
        self.config_password = self.database_credentials["mysql_password"]
        # self.config_password = quote(self.database_credentials["mysql_password"], safe=" +")

        self.connect_str = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            self.config_username,
            # self.config_password,
            quote(self.config_password, safe=" +"),
            self.config_hostname,
            self.config_port,
            self.config_database,
        )

        # Esablish a SQLAlchemy connection to the DBImport database
        try:
            self.configDB = sa.create_engine(self.connect_str, echo=self.debugLogLevel)
            self.configDB.connect()
            self.configDBSession = sessionmaker(bind=self.configDB)

        except sa.exc.OperationalError as err:
            logging.error("%s", err)
            sys.exit(1)
        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            sys.exit(1)

        # Setup configuration for Alembic
        self.alembic_schema_dir = dbimport_home + "/bin/SchemaUpgrade"
        self.alembic_config = Config()
        self.alembic_config.set_main_option("script_location", self.alembic_schema_dir)
        self.connect_str = self.connect_str.replace("%", "%%")
        self.alembic_config.set_main_option("sqlalchemy.url", self.connect_str)

        # Esablish a connection to the DBImport database in MySQL
        try:
            self.mysql_conn = mysql.connector.connect(
                host=self.config_hostname,
                port=self.config_port,
                database=self.config_database,
                user=self.config_username,
                password=self.config_password,
            )
        except mysql.connector.errors.ProgrammingError as err:
            logging.error("%s", err)
            # 			self.remove_temporary_files()
            sys.exit(1)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error("Database does not exist")
            else:
                logging.error("%s", err)
            logging.error(
                "Error: There was a problem connecting to the MySQL database. Please check configuration and serverstatus and try again"
            )
            # 			self.remove_temporary_files()
            sys.exit(1)
        else:
            self.mysql_cursor = self.mysql_conn.cursor(buffered=False)

        logging.debug("Executing database.__init__() - Finished")

    def generate_schema_rst(self):

        query = "select max(length(c.column_name)), max(length(c.column_comment)) "
        query += "from information_schema.columns c "
        query += "left join information_schema.tables t "
        query += (
            "   on c.table_schema = t.table_schema and c.table_name = t.table_name "
        )
        query += "where c.table_schema = 'DBImport' "

        logging.debug("SQL Statement executed: %s", query)
        self.mysql_cursor.execute(query)

        row = self.mysql_cursor.fetchone()
        max_column_length = row[0]
        max_desc_length = row[1]

        line_single = "+"
        line_double = "+"
        line_header_column = "Column"
        line_header_desc = "Documentation"

        for _i in range(max_column_length + 2):
            line_single += "-"
            line_double += "="
        line_single += "+"
        line_double += "+"
        for _i in range(max_desc_length + 2):
            line_single += "-"
            line_double += "="
        line_single += "+"
        line_double += "+"

        line_header = "| %s" % (line_header_column)
        for _i in range(max_column_length + 1 - len(line_header_column)):
            line_header += " "
        line_header += "| %s" % (line_header_desc)
        for _i in range(max_desc_length + 1 - len(line_header_desc)):
            line_header += " "
        line_header += "|"

        print("Database Tables")
        print("===============")
        print("")

        print(
            "As this version of DBImport dont have the web admin tool available, the documentation will be agains each column in the configuration database tables. The admin tool will later use the same fields so whats said in here will later be applicable on the admin tool aswell."
        )
        print("")

        query = "select "
        query += "   c.table_name, "
        query += "   t.table_comment, "
        query += "   c.column_name, "
        query += "   c.column_comment "
        query += "from information_schema.columns c "
        query += "left join information_schema.tables t "
        query += (
            "   on c.table_schema = t.table_schema and c.table_name = t.table_name "
        )
        query += "where c.table_schema = 'DBImport' "
        # 		query += "   and c.table_name not like 'airflow%' "
        # 		query += "   and c.table_name != 'etl_jobs' "
        query += "   and c.table_name != 'auto_discovered_tables' "
        query += "   and c.table_name != 'airflow_dag_triggers' "
        query += "   and c.table_name != 'airflow_import_dag_execution' "
        query += "   and c.table_name != 'airflow_import_task_execution' "
        query += "   and c.table_name != 'airflow_execution_type' "
        query += "   and c.table_name != 'airflow_dag_sensors' "
        query += "   and c.table_name != 'alembic_version' "
        query += "order by c.table_name, c.ordinal_position "

        logging.debug("SQL Statement executed: %s", query)
        self.mysql_cursor.execute(query)

        previous_table = ""
        for row in self.mysql_cursor.fetchall():
            table_name = row[0]
            table_comment = row[1]
            column_name = row[2]
            column_comment = row[3]
            if previous_table != table_name:
                previous_table = table_name
                print(" ")
                print(" ")
                print(" ")
                print("Table - %s" % (table_name))
                line = ""
                for _i in range(len(table_name) + 8):
                    line += "-"
                print(line)
                print("")
                if table_comment != None:
                    print(table_comment)
                print("")
                print(line_single)
                print(line_header)
                print(line_double)

            line = "| %s" % (column_name)
            for _i in range(max_column_length + 1 - len(column_name)):
                line += " "
            line += "| %s" % (column_comment)
            for _i in range(max_desc_length + 1 - len(column_comment)):
                line += " "
            line += "|"
            print(line)
            print(line_single)

    def get_list_of_config_tables(self):
        logging.debug("Executing common_config.get_list_of_config_tables()")

        query = "select "
        query += "   table_name "
        query += "from information_schema.tables  "
        query += "where table_schema = %s "
        query += "order by table_name "

        logging.debug("SQL Statement executed: %s", query)
        self.mysql_cursor.execute(query, (self.config_database,))

        tables_df = pd.DataFrame(self.mysql_cursor.fetchall())

        if len(tables_df) > 0:
            tables_df.columns = ["table"]

        logging.debug("Executing common_config.get_list_of_config_tables() - Finished")
        return tables_df

    def create_db(self, create_only_table=None):

        inspector = sa.inspect(self.configDB)
        all_tables = inspector.get_table_names()
        if "alembic_version" in all_tables:
            print(
                "DBImport configuration database is already created. If you are deploying a new version, please run --upgradeDB"
            )
            return

        alembicCommand.upgrade(self.alembic_config, "head")
        self.create_default_environment_types()
        self.update_configuration_values()

        print("DBImport configuration database is created successfully")

    def upgrade_db(self):

        print("Upgrading database to latest level")
        alembicCommand.upgrade(self.alembic_config, "head")
        self.create_default_environment_types()
        self.update_configuration_values()

    def create_default_environment_types(self):
        session = self.configDBSession()

        number_of_environemnt_types = session.query(
            configSchema.jdbcConnectionsEnvironments.__table__
        ).count()
        if number_of_environemnt_types == 0:
            objects_to_add = [
                configSchema.jdbcConnectionsEnvironments(environment="Production"),
                configSchema.jdbcConnectionsEnvironments(
                    environment="Integration Test"
                ),
                configSchema.jdbcConnectionsEnvironments(environment="System Test"),
                configSchema.jdbcConnectionsEnvironments(environment="Development"),
                configSchema.jdbcConnectionsEnvironments(environment="Sandbox"),
            ]
            session.add_all(objects_to_add)
            session.commit()

    def update_configuration_values(self):
        session = self.configDBSession()

        # Create a list of all available JDBC drivers
        result_df = (
            session.query(configSchema.jdbcConnectionsDrivers.database_type)
            .select_from(configSchema.jdbcConnectionsDrivers)
            .all()
        )

        list_of_db_types = []
        for i in result_df:
            list_of_db_types.append(i[0])

        # Create a list of all available configuration keys
        result_df = (
            session.query(configSchema.configuration.configKey)
            .select_from(configSchema.configuration)
            .all()
        )

        list_of_conf_keys = []
        for i in result_df:
            list_of_conf_keys.append(i[0])

        # JDBC Connection Drivers table
        if "DB2 AS400" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="DB2 AS400",
                version="default",
                driver="com.ibm.as400.access.AS400JDBCDriver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "DB2 UDB" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="DB2 UDB",
                version="default",
                driver="com.ibm.db2.jcc.DB2Driver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "MySQL" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="MySQL",
                version="default",
                driver="com.mysql.jdbc.Driver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "Oracle" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="Oracle",
                version="default",
                driver="oracle.jdbc.driver.OracleDriver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "PostgreSQL" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="PostgreSQL",
                version="default",
                driver="org.postgresql.Driver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "Progress DB" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="Progress DB",
                version="default",
                driver="com.ddtek.jdbc.openedge.OpenEdgeDriver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "SQL Server" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="SQL Server",
                version="default",
                driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="SQL Server",
                version="jTDS",
                driver="net.sourceforge.jtds.jdbc.Driver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "CacheDB" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="CacheDB",
                version="default",
                driver="com.intersys.jdbc.CacheDriver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "Snowflake" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="Snowflake",
                version="default",
                driver="net.snowflake.client.jdbc.SnowflakeDriver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "Informix" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="Informix",
                version="default",
                driver="com.informix.jdbc.IfxDriver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        if "SQL Anywhere" not in list_of_db_types:
            query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
                database_type="SQL Anywhere",
                version="default",
                driver="com.sybase.jdbc4.jdbc.SybDriver",
                classpath="add path to JAR file",
            )
            session.execute(query)
            session.commit()

        # Configuration table

        if "airflow_disable" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_disable",
                valueInt="0",
                description='Disable All executions from Airflow. This is what the "start" Task is looking at',
            )
            session.execute(query)
            session.commit()

        if "export_stage_disable" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="export_stage_disable",
                valueInt="0",
                description="With 1, you prevent new Export tasks from starting and running tasks will stop after the current stage is completed.",
            )
            session.execute(query)
            session.commit()

        if "export_staging_database" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="export_staging_database",
                valueStr="etl_export_staging",
                description="Name of staging database to use during Exports",
            )
            session.execute(query)
            session.commit()

        if "export_start_disable" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="export_start_disable",
                valueInt="0",
                description="With 1, you prevent new Export tasks from starting. Running tasks will be completed",
            )
            session.execute(query)
            session.commit()

        if "import_stage_disable" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_stage_disable",
                valueInt="0",
                description="With 1, you prevent new tasks from starting and running Import tasks will stop after the current stage is completed.",
            )
            session.execute(query)
            session.commit()

        if "import_staging_database" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_staging_database",
                valueStr="etl_import_staging",
                description="Name of staging database to use during Imports. {DATABASE} is supported within the name and will be replaced during import with the name of the import database.",
            )
            session.execute(query)
            session.commit()

        if "import_staging_table" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_staging_table",
                valueStr="{DATABASE}__{TABLE}__staging",
                description='Name of staging table to use during Imports. Both {DATABASE} and {TABLE} is supported within the name and will be replaced during import with the name of the import database and table.  Both {DATABASE} and {TABLE} must be used once, but {DATABASE} could be specified in the "import_staging_database" setting instead',
            )
            session.execute(query)
            session.commit()

        if "import_work_database" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_work_database",
                valueStr="etl_import_staging",
                description="Name of work database to use during Imports. This is used for internal tables and should not be accessed by end-users. {DATABASE} is supported within the name and will be replaced during import with the name of the import database.",
            )
            session.execute(query)
            session.commit()

        if "import_work_table" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_work_table",
                valueStr="{DATABASE}__{TABLE}__staging",
                description='Prefix-name of work tables to use during Imports. These tables is used for internal DBImport tables and should not be access by end-users. Both {DATABASE} and {TABLE} is supported within the name and will be replaced during import with the name of the import database and table.  Both {DATABASE} and {TABLE} must be used once, but {DATABASE} could be specified in the "import_work_database" setting instead',
            )
            session.execute(query)
            session.commit()

        if "import_history_database" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_history_database",
                valueStr="{DATABASE}",
                description="Name of history database to use during Imports. {DATABASE} is supported within the name and will be replaced during import with the name of the import database.",
            )
            session.execute(query)
            session.commit()

        if "import_history_table" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_history_table",
                valueStr="{TABLE}_history",
                description='Name of history table to use during Imports. Both {DATABASE} and {TABLE} is supported within the name and will be replaced during import with the name of the import database and table.  Both {DATABASE} and {TABLE} must be used once, but {DATABASE} could be specified in the "import_history_database" setting instead',
            )
            session.execute(query)
            session.commit()

        if "import_start_disable" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_start_disable",
                valueInt="0",
                description="With 1, you prevent new Import tasks from starting. Running tasks will be completed",
            )
            session.execute(query)
            session.commit()

        if "hive_remove_locks_by_force" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hive_remove_locks_by_force",
                valueInt="0",
                description="With 1, DBImport will remove Hive locks before import by force",
            )
            session.execute(query)
            session.commit()

        if "hive_major_compact_after_merge" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hive_major_compact_after_merge",
                valueInt="0",
                description="With 1, DBImport will run a major compaction after the merge operations is completed",
            )
            session.execute(query)
            session.commit()

        if "hive_validate_before_execution" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hive_validate_before_execution",
                valueInt="1",
                description="With 1, DBImport will run a group by query agains the validate table and verify the result against reference values hardcoded in DBImport",
            )
            session.execute(query)
            session.commit()

        if "hive_validate_table" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hive_validate_table",
                valueStr="dbimport.validate_table",
                description="The table to run the validate query against",
            )
            session.execute(query)
            session.commit()

        if "hive_print_messages" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hive_print_messages",
                valueInt="0",
                description="With 1, Hive will print additional messages during SQL operations",
            )
            session.execute(query)
            session.commit()

        if "airflow_dbimport_commandpath" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_dbimport_commandpath",
                valueStr="sudo -iu ${SUDO_USER} /usr/local/dbimport/",
                description="This is the path to DBImport. If sudo is required, this can be added here aswell. Use the variable ${SUDO_USER} instead of hardcoding the sudo username. Must end with a /",
            )
            session.execute(query)
            session.commit()

        if "airflow_sudo_user" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_sudo_user",
                valueStr="dbimport",
                description="What user will Airflow sudo to for executing DBImport. This value will replace the ${SUDO_USER} variable in airflow_dbimport_commandpath setting",
            )
            session.execute(query)
            session.commit()

        if "airflow_major_version" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_major_version",
                valueInt="2",
                description="What is the major version of Airflow? 1 or 2 is valid options. Controls how the DAG files are generated",
            )
            session.execute(query)
            session.commit()

        if "airflow_default_pool_size" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_default_pool_size",
                valueInt="24",
                description="If DBImport creates the Airflow pool, this is the initial size of it",
            )
            session.execute(query)
            session.commit()

        if "airflow_create_pool_with_task" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_create_pool_with_task",
                valueInt="1",
                description="If 1, then the Airflow pool will be created with standard tasks. If 0, then a direct database connection is required to add the pool through sql commands directly the airflow database. Connection details are available in dbimport.cfg for this.",
            )
            session.execute(query)
            session.commit()

        if "airflow_aws_pool_to_instanceid" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_aws_pool_to_instanceid",
                valueInt="1",
                description="When using AWS, should the Airflow pool be set to the same name as the instance id?",
            )
            session.execute(query)
            session.commit()

        if "airflow_aws_instanceids" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_aws_instanceids",
                valueStr="i-xxxxxxxxxxxxxxxxx",
                description="AWS Instance IDs to start the DBImport commands on. Comma-separated list of instance IDs. Tasks will be randomly assigned to one of the EMR clusters specified.",
            )
            session.execute(query)
            session.commit()

        if "airflow_dag_directory" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_dag_directory",
                valueStr="/usr/local/airflow/dags",
                description="Airflow path to DAG directory",
            )
            session.execute(query)
            session.commit()

        if "airflow_dag_staging_directory" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_dag_staging_directory",
                valueStr="/usr/local/airflow/dags_generated_from_dbimport",
                description="Airflow path to staging DAG directory",
            )
            session.execute(query)
            session.commit()

        if "airflow_dummy_task_queue" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_dummy_task_queue",
                valueStr="default",
                description="Queue to use for dummy tasks (stop, stage_one_complete and more)",
            )
            session.execute(query)
            session.commit()

        if "airflow_dag_file_permission" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_dag_file_permission",
                valueStr="660",
                description="File permission of created DAG file",
            )
            session.execute(query)
            session.commit()

        if "airflow_dag_file_group" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_dag_file_group",
                valueStr="airflow",
                description="Group owner of created DAG file",
            )
            session.execute(query)
            session.commit()

        if "airflow_url" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="airflow_url",
                valueStr="http://localhost:8080",
                description="URL to Airflow webUI",
            )
            session.execute(query)
            session.commit()

        if "timezone" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="timezone",
                valueStr="UTC",
                description="The timezone that the configured times are meant for (example is Europe/Stockholm) ",
            )
            session.execute(query)
            session.commit()

        if "timestamp_with_timezone" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="timestamp_with_timezone",
                valueInt="0",
                description="With Spark as the ETL engine, it's possible to save timestamp columns with or without the timezone",
            )
            session.execute(query)
            session.commit()

        if "cluster_name" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="cluster_name",
                valueStr="hadoopcluster",
                description="Name of Hadoop cluster",
            )
            session.execute(query)
            session.commit()

        if "hdfs_address" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hdfs_address",
                valueStr="hdfs://hadoopcluster:8020",
                description="Address to HDFS",
            )
            session.execute(query)
            session.commit()

        if "hdfs_blocksize" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hdfs_blocksize",
                valueStr="134217728",
                description="The HDFS blocksize in bytes. Can usually be found in /etc/hadoop/conf/hdfs-site.xml (search for dfs.blocksize)",
            )
            session.execute(query)
            session.commit()

        if "hdfs_basedir" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hdfs_basedir",
                valueStr="/apps/dbimport",
                description="The base dir to write data to. Example /apps/dbimport",
            )
            session.execute(query)
            session.commit()

        if "export_default_sessions" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="export_default_sessions",
                valueInt="4",
                description="How many SQL sessions should be used for tables that have never been exported before",
            )
            session.execute(query)
            session.commit()

        if "export_max_sessions" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="export_max_sessions",
                valueInt="32",
                description="The maximum number of SQL sessions to use during export",
            )
            session.execute(query)
            session.commit()

        if "import_default_sessions" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_default_sessions",
                valueInt="4",
                description="How many SQL sessions should be used for tables that have never been imported before",
            )
            session.execute(query)
            session.commit()

        if "import_max_sessions" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_max_sessions",
                valueInt="32",
                description="The maximum number of SQL sessions to use during imports",
            )
            session.execute(query)
            session.commit()

        if "spark_max_executors" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="spark_max_executors",
                valueInt="128",
                description="The default value for maximum number of spark executors",
            )
            session.execute(query)
            session.commit()

        if "atlas_discovery_interval" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="atlas_discovery_interval",
                valueInt="24",
                description="How many hours there should pass between each Atlas discovery of a jdbc connection",
            )
            session.execute(query)
            session.commit()

        if "import_process_empty" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_process_empty",
                valueInt="0",
                description="If 1, then the import will do a full processing of import even if they contain no data.",
            )
            session.execute(query)
            session.commit()

        if "hive_insert_only_tables" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hive_insert_only_tables",
                valueInt="1",
                description="If 1, then the non-merge tables in Hive will be ACID insert-only",
            )
            session.execute(query)
            session.commit()

        if "hive_acid_with_clusteredby" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="hive_acid_with_clusteredby",
                valueInt="0",
                description="If 1, then ACID tables will be created with a clustered by option based on the PK. Not required with Hive3 and later",
            )
            session.execute(query)
            session.commit()

        if "post_airflow_dag_operations" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="post_airflow_dag_operations",
                valueInt="0",
                description="Post start and stop activities for Airflow DAGs to Kafka and/or Rest, depending on what is enabled",
            )
            session.execute(query)
            session.commit()

        if "post_data_to_rest" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="post_data_to_rest",
                valueInt="0",
                description="Enable the REST endpoint to be able to receive information regarding completed imports and exports",
            )
            session.execute(query)
            session.commit()

        if "post_data_to_rest_extended" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="post_data_to_rest_extended",
                valueInt="0",
                description="Enable extended statistics in the REST data",
            )
            session.execute(query)
            session.commit()

        if "post_data_to_awssns" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="post_data_to_awssns",
                valueInt="0",
                description="Enable the AWS SNS endpoint to be able to receive information regarding completed imports and exports",
            )
            session.execute(query)
            session.commit()

        if "post_data_to_awssns_extended" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="post_data_to_awssns_extended",
                valueInt="0",
                description="Enable extended statistics in AWS SNS data",
            )
            session.execute(query)
            session.commit()

        if "post_data_to_awssns_topic" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="post_data_to_awssns_topic",
                valueStr="arn:aws:sns:region:account:topic",
                description="Full ARN to AWS SNS Topic where the json message will be sent",
            )
            session.execute(query)
            session.commit()

        if "post_data_to_kafka" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="post_data_to_kafka",
                valueInt="0",
                description="Enable the Kafka endpoint to be able to receive information regarding completed imports and exports",
            )
            session.execute(query)
            session.commit()

        if "post_data_to_kafka_extended" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="post_data_to_kafka_extended",
                valueInt="0",
                description="Enable extended statistics in Kafka data",
            )
            session.execute(query)
            session.commit()

        if "kafka_brokers" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="kafka_brokers",
                valueStr="localhost:9092",
                description="Comma separeted list of Kafka brokers",
            )
            session.execute(query)
            session.commit()

        if "kafka_trustcafile" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="kafka_trustcafile",
                valueStr="/etc/pki/tls/certs/ca-bundle.crt",
                description="Kafka CA Trust file for SSL",
            )
            session.execute(query)
            session.commit()

        if "kafka_securityprotocol" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="kafka_securityprotocol",
                valueStr="SASL_SSL",
                description="Kafka Security Protocol",
            )
            session.execute(query)
            session.commit()

        if "kafka_saslmechanism" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="kafka_saslmechanism",
                valueStr="GSSAPI",
                description="Kafka SASL mechanism",
            )
            session.execute(query)
            session.commit()

        if "kafka_topic" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="kafka_topic",
                valueStr="dbimport_topic",
                description="Kafka topic to send the data to",
            )
            session.execute(query)
            session.commit()

        if "rest_url" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="rest_url",
                valueStr="https://localhost:8443/dbimport",
                description="Rest server URL",
            )
            session.execute(query)
            session.commit()

        if "rest_timeout" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="rest_timeout",
                valueInt="5",
                description="Timeout for the REST call",
            )
            session.execute(query)
            session.commit()

        if "rest_verifyssl" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="rest_verifyssl",
                valueInt="1",
                description="Verify SSL certificate during REST call",
            )
            session.execute(query)
            session.commit()

        if "rest_trustcafile" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="rest_trustcafile",
                valueStr="/etc/pki/tls/certs/ca-bundle.crt",
                description="REST CA Trust file for SSL",
            )
            session.execute(query)
            session.commit()

        if "impala_invalidate_metadata" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="impala_invalidate_metadata",
                valueInt="0",
                description="If 1, then DBImport will connect to Impala and invalidate or refresh the metadata after import is completed",
            )
            session.execute(query)
            session.commit()

        if "import_columnname_import" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_columnname_import",
                valueStr="datalake_import",
                description="Column name for timestamp column added to table during none-merge operations",
            )
            session.execute(query)
            session.commit()

        if "import_columnname_insert" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_columnname_insert",
                valueStr="datalake_insert",
                description="Column name for timestamp column with insert time that is added to table during merge operations",
            )
            session.execute(query)
            session.commit()

        if "import_columnname_update" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_columnname_update",
                valueStr="datalake_update",
                description="Column name for timestamp column with update time that is added to table during merge operations",
            )
            session.execute(query)
            session.commit()

        if "import_columnname_delete" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_columnname_delete",
                valueStr="datalake_delete",
                description="Column name for timestamp column with delete date that is added to table during merge operations. Only exists if soft-delete is enabled for the import",
            )
            session.execute(query)
            session.commit()

        if "import_columnname_iud" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_columnname_iud",
                valueStr="datalake_iud",
                description="Column name for char(1) column added to table during merge operations with information if the last operation is an insert, update or delete",
            )
            session.execute(query)
            session.commit()

        if "import_columnname_histtime" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_columnname_histtime",
                valueStr="datalake_timestamp",
                description="Column name for timestamp column added to history table to contain information about when the insert, update or delete operation was done",
            )
            session.execute(query)
            session.commit()

        if "import_columnname_source" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="import_columnname_source",
                valueStr="dbimport_source",
                description="Column name for source information if that is configured in jdbc_connections table",
            )
            session.execute(query)
            session.commit()

        if "restserver_secret_key" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="restserver_secret_key",
                valueStr="".join(random.choices(string.ascii_letters, k=64)),
                description="Secret key used for oauth2 tokens",
            )
            session.execute(query)
            session.commit()

        if "restserver_token_ttl" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="restserver_token_ttl",
                valueInt="120",
                description="Time-to-live, in minutes, that the oauth2 tokens are valid",
            )
            session.execute(query)
            session.commit()

        if "restserver_admin_user" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="restserver_admin_user",
                valueStr="admin",
                description="Username of administrator on the REST server with all permission",
            )
            session.execute(query)
            session.commit()

        if "restserver_authentication_method" not in list_of_conf_keys:
            query = sa.insert(configSchema.configuration).values(
                configKey="restserver_authentication_method",
                valueStr="local",
                description="REST server authentication method. The following options are supported: local, pam",
            )
            session.execute(query)
            session.commit()

        query = (
            sa.update(configSchema.configuration)
            .where(configSchema.configuration.configKey == "airflow_dag_directory")
            .values(
                description="Airflow path to DAG directory. When using AWS MWAA, this is the name of the S3 bucket."
            )
        )
        session.execute(query)
        session.commit()

        query = (
            sa.update(configSchema.configuration)
            .where(configSchema.configuration.configKey == "import_staging_database")
            .values(
                description="Name of staging database to use during Imports. {DATABASE} is supported within the name and will be replaced during import with the name of the import database."
            )
        )
        session.execute(query)
        session.commit()

        query = (
            sa.update(configSchema.configuration)
            .where(configSchema.configuration.configKey == "airflow_aws_instanceids")
            .values(
                description="AWS Instance IDs to start the DBImport commands on. Comma-separated list of instance IDs. Tasks will be randomly assigned to one of the EMR clusters specified."
            )
        )
        session.execute(query)
        session.commit()
