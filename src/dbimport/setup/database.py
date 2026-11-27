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

import mysql.connector
import pandas as pd
import sqlalchemy as sa
from alembic import command as alembic_command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from mysql.connector import errorcode
from sqlalchemy.orm import sessionmaker

from dbimport.config_db import common_config
from dbimport.config_db import engine as config_engine
from dbimport.config_db.repositories.configuration import (
    ConfigurationRepository,
)
from dbimport.config_db.repositories.connections import ConnectionsRepository


class Initialize:
    def __init__(self):
        logging.debug("Executing database.__init__()")

        self.mysql_conn = None
        self.mysql_cursor = None
        self.debug_log_level = False

        if logging.root.level == 10:  # DEBUG
            self.debug_log_level = True

        self.common_config = common_config.Config()

        # Fetch configuration about MySQL database and how to connect to it
        self.database_credentials = self.common_config.get_mysql_credentials()
        self.config_hostname = self.database_credentials["mysql_hostname"]
        self.config_port = self.database_credentials["mysql_port"]
        self.config_database = self.database_credentials["mysql_database"]
        self.config_username = self.database_credentials["mysql_username"]
        self.config_password = self.database_credentials["mysql_password"]

        # Esablish a SQLAlchemy connection to the DBImport database
        try:
            self.config_db = config_engine.create(echo=self.debug_log_level)
            self.config_db.connect().close()
            self.config_db_session = sessionmaker(bind=self.config_db)

        except sa.exc.OperationalError as err:
            logging.error("%s", err)
            sys.exit(1)
        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            sys.exit(1)

        # Setup configuration for Alembic
        self.alembic_schema_dir = "dbimport:migrations"
        self.alembic_config = Config()
        self.alembic_config.set_main_option("script_location", self.alembic_schema_dir)
        self.alembic_config.set_main_option(
            "sqlalchemy.url",
            self.config_db.url.render_as_string(hide_password=False).replace("%", "%%"),
        )

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
        query += "where c.table_schema = %s "

        logging.debug("SQL Statement executed: %s", query)
        self.mysql_cursor.execute(query, (self.config_database,))

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

        line_header = f"| {line_header_column}"
        for _i in range(max_column_length + 1 - len(line_header_column)):
            line_header += " "
        line_header += f"| {line_header_desc}"
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
        query += "where c.table_schema = %s "
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
        self.mysql_cursor.execute(query, (self.config_database,))

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

        inspector = sa.inspect(self.config_db)
        all_tables = inspector.get_table_names()
        if "alembic_version" in all_tables:
            print(
                "DBImport configuration database is already created. If you are deploying a new version, please run --upgradeDB"
            )
            return

        alembic_command.upgrade(self.alembic_config, "head")
        self.create_default_environment_types()
        self.update_configuration_values()

        print("DBImport configuration database is created successfully")

    def upgrade_db(self):

        print("Upgrading database to latest level")
        script = ScriptDirectory.from_config(self.alembic_config)
        self.exit_on_unknown_revision(self.current_revisions(), script)
        alembic_command.upgrade(self.alembic_config, "head")
        self.create_default_environment_types()
        self.update_configuration_values()

    def downgrade_db(self) -> None:
        """Downgrade the schema by one revision. Exits with an error, without
        changing the database, at the baseline, at an unknown revision or
        without a revision."""
        current = self.current_revisions()
        script = ScriptDirectory.from_config(self.alembic_config)
        self.exit_on_unknown_revision(current, script)
        if not current:
            logging.error(
                "The configuration database has no schema revision. Create the "
                "schema with setup --createDB."
            )
            sys.exit(1)
        if set(current) == {script.get_base()}:
            logging.error(
                "The configuration database is at the baseline revision %s, the "
                "schema of DBImport 1.00.006. It cannot be downgraded further.",
                script.get_base(),
            )
            sys.exit(1)
        print("Downgrading database by one revision")
        alembic_command.downgrade(self.alembic_config, "-1")

    def current_revisions(self) -> tuple[str, ...]:
        with self.config_db.connect() as conn:
            return MigrationContext.configure(conn).get_current_heads()

    def exit_on_unknown_revision(
        self, current: tuple[str, ...], script: ScriptDirectory
    ) -> None:
        """Exit with an error if the database is at a schema revision that
        this version does not have, without changing the database."""
        known = {revision.revision for revision in script.walk_revisions()}
        unknown = [revision for revision in current if revision not in known]
        if unknown:
            logging.error(
                "The configuration database is at schema revision %s, which this "
                "version of DBImport does not have. A database of a version "
                "before DBImport 1.0.0 must first be upgraded with DBImport "
                "1.00.006 (setup --upgradeDB). A database of a newer version "
                "cannot be used with this one.",
                ", ".join(unknown),
            )
            sys.exit(1)

    def create_default_environment_types(self):
        session = self.config_db_session()
        ConnectionsRepository(session).seed_environments()
        session.commit()

    def update_configuration_values(self):
        session = self.config_db_session()

        ConnectionsRepository(session).seed_drivers()
        session.commit()
        ConfigurationRepository(session).seed()
        session.commit()
