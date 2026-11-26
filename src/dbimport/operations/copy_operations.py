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

import fnmatch
import logging
import subprocess
import sys
from datetime import datetime

from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.common.singleton import Singleton
from dbimport.config_db import config_schema, import_config, instances
from dbimport.config_db import engine as config_engine
from dbimport.config_db.repositories.airflow_dags import AirflowDAGsRepository
from dbimport.config_db.repositories.copies import CopiesRepository
from dbimport.config_db.repositories.export_tables import ExportTablesRepository
from dbimport.config_db.repositories.import_tables import ImportTablesRepository
from dbimport.config_db.repositories.instance_copy import InstanceCopy
from dbimport.config_db.repositories.schema_version import SchemaVersionRepository
from dbimport.operations import common_operations
from dbimport.settings import get_settings

JDBC_SECRETS = ("credentials", "private_key_path", "public_key_path")


class Operation(metaclass=Singleton):
    def __init__(self):
        logging.debug("Executing copy_operations.__init__()")
        self.hive_db = None
        self.hive_table = None
        self.start_date = None
        self.remote_instance_config_db_engine = None
        self.remote_instance_config_db_session = None
        self.copy_destinations = None

        self.common_operations = common_operations.Operation()
        self.import_config = import_config.Config()

        self.hive_db = self.common_operations.hive_db
        self.hive_table = self.common_operations.hive_table
        self.start_date = self.import_config.start_date

        self.crypto = self.import_config.common_config.crypto
        self.debug_log_level = self.import_config.common_config.debug_log_level

        logging.debug("Executing copy_operations.__init__() - Finished")

    def remove_temporary_files(self):
        self.import_config.remove_temporary_files()

    def check_dbimport_instance(self, instance):
        """Checks if instance exists in the dbimport_instances table"""

        with Session(config_engine.engine()) as session:
            exists = CopiesRepository(session).instance_exists(instance)

        if not exists:
            logging.error(
                "No DBImport Instance with that name can be found in table 'dbimport_instances'"
            )
            self.remove_temporary_files()
            sys.exit(1)

    def encrypt_user_password(self, instance, username, password):
        """Encrypts the username and password and store them in the 'credentials column' of 'dbimport_instances' table"""

        self.crypto.set_private_key_file(get_settings().credentials.private_key)
        self.crypto.set_public_key_file(get_settings().credentials.public_key)

        str_to_encrypt = "%s %s\n" % (username, password)
        encrypted_str = self.crypto.encrypt(str_to_encrypt)

        if encrypted_str == None or encrypted_str == "":
            logging.error("Cant encrypt username and password. Please contact support")
            self.remove_temporary_files()
            sys.exit(1)

        with Session(config_engine.engine()) as session:
            CopiesRepository(session).save_credentials(instance, encrypted_str)
            session.commit()

    def get_copy_destinations(self, hive_db, hive_table):
        """Return the Copy Phase that the specified table have"""

        with Session(config_engine.engine()) as session:
            rules = CopiesRepository(session).copy_rules()

        self.copy_destinations = []

        for rule in rules:
            if ";" in rule.destination:
                logging.error("';' not supported in the Copy Destination name")
                self.remove_temporary_files()
                sys.exit(1)

            for hive_filter_split in rule.hive_filter.split(";"):
                if "." not in hive_filter_split:
                    logging.warning(
                        "The filter in table 'copy_tables' with copy_id = %s contains an invalid filter. Missing a . in the database.table format"
                        % (rule.copy_id)
                    )
                else:
                    filter_db = hive_filter_split.split(".")[0]
                    filter_table = hive_filter_split.split(".")[1]

                    if fnmatch.fnmatch(hive_db, filter_db) and fnmatch.fnmatch(
                        hive_table, filter_table
                    ):
                        dest_string = "%s;%s" % (
                            rule.destination,
                            rule.data_transfer,
                        )
                        dest_string_async = "%s;Asynchronous" % (rule.destination)
                        dest_string_sync = "%s;Synchronous" % (rule.destination)
                        if (
                            dest_string_sync in self.copy_destinations
                            and dest_string == dest_string_async
                        ):
                            # ASync have priority. So if sync is already in there, we remove it and add async
                            self.copy_destinations.remove(dest_string_sync)
                        if (
                            dest_string_async not in self.copy_destinations
                            and dest_string_sync not in self.copy_destinations
                        ):
                            self.copy_destinations.append(dest_string)

        if self.copy_destinations == []:
            self.copy_destinations = None
        return self.copy_destinations

    def connect_remote_dbimport_instance(self, instance):
        """Connects to the configuration database of the DBImport instance
        and makes remote_instance_config_db_session its sessions; False
        when its credentials cannot be used or it cannot be reached. Raises
        NoResultFound for an unknown instance."""

        # Set the keys to the value in the configuration file. This is needed if the key was overridden in jdbc_connections
        self.crypto.set_private_key_file(get_settings().credentials.private_key)
        self.crypto.set_public_key_file(get_settings().credentials.public_key)

        if self.remote_instance_config_db_engine is not None:
            self.remote_instance_config_db_engine.dispose()
        self.remote_instance_config_db_engine = None
        self.remote_instance_config_db_session = None

        with Session(config_engine.engine()) as session:
            connection = CopiesRepository(session).instance_connection(instance)
        try:
            engine = instances.instance_engine(
                connection, self.crypto, echo=self.debug_log_level
            )
        except (InvalidConfigurationError, DBAPIError) as err:
            logging.warning("%s", err)
            return False

        self.remote_instance_config_db_engine = engine
        self.remote_instance_config_db_session = sessionmaker(bind=engine)
        return True

    def is_previous_copy_completed(self):
        """Returns True or False if the previous copy was completed. Returns True if there is nothing to copy.
        If it is a full import, then it will return True if the previous copy have status 0 (not started)."""

        if self.copy_destinations == None:
            return True

        ongoing_copy = False

        with Session(config_engine.engine()) as session:
            copies = CopiesRepository(session)
            for dest_and_method in self.copy_destinations:
                destination = dest_and_method.split(";")[0]

                copy_status = copies.async_copy_status(
                    self.import_config.table_id, destination
                )
                if copy_status is not None and (
                    self.import_config.import_is_incremental or copy_status > 0
                ):
                    ongoing_copy = True

        if ongoing_copy:
            return False
        else:
            return True

    def copy_data_to_destinations(self):
        source_hdfs_address = self.common_operations.hdfs_address
        source_hdfs_basedir = self.common_operations.hdfs_basedir

        if self.copy_destinations == None:
            logging.warning("There are no destination for this table to receive a copy")
            return

        for dest_and_method in self.copy_destinations:
            destination = dest_and_method.split(";")[0]
            method = dest_and_method.split(";")[1]

            # Calculate the source and target HDFS directories
            with Session(config_engine.engine()) as session:
                target = CopiesRepository(session).hdfs_location(destination)

            target_hdfs_address = target.hdfs_address
            target_hdfs_basedir = target.hdfs_basedir

            source_hdfs_dir = (
                (source_hdfs_basedir + "/" + self.hive_db + "/" + self.hive_table)
                .replace("$", "")
                .replace(" ", "")
            )
            target_hdfs_dir = (
                (target_hdfs_basedir + "/" + self.hive_db + "/" + self.hive_table)
                .replace("$", "")
                .replace(" ", "")
            )

            if method == "Asynchronous":
                table_id = self.import_config.table_id
                source_path = "%s%s" % (source_hdfs_address, source_hdfs_dir)
                target_path = "%s%s" % (target_hdfs_address, target_hdfs_dir)

                with Session(config_engine.engine()) as session:
                    copies = CopiesRepository(session)
                    copy_status = copies.async_copy_status(table_id, destination)

                    if copy_status is None:
                        # No current copy ongoing for this table. Just insert it into the table
                        copies.schedule_async_copy(
                            table_id,
                            self.import_config.hive_db,
                            self.import_config.hive_table,
                            destination,
                            source_path=source_path,
                            target_path=target_path,
                        )
                        session.commit()
                        logging.info(
                            "DBImport server was notified about asynchronous copy of imported data to '%s'"
                            % (destination)
                        )
                    else:
                        # This table is already scheduled for ASync copy to another cluster
                        # If it is a full import without history, it's ok to overwrite if it havent started
                        # If not, we need to exit here to make sure that the other cluster first process the data before we
                        # write new data to it.

                        if self.import_config.import_is_incremental:
                            logging.error(
                                "This is an incremental import and the previous copy to the other cluster is not finnished yet."
                            )
                            logging.error(
                                "Make sure that the other cluster received the data before you make a new copy"
                            )
                            self.remove_temporary_files()
                            sys.exit(1)

                        if self.import_config.import_with_history_table:
                            logging.error(
                                "This is an import that will create a history table and the previous copy to the other cluster"
                            )
                            logging.error(
                                "is not finnished yet. Make sure that the other cluster received the data before you make a new copy"
                            )
                            self.remove_temporary_files()
                            sys.exit(1)

                        # If we passed the two tests above, it means that this is not an incremental or history table that we are going to copy
                        # If the copy havent started yet (copy_status = 0), we can just overwrite it. Otherwise we have to wait until the copy
                        # is completed

                        if copy_status > 0:
                            logging.error(
                                "There is an ongoing copy of this table to destination '%s'. Please wait until that copy is finished and try again"
                                % (destination)
                            )
                            self.remove_temporary_files()
                            sys.exit(1)

                        copies.reschedule_async_copy(
                            table_id,
                            destination,
                            source_path=source_path,
                            target_path=target_path,
                            updated=datetime.now(),
                        )
                        session.commit()
                        logging.info(
                            "DBImport server was notified about asynchronous copy of imported data to '%s'"
                            % (destination)
                        )

            else:
                if self.connect_remote_dbimport_instance(instance=destination):
                    logging.info("Copy HDFS data to instance '%s'" % (destination))

                    distcp_command = [
                        "hadoop",
                        "distcp",
                        "-D",
                        "yarn.timeline-service.enabled=false",
                        "-D",
                        f"mapreduce.job.queuename={get_settings().sqoop.yarnqueue}",
                        "-overwrite",
                        "-delete",
                        "%s%s" % (source_hdfs_address, source_hdfs_dir),
                        "%s%s" % (target_hdfs_address, target_hdfs_dir),
                    ]

                    print(" ______________________ ")
                    print("|                      |")
                    print("| Hadoop distCp starts |")
                    print("|______________________|")
                    print("")

                    # Start distcp
                    sh_session = subprocess.Popen(
                        distcp_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
                    )

                    # Print Stdout and stderr while distcp is running
                    while sh_session.poll() is None:
                        row = sh_session.stdout.readline().decode("utf-8").rstrip()
                        if row != "":
                            print(row)
                            sys.stdout.flush()

                    # Print what is left in output after distcp is finished
                    for row in sh_session.stdout.readlines():
                        row = row.decode("utf-8").rstrip()
                        if row != "":
                            print(row)
                            sys.stdout.flush()

                    print(" _________________________ ")
                    print("|                         |")
                    print("| Hadoop distCp completed |")
                    print("|_________________________|")
                    print("")

    def schedule_table_async_copy(
        self,
        hive_filter_db,
        hive_filter_table,
        copy_destination,
        include_incr_imports=False,
    ):
        """Schdeule an asynchronous copy of one or more Hive table to the specified destination"""
        logging.debug("Executing copy_operations.schedule_table_async_copy()")

        if self.check_dbimport_instance(instance=copy_destination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        hive_filter_db = hive_filter_db.replace("*", "%").strip()
        hive_filter_table = hive_filter_table.replace("*", "%").strip()

        # Check if there are any tabled in this DAG that is marked for copy against the specified destination
        with Session(config_engine.engine()) as session:
            scheduled = CopiesRepository(session).has_async_copies(
                hive_filter_db, hive_filter_table, copy_destination
            )

        if scheduled:
            logging.error(
                "There is already tables that matches this DAG's filter that is scheduled for copy against"
            )
            logging.error(
                "the specified destination. This operation cant continue until all the current copies are completed"
            )
            self.remove_temporary_files()
            sys.exit(1)

        self._schedule_async_copies(
            [(hive_filter_db, hive_filter_table)],
            copy_destination,
            include_incr_imports,
        )

        logging.debug(
            "Executing copy_operations.schedule_table_async_copy() - Finished"
        )

    def schedule_dag_async_copy(
        self, airflow_dag_name, copy_destination, include_incr_imports
    ):
        """Schedule an asynchronous copy of all tables that are included in the specified DAG to the specified destination"""

        if self.check_dbimport_instance(instance=copy_destination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        with Session(config_engine.engine()) as session:
            dag = AirflowDAGsRepository(session).import_dag(airflow_dag_name)

        if dag is None:
            logging.error("The specified Airflow DAG does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        hive_filters = []
        for hive_filter in dag.filter_hive.split(";"):
            hive_filter_db = hive_filter.split(".")[0]
            hive_filter_table = hive_filter.split(".")[1]

            hive_filters.append(
                (
                    hive_filter_db.replace("*", "%").strip(),
                    hive_filter_table.replace("*", "%").strip(),
                )
            )

        with Session(config_engine.engine()) as session:
            copies = CopiesRepository(session)
            for hive_filter_db, hive_filter_table in hive_filters:
                # Check if there are any tabled in this DAG that is marked for copy against the specified destination
                if copies.has_async_copies(
                    hive_filter_db, hive_filter_table, copy_destination
                ):
                    logging.error(
                        "There is already tables that matches this DAG's filter that is scheduled for copy against"
                    )
                    logging.error(
                        "the specified destination. This operation cant continue until all the current copies are completed"
                    )
                    self.remove_temporary_files()
                    sys.exit(1)

        # We need this for loop here again as there might be multi table specifications that ovarlap each other. So it's important that we
        # dont start the actual schedule of copies before we tested them all. Otherwise we might block ourself during the schedule
        self._schedule_async_copies(
            hive_filters, copy_destination, include_incr_imports
        )

    def _schedule_async_copies(
        self, hive_filters, copy_destination, include_incr_imports
    ):
        """Schedules an asynchronous copy to copy_destination for every
        table matching one of the (database, table) LIKE patterns that has
        none yet; incremental imports only with include_incr_imports."""

        # Calculate the source and target HDFS directories
        source_hdfs_address = self.common_operations.hdfs_address
        source_hdfs_basedir = self.common_operations.hdfs_basedir

        with Session(config_engine.engine()) as session:
            target = CopiesRepository(session).hdfs_location(copy_destination)

        target_hdfs_address = target.hdfs_address
        target_hdfs_basedir = target.hdfs_basedir

        logging.debug("sourceHDFSaddress: %s" % (source_hdfs_address))
        logging.debug("sourceHDFSbasedir: %s" % (source_hdfs_basedir))
        logging.debug("targetHDFSaddress: %s" % (target_hdfs_address))
        logging.debug("targetHDFSbasedir: %s" % (target_hdfs_basedir))

        for hive_filter_db, hive_filter_table in hive_filters:
            logging.debug("hiveFilterDB: %s" % (hive_filter_db))
            logging.debug("hiveFilterTable: %s" % (hive_filter_table))

            # Fetch a list of tables that match the database and table filter
            with Session(config_engine.engine()) as session:
                tables = ImportTablesRepository(session).tables_like(
                    hive_filter_db, hive_filter_table
                )

            for table_id, hive_db, hive_table in tables:
                logging.info(
                    "Schedule asynchronous copy for %s.%s", hive_db, hive_table
                )
                self.import_config.hive_db = hive_db
                self.import_config.hive_table = hive_table

                try:
                    self.import_config.get_import_config()
                except InvalidConfigurationError as errMsg:
                    logging.error(errMsg)
                    self.import_config.remove_temporary_files()
                    sys.exit(1)

                logging.debug("table_id: %s", self.import_config.table_id)
                logging.debug(
                    "import_is_incremental: %s",
                    self.import_config.import_is_incremental,
                )

                if (
                    self.import_config.import_is_incremental
                    and include_incr_imports == False
                ):
                    logging.warning(
                        "Asynchronous copy for incremental table %s.%s skipped",
                        hive_db,
                        hive_table,
                    )
                    continue

                source_hdfs_dir = (
                    (source_hdfs_basedir + "/" + hive_db + "/" + hive_table)
                    .replace("$", "")
                    .replace(" ", "")
                )
                target_hdfs_dir = (
                    (target_hdfs_basedir + "/" + hive_db + "/" + hive_table)
                    .replace("$", "")
                    .replace(" ", "")
                )
                logging.debug("sourceHDFSdir: %s" % (source_hdfs_dir))
                logging.debug("targetHDFSdir: %s" % (target_hdfs_dir))

                with Session(config_engine.engine()) as session:
                    copies = CopiesRepository(session)
                    if copies.async_copy_status(table_id, copy_destination) is None:
                        copies.schedule_async_copy(
                            table_id,
                            hive_db,
                            hive_table,
                            copy_destination,
                            source_path="%s%s" % (source_hdfs_address, source_hdfs_dir),
                            target_path="%s%s" % (target_hdfs_address, target_hdfs_dir),
                        )
                        session.commit()

    def copy_airflow_export_dag(
        self,
        airflow_dag_name,
        copy_destination,
        set_auto_regenerate_dag=False,
        deploy_mode=False,
    ):
        """Copy an Airflow Export DAG, including connections, tables and columns to a remote DBImport instance, in one transaction"""

        if self.check_dbimport_instance(instance=copy_destination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        with Session(config_engine.engine()) as session:
            dag = AirflowDAGsRepository(session).export_dag(airflow_dag_name)

        if dag is None:
            logging.error("The specified Airflow DAG does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        if not self.connect_remote_dbimport_instance(instance=copy_destination):
            return

        filter_schema = dag.filter_target_schema
        filter_table = dag.filter_target_table
        with (
            Session(config_engine.engine()) as local_session,
            self.remote_instance_config_db_session() as remote_session,
        ):
            instance_copy = InstanceCopy(local_session, remote_session)
            instance = CopiesRepository(local_session).instance_connection(
                copy_destination
            )
            dag_name = {"dag_name": airflow_dag_name}
            dag_row = instance_copy.rows(config_schema.AirflowExportDAGs, dag_name)[0]
            if set_auto_regenerate_dag == True:
                dag_row["auto_regenerate_dag"] = 1
            instance_copy.copy(config_schema.AirflowExportDAGs, dag_row, dag_name)

            for table_id, hive_db, hive_table in ExportTablesRepository(
                local_session
            ).tables_like(
                dag.filter_dbalias.replace("*", "%"),
                "%" if filter_schema is None else filter_schema.replace("*", "%"),
                "%" if filter_table is None else filter_table.replace("*", "%"),
            ):
                if deploy_mode == False:
                    logging.info(
                        "Copy schema definitions for %s.%s", hive_db, hive_table
                    )
                else:
                    logging.info(
                        "Deploying schema definitions for %s.%s", hive_db, hive_table
                    )
                self._copy_export_table(
                    instance_copy, table_id, instance.sync_credentials
                )

            tasks = instance_copy.rows(config_schema.AirflowTasks, dag_name)
            copied_jdbc_connections = set()
            for task in tasks:
                dbalias = task["jdbc_dbalias"]
                if dbalias is not None and dbalias not in copied_jdbc_connections:
                    self._copy_jdbc_connection(
                        instance_copy, dbalias, instance.sync_credentials
                    )
                    copied_jdbc_connections.add(dbalias)
                instance_copy.copy(
                    config_schema.AirflowTasks,
                    task,
                    {"dag_name": airflow_dag_name, "task_name": task["task_name"]},
                )

            sensors = instance_copy.rows(config_schema.AirflowDAGSensors, dag_name)
            for sensor in sensors:
                wait_for_task = sensor["wait_for_task"]
                instance_copy.copy(
                    config_schema.AirflowTasks,
                    {
                        "task_type": "DAG Sensor",
                        "placement": "before main",
                        "task_config": "%s.%s"
                        % (
                            sensor["wait_for_dag"],
                            "stop" if wait_for_task is None else wait_for_task,
                        ),
                    },
                    {"dag_name": airflow_dag_name, "task_name": sensor["sensor_name"]},
                )

            remote_session.commit()

        if deploy_mode == False:
            logging.info("DAG definition copied to remote DBImport successfully")
            logging.info("Schema definitions copied to remote DBImport successfully")
            if tasks:
                logging.info("DAG custom tasks copied to remote DBImport successfully")
        else:
            logging.info("DAG definition deployed successfully")
            logging.info("Schema definitions deployed successfully")
            if tasks:
                logging.info("DAG custom tasks deployed successfully")
        for sensor in sensors:
            logging.info(
                "Converted DAG sensor '%s' to an airflow_tasks entry",
                sensor["sensor_name"],
            )

    def _copy_export_table(self, instance_copy, table_id, sync_credentials):
        """Copy the export table, its JDBC connection and its columns"""
        table = instance_copy.rows(config_schema.ExportTables, {"table_id": table_id})[
            0
        ]
        self._copy_jdbc_connection(instance_copy, table["dbalias"], sync_credentials)

        match = {
            "dbalias": table["dbalias"],
            "target_schema": table["target_schema"],
            "target_table": table["target_table"],
        }
        instance_copy.copy(
            config_schema.ExportTables,
            table,
            match,
            leave_out={"table_id", "sqoop_last_execution"},
        )
        remote_table_id = instance_copy.remote_value(
            config_schema.ExportTables, "table_id", match
        )

        for column in instance_copy.rows(
            config_schema.ExportColumns,
            {"table_id": table_id},
            order_by=("column_order",),
        ):
            instance_copy.copy(
                config_schema.ExportColumns,
                column,
                {"table_id": remote_table_id, "column_name": column["column_name"]},
                leave_out={"column_id"},
            )

    def check_remote_instance_schema(self, remote_instance):
        """Checks that the remote instance is using the same database schema version as the local instance is"""

        if self.check_dbimport_instance(instance=remote_instance) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        if not self.connect_remote_dbimport_instance(instance=remote_instance):
            logging.error("Cant connect to remote DBImport instance")
            self.remove_temporary_files()
            sys.exit(1)

        with Session(config_engine.engine()) as local_session:
            local_version = SchemaVersionRepository(local_session).version()
        with self.remote_instance_config_db_session() as remote_session:
            remote_version = SchemaVersionRepository(remote_session).version()

        if remote_version is None or local_version != remote_version:
            logging.error(
                "The remote DBImport instance is not running with the same version."
            )
            self.remove_temporary_files()
            sys.exit(1)

    def copy_airflow_import_dag(
        self,
        airflow_dag_name,
        copy_destination,
        copy_dag_no_slave=False,
        set_auto_regenerate_dag=False,
        deploy_mode=False,
    ):
        """Copy an Airflow Import DAG, including connections, tables and columns to a remote DBImport instance, in one transaction"""

        if self.check_dbimport_instance(instance=copy_destination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        with Session(config_engine.engine()) as session:
            dag = AirflowDAGsRepository(session).import_dag(airflow_dag_name)

        if dag is None:
            logging.error("The specified Airflow DAG does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        if not self.connect_remote_dbimport_instance(instance=copy_destination):
            logging.error("Cant connect to remote DBImport instance")
            self.remove_temporary_files()
            sys.exit(1)

        with (
            Session(config_engine.engine()) as local_session,
            self.remote_instance_config_db_session() as remote_session,
        ):
            instance_copy = InstanceCopy(local_session, remote_session)
            instance = CopiesRepository(local_session).instance_connection(
                copy_destination
            )
            dag_name = {"dag_name": airflow_dag_name}
            dag_row = instance_copy.rows(config_schema.AirflowImportDAGs, dag_name)[0]
            if set_auto_regenerate_dag == True:
                dag_row["auto_regenerate_dag"] = 1
            instance_copy.copy(config_schema.AirflowImportDAGs, dag_row, dag_name)

            for hive_filter in dag.filter_hive.split(";"):
                hive_filter_db = hive_filter.split(".")[0].replace("*", "%")
                hive_filter_table = hive_filter.split(".")[1].replace("*", "%")
                for table_id, hive_db, hive_table in ImportTablesRepository(
                    local_session
                ).tables_like(hive_filter_db, hive_filter_table):
                    if deploy_mode == False:
                        logging.info(
                            "Copy schema definitions for %s.%s", hive_db, hive_table
                        )
                    else:
                        logging.info(
                            "Deploying schema definitions for %s.%s",
                            hive_db,
                            hive_table,
                        )
                    self._copy_import_table(
                        instance_copy,
                        table_id,
                        instance.sync_credentials,
                        copy_slave=0 if copy_dag_no_slave or deploy_mode else 1,
                        copy_finished=None,
                    )

            tasks = instance_copy.rows(config_schema.AirflowTasks, dag_name)
            copied_jdbc_connections = set()
            for task in tasks:
                dbalias = task["jdbc_dbalias"]
                if dbalias is not None and dbalias not in copied_jdbc_connections:
                    self._copy_jdbc_connection(
                        instance_copy, dbalias, instance.sync_credentials
                    )
                    copied_jdbc_connections.add(dbalias)
                instance_copy.copy(
                    config_schema.AirflowTasks,
                    task,
                    {"dag_name": airflow_dag_name, "task_name": task["task_name"]},
                )

            remote_session.commit()

        if deploy_mode == False:
            logging.info("DAG definition copied to remote DBImport successfully")
            logging.info("Schema definitions copied to remote DBImport successfully")
            if tasks:
                logging.info("DAG custom tasks copied to remote DBImport successfully")
        else:
            logging.info("DAG definition deployed successfully")
            logging.info("Schema definitions deployed successfully")
            if tasks:
                logging.info("DAG custom tasks deployed successfully")

    def copy_import_schema_to_destinations(self):
        """Copy the definitions of the imported table to its copy
        destinations, to each in one transaction"""
        if self.copy_destinations == None:
            logging.warning("There are no destination for this table to receive a copy")
            return

        for dest_and_method in self.copy_destinations:
            destination = dest_and_method.split(";")[0]
            method = dest_and_method.split(";")[1]
            if not self.connect_remote_dbimport_instance(instance=destination):
                logging.warning(
                    "Connection failed! No data will be copied to instance '%s'",
                    destination,
                )
                continue

            logging.info(
                "Copy schema definitions for %s.%s to instance '%s'",
                self.import_config.hive_db,
                self.import_config.hive_table,
                destination,
            )
            with (
                Session(config_engine.engine()) as local_session,
                self.remote_instance_config_db_session() as remote_session,
            ):
                instance_copy = InstanceCopy(local_session, remote_session)
                instance = CopiesRepository(local_session).instance_connection(
                    destination
                )
                self._copy_import_table(
                    instance_copy,
                    self.import_config.table_id,
                    instance.sync_credentials,
                    copy_slave=1,
                    copy_finished=datetime.now() if method == "Synchronous" else None,
                )
                remote_session.commit()

    def _copy_import_table(
        self, instance_copy, table_id, sync_credentials, copy_slave, copy_finished
    ):
        """Copy the import table, its JDBC connection and its columns"""
        table = instance_copy.rows(config_schema.ImportTables, {"table_id": table_id})[
            0
        ]
        self._copy_jdbc_connection(instance_copy, table["dbalias"], sync_credentials)

        table["copy_slave"] = copy_slave
        table["copy_finished"] = copy_finished
        match = {"hive_db": table["hive_db"], "hive_table": table["hive_table"]}
        instance_copy.copy(
            config_schema.ImportTables, table, match, leave_out={"table_id"}
        )
        remote_table_id = instance_copy.remote_value(
            config_schema.ImportTables, "table_id", match
        )

        for column in instance_copy.rows(
            config_schema.ImportColumns,
            {"table_id": table_id},
            order_by=("column_order",),
        ):
            instance_copy.copy(
                config_schema.ImportColumns,
                column,
                {
                    "table_id": remote_table_id,
                    "source_column_name": column["source_column_name"],
                },
                leave_out={"column_id"},
            )

    def _copy_jdbc_connection(self, instance_copy, dbalias, sync_credentials):
        """Copy the JDBC connection; its credentials and key paths only when
        the instance syncs them (sync_credentials 1)."""
        connections = instance_copy.rows(
            config_schema.JDBCConnections, {"dbalias": dbalias}
        )
        if not connections:
            logging.warning(
                "The JDBC connection '%s' does not exist and is not copied", dbalias
            )
        for connection in connections:
            instance_copy.copy(
                config_schema.JDBCConnections,
                connection,
                {"dbalias": dbalias},
                leave_out=() if sync_credentials == 1 else JDBC_SECRETS,
            )

    def import_copied_table(self, local_hdfs_path, hive_db, hive_table):
        """Import a table from the specified directory"""
        logging.debug("Executing copy_operations.import_copied_table()")

        self.common_operations.connect_to_hive(force_skip_test=True)
        local_hdfs_path = (
            (local_hdfs_path + "/" + hive_db + "/" + hive_table)
            .replace("$", "")
            .replace(" ", "")
        )

        logging.info("Importing table")
        query = "import table `%s`.`%s` from '%s'" % (
            hive_db,
            hive_table,
            local_hdfs_path,
        )
        try:
            self.common_operations.execute_hive_query(query)
        except Exception as ex:
            logging.error(ex)
            logging.error("The import failed! The data was not loaded to the table")
            self.remove_temporary_files()
            sys.exit(1)

        logging.debug("Executing copy_operations.import_copied_table() - Finished")

    def export_table(self, local_hdfs_path, hive_db, hive_table):
        """Export the Hive table"""
        logging.debug("Executing copy_operations.export_table()")

        if local_hdfs_path == None:
            logging.error("You need to specify a local HDFS path")
            self.remove_temporary_files()
            sys.exit(1)

        if hive_db == None:
            hive_db = self.hive_db
        if hive_table == None:
            hive_table = self.hive_table

        local_hdfs_path = (
            (local_hdfs_path + "/" + hive_db + "/" + hive_table)
            .replace("$", "")
            .replace(" ", "")
        )
        # 		remoteHDFSpath = (remoteHDFSpath + "/"+ hiveDB + "/" + hiveTable).replace('$', '').replace(' ', '')

        logging.info("Deleting local HDFS directory before export")

        hdfs_delete_command = [
            "hdfs",
            "dfs",
            "-rm",
            "-r",
            "-skipTrash",
            local_hdfs_path,
        ]
        sh_session = subprocess.Popen(
            hdfs_delete_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        hdfs_deleteoutput = ""

        # Print Stdout and stderr while distcp is running
        while sh_session.poll() is None:
            row = sh_session.stdout.readline().decode("utf-8").rstrip()
            if row != "" and "No such file or directory" not in row:
                logging.info(row)
                hdfs_deleteoutput += row + "\n"
                sys.stdout.flush()

        # Print what is left in output after distcp is finished
        for row in sh_session.stdout.readlines():
            row = row.decode("utf-8").rstrip()
            if row != "" and "No such file or directory" not in row:
                logging.info(row)
                hdfs_deleteoutput += row + "\n"
                sys.stdout.flush()

        self.common_operations.connect_to_hive(force_skip_test=True)

        logging.info("Exporting table")
        # 		query = "export table `%s`.`%s` to '%s'"%(hiveDB, hiveTable, localHDFSpath)
        query = "export table %s.%s to '%s'" % (hive_db, hive_table, local_hdfs_path)
        self.common_operations.execute_hive_query(query)

        logging.debug("Executing copy_operations.export_table() - Finished")

    def copy_table_to_from_cluster(
        self,
        remote_instance,
        local_hdfs_path,
        remote_hdfs_path,
        hive_db,
        hive_table,
        from_cluster_mode,
    ):
        logging.debug("Executing copy_operations.copy_table_to_from_cluster()")

        if (
            remote_instance == None
            or self.check_dbimport_instance(instance=remote_instance) == False
        ):
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        if local_hdfs_path == None or remote_hdfs_path == None:
            logging.error("You need to specify a local and remote HDFS path")
            self.remove_temporary_files()
            sys.exit(1)

        if hive_db == None:
            hive_db = self.hive_db
        if hive_table == None:
            hive_table = self.hive_table

        local_hdfs_path = (
            (local_hdfs_path + "/" + hive_db + "/" + hive_table)
            .replace("$", "")
            .replace(" ", "")
        )
        remote_hdfs_path = (
            (remote_hdfs_path + "/" + hive_db + "/" + hive_table)
            .replace("$", "")
            .replace(" ", "")
        )

        # Calculate the source and target HDFS directories
        with Session(config_engine.engine()) as session:
            remote = CopiesRepository(session).hdfs_location(remote_instance)

        remote_hdfs_address = remote.hdfs_address
        local_hdfs_address = self.common_operations.hdfs_address

        if from_cluster_mode == True:
            logging.info("Copy table directory from remote cluster")
            hdfs_source_path = "%s%s" % (remote_hdfs_address, remote_hdfs_path)
            hdfs_target_path = "%s%s" % (local_hdfs_address, local_hdfs_path)

        else:
            logging.info("Copy table directory to remote cluster")
            hdfs_source_path = "%s%s" % (local_hdfs_address, local_hdfs_path)
            hdfs_target_path = "%s%s" % (remote_hdfs_address, remote_hdfs_path)

        logging.debug("HDFSsourcePath = %s", hdfs_source_path)
        logging.debug("HDFStargetPath = %s", hdfs_target_path)

        distcp_command = [
            "hadoop",
            "distcp",
            "-D",
            f"mapreduce.job.queuename={get_settings().sqoop.yarnqueue}",
            "-overwrite",
            "-delete",
            hdfs_source_path,
            hdfs_target_path,
        ]

        # Start distcp
        sh_session = subprocess.Popen(
            distcp_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        dist_cp_output = ""

        # Print Stdout and stderr while distcp is running
        while sh_session.poll() is None:
            row = sh_session.stdout.readline().decode("utf-8").rstrip()
            if row != "":
                logging.info(row)
                dist_cp_output += row + "\n"
                sys.stdout.flush()

        # Print what is left in output after distcp is finished
        for row in sh_session.stdout.readlines():
            row = row.decode("utf-8").rstrip()
            if row != "":
                logging.info(row)
                dist_cp_output += row + "\n"
                sys.stdout.flush()

        logging.debug(
            "Executing copy_operations.copy_table_to_from_cluster() - Finished"
        )
