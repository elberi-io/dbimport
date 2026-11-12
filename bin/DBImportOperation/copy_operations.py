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

import binascii
import fnmatch
import logging
import subprocess
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import aliased, sessionmaker

from common.Exceptions import InvalidConfigurationError
from common.Singleton import Singleton
from ConfigReader import configuration
from DBImportConfig import configSchema, import_config
from DBImportOperation import common_operations


class operation(metaclass=Singleton):
    def __init__(self):
        logging.debug("Executing copy_operations.__init__()")
        self.hive_db = None
        self.hive_table = None
        self.start_date = None
        self.config_db_session = None
        self.remote_instance_config_db_engine = None
        self.remote_instance_config_db_session = None
        self.copy_destinations = None

        self.common_operations = common_operations.operation()
        self.import_config = import_config.config()

        self.hive_db = self.common_operations.hive_db
        self.hive_table = self.common_operations.hive_table
        self.start_date = self.import_config.start_date

        self.import_config.common_config.connect_sqlalchemy()
        self.config_db_session = self.import_config.common_config.config_db_session
        self.crypto = self.import_config.common_config.crypto
        self.debug_log_level = self.import_config.common_config.debug_log_level

        logging.debug("Executing copy_operations.__init__() - Finished")

    def remove_temporary_files(self):
        self.import_config.remove_temporary_files()

    def check_dbimport_instance(self, instance):
        """Checks if instance exists in the dbimport_instances table"""

        session = self.config_db_session()
        dbimport_instances = aliased(configSchema.DBImportInstances)

        result = (
            session.query(dbimport_instances.name)
            .select_from(dbimport_instances)
            .filter(dbimport_instances.name == instance)
            .count()
        )

        if result == 0:
            logging.error(
                "No DBImport Instance with that name can be found in table 'dbimport_instances'"
            )
            self.remove_temporary_files()
            sys.exit(1)

    def encrypt_user_password(self, instance, username, password):
        """Encrypts the username and password and store them in the 'credentials column' of 'dbimport_instances' table"""

        self.crypto.set_private_key_file(
            configuration.get("Credentials", "private_key")
        )
        self.crypto.set_public_key_file(configuration.get("Credentials", "public_key"))

        str_to_encrypt = "%s %s\n" % (username, password)
        encrypted_str = self.crypto.encrypt(str_to_encrypt)

        if encrypted_str == None or encrypted_str == "":
            logging.error("Cant encrypt username and password. Please contact support")
            self.remove_temporary_files()
            sys.exit(1)

        session = self.config_db_session()
        dbimport_instances = aliased(configSchema.DBImportInstances)

        dbimport_instance = (
            session.query(dbimport_instances)
            .filter(dbimport_instances.name == instance)
            .one()
        )

        dbimport_instance.db_credentials = encrypted_str
        session.commit()

    def get_copy_destinations(self, hive_db, hive_table):
        """Return the Copy Phase that the specified table have"""

        session = self.config_db_session()
        copy_tables = aliased(configSchema.CopyTables)

        self.copy_destinations = []

        copy_tables_result = pd.DataFrame(
            session.query(
                copy_tables.copy_id,
                copy_tables.hive_filter,
                copy_tables.destination,
                copy_tables.data_transfer,
            ).all()
        ).fillna("")
        for _index, row in copy_tables_result.iterrows():
            if ";" in row["destination"]:
                logging.error("';' not supported in the Copy Destination name")
                self.remove_temporary_files()
                sys.exit(1)

            for hive_filter_split in row["hive_filter"].split(";"):
                if "." not in hive_filter_split:
                    logging.warning(
                        "The filter in table 'copy_tables' with copy_id = %s contains an invalid filter. Missing a . in the database.table format"
                        % (row["copy_id"])
                    )
                else:
                    filter_db = hive_filter_split.split(".")[0]
                    filter_table = hive_filter_split.split(".")[1]

                    if fnmatch.fnmatch(hive_db, filter_db) and fnmatch.fnmatch(
                        hive_table, filter_table
                    ):
                        dest_string = "%s;%s" % (
                            row["destination"],
                            row["data_transfer"],
                        )
                        dest_string_async = "%s;Asynchronous" % (row["destination"])
                        dest_string_sync = "%s;Synchronous" % (row["destination"])
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
        """Connects to the configuration database with SQLAlchemy"""

        connect_status = True
        session = self.config_db_session()
        dbimport_instances = aliased(configSchema.DBImportInstances)

        row = (
            session.query(
                dbimport_instances.db_hostname,
                dbimport_instances.db_port,
                dbimport_instances.db_database,
                dbimport_instances.db_credentials,
            )
            .filter(dbimport_instances.name == instance)
            .one()
        )

        if row[3] == None:
            logging.warning(
                "There is no credentials saved in 'dbimport_instance' for %s"
                % (instance)
            )
            return False

        # Set the keys to the value in the configuration file. This is needed if the key was overridden in jdbc_connections
        self.crypto.set_private_key_file(
            configuration.get("Credentials", "private_key")
        )
        self.crypto.set_public_key_file(configuration.get("Credentials", "public_key"))

        try:
            db_credentials = self.crypto.decrypt(row[3])
        except binascii.Error as err:
            logging.warning(
                "Decryption of credentials resulted in error with text: '%s'", err
            )
            return False
        except:
            print("Unexpected warning: ")
            print(sys.exc_info())
            return False

        if db_credentials == None:
            logging.warning(
                "Cant decrypt username and password. Check private/public key in config file"
            )
            return False

        username = db_credentials.split(" ")[0]
        password = db_credentials.split(" ")[1]

        instance_connect_str = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            username,
            password,
            row[0],
            row[1],
            row[2],
        )

        if self.remote_instance_config_db_engine != None:
            try:
                self.remote_instance_config_db_engine.dispose()
            except:
                print(
                    "Unexpected warning when closing connection to DBImport Instance database: "
                )
                print(sys.exc_info())

        try:
            self.remote_instance_config_db_engine = sa.create_engine(
                instance_connect_str, echo=self.debug_log_level, pool_pre_ping=True
            )
            self.remote_instance_config_db_engine.connect()
            self.remote_instance_config_db_session = sessionmaker(
                bind=self.remote_instance_config_db_engine
            )

        except sa.exc.OperationalError as err:
            logging.warning("%s", err)
            connect_status = False
        # 			self.remove_temporary_files()
        # 			sys.exit(1)
        except:
            print("Unexpected warning: ")
            print(sys.exc_info())
            connect_status = False
        # 			self.remove_temporary_files()
        # 			sys.exit(1)

        return connect_status

    def is_previous_copy_completed(self):
        """Returns True or False if the previous copy was completed. Returns True if there is nothing to copy.
        If it is a full import, then it will return True if the previous copy have status 0 (not started)."""

        session = self.config_db_session()
        copy_async_status = aliased(configSchema.CopyAsyncStatus)

        if self.copy_destinations == None:
            return True

        ongoing_copy = False

        for dest_and_method in self.copy_destinations:
            destination = dest_and_method.split(";")[0]
            method = dest_and_method.split(";")[1]

            if self.import_config.import_is_incremental:
                result = (
                    session.query(copy_async_status)
                    .filter(copy_async_status.table_id == self.import_config.table_id)
                    .filter(copy_async_status.destination == destination)
                    .count()
                )
            else:
                result = (
                    session.query(copy_async_status)
                    .filter(copy_async_status.table_id == self.import_config.table_id)
                    .filter(copy_async_status.destination == destination)
                    .filter(copy_async_status.copy_status > 0)
                    .count()
                )

            if result > 0:
                ongoing_copy = True

        session.close()

        if ongoing_copy:
            return False
        else:
            return True

    def copy_data_to_destinations(self):
        session = self.config_db_session()

        source_hdfs_address = self.common_operations.hdfs_address
        source_hdfs_basedir = self.common_operations.hdfs_basedir

        if self.copy_destinations == None:
            logging.warning("There are no destination for this table to receive a copy")
            return

        for dest_and_method in self.copy_destinations:
            destination = dest_and_method.split(";")[0]
            method = dest_and_method.split(";")[1]

            # Calculate the source and target HDFS directories
            dbimport_instances = aliased(configSchema.DBImportInstances)

            row = (
                session.query(
                    dbimport_instances.hdfs_address, dbimport_instances.hdfs_basedir
                )
                .filter(dbimport_instances.name == destination)
                .one()
            )

            target_hdfs_address = row[0]
            target_hdfs_basedir = row[1]

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
                copy_async_status = aliased(configSchema.CopyAsyncStatus)

                result = (
                    session.query(copy_async_status)
                    .filter(copy_async_status.table_id == self.import_config.table_id)
                    .filter(copy_async_status.destination == destination)
                    .count()
                )

                if result == 0:
                    # No current copy ongoing for this table. Just insert it into the table
                    newcopy_async_status = configSchema.CopyAsyncStatus(
                        table_id=self.import_config.table_id,
                        hive_db=self.import_config.hive_db,
                        hive_table=self.import_config.hive_table,
                        destination=destination,
                        hdfs_source_path="%s%s"
                        % (source_hdfs_address, source_hdfs_dir),
                        hdfs_target_path="%s%s"
                        % (target_hdfs_address, target_hdfs_dir),
                        copy_status=0,
                    )
                    session.add(newcopy_async_status)
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

                    result = (
                        session.query(copy_async_status)
                        .filter(
                            copy_async_status.table_id == self.import_config.table_id
                        )
                        .filter(copy_async_status.destination == destination)
                        .filter(copy_async_status.copy_status > 0)
                        .count()
                    )

                    if result == 0:
                        update_dict = {}
                        update_dict["last_status_update"] = str(
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        )
                        update_dict["hdfs_source_path"] = "%s%s" % (
                            source_hdfs_address,
                            source_hdfs_dir,
                        )
                        update_dict["hdfs_target_path"] = "%s%s" % (
                            target_hdfs_address,
                            target_hdfs_dir,
                        )

                        # Update the date in CopyAsyncStatus table
                        (
                            session.query(configSchema.CopyAsyncStatus)
                            .filter(
                                configSchema.CopyAsyncStatus.table_id
                                == self.import_config.table_id
                            )
                            .filter(
                                configSchema.CopyAsyncStatus.destination == destination
                            )
                            .update(update_dict)
                        )
                        session.commit()
                        logging.info(
                            "DBImport server was notified about asynchronous copy of imported data to '%s'"
                            % (destination)
                        )
                    else:
                        logging.error(
                            "There is an ongoing copy of this table to destination '%s'. Please wait until that copy is finished and try again"
                            % (destination)
                        )
                        self.remove_temporary_files()
                        sys.exit(1)

            else:
                if self.connect_remote_dbimport_instance(instance=destination):
                    logging.info("Copy HDFS data to instance '%s'" % (destination))

                    distcp_command = [
                        "hadoop",
                        "distcp",
                        "-D",
                        "yarn.timeline-service.enabled=false",
                        "-D",
                        f"mapreduce.job.queuename={configuration.get('Sqoop', 'yarnqueue')}",
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
        self, hive_filter_db, hive_filter_table, copy_destination
    ):
        """Schdeule an asynchronous copy of one or more Hive table to the specified destination"""
        logging.debug("Executing copy_operations.schedule_table_async_copy()")

        if self.check_dbimport_instance(instance=copy_destination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        hive_filter_db = hive_filter_db.replace("*", "%").strip()
        hive_filter_table = hive_filter_table.replace("*", "%").strip()

        local_session = self.config_db_session()
        import_tables = aliased(configSchema.ImportTables)
        copy_async_status = aliased(configSchema.CopyAsyncStatus)

        # Check if there are any tabled in this DAG that is marked for copy against the specified destination
        result = (
            local_session.query(copy_async_status)
            .filter(copy_async_status.hive_db.like(hive_filter_db))
            .filter(copy_async_status.hive_table.like(hive_filter_table))
            .filter(copy_async_status.destination == copy_destination)
            .count()
        )

        if result > 0:
            logging.error(
                "There is already tables that matches this DAG's filter that is scheduled for copy against"
            )
            logging.error(
                "the specified destination. This operation cant continue until all the current copies are completed"
            )
            self.remove_temporary_files()
            sys.exit(1)

        # Calculate the source and target HDFS directories
        dbimport_instances = aliased(configSchema.DBImportInstances)

        source_hdfs_address = self.common_operations.hdfs_address
        source_hdfs_basedir = self.common_operations.hdfs_basedir

        row = (
            local_session.query(
                dbimport_instances.hdfs_address, dbimport_instances.hdfs_basedir
            )
            .filter(dbimport_instances.name == copy_destination)
            .one()
        )

        target_hdfs_address = row[0]
        target_hdfs_basedir = row[1]

        logging.debug("sourceHDFSaddress: %s" % (source_hdfs_address))
        logging.debug("sourceHDFSbasedir: %s" % (source_hdfs_basedir))
        logging.debug("targetHDFSaddress: %s" % (target_hdfs_address))
        logging.debug("targetHDFSbasedir: %s" % (target_hdfs_basedir))

        # Fetch a list of tables that match the database and table filter
        result = pd.DataFrame(
            local_session.query(
                import_tables.table_id,
                import_tables.hive_db,
                import_tables.hive_table,
                import_tables.dbalias,
            )
            .filter(import_tables.hive_db.like(hive_filter_db))
            .filter(import_tables.hive_table.like(hive_filter_table))
        )

        for _index, row in result.iterrows():
            logging.info(
                "Schedule asynchronous copy for %s.%s",
                row["hive_db"],
                row["hive_table"],
            )
            self.import_config.hive_db = row["hive_db"]
            self.import_config.hive_table = row["hive_table"]

            try:
                self.import_config.get_import_config()
            except InvalidConfigurationError as errMsg:
                logging.error(errMsg)
                self.import_config.remove_temporary_files()
                sys.exit(1)

            logging.debug("table_id: %s", self.import_config.table_id)
            logging.debug(
                "import_is_incremental: %s", self.import_config.import_is_incremental
            )

            if (
                self.import_config.import_is_incremental and includeIncrImports == False  # noqa: F821  # TODO(#163): not a parameter of this method
            ):
                logging.warning(
                    "Asynchronous copy for incremental table %s.%s skipped",
                    row["hive_db"],
                    row["hive_table"],
                )
                continue

            source_hdfs_dir = (
                (source_hdfs_basedir + "/" + row["hive_db"] + "/" + row["hive_table"])
                .replace("$", "")
                .replace(" ", "")
            )
            target_hdfs_dir = (
                (target_hdfs_basedir + "/" + row["hive_db"] + "/" + row["hive_table"])
                .replace("$", "")
                .replace(" ", "")
            )
            logging.debug("sourceHDFSdir: %s" % (source_hdfs_dir))
            logging.debug("targetHDFSdir: %s" % (target_hdfs_dir))

            result = (
                local_session.query(copy_async_status)
                .filter(copy_async_status.table_id == row["table_id"])
                .filter(copy_async_status.destination == copy_destination)
                .count()
            )

            if result == 0:
                newcopy_async_status = configSchema.CopyAsyncStatus(
                    table_id=row["table_id"],
                    hive_db=row["hive_db"],
                    hive_table=row["hive_table"],
                    destination=copy_destination,
                    hdfs_source_path="%s%s" % (source_hdfs_address, source_hdfs_dir),
                    hdfs_target_path="%s%s" % (target_hdfs_address, target_hdfs_dir),
                    copy_status=0,
                )
                local_session.add(newcopy_async_status)
                local_session.commit()

        local_session.close()

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

        local_session = self.config_db_session()
        airflow_import_dags = aliased(configSchema.AirflowImportDAGs)
        import_tables = aliased(configSchema.ImportTables)
        copy_async_status = aliased(configSchema.CopyAsyncStatus)

        hive_filter_str = (
            local_session.query(airflow_import_dags.filter_hive)
            .select_from(airflow_import_dags)
            .filter(airflow_import_dags.dag_name == airflow_dag_name)
            .one()
        )

        for hive_filter in hive_filter_str[0].split(";"):
            hive_filter_db = hive_filter.split(".")[0]
            hive_filter_table = hive_filter.split(".")[1]

            hive_filter_db = hive_filter_db.replace("*", "%").strip()
            hive_filter_table = hive_filter_table.replace("*", "%").strip()

            # Check if there are any tabled in this DAG that is marked for copy against the specified destination
            result = (
                local_session.query(copy_async_status)
                .filter(copy_async_status.hive_db.like(hive_filter_db))
                .filter(copy_async_status.hive_table.like(hive_filter_table))
                .filter(copy_async_status.destination == copy_destination)
                .count()
            )

            if result > 0:
                logging.error(
                    "There is already tables that matches this DAG's filter that is scheduled for copy against"
                )
                logging.error(
                    "the specified destination. This operation cant continue until all the current copies are completed"
                )
                self.remove_temporary_files()
                sys.exit(1)

        # Calculate the source and target HDFS directories
        dbimport_instances = aliased(configSchema.DBImportInstances)

        source_hdfs_address = self.common_operations.hdfs_address
        source_hdfs_basedir = self.common_operations.hdfs_basedir

        row = (
            local_session.query(
                dbimport_instances.hdfs_address, dbimport_instances.hdfs_basedir
            )
            .filter(dbimport_instances.name == copy_destination)
            .one()
        )

        target_hdfs_address = row[0]
        target_hdfs_basedir = row[1]

        logging.debug("sourceHDFSaddress: %s" % (source_hdfs_address))
        logging.debug("sourceHDFSbasedir: %s" % (source_hdfs_basedir))
        logging.debug("targetHDFSaddress: %s" % (target_hdfs_address))
        logging.debug("targetHDFSbasedir: %s" % (target_hdfs_basedir))

        # We need this for loop here again as there might be multi table specifications that ovarlap each other. So it's important that we
        # dont start the actual schedule of copies before we tested them all. Otherwise we might block ourself during the schedule
        for hive_filter in hive_filter_str[0].split(";"):
            hive_filter_db = hive_filter.split(".")[0]
            hive_filter_table = hive_filter.split(".")[1]

            hive_filter_db = hive_filter_db.replace("*", "%").strip()
            hive_filter_table = hive_filter_table.replace("*", "%").strip()

            logging.debug("hiveFilterDB: %s" % (hive_filter_db))
            logging.debug("hiveFilterTable: %s" % (hive_filter_table))

            # Fetch a list of tables that match the database and table filter
            result = pd.DataFrame(
                local_session.query(
                    import_tables.table_id,
                    import_tables.hive_db,
                    import_tables.hive_table,
                    import_tables.dbalias,
                )
                .filter(import_tables.hive_db.like(hive_filter_db))
                .filter(import_tables.hive_table.like(hive_filter_table))
            )

            for _index, row in result.iterrows():
                logging.info(
                    "Schedule asynchronous copy for %s.%s",
                    row["hive_db"],
                    row["hive_table"],
                )
                self.import_config.hive_db = row["hive_db"]
                self.import_config.hive_table = row["hive_table"]

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
                        row["hive_db"],
                        row["hive_table"],
                    )
                    continue

                source_hdfs_dir = (
                    (
                        source_hdfs_basedir
                        + "/"
                        + row["hive_db"]
                        + "/"
                        + row["hive_table"]
                    )
                    .replace("$", "")
                    .replace(" ", "")
                )
                target_hdfs_dir = (
                    (
                        target_hdfs_basedir
                        + "/"
                        + row["hive_db"]
                        + "/"
                        + row["hive_table"]
                    )
                    .replace("$", "")
                    .replace(" ", "")
                )
                logging.debug("sourceHDFSdir: %s" % (source_hdfs_dir))
                logging.debug("targetHDFSdir: %s" % (target_hdfs_dir))

                result = (
                    local_session.query(copy_async_status)
                    .filter(copy_async_status.table_id == row["table_id"])
                    .filter(copy_async_status.destination == copy_destination)
                    .count()
                )

                if result == 0:
                    newcopy_async_status = configSchema.CopyAsyncStatus(
                        table_id=row["table_id"],
                        hive_db=row["hive_db"],
                        hive_table=row["hive_table"],
                        destination=copy_destination,
                        hdfs_source_path="%s%s"
                        % (source_hdfs_address, source_hdfs_dir),
                        hdfs_target_path="%s%s"
                        % (target_hdfs_address, target_hdfs_dir),
                        copy_status=0,
                    )
                    local_session.add(newcopy_async_status)
                    local_session.commit()

        local_session.close()

    def copy_airflow_export_dag(
        self,
        airflow_dag_name,
        copy_destination,
        set_auto_regenerate_dag=False,
        deploy_mode=False,
    ):
        """Copy an Airflow Export DAG, including connections, tables and columns to a remote DBImport instance"""

        if self.check_dbimport_instance(instance=copy_destination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        local_session = self.config_db_session()
        airflow_export_dags = aliased(configSchema.AirflowExportDAGs)
        airflow_tasks = aliased(configSchema.AirflowTasks)
        export_tables = aliased(configSchema.ExportTables)

        # Check if Airflow DAG exists with that name, and if so, get all the details
        airflow_dag = pd.DataFrame(
            local_session.query(configSchema.AirflowExportDAGs.__table__).filter(
                configSchema.AirflowExportDAGs.dag_name == airflow_dag_name
            )
        )

        if airflow_dag.empty == True:
            logging.error("The specified Airflow DAG does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        # airflowDAG now contains a Pandas DF with the complete DAG configuraiton. This needs to be synced to the remote DBImport instance
        if self.connect_remote_dbimport_instance(instance=copy_destination):
            remote_session = self.remote_instance_config_db_session()

            # Check if the DAG exists on the remote DBImport instance
            result = (
                remote_session.query(airflow_export_dags)
                .filter(airflow_export_dags.dag_name == airflow_dag_name)
                .count()
            )

            if result == 0:
                # Table does not exist in target system. Lets create a skeleton record
                new_airflow_export_dags = configSchema.AirflowExportDAGs(
                    dag_name=airflow_dag_name, filter_dbalias="None"
                )
                remote_session.add(new_airflow_export_dags)
                remote_session.commit()

            # Create dictonary to be used to update the values in airflow_import_dag on the remote Instance
            update_dict = {}
            filter_db_alias = ""
            filter_schema = ""
            filter_table = ""
            for name, values in airflow_dag.iteritems():
                if name in ("dag_name"):
                    continue

                if name == "filter_dbalias":
                    filter_db_alias = str(values[0])

                if name == "filter_target_schema":
                    filter_schema = str(values[0])
                    if filter_schema == "None":
                        filter_schema = "%"

                if name == "filter_target_table":
                    filter_table = str(values[0])
                    if filter_table == "None":
                        filter_table = "%"

                value = str(values[0])
                if value == "None":
                    value = None

                update_dict["%s" % (name)] = value

            if set_auto_regenerate_dag == True:
                update_dict["auto_regenerate_dag"] = "1"

            if update_dict["schedule_interval"] == None:
                update_dict["schedule_interval"] = "None"

            # Update the values in airflow_import_dag on the remote instance
            (
                remote_session.query(configSchema.AirflowExportDAGs)
                .filter(configSchema.AirflowExportDAGs.dag_name == airflow_dag_name)
                .update(update_dict)
            )
            remote_session.commit()

            if deploy_mode == False:
                logging.info("DAG definition copied to remote DBImport successfully")
            else:
                logging.info("DAG definition deployed successfully")

            # **************************
            # Prepair and trigger a copy of all schemas to the other cluster
            # **************************

            self.copy_export_schema_to_destination(
                filter_db_alias=filter_db_alias,
                filter_schema=filter_schema,
                filter_table=filter_table,
                destination=copy_destination,
                deploy_mode=deploy_mode,
            )

            if deploy_mode == False:
                logging.info(
                    "Schema definitions copied to remote DBImport successfully"
                )
            else:
                logging.info("Schema definitions deployed successfully")

            # **************************
            # Copy custom tasks from airflow_tasks table
            # **************************

            airflow_tasks_result = pd.DataFrame(
                local_session.query(configSchema.AirflowTasks.__table__).filter(
                    configSchema.AirflowTasks.dag_name == airflow_dag_name
                )
            )

            for _index, row in airflow_tasks_result.iterrows():
                # Check if the Airflow Task exists on the remote DBImport instance
                result = (
                    remote_session.query(airflow_tasks)
                    .filter(airflow_tasks.dag_name == airflow_dag_name)
                    .filter(airflow_tasks.task_name == row["task_name"])
                    .count()
                )

                if result == 0:
                    # Create a new row in ImportColumns if it doesnt exists
                    new_airflow_task = configSchema.AirflowTasks(
                        dag_name=airflow_dag_name, task_name=row["task_name"]
                    )
                    remote_session.add(new_airflow_task)
                    remote_session.commit()

                update_dict = {}
                for name, value in row.iteritems():
                    if name in ("dag_name", "task_name"):
                        continue

                    update_dict["%s" % (name)] = value

                # Update the values in airflow_tasks on the remote instance
                (
                    remote_session.query(configSchema.AirflowTasks)
                    .filter(configSchema.AirflowTasks.dag_name == airflow_dag_name)
                    .filter(configSchema.AirflowTasks.task_name == row["task_name"])
                    .update(update_dict)
                )
                remote_session.commit()

            if airflow_tasks_result.empty == False:
                if deploy_mode == False:
                    logging.info(
                        "DAG custom tasks copied to remote DBImport successfully"
                    )
                else:
                    logging.info("DAG custom tasks deployed successfully")

            # **************************
            # Convert rows in airflow_dag_sensors to airflow_tasks in remote system
            # **************************

            airflow_dag_sensors_result = pd.DataFrame(
                local_session.query(configSchema.AirflowDAGSensors.__table__).filter(
                    configSchema.AirflowDAGSensors.dag_name == airflow_dag_name
                )
            )

            for _index, row in airflow_dag_sensors_result.iterrows():
                # Check if the Airflow Task exists on the remote DBImport instance
                result = (
                    remote_session.query(airflow_tasks)
                    .filter(airflow_tasks.dag_name == airflow_dag_name)
                    .filter(airflow_tasks.task_name == row["sensor_name"])
                    .count()
                )

                if result == 0:
                    # Create a new row in ImportColumns if it doesnt exists
                    new_airflow_task = configSchema.AirflowTasks(
                        dag_name=airflow_dag_name, task_name=row["sensor_name"]
                    )
                    remote_session.add(new_airflow_task)
                    remote_session.commit()

                update_dict = {}
                update_dict["task_type"] = "DAG Sensor"
                update_dict["placement"] = "before main"
                if row["wait_for_task"] == None:
                    update_dict["task_config"] = "%s.stop" % (row["wait_for_dag"])
                else:
                    update_dict["task_config"] = "%s.%s" % (
                        row["wait_for_dag"],
                        row["wait_for_task"],
                    )

                # Update the values in airflow_tasks on the remote instance
                (
                    remote_session.query(configSchema.AirflowTasks)
                    .filter(configSchema.AirflowTasks.dag_name == airflow_dag_name)
                    .filter(configSchema.AirflowTasks.task_name == row["sensor_name"])
                    .update(update_dict)
                )
                remote_session.commit()

                logging.info(
                    "Converted DAG sensor '%s' to an airflow_tasks entry"
                    % (row["sensor_name"])
                )

    def check_remote_instance_schema(self, remote_instance):
        """Checks that the remote instance is using the same database schema version as the local instance is"""

        if self.check_dbimport_instance(instance=remote_instance) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        local_session = self.config_db_session()
        alembic_version = aliased(configSchema.AlembicVersion)

        if self.connect_remote_dbimport_instance(instance=remote_instance):
            remote_session = self.remote_instance_config_db_session()

            local_version = local_session.query(
                alembic_version.version_num
            ).one_or_none()
            remote_version = remote_session.query(
                alembic_version.version_num
            ).one_or_none()

            if local_version[0] != remote_version[0]:
                logging.error(
                    "The remote DBImport instance is not running with the same version."
                )
                self.remove_temporary_files()
                sys.exit(1)

        else:
            logging.error("Cant connect to remote DBImport instance")
            self.remove_temporary_files()
            sys.exit(1)

        local_session.close()
        remote_session.close()

    def copy_airflow_import_dag(
        self,
        airflow_dag_name,
        copy_destination,
        copy_dag_no_slave=False,
        set_auto_regenerate_dag=False,
        deploy_mode=False,
    ):
        """Copy an Airflow Import DAG, including connections, tables and columns to a remote DBImport instance"""

        if self.check_dbimport_instance(instance=copy_destination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        local_session = self.config_db_session()
        airflow_import_dags = aliased(configSchema.AirflowImportDAGs)
        airflow_tasks = aliased(configSchema.AirflowTasks)
        import_tables = aliased(configSchema.ImportTables)

        # Check if Airflow DAG exists with that name, and if so, get all the details
        airflow_dag = pd.DataFrame(
            local_session.query(configSchema.AirflowImportDAGs.__table__).filter(
                configSchema.AirflowImportDAGs.dag_name == airflow_dag_name
            )
        )

        if airflow_dag.empty == True:
            logging.error("The specified Airflow DAG does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        # airflowDAG now contains a Pandas DF with the complete DAG configuraiton. This needs to be synced to the remote DBImport instance
        if self.connect_remote_dbimport_instance(instance=copy_destination):
            remote_session = self.remote_instance_config_db_session()

            # Check if the DAG exists on the remote DBImport instance
            result = (
                remote_session.query(airflow_import_dags)
                .filter(airflow_import_dags.dag_name == airflow_dag_name)
                .count()
            )

            if result == 0:
                # Table does not exist in target system. Lets create a skeleton record
                new_airflow_import_dags = configSchema.AirflowImportDAGs(
                    dag_name=airflow_dag_name, filter_hive="None"
                )
                remote_session.add(new_airflow_import_dags)
                remote_session.commit()

            # Create dictonary to be used to update the values in airflow_import_dag on the remote Instance
            update_dict = {}
            for name, values in airflow_dag.iteritems():
                if name in ("dag_name"):
                    continue

                if name == "filter_hive":
                    hive_filter_str = str(values[0])

                value = str(values[0])
                if value == "None":
                    value = None

                update_dict["%s" % (name)] = value

            if set_auto_regenerate_dag == True:
                update_dict["auto_regenerate_dag"] = "1"

            if update_dict["schedule_interval"] == None:
                update_dict["schedule_interval"] = "None"

            # Update the values in airflow_import_dag on the remote instance
            (
                remote_session.query(configSchema.AirflowImportDAGs)
                .filter(configSchema.AirflowImportDAGs.dag_name == airflow_dag_name)
                .update(update_dict)
            )
            remote_session.commit()

            if deploy_mode == False:
                logging.info("DAG definition copied to remote DBImport successfully")
            else:
                logging.info("DAG definition deployed successfully")

            # **************************
            # Prepair and trigger a copy of all schemas to the other cluster
            # **************************
            self.copy_destinations = []
            dest_string = "%s;Asynchronous" % (copy_destination)
            self.copy_destinations.append(dest_string)

            for hive_filter in hive_filter_str.split(";"):
                hive_filter_db = hive_filter.split(".")[0]
                hive_filter_table = hive_filter.split(".")[1]

                hive_filter_db = hive_filter_db.replace("*", "%")
                hive_filter_table = hive_filter_table.replace("*", "%")

                result = pd.DataFrame(
                    local_session.query(
                        import_tables.table_id,
                        import_tables.hive_db,
                        import_tables.hive_table,
                        import_tables.dbalias,
                    )
                    .filter(import_tables.hive_db.like(hive_filter_db))
                    .filter(import_tables.hive_table.like(hive_filter_table))
                )

                for _index, row in result.iterrows():
                    self.copy_import_schema_to_destinations(
                        table_id=row["table_id"],
                        hive_db=row["hive_db"],
                        hive_table=row["hive_table"],
                        connection_alias=row["dbalias"],
                        copy_dag_no_slave=copy_dag_no_slave,
                        deploy_mode=deploy_mode,
                    )
            if deploy_mode == False:
                logging.info(
                    "Schema definitions copied to remote DBImport successfully"
                )
            else:
                logging.info("Schema definitions deployed successfully")

            # **************************
            # Copy custom tasks from airflow_tasks table
            # **************************

            airflow_tasks_result = pd.DataFrame(
                local_session.query(configSchema.AirflowTasks.__table__).filter(
                    configSchema.AirflowTasks.dag_name == airflow_dag_name
                )
            )

            copied_jdbc_connections = []
            for _index, row in airflow_tasks_result.iterrows():
                # Check if the Airflow Task exists on the remote DBImport instance
                result = (
                    remote_session.query(airflow_tasks)
                    .filter(airflow_tasks.dag_name == airflow_dag_name)
                    .filter(airflow_tasks.task_name == row["task_name"])
                    .count()
                )

                if result == 0:
                    # Create a new row in ImportColumns if it doesnt exists
                    new_airflow_task = configSchema.AirflowTasks(
                        dag_name=airflow_dag_name, task_name=row["task_name"]
                    )
                    remote_session.add(new_airflow_task)
                    remote_session.commit()

                update_dict = {}
                for name, value in row.iteritems():
                    if name in ("dag_name", "task_name"):
                        continue

                    update_dict["%s" % (name)] = value

                if row["jdbc_dbalias"] not in copied_jdbc_connections:
                    self.copy_jdbc_connection_to_destination(
                        jdbc_connection=row["jdbc_dbalias"],
                        destination=copy_destination,
                        deploy_mode=deploy_mode,
                    )
                    copied_jdbc_connections.append(row["jdbc_dbalias"])

                # Update the values in airflow_tasks on the remote instance
                (
                    remote_session.query(configSchema.AirflowTasks)
                    .filter(configSchema.AirflowTasks.dag_name == airflow_dag_name)
                    .filter(configSchema.AirflowTasks.task_name == row["task_name"])
                    .update(update_dict)
                )
                remote_session.commit()

            if airflow_tasks_result.empty == False:
                if deploy_mode == False:
                    logging.info(
                        "DAG custom tasks copied to remote DBImport successfully"
                    )
                else:
                    logging.info("DAG custom tasks deployed successfully")

        else:
            logging.error("Cant connect to remote DBImport instance")
            self.remove_temporary_files()
            sys.exit(1)

        local_session.close()
        remote_session.close()

    def copy_export_schema_to_destination(
        self,
        filter_db_alias,
        filter_schema,
        filter_table,
        destination,
        deploy_mode=False,
    ):
        """Copy the schema definitions to the target instances"""
        local_session = self.config_db_session()
        export_tables = aliased(configSchema.ExportTables)
        export_columns = aliased(configSchema.ExportColumns)
        jdbc_connections = aliased(configSchema.JDBCConnections)
        dbimport_instances = aliased(configSchema.DBImportInstances)

        if self.connect_remote_dbimport_instance(instance=destination):
            remote_session = self.remote_instance_config_db_session()

            # Check if if we are going to sync the credentials for this destination
            result = (
                local_session.query(dbimport_instances.sync_credentials)
                .select_from(dbimport_instances)
                .filter(dbimport_instances.name == destination)
                .one()
            )

            if result[0] == 1:
                sync_credentials = True
            else:
                sync_credentials = False

            filter_db_alias = filter_db_alias.replace("*", "%")
            filter_schema = filter_schema.replace("*", "%")
            filter_table = filter_table.replace("*", "%")

            result = pd.DataFrame(
                local_session.query(
                    export_tables.table_id,
                    export_tables.hive_db,
                    export_tables.hive_table,
                    export_tables.dbalias,
                    export_tables.target_schema,
                    export_tables.target_table,
                )
                .filter(export_tables.dbalias.like(filter_db_alias))
                .filter(export_tables.target_schema.like(filter_schema))
                .filter(export_tables.target_table.like(filter_table))
            )

            for _index, row in result.iterrows():
                if deploy_mode == False:
                    logging.info(
                        "Copy schema definitions for %s.%s",
                        row["hive_db"],
                        row["hive_table"],
                    )
                else:
                    logging.info(
                        "Deploying schema definitions for %s.%s",
                        row["hive_db"],
                        row["hive_table"],
                    )

                ##################################
                # Update jdbc_connections
                ##################################

                # Check if the jdbcConnection exists on the remote DBImport instance
                result = (
                    remote_session.query(jdbc_connections)
                    .filter(jdbc_connections.dbalias == row["dbalias"])
                    .count()
                )

                if result == 0:
                    new_jdbc_connection = configSchema.JDBCConnections(
                        dbalias=row["dbalias"], jdbc_url=""
                    )
                    remote_session.add(new_jdbc_connection)
                    remote_session.commit()

                # Read the entire import_table row from the source database
                source_jdbc_connection = pd.DataFrame(
                    local_session.query(configSchema.JDBCConnections.__table__).filter(
                        configSchema.JDBCConnections.dbalias == row["dbalias"]
                    )
                )

                # Table to update with values from import_table source
                remote_jdbc_connection = (
                    remote_session.query(configSchema.JDBCConnections.__table__)
                    .filter(configSchema.JDBCConnections.dbalias == row["dbalias"])
                    .one()
                )

                # Create dictonary to be used to update the values in import_table on the remote Instance
                update_dict = {}
                for name, values in source_jdbc_connection.iteritems():
                    if name == "dbalias":
                        continue

                    if not sync_credentials and name in (
                        "credentials",
                        "private_key_path",
                        "public_key_path",
                    ):
                        continue

                    value = str(values[0])
                    if value == "None":
                        value = None

                    update_dict["%s" % (name)] = value

                # Update the values in import_table on the remote instance
                (
                    remote_session.query(configSchema.JDBCConnections)
                    .filter(configSchema.JDBCConnections.dbalias == row["dbalias"])
                    .update(update_dict)
                )
                remote_session.commit()

                ##################################
                # Update export_tables
                ##################################

                # Check if the table exists on the remote DBImport instance
                result = (
                    remote_session.query(export_tables)
                    .filter(export_tables.dbalias == row["dbalias"])
                    .filter(export_tables.target_schema == row["target_schema"])
                    .filter(export_tables.target_table == row["target_table"])
                    .count()
                )

                if result == 0:
                    # Table does not exist in target system. Lets create a skeleton record
                    new_export_table = configSchema.ExportTables(
                        dbalias=row["dbalias"],
                        target_schema=row["target_schema"],
                        target_table=row["target_table"],
                        hive_db=row["hive_db"],
                        hive_table=row["hive_table"],
                    )
                    remote_session.add(new_export_table)
                    remote_session.commit()

                # Get the table_id from the table at the remote instance
                remote_export_table_id = (
                    remote_session.query(export_tables.table_id)
                    .select_from(export_tables)
                    .filter(export_tables.dbalias == row["dbalias"])
                    .filter(export_tables.target_schema == row["target_schema"])
                    .filter(export_tables.target_table == row["target_table"])
                    .one()
                )

                remote_table_id = remote_export_table_id[0]

                # Read the entire export_table row from the source database
                source_table_definition = pd.DataFrame(
                    local_session.query(configSchema.ExportTables.__table__).filter(
                        configSchema.ExportTables.table_id == row["table_id"]
                    )
                )

                # Table to update with values from import_table source
                remote_table_definition = (
                    remote_session.query(configSchema.ExportTables.__table__)
                    .filter(configSchema.ExportTables.table_id == remote_table_id)
                    .one()
                )

                # Create dictonary to be used to update the values in import_table on the remote Instance
                update_dict = {}
                for name, values in source_table_definition.iteritems():
                    if name in (
                        "table_id",
                        "dbalias",
                        "target_schema",
                        "target_table",
                        "sqoop_last_execution",
                    ):
                        continue

                    value = str(values[0])
                    if value == "None":
                        value = None

                    update_dict["%s" % (name)] = value

                # Update the values in import_table on the remote instance
                (
                    remote_session.query(configSchema.ExportTables)
                    .filter(configSchema.ExportTables.table_id == remote_table_id)
                    .update(update_dict)
                )
                remote_session.commit()

                ##################################
                # Update export_colums
                ##################################

                # Read the entire export_columns row from the source database
                source_all_column_definitions = pd.DataFrame(
                    local_session.query(configSchema.ExportColumns.__table__).filter(
                        configSchema.ExportColumns.table_id == row["table_id"]
                    )
                )

                for (
                    _column_index,
                    column_row,
                ) in source_all_column_definitions.iterrows():
                    # Check if the column exists on the remote DBImport instance
                    result = (
                        remote_session.query(export_columns)
                        .filter(export_columns.table_id == remote_table_id)
                        .filter(export_columns.column_name == column_row["column_name"])
                        .count()
                    )

                    if result == 0:
                        # Create a new row in ExportColumns if it doesnt exists
                        new_export_column = configSchema.ExportColumns(
                            table_id=remote_table_id,
                            column_name=column_row["column_name"],
                            last_update_from_hive=str(
                                column_row["last_update_from_hive"]
                            ),
                        )
                        remote_session.add(new_export_column)
                        remote_session.commit()

                    # Get the table_id from the table at the remote instance
                    remote_export_column_id = (
                        remote_session.query(export_columns.column_id)
                        .select_from(export_columns)
                        .filter(export_columns.table_id == remote_table_id)
                        .filter(export_columns.column_name == column_row["column_name"])
                        .one()
                    )

                    remote_column_id = remote_export_column_id[0]

                    # Read the entire export_columnis row from the source database
                    source_column_definition = pd.DataFrame(
                        local_session.query(
                            configSchema.ExportColumns.__table__
                        ).filter(
                            configSchema.ExportColumns.column_id
                            == column_row["column_id"]
                        )
                    )

                    # Table to update with values from export_columns source
                    remote_column_definition = (
                        remote_session.query(configSchema.ExportColumns.__table__)
                        .filter(
                            configSchema.ExportColumns.column_id == remote_column_id
                        )
                        .one()
                    )

                    # Create dictonary to be used to update the values in export_table on the remote Instance
                    update_dict = {}
                    for name, values in source_column_definition.iteritems():
                        if name in ("table_id", "column_id", "column_name"):
                            continue

                        value = str(values[0])
                        if value == "None":
                            value = None

                        update_dict["%s" % (name)] = value

                    # Update the values in export_table on the remote instance
                    (
                        remote_session.query(configSchema.ExportColumns)
                        .filter(
                            configSchema.ExportColumns.column_id == remote_column_id
                        )
                        .update(update_dict)
                    )
                    remote_session.commit()

            remote_session.close()
        else:
            logging.warning(
                "Connection failed! No data will be copied to instance '%s'"
                % (destination)
            )

        local_session.close()

    def copy_import_schema_to_destinations(
        self,
        table_id=None,
        hive_db=None,
        hive_table=None,
        connection_alias=None,
        copy_dag_no_slave=False,
        deploy_mode=False,
    ):
        """Copy the schema definitions to the target instances"""
        local_session = self.config_db_session()

        if self.copy_destinations == None:
            if deploy_mode == False:
                logging.warning(
                    "There are no destination for this table to receive a copy"
                )
            else:
                logging.warning("There are no destination for this deployment")
            return

        if (
            table_id == None
            and hive_db == None
            and hive_table == None
            and connection_alias == None
        ):
            # This happens during a normal import
            table_id = self.import_config.table_id
            hive_db = self.import_config.hive_db
            hive_table = self.import_config.hive_table
            connection_alias = self.import_config.connection_alias
            print_destination = True
        else:
            # This happens during a "manage --copyAirflowImportDAG" or during deployment.
            # And then the destination is specified in cmd and not needed to be printed
            print_destination = False

        for dest_and_method in self.copy_destinations:
            destination = dest_and_method.split(";")[0]
            method = dest_and_method.split(";")[1]
            if self.connect_remote_dbimport_instance(instance=destination):
                if print_destination:
                    logging.info(
                        "Copy schema definitions for %s.%s to instance '%s'",
                        hive_db,
                        hive_table,
                        destination,
                    )
                else:
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
                remote_session = self.remote_instance_config_db_session()

                jdbc_connections = aliased(configSchema.JDBCConnections)
                import_tables = aliased(configSchema.ImportTables)
                import_columns = aliased(configSchema.ImportColumns)
                dbimport_instances = aliased(configSchema.DBImportInstances)

                # Check if if we are going to sync the credentials for this destination
                result = (
                    local_session.query(dbimport_instances.sync_credentials)
                    .select_from(dbimport_instances)
                    .filter(dbimport_instances.name == destination)
                    .one()
                )

                if result[0] == 1:
                    sync_credentials = True
                else:
                    sync_credentials = False

                # Check if the table exists on the remote DBImport instance
                result = (
                    remote_session.query(import_tables)
                    .filter(import_tables.hive_db == hive_db)
                    .filter(import_tables.hive_table == hive_table)
                    .count()
                )

                if result == 0:
                    # Table does not exist in target system. Lets create a skeleton record
                    new_import_table = configSchema.ImportTables(
                        hive_db=hive_db,
                        hive_table=hive_table,
                        dbalias=connection_alias,
                        source_schema="",
                        source_table="",
                    )
                    remote_session.add(new_import_table)
                    remote_session.commit()

                # Get the table_id from the table at the remote instance
                remote_import_table_id = (
                    remote_session.query(import_tables.table_id, import_tables.dbalias)
                    .select_from(import_tables)
                    .filter(import_tables.hive_db == hive_db)
                    .filter(import_tables.hive_table == hive_table)
                    .one()
                )

                remote_table_id = remote_import_table_id[0]
                jdbc_connection = remote_import_table_id[1]

                ##################################
                # Update jdbc_connections
                ##################################

                self.copy_jdbc_connection_to_destination(
                    jdbc_connection=jdbc_connection,
                    deploy_mode=deploy_mode,
                    destination=destination,
                )

                ##################################
                # Update import_colums
                ##################################

                # Read the entire import_table row from the source database
                source_all_column_definitions = pd.DataFrame(
                    local_session.query(configSchema.ImportColumns.__table__)
                    .filter(configSchema.ImportColumns.table_id == table_id)
                    .order_by(configSchema.ImportColumns.column_order)
                )

                target_all_column_definitions = pd.DataFrame(
                    remote_session.query(configSchema.ImportColumns.__table__)
                    .filter(configSchema.ImportColumns.table_id == remote_table_id)
                    .order_by(configSchema.ImportColumns.column_order)
                )

                if not source_all_column_definitions.empty:
                    # sourceAllColumnDefinitions might be empty if the table data only exists in import_tables and not in import_columns

                    if target_all_column_definitions.empty:
                        # If the target DF is empty, it means that the table does not exist in the target system. So to be able to continue with the merge, we need the columns
                        # to be presented. So we set them to the same as the sourceDefinition
                        target_all_column_definitions = pd.DataFrame(
                            data=None, columns=source_all_column_definitions.columns
                        )

                    source_all_column_definitions.rename(
                        columns={
                            "table_id": "source_table_id",
                            "column_id": "source_column_id",
                        },
                        inplace=True,
                    )
                    target_all_column_definitions.rename(
                        columns={
                            "table_id": "target_table_id",
                            "column_id": "target_column_id",
                        },
                        inplace=True,
                    )
                    # sourceAllColumnDefinitions = sourceAllColumnDefinitions.replace({np.nan: None})
                    # targetAllColumnDefinitions = targetAllColumnDefinitions.replace({np.nan: None})

                    # 					pd.set_option('display.max_columns', None)
                    # 					print("DEBUG 01")
                    # 					print(sourceAllColumnDefinitions)
                    # 					print("===============================================")
                    # 					print(targetAllColumnDefinitions)
                    # 					print("===============================================")
                    # Get the difference between source and target column definitions
                    column_difference = pd.merge(
                        source_all_column_definitions,
                        target_all_column_definitions,
                        on=None,
                        how="outer",
                        indicator="Exist",
                    )
                    # 					print(columnDifference)
                    # 					print("DEBUG 02")
                    column_difference_left_only = column_difference[
                        column_difference.Exist == "left_only"
                    ]
                    column_difference_left_only = column_difference_left_only.replace(
                        {np.nan: None}
                    )

                    for (
                        _column_index,
                        column_row,
                    ) in column_difference_left_only.iterrows():
                        source_column_name = column_row["source_column_name"]

                        # Check if column exists in target database
                        if (
                            len(
                                target_all_column_definitions.loc[
                                    target_all_column_definitions["source_column_name"]
                                    == source_column_name
                                ]
                            )
                            == 0
                        ):
                            logging.debug(
                                "Source Column Name '%s' does not exists in target"
                                % (source_column_name)
                            )
                            new_import_column = configSchema.ImportColumns(
                                table_id=remote_table_id,
                                column_name=column_row["column_name"],
                                hive_db=hive_db,
                                hive_table=hive_table,
                                source_column_name=column_row["source_column_name"],
                                column_type="",
                                source_column_type="",
                                last_update_from_source=str(
                                    column_row["last_update_from_source"]
                                ),
                            )
                            remote_session.add(new_import_column)
                            remote_session.commit()

                        # Get the table_id from the table at the remote instance
                        remote_import_column_id = (
                            remote_session.query(import_columns.column_id)
                            .select_from(import_columns)
                            .filter(import_columns.table_id == remote_table_id)
                            .filter(
                                import_columns.source_column_name
                                == column_row["source_column_name"]
                            )
                            .one()
                        )

                        remote_column_id = remote_import_column_id[0]

                        # Create dictonary to be used to update the values in import_table on the remote Instance
                        update_dict = {}
                        for name, values in column_row.iteritems():
                            if name in (
                                "source_table_id",
                                "source_column_id",
                                "source_column_name",
                                "target_table_id",
                                "target_column_id",
                                "hive_db",
                                "hive_table",
                                "Exist",
                            ):
                                continue

                            # 							print("%s = %s"%(name, values))
                            value = str(values)
                            if value == "None" and name != "anonymization_function":
                                # The 'anonymization_function' column contains the text 'None' if it doesnt anonymize anything.
                                # It's a Enum, so it's ok. But we need to handle it here
                                value = None

                            update_dict["%s" % (name)] = value

                        # Update the values in import_table on the remote instance
                        (
                            remote_session.query(configSchema.ImportColumns)
                            .filter(
                                configSchema.ImportColumns.column_id == remote_column_id
                            )
                            .update(update_dict)
                        )
                        remote_session.commit()

                ##################################
                # Update import_tables
                ##################################

                # Read the entire import_table row from the source database
                source_table_definition = pd.DataFrame(
                    local_session.query(configSchema.ImportTables.__table__).filter(
                        configSchema.ImportTables.table_id == table_id
                    )
                )

                # Table to update with values from import_table source
                remote_table_definition = (
                    remote_session.query(configSchema.ImportTables.__table__)
                    .filter(configSchema.ImportTables.table_id == remote_table_id)
                    .one()
                )

                # Create dictonary to be used to update the values in import_table on the remote Instance
                update_dict = {}
                jdbc_connection = ""
                for name, values in source_table_definition.iteritems():
                    if name in (
                        "table_id",
                        "hive_db",
                        "hive_table",
                        "copy_finished",
                        "copy_slave",
                    ):
                        continue

                    value = str(values[0])
                    if value == "None":
                        value = None

                    update_dict["%s" % (name)] = value

                if deploy_mode == False:
                    if copy_dag_no_slave == True:
                        update_dict["copy_slave"] = 0
                        update_dict["copy_finished"] = None
                    else:
                        update_dict["copy_slave"] = 1
                        if method == "Synchronous":
                            update_dict["copy_finished"] = str(
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                            )
                        else:
                            update_dict["copy_finished"] = None
                else:
                    update_dict["copy_slave"] = 0
                    update_dict["copy_finished"] = None

                # Update the values in import_table on the remote instance
                (
                    remote_session.query(configSchema.ImportTables)
                    .filter(configSchema.ImportTables.table_id == remote_table_id)
                    .update(update_dict)
                )
                remote_session.commit()
                remote_session.close()

            else:
                logging.warning(
                    "Connection failed! No data will be copied to instance '%s'"
                    % (destination)
                )

        local_session.close()

    def copy_jdbc_connection_to_destination(
        self, jdbc_connection, destination, deploy_mode=False
    ):
        """Copy the JDBC Connection to the target instances"""
        local_session = self.config_db_session()

        if self.copy_destinations == None:
            if deploy_mode == False:
                logging.warning(
                    "There are no destination for this table to receive a copy"
                )
            else:
                logging.warning("There are no destination for this deployment")
            return

        remote_session = self.remote_instance_config_db_session()

        jdbc_connections = aliased(configSchema.JDBCConnections)
        dbimport_instances = aliased(configSchema.DBImportInstances)

        # Check if if we are going to sync the credentials for this destination
        result = (
            local_session.query(dbimport_instances.sync_credentials)
            .select_from(dbimport_instances)
            .filter(dbimport_instances.name == destination)
            .one()
        )

        if result[0] == 1:
            sync_credentials = True
        else:
            sync_credentials = False

        # Check if the jdbcConnection exists on the remote DBImport instance
        result = (
            remote_session.query(jdbc_connections)
            .filter(jdbc_connections.dbalias == jdbc_connection)
            .count()
        )

        if result == 0:
            new_jdbc_connection = configSchema.JDBCConnections(
                dbalias=jdbc_connection, jdbc_url=""
            )
            remote_session.add(new_jdbc_connection)
            remote_session.commit()

        # Read the entire import_table row from the source database
        source_jdbc_connection = pd.DataFrame(
            local_session.query(configSchema.JDBCConnections.__table__).filter(
                configSchema.JDBCConnections.dbalias == jdbc_connection
            )
        )

        # Table to update with values from import_table source
        remote_jdbc_connection = (
            remote_session.query(configSchema.JDBCConnections.__table__)
            .filter(configSchema.JDBCConnections.dbalias == jdbc_connection)
            .one()
        )

        # Create dictonary to be used to update the values in import_table on the remote Instance
        update_dict = {}
        for name, values in source_jdbc_connection.iteritems():
            if name == "dbalias":
                continue

            if not sync_credentials and name in (
                "credentials",
                "private_key_path",
                "public_key_path",
            ):
                continue

            value = str(values[0])
            if value == "None":
                value = None

            update_dict["%s" % (name)] = value

        # Update the values in jdbc_connections on the remote instance
        (
            remote_session.query(configSchema.JDBCConnections)
            .filter(configSchema.JDBCConnections.dbalias == jdbc_connection)
            .update(update_dict)
        )
        remote_session.commit()

        local_session.close()
        remote_session.close()

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
        session = self.config_db_session()
        dbimport_instances = aliased(configSchema.DBImportInstances)

        row = (
            session.query(dbimport_instances.hdfs_address)
            .filter(dbimport_instances.name == remote_instance)
            .one()
        )

        remote_hdfs_address = row[0]
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
            f"mapreduce.job.queuename={configuration.get('Sqoop', 'yarnqueue')}",
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
