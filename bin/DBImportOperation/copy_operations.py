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
import fnmatch
import Crypto
import binascii
from subprocess import Popen, PIPE
from ConfigReader import configuration
from common.Singleton import Singleton
from common.Exceptions import *
import mysql.connector
from mysql.connector import errorcode
from common.Singleton import Singleton
from common import constants as constant
from DBImportConfig import import_config
from DBImportConfig import configSchema
from DBImportOperation import common_operations
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select
from sqlalchemy.orm import aliased, sessionmaker, Query


class operation(object, metaclass=Singleton):
    def __init__(self):
        logging.debug("Executing copy_operations.__init__()")
        self.Hive_DB = None
        self.Hive_Table = None
        self.startDate = None
        self.configDBSession = None
        self.remoteInstanceConfigDBEngine = None
        self.remoteInstanceConfigDBSession = None
        self.copyDestinations = None

        self.common_operations = common_operations.operation()
        self.import_config = import_config.config()

        self.Hive_DB = self.common_operations.Hive_DB
        self.Hive_Table = self.common_operations.Hive_Table
        self.startDate = self.import_config.startDate

        self.import_config.common_config.connectSQLAlchemy()
        self.configDBSession = self.import_config.common_config.configDBSession
        self.crypto = self.import_config.common_config.crypto
        self.debugLogLevel = self.import_config.common_config.debugLogLevel

        logging.debug("Executing copy_operations.__init__() - Finished")

    def remove_temporary_files(self):
        self.import_config.remove_temporary_files()

    def checkDBImportInstance(self, instance):
        """Checks if instance exists in the dbimport_instances table"""

        session = self.configDBSession()
        dbimportInstances = aliased(configSchema.dbimportInstances)

        result = (
            session.query(dbimportInstances.name)
            .select_from(dbimportInstances)
            .filter(dbimportInstances.name == instance)
            .count()
        )

        if result == 0:
            logging.error(
                "No DBImport Instance with that name can be found in table 'dbimport_instances'"
            )
            self.remove_temporary_files()
            sys.exit(1)

    def encryptUserPassword(self, instance, username, password):
        """Encrypts the username and password and store them in the 'credentials column' of 'dbimport_instances' table"""

        self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
        self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))

        strToEncrypt = "%s %s\n" % (username, password)
        encryptedStr = self.crypto.encrypt(strToEncrypt)

        if encryptedStr == None or encryptedStr == "":
            logging.error("Cant encrypt username and password. Please contact support")
            self.remove_temporary_files()
            sys.exit(1)

        session = self.configDBSession()
        dbimportInstances = aliased(configSchema.dbimportInstances)

        DBImportInstance = (
            session.query(dbimportInstances)
            .filter(dbimportInstances.name == instance)
            .one()
        )

        DBImportInstance.db_credentials = encryptedStr
        session.commit()

    def getCopyDestinations(self, hiveDB, hiveTable):
        """Return the Copy Phase that the specified table have"""

        session = self.configDBSession()
        copyTables = aliased(configSchema.copyTables)

        self.copyDestinations = []

        copyTablesResult = pd.DataFrame(
            session.query(
                copyTables.copy_id,
                copyTables.hive_filter,
                copyTables.destination,
                copyTables.data_transfer,
            ).all()
        ).fillna("")
        for index, row in copyTablesResult.iterrows():
            if ";" in row["destination"]:
                logging.error("';' not supported in the Copy Destination name")
                self.remove_temporary_files()
                sys.exit(1)

            for hiveFilterSplit in row["hive_filter"].split(";"):
                if "." not in hiveFilterSplit:
                    logging.warning(
                        "The filter in table 'copy_tables' with copy_id = %s contains an invalid filter. Missing a . in the database.table format"
                        % (row["copy_id"])
                    )
                else:
                    filterDB = hiveFilterSplit.split(".")[0]
                    filterTable = hiveFilterSplit.split(".")[1]

                    if fnmatch.fnmatch(hiveDB, filterDB) and fnmatch.fnmatch(
                        hiveTable, filterTable
                    ):
                        destString = "%s;%s" % (
                            row["destination"],
                            row["data_transfer"],
                        )
                        destStringASync = "%s;Asynchronous" % (row["destination"])
                        destStringSync = "%s;Synchronous" % (row["destination"])
                        if (
                            destStringSync in self.copyDestinations
                            and destString == destStringASync
                        ):
                            # ASync have priority. So if sync is already in there, we remove it and add async
                            self.copyDestinations.remove(destStringSync)
                        if (
                            destStringASync not in self.copyDestinations
                            and destStringSync not in self.copyDestinations
                        ):
                            self.copyDestinations.append(destString)

        if self.copyDestinations == []:
            self.copyDestinations = None
        return self.copyDestinations

    def connectRemoteDBImportInstance(self, instance):
        """Connects to the configuration database with SQLAlchemy"""

        connectStatus = True
        session = self.configDBSession()
        dbimportInstances = aliased(configSchema.dbimportInstances)

        row = (
            session.query(
                dbimportInstances.db_hostname,
                dbimportInstances.db_port,
                dbimportInstances.db_database,
                dbimportInstances.db_credentials,
            )
            .filter(dbimportInstances.name == instance)
            .one()
        )

        if row[3] == None:
            logging.warning(
                "There is no credentials saved in 'dbimport_instance' for %s"
                % (instance)
            )
            return False

        # Set the keys to the value in the configuration file. This is needed if the key was overridden in jdbc_connections
        self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
        self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))

        try:
            db_credentials = self.crypto.decrypt(row[3])
        except binascii.Error as err:
            logging.warning(
                "Decryption of credentials resulted in error with text: '%s'" % err
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

        instanceConnectStr = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            username,
            password,
            row[0],
            row[1],
            row[2],
        )

        if self.remoteInstanceConfigDBEngine != None:
            try:
                self.remoteInstanceConfigDBEngine.dispose()
            except:
                print(
                    "Unexpected warning when closing connection to DBImport Instance database: "
                )
                print(sys.exc_info())

        try:
            self.remoteInstanceConfigDBEngine = sa.create_engine(
                instanceConnectStr, echo=self.debugLogLevel, pool_pre_ping=True
            )
            self.remoteInstanceConfigDBEngine.connect()
            self.remoteInstanceConfigDBSession = sessionmaker(
                bind=self.remoteInstanceConfigDBEngine
            )

        except sa.exc.OperationalError as err:
            logging.warning("%s" % err)
            connectStatus = False
        # 			self.remove_temporary_files()
        # 			sys.exit(1)
        except:
            print("Unexpected warning: ")
            print(sys.exc_info())
            connectStatus = False
        # 			self.remove_temporary_files()
        # 			sys.exit(1)

        return connectStatus

    def isPreviousCopyCompleted(self):
        """Returns True or False if the previous copy was completed. Returns True if there is nothing to copy.
        If it is a full import, then it will return True if the previous copy have status 0 (not started)."""

        session = self.configDBSession()
        copyASyncStatus = aliased(configSchema.copyASyncStatus)

        if self.copyDestinations == None:
            return True

        ongoingCopy = False

        for destAndMethod in self.copyDestinations:
            destination = destAndMethod.split(";")[0]
            method = destAndMethod.split(";")[1]

            if self.import_config.import_is_incremental == True:
                result = (
                    session.query(copyASyncStatus)
                    .filter(copyASyncStatus.table_id == self.import_config.table_id)
                    .filter(copyASyncStatus.destination == destination)
                    .count()
                )
            else:
                result = (
                    session.query(copyASyncStatus)
                    .filter(copyASyncStatus.table_id == self.import_config.table_id)
                    .filter(copyASyncStatus.destination == destination)
                    .filter(copyASyncStatus.copy_status > 0)
                    .count()
                )

            if result > 0:
                ongoingCopy = True

        session.close()

        if ongoingCopy == True:
            return False
        else:
            return True

    def copyDataToDestinations(self):
        session = self.configDBSession()

        sourceHDFSaddress = self.common_operations.hdfs_address
        sourceHDFSbasedir = self.common_operations.hdfs_basedir

        if self.copyDestinations == None:
            logging.warning("There are no destination for this table to receive a copy")
            return

        for destAndMethod in self.copyDestinations:
            destination = destAndMethod.split(";")[0]
            method = destAndMethod.split(";")[1]

            # Calculate the source and target HDFS directories
            dbimportInstances = aliased(configSchema.dbimportInstances)

            row = (
                session.query(
                    dbimportInstances.hdfs_address, dbimportInstances.hdfs_basedir
                )
                .filter(dbimportInstances.name == destination)
                .one()
            )

            targetHDFSaddress = row[0]
            targetHDFSbasedir = row[1]

            sourceHDFSdir = (
                (sourceHDFSbasedir + "/" + self.Hive_DB + "/" + self.Hive_Table)
                .replace("$", "")
                .replace(" ", "")
            )
            targetHDFSdir = (
                (targetHDFSbasedir + "/" + self.Hive_DB + "/" + self.Hive_Table)
                .replace("$", "")
                .replace(" ", "")
            )

            if method == "Asynchronous":
                copyASyncStatus = aliased(configSchema.copyASyncStatus)

                result = (
                    session.query(copyASyncStatus)
                    .filter(copyASyncStatus.table_id == self.import_config.table_id)
                    .filter(copyASyncStatus.destination == destination)
                    .count()
                )

                if result == 0:
                    # No current copy ongoing for this table. Just insert it into the table
                    newcopyASyncStatus = configSchema.copyASyncStatus(
                        table_id=self.import_config.table_id,
                        hive_db=self.import_config.Hive_DB,
                        hive_table=self.import_config.Hive_Table,
                        destination=destination,
                        hdfs_source_path="%s%s" % (sourceHDFSaddress, sourceHDFSdir),
                        hdfs_target_path="%s%s" % (targetHDFSaddress, targetHDFSdir),
                        copy_status=0,
                    )
                    session.add(newcopyASyncStatus)
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

                    if self.import_config.import_is_incremental == True:
                        logging.error(
                            "This is an incremental import and the previous copy to the other cluster is not finnished yet."
                        )
                        logging.error(
                            "Make sure that the other cluster received the data before you make a new copy"
                        )
                        self.remove_temporary_files()
                        sys.exit(1)

                    if self.import_config.import_with_history_table == True:
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
                        session.query(copyASyncStatus)
                        .filter(copyASyncStatus.table_id == self.import_config.table_id)
                        .filter(copyASyncStatus.destination == destination)
                        .filter(copyASyncStatus.copy_status > 0)
                        .count()
                    )

                    if result == 0:
                        updateDict = {}
                        updateDict["last_status_update"] = str(
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        )
                        updateDict["hdfs_source_path"] = "%s%s" % (
                            sourceHDFSaddress,
                            sourceHDFSdir,
                        )
                        updateDict["hdfs_target_path"] = "%s%s" % (
                            targetHDFSaddress,
                            targetHDFSdir,
                        )

                        # Update the date in copyASyncStatus table
                        (
                            session.query(configSchema.copyASyncStatus)
                            .filter(
                                configSchema.copyASyncStatus.table_id
                                == self.import_config.table_id
                            )
                            .filter(
                                configSchema.copyASyncStatus.destination == destination
                            )
                            .update(updateDict)
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
                if self.connectRemoteDBImportInstance(instance=destination):
                    logging.info("Copy HDFS data to instance '%s'" % (destination))

                    distcpCommand = [
                        "hadoop",
                        "distcp",
                        "-D",
                        "yarn.timeline-service.enabled=false",
                        "-D",
                        "mapreduce.job.queuename=%s"
                        % (configuration.get("Sqoop", "yarnqueue")),
                        "-overwrite",
                        "-delete",
                        "%s%s" % (sourceHDFSaddress, sourceHDFSdir),
                        "%s%s" % (targetHDFSaddress, targetHDFSdir),
                    ]

                    print(" ______________________ ")
                    print("|                      |")
                    print("| Hadoop distCp starts |")
                    print("|______________________|")
                    print("")

                    # Start distcp
                    sh_session = subprocess.Popen(
                        distcpCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
                    )

                    # Print Stdout and stderr while distcp is running
                    while sh_session.poll() == None:
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

    def scheduleTableAsyncCopy(self, hiveFilterDB, hiveFilterTable, copyDestination):
        """Schdeule an asynchronous copy of one or more Hive table to the specified destination"""
        logging.debug("Executing copy_operations.scheduleTableAsyncCopy()")

        if self.checkDBImportInstance(instance=copyDestination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        hiveFilterDB = hiveFilterDB.replace("*", "%").strip()
        hiveFilterTable = hiveFilterTable.replace("*", "%").strip()

        localSession = self.configDBSession()
        importTables = aliased(configSchema.importTables)
        copyASyncStatus = aliased(configSchema.copyASyncStatus)

        # Check if there are any tabled in this DAG that is marked for copy against the specified destination
        result = (
            localSession.query(copyASyncStatus)
            .filter(copyASyncStatus.hive_db.like(hiveFilterDB))
            .filter(copyASyncStatus.hive_table.like(hiveFilterTable))
            .filter(copyASyncStatus.destination == copyDestination)
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
        dbimportInstances = aliased(configSchema.dbimportInstances)

        sourceHDFSaddress = self.common_operations.hdfs_address
        sourceHDFSbasedir = self.common_operations.hdfs_basedir

        row = (
            localSession.query(
                dbimportInstances.hdfs_address, dbimportInstances.hdfs_basedir
            )
            .filter(dbimportInstances.name == copyDestination)
            .one()
        )

        targetHDFSaddress = row[0]
        targetHDFSbasedir = row[1]

        logging.debug("sourceHDFSaddress: %s" % (sourceHDFSaddress))
        logging.debug("sourceHDFSbasedir: %s" % (sourceHDFSbasedir))
        logging.debug("targetHDFSaddress: %s" % (targetHDFSaddress))
        logging.debug("targetHDFSbasedir: %s" % (targetHDFSbasedir))

        # Fetch a list of tables that match the database and table filter
        result = pd.DataFrame(
            localSession.query(
                importTables.table_id,
                importTables.hive_db,
                importTables.hive_table,
                importTables.dbalias,
            )
            .filter(importTables.hive_db.like(hiveFilterDB))
            .filter(importTables.hive_table.like(hiveFilterTable))
        )

        for index, row in result.iterrows():
            logging.info(
                "Schedule asynchronous copy for %s.%s"
                % (row["hive_db"], row["hive_table"])
            )
            self.import_config.Hive_DB = row["hive_db"]
            self.import_config.Hive_Table = row["hive_table"]

            try:
                self.import_config.getImportConfig()
            except invalidConfiguration as errMsg:
                logging.error(errMsg)
                self.import_config.remove_temporary_files()
                sys.exit(1)

            logging.debug("table_id: %s" % (self.import_config.table_id))
            logging.debug(
                "import_is_incremental: %s" % (self.import_config.import_is_incremental)
            )

            if (
                self.import_config.import_is_incremental == True
                and includeIncrImports == False
            ):
                logging.warning(
                    "Asynchronous copy for incremental table %s.%s skipped"
                    % (row["hive_db"], row["hive_table"])
                )
                continue

            sourceHDFSdir = (
                (sourceHDFSbasedir + "/" + row["hive_db"] + "/" + row["hive_table"])
                .replace("$", "")
                .replace(" ", "")
            )
            targetHDFSdir = (
                (targetHDFSbasedir + "/" + row["hive_db"] + "/" + row["hive_table"])
                .replace("$", "")
                .replace(" ", "")
            )
            logging.debug("sourceHDFSdir: %s" % (sourceHDFSdir))
            logging.debug("targetHDFSdir: %s" % (targetHDFSdir))

            result = (
                localSession.query(copyASyncStatus)
                .filter(copyASyncStatus.table_id == row["table_id"])
                .filter(copyASyncStatus.destination == copyDestination)
                .count()
            )

            if result == 0:
                newcopyASyncStatus = configSchema.copyASyncStatus(
                    table_id=row["table_id"],
                    hive_db=row["hive_db"],
                    hive_table=row["hive_table"],
                    destination=copyDestination,
                    hdfs_source_path="%s%s" % (sourceHDFSaddress, sourceHDFSdir),
                    hdfs_target_path="%s%s" % (targetHDFSaddress, targetHDFSdir),
                    copy_status=0,
                )
                localSession.add(newcopyASyncStatus)
                localSession.commit()

        localSession.close()

        logging.debug("Executing copy_operations.scheduleTableAsyncCopy() - Finished")

    def scheduleDAGasyncCopy(self, airflowDAGname, copyDestination, includeIncrImports):
        """Schedule an asynchronous copy of all tables that are included in the specified DAG to the specified destination"""

        if self.checkDBImportInstance(instance=copyDestination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        localSession = self.configDBSession()
        airflowImportDags = aliased(configSchema.airflowImportDags)
        importTables = aliased(configSchema.importTables)
        copyASyncStatus = aliased(configSchema.copyASyncStatus)

        hiveFilterStr = (
            localSession.query(airflowImportDags.filter_hive)
            .select_from(airflowImportDags)
            .filter(airflowImportDags.dag_name == airflowDAGname)
            .one()
        )

        for hiveFilter in hiveFilterStr[0].split(";"):
            hiveFilterDB = hiveFilter.split(".")[0]
            hiveFilterTable = hiveFilter.split(".")[1]

            hiveFilterDB = hiveFilterDB.replace("*", "%").strip()
            hiveFilterTable = hiveFilterTable.replace("*", "%").strip()

            # Check if there are any tabled in this DAG that is marked for copy against the specified destination
            result = (
                localSession.query(copyASyncStatus)
                .filter(copyASyncStatus.hive_db.like(hiveFilterDB))
                .filter(copyASyncStatus.hive_table.like(hiveFilterTable))
                .filter(copyASyncStatus.destination == copyDestination)
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
        dbimportInstances = aliased(configSchema.dbimportInstances)

        sourceHDFSaddress = self.common_operations.hdfs_address
        sourceHDFSbasedir = self.common_operations.hdfs_basedir

        row = (
            localSession.query(
                dbimportInstances.hdfs_address, dbimportInstances.hdfs_basedir
            )
            .filter(dbimportInstances.name == copyDestination)
            .one()
        )

        targetHDFSaddress = row[0]
        targetHDFSbasedir = row[1]

        logging.debug("sourceHDFSaddress: %s" % (sourceHDFSaddress))
        logging.debug("sourceHDFSbasedir: %s" % (sourceHDFSbasedir))
        logging.debug("targetHDFSaddress: %s" % (targetHDFSaddress))
        logging.debug("targetHDFSbasedir: %s" % (targetHDFSbasedir))

        # We need this for loop here again as there might be multi table specifications that ovarlap each other. So it's important that we
        # dont start the actual schedule of copies before we tested them all. Otherwise we might block ourself during the schedule
        for hiveFilter in hiveFilterStr[0].split(";"):
            hiveFilterDB = hiveFilter.split(".")[0]
            hiveFilterTable = hiveFilter.split(".")[1]

            hiveFilterDB = hiveFilterDB.replace("*", "%").strip()
            hiveFilterTable = hiveFilterTable.replace("*", "%").strip()

            logging.debug("hiveFilterDB: %s" % (hiveFilterDB))
            logging.debug("hiveFilterTable: %s" % (hiveFilterTable))

            # Fetch a list of tables that match the database and table filter
            result = pd.DataFrame(
                localSession.query(
                    importTables.table_id,
                    importTables.hive_db,
                    importTables.hive_table,
                    importTables.dbalias,
                )
                .filter(importTables.hive_db.like(hiveFilterDB))
                .filter(importTables.hive_table.like(hiveFilterTable))
            )

            for index, row in result.iterrows():
                logging.info(
                    "Schedule asynchronous copy for %s.%s"
                    % (row["hive_db"], row["hive_table"])
                )
                self.import_config.Hive_DB = row["hive_db"]
                self.import_config.Hive_Table = row["hive_table"]

                try:
                    self.import_config.getImportConfig()
                except invalidConfiguration as errMsg:
                    logging.error(errMsg)
                    self.import_config.remove_temporary_files()
                    sys.exit(1)

                logging.debug("table_id: %s" % (self.import_config.table_id))
                logging.debug(
                    "import_is_incremental: %s"
                    % (self.import_config.import_is_incremental)
                )

                if (
                    self.import_config.import_is_incremental == True
                    and includeIncrImports == False
                ):
                    logging.warning(
                        "Asynchronous copy for incremental table %s.%s skipped"
                        % (row["hive_db"], row["hive_table"])
                    )
                    continue

                sourceHDFSdir = (
                    (sourceHDFSbasedir + "/" + row["hive_db"] + "/" + row["hive_table"])
                    .replace("$", "")
                    .replace(" ", "")
                )
                targetHDFSdir = (
                    (targetHDFSbasedir + "/" + row["hive_db"] + "/" + row["hive_table"])
                    .replace("$", "")
                    .replace(" ", "")
                )
                logging.debug("sourceHDFSdir: %s" % (sourceHDFSdir))
                logging.debug("targetHDFSdir: %s" % (targetHDFSdir))

                result = (
                    localSession.query(copyASyncStatus)
                    .filter(copyASyncStatus.table_id == row["table_id"])
                    .filter(copyASyncStatus.destination == copyDestination)
                    .count()
                )

                if result == 0:
                    newcopyASyncStatus = configSchema.copyASyncStatus(
                        table_id=row["table_id"],
                        hive_db=row["hive_db"],
                        hive_table=row["hive_table"],
                        destination=copyDestination,
                        hdfs_source_path="%s%s" % (sourceHDFSaddress, sourceHDFSdir),
                        hdfs_target_path="%s%s" % (targetHDFSaddress, targetHDFSdir),
                        copy_status=0,
                    )
                    localSession.add(newcopyASyncStatus)
                    localSession.commit()

        localSession.close()

    def copyAirflowExportDAG(
        self,
        airflowDAGname,
        copyDestination,
        setAutoRegenerateDAG=False,
        deployMode=False,
    ):
        """Copy an Airflow Export DAG, including connections, tables and columns to a remote DBImport instance"""

        if self.checkDBImportInstance(instance=copyDestination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        localSession = self.configDBSession()
        airflowExportDags = aliased(configSchema.airflowExportDags)
        airflowTasks = aliased(configSchema.airflowTasks)
        exportTables = aliased(configSchema.exportTables)

        # Check if Airflow DAG exists with that name, and if so, get all the details
        airflowDAG = pd.DataFrame(
            localSession.query(configSchema.airflowExportDags.__table__).filter(
                configSchema.airflowExportDags.dag_name == airflowDAGname
            )
        )

        if airflowDAG.empty == True:
            logging.error("The specified Airflow DAG does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        # airflowDAG now contains a Pandas DF with the complete DAG configuraiton. This needs to be synced to the remote DBImport instance
        if self.connectRemoteDBImportInstance(instance=copyDestination):
            remoteSession = self.remoteInstanceConfigDBSession()

            # Check if the DAG exists on the remote DBImport instance
            result = (
                remoteSession.query(airflowExportDags)
                .filter(airflowExportDags.dag_name == airflowDAGname)
                .count()
            )

            if result == 0:
                # Table does not exist in target system. Lets create a skeleton record
                newAirflowExportDags = configSchema.airflowExportDags(
                    dag_name=airflowDAGname, filter_dbalias="None"
                )
                remoteSession.add(newAirflowExportDags)
                remoteSession.commit()

            # Create dictonary to be used to update the values in airflow_import_dag on the remote Instance
            updateDict = {}
            filterDBalias = ""
            filterSchema = ""
            filterTable = ""
            for name, values in airflowDAG.iteritems():
                if name in ("dag_name"):
                    continue

                if name == "filter_dbalias":
                    filterDBalias = str(values[0])

                if name == "filter_target_schema":
                    filterSchema = str(values[0])
                    if filterSchema == "None":
                        filterSchema = "%"

                if name == "filter_target_table":
                    filterTable = str(values[0])
                    if filterTable == "None":
                        filterTable = "%"

                value = str(values[0])
                if value == "None":
                    value = None

                updateDict["%s" % (name)] = value

            if setAutoRegenerateDAG == True:
                updateDict["auto_regenerate_dag"] = "1"

            if updateDict["schedule_interval"] == None:
                updateDict["schedule_interval"] = "None"

            # Update the values in airflow_import_dag on the remote instance
            (
                remoteSession.query(configSchema.airflowExportDags)
                .filter(configSchema.airflowExportDags.dag_name == airflowDAGname)
                .update(updateDict)
            )
            remoteSession.commit()

            if deployMode == False:
                logging.info("DAG definition copied to remote DBImport successfully")
            else:
                logging.info("DAG definition deployed successfully")

            # **************************
            # Prepair and trigger a copy of all schemas to the other cluster
            # **************************

            self.copyExportSchemaToDestination(
                filterDBalias=filterDBalias,
                filterSchema=filterSchema,
                filterTable=filterTable,
                destination=copyDestination,
                deployMode=deployMode,
            )

            if deployMode == False:
                logging.info(
                    "Schema definitions copied to remote DBImport successfully"
                )
            else:
                logging.info("Schema definitions deployed successfully")

            # **************************
            # Copy custom tasks from airflow_tasks table
            # **************************

            airflowTasksResult = pd.DataFrame(
                localSession.query(configSchema.airflowTasks.__table__).filter(
                    configSchema.airflowTasks.dag_name == airflowDAGname
                )
            )

            for index, row in airflowTasksResult.iterrows():
                # Check if the Airflow Task exists on the remote DBImport instance
                result = (
                    remoteSession.query(airflowTasks)
                    .filter(airflowTasks.dag_name == airflowDAGname)
                    .filter(airflowTasks.task_name == row["task_name"])
                    .count()
                )

                if result == 0:
                    # Create a new row in importColumns if it doesnt exists
                    newAirflowTask = configSchema.airflowTasks(
                        dag_name=airflowDAGname, task_name=row["task_name"]
                    )
                    remoteSession.add(newAirflowTask)
                    remoteSession.commit()

                updateDict = {}
                for name, value in row.iteritems():
                    if name in ("dag_name", "task_name"):
                        continue

                    updateDict["%s" % (name)] = value

                # Update the values in airflow_tasks on the remote instance
                (
                    remoteSession.query(configSchema.airflowTasks)
                    .filter(configSchema.airflowTasks.dag_name == airflowDAGname)
                    .filter(configSchema.airflowTasks.task_name == row["task_name"])
                    .update(updateDict)
                )
                remoteSession.commit()

            if airflowTasksResult.empty == False:
                if deployMode == False:
                    logging.info(
                        "DAG custom tasks copied to remote DBImport successfully"
                    )
                else:
                    logging.info("DAG custom tasks deployed successfully")

            # **************************
            # Convert rows in airflow_dag_sensors to airflow_tasks in remote system
            # **************************

            airflowDAGsensorsResult = pd.DataFrame(
                localSession.query(configSchema.airflowDagSensors.__table__).filter(
                    configSchema.airflowDagSensors.dag_name == airflowDAGname
                )
            )

            for index, row in airflowDAGsensorsResult.iterrows():
                # Check if the Airflow Task exists on the remote DBImport instance
                result = (
                    remoteSession.query(airflowTasks)
                    .filter(airflowTasks.dag_name == airflowDAGname)
                    .filter(airflowTasks.task_name == row["sensor_name"])
                    .count()
                )

                if result == 0:
                    # Create a new row in importColumns if it doesnt exists
                    newAirflowTask = configSchema.airflowTasks(
                        dag_name=airflowDAGname, task_name=row["sensor_name"]
                    )
                    remoteSession.add(newAirflowTask)
                    remoteSession.commit()

                updateDict = {}
                updateDict["task_type"] = "DAG Sensor"
                updateDict["placement"] = "before main"
                if row["wait_for_task"] == None:
                    updateDict["task_config"] = "%s.stop" % (row["wait_for_dag"])
                else:
                    updateDict["task_config"] = "%s.%s" % (
                        row["wait_for_dag"],
                        row["wait_for_task"],
                    )

                # Update the values in airflow_tasks on the remote instance
                (
                    remoteSession.query(configSchema.airflowTasks)
                    .filter(configSchema.airflowTasks.dag_name == airflowDAGname)
                    .filter(configSchema.airflowTasks.task_name == row["sensor_name"])
                    .update(updateDict)
                )
                remoteSession.commit()

                logging.info(
                    "Converted DAG sensor '%s' to an airflow_tasks entry"
                    % (row["sensor_name"])
                )

    def checkRemoteInstanceSchema(self, remoteInstance):
        """Checks that the remote instance is using the same database schema version as the local instance is"""

        if self.checkDBImportInstance(instance=remoteInstance) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        localSession = self.configDBSession()
        alembicVersion = aliased(configSchema.alembicVersion)

        if self.connectRemoteDBImportInstance(instance=remoteInstance):
            remoteSession = self.remoteInstanceConfigDBSession()

            localVersion = localSession.query(alembicVersion.version_num).one_or_none()
            remoteVersion = remoteSession.query(
                alembicVersion.version_num
            ).one_or_none()

            if localVersion[0] != remoteVersion[0]:
                logging.error(
                    "The remote DBImport instance is not running with the same version."
                )
                self.remove_temporary_files()
                sys.exit(1)

        else:
            logging.error("Cant connect to remote DBImport instance")
            self.remove_temporary_files()
            sys.exit(1)

        localSession.close()
        remoteSession.close()

    def copyAirflowImportDAG(
        self,
        airflowDAGname,
        copyDestination,
        copyDAGnoSlave=False,
        setAutoRegenerateDAG=False,
        deployMode=False,
    ):
        """Copy an Airflow Import DAG, including connections, tables and columns to a remote DBImport instance"""

        if self.checkDBImportInstance(instance=copyDestination) == False:
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        localSession = self.configDBSession()
        airflowImportDags = aliased(configSchema.airflowImportDags)
        airflowTasks = aliased(configSchema.airflowTasks)
        importTables = aliased(configSchema.importTables)

        # Check if Airflow DAG exists with that name, and if so, get all the details
        airflowDAG = pd.DataFrame(
            localSession.query(configSchema.airflowImportDags.__table__).filter(
                configSchema.airflowImportDags.dag_name == airflowDAGname
            )
        )

        if airflowDAG.empty == True:
            logging.error("The specified Airflow DAG does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        # airflowDAG now contains a Pandas DF with the complete DAG configuraiton. This needs to be synced to the remote DBImport instance
        if self.connectRemoteDBImportInstance(instance=copyDestination):
            remoteSession = self.remoteInstanceConfigDBSession()

            # Check if the DAG exists on the remote DBImport instance
            result = (
                remoteSession.query(airflowImportDags)
                .filter(airflowImportDags.dag_name == airflowDAGname)
                .count()
            )

            if result == 0:
                # Table does not exist in target system. Lets create a skeleton record
                newAirflowImportDags = configSchema.airflowImportDags(
                    dag_name=airflowDAGname, filter_hive="None"
                )
                remoteSession.add(newAirflowImportDags)
                remoteSession.commit()

            # Create dictonary to be used to update the values in airflow_import_dag on the remote Instance
            updateDict = {}
            for name, values in airflowDAG.iteritems():
                if name in ("dag_name"):
                    continue

                if name == "filter_hive":
                    hiveFilterStr = str(values[0])

                value = str(values[0])
                if value == "None":
                    value = None

                updateDict["%s" % (name)] = value

            if setAutoRegenerateDAG == True:
                updateDict["auto_regenerate_dag"] = "1"

            if updateDict["schedule_interval"] == None:
                updateDict["schedule_interval"] = "None"

            # Update the values in airflow_import_dag on the remote instance
            (
                remoteSession.query(configSchema.airflowImportDags)
                .filter(configSchema.airflowImportDags.dag_name == airflowDAGname)
                .update(updateDict)
            )
            remoteSession.commit()

            if deployMode == False:
                logging.info("DAG definition copied to remote DBImport successfully")
            else:
                logging.info("DAG definition deployed successfully")

            # **************************
            # Prepair and trigger a copy of all schemas to the other cluster
            # **************************
            self.copyDestinations = []
            destString = "%s;Asynchronous" % (copyDestination)
            self.copyDestinations.append(destString)

            for hiveFilter in hiveFilterStr.split(";"):
                hiveFilterDB = hiveFilter.split(".")[0]
                hiveFilterTable = hiveFilter.split(".")[1]

                hiveFilterDB = hiveFilterDB.replace("*", "%")
                hiveFilterTable = hiveFilterTable.replace("*", "%")

                result = pd.DataFrame(
                    localSession.query(
                        importTables.table_id,
                        importTables.hive_db,
                        importTables.hive_table,
                        importTables.dbalias,
                    )
                    .filter(importTables.hive_db.like(hiveFilterDB))
                    .filter(importTables.hive_table.like(hiveFilterTable))
                )

                for index, row in result.iterrows():
                    self.copyImportSchemaToDestinations(
                        tableID=row["table_id"],
                        hiveDB=row["hive_db"],
                        hiveTable=row["hive_table"],
                        connectionAlias=row["dbalias"],
                        copyDAGnoSlave=copyDAGnoSlave,
                        deployMode=deployMode,
                    )
            if deployMode == False:
                logging.info(
                    "Schema definitions copied to remote DBImport successfully"
                )
            else:
                logging.info("Schema definitions deployed successfully")

            # **************************
            # Copy custom tasks from airflow_tasks table
            # **************************

            airflowTasksResult = pd.DataFrame(
                localSession.query(configSchema.airflowTasks.__table__).filter(
                    configSchema.airflowTasks.dag_name == airflowDAGname
                )
            )

            copiedJdbcConnections = []
            for index, row in airflowTasksResult.iterrows():
                # Check if the Airflow Task exists on the remote DBImport instance
                result = (
                    remoteSession.query(airflowTasks)
                    .filter(airflowTasks.dag_name == airflowDAGname)
                    .filter(airflowTasks.task_name == row["task_name"])
                    .count()
                )

                if result == 0:
                    # Create a new row in importColumns if it doesnt exists
                    newAirflowTask = configSchema.airflowTasks(
                        dag_name=airflowDAGname, task_name=row["task_name"]
                    )
                    remoteSession.add(newAirflowTask)
                    remoteSession.commit()

                updateDict = {}
                for name, value in row.iteritems():
                    if name in ("dag_name", "task_name"):
                        continue

                    updateDict["%s" % (name)] = value

                if row["jdbc_dbalias"] not in copiedJdbcConnections:
                    self.copyJdbcConnectionToDestination(
                        jdbcConnection=row["jdbc_dbalias"],
                        destination=copyDestination,
                        deployMode=deployMode,
                    )
                    copiedJdbcConnections.append(row["jdbc_dbalias"])

                # Update the values in airflow_tasks on the remote instance
                (
                    remoteSession.query(configSchema.airflowTasks)
                    .filter(configSchema.airflowTasks.dag_name == airflowDAGname)
                    .filter(configSchema.airflowTasks.task_name == row["task_name"])
                    .update(updateDict)
                )
                remoteSession.commit()

            if airflowTasksResult.empty == False:
                if deployMode == False:
                    logging.info(
                        "DAG custom tasks copied to remote DBImport successfully"
                    )
                else:
                    logging.info("DAG custom tasks deployed successfully")

        else:
            logging.error("Cant connect to remote DBImport instance")
            self.remove_temporary_files()
            sys.exit(1)

        localSession.close()
        remoteSession.close()

    def copyExportSchemaToDestination(
        self, filterDBalias, filterSchema, filterTable, destination, deployMode=False
    ):
        """Copy the schema definitions to the target instances"""
        localSession = self.configDBSession()
        exportTables = aliased(configSchema.exportTables)
        exportColumns = aliased(configSchema.exportColumns)
        jdbcConnections = aliased(configSchema.jdbcConnections)
        dbimportInstances = aliased(configSchema.dbimportInstances)

        if self.connectRemoteDBImportInstance(instance=destination):
            remoteSession = self.remoteInstanceConfigDBSession()

            # Check if if we are going to sync the credentials for this destination
            result = (
                localSession.query(dbimportInstances.sync_credentials)
                .select_from(dbimportInstances)
                .filter(dbimportInstances.name == destination)
                .one()
            )

            if result[0] == 1:
                syncCredentials = True
            else:
                syncCredentials = False

            filterDBalias = filterDBalias.replace("*", "%")
            filterSchema = filterSchema.replace("*", "%")
            filterTable = filterTable.replace("*", "%")

            result = pd.DataFrame(
                localSession.query(
                    exportTables.table_id,
                    exportTables.hive_db,
                    exportTables.hive_table,
                    exportTables.dbalias,
                    exportTables.target_schema,
                    exportTables.target_table,
                )
                .filter(exportTables.dbalias.like(filterDBalias))
                .filter(exportTables.target_schema.like(filterSchema))
                .filter(exportTables.target_table.like(filterTable))
            )

            for index, row in result.iterrows():
                if deployMode == False:
                    logging.info(
                        "Copy schema definitions for %s.%s"
                        % (row["hive_db"], row["hive_table"])
                    )
                else:
                    logging.info(
                        "Deploying schema definitions for %s.%s"
                        % (row["hive_db"], row["hive_table"])
                    )

                ##################################
                # Update jdbc_connections
                ##################################

                # Check if the jdbcConnection exists on the remote DBImport instance
                result = (
                    remoteSession.query(jdbcConnections)
                    .filter(jdbcConnections.dbalias == row["dbalias"])
                    .count()
                )

                if result == 0:
                    newJdbcConnection = configSchema.jdbcConnections(
                        dbalias=row["dbalias"], jdbc_url=""
                    )
                    remoteSession.add(newJdbcConnection)
                    remoteSession.commit()

                # Read the entire import_table row from the source database
                sourceJdbcConnection = pd.DataFrame(
                    localSession.query(configSchema.jdbcConnections.__table__).filter(
                        configSchema.jdbcConnections.dbalias == row["dbalias"]
                    )
                )

                # Table to update with values from import_table source
                remoteJdbcConnection = (
                    remoteSession.query(configSchema.jdbcConnections.__table__)
                    .filter(configSchema.jdbcConnections.dbalias == row["dbalias"])
                    .one()
                )

                # Create dictonary to be used to update the values in import_table on the remote Instance
                updateDict = {}
                for name, values in sourceJdbcConnection.iteritems():
                    if name == "dbalias":
                        continue

                    if syncCredentials == False and name in (
                        "credentials",
                        "private_key_path",
                        "public_key_path",
                    ):
                        continue

                    value = str(values[0])
                    if value == "None":
                        value = None

                    updateDict["%s" % (name)] = value

                # Update the values in import_table on the remote instance
                (
                    remoteSession.query(configSchema.jdbcConnections)
                    .filter(configSchema.jdbcConnections.dbalias == row["dbalias"])
                    .update(updateDict)
                )
                remoteSession.commit()

                ##################################
                # Update export_tables
                ##################################

                # Check if the table exists on the remote DBImport instance
                result = (
                    remoteSession.query(exportTables)
                    .filter(exportTables.dbalias == row["dbalias"])
                    .filter(exportTables.target_schema == row["target_schema"])
                    .filter(exportTables.target_table == row["target_table"])
                    .count()
                )

                if result == 0:
                    # Table does not exist in target system. Lets create a skeleton record
                    newExportTable = configSchema.exportTables(
                        dbalias=row["dbalias"],
                        target_schema=row["target_schema"],
                        target_table=row["target_table"],
                        hive_db=row["hive_db"],
                        hive_table=row["hive_table"],
                    )
                    remoteSession.add(newExportTable)
                    remoteSession.commit()

                # Get the table_id from the table at the remote instance
                remoteExportTableID = (
                    remoteSession.query(exportTables.table_id)
                    .select_from(exportTables)
                    .filter(exportTables.dbalias == row["dbalias"])
                    .filter(exportTables.target_schema == row["target_schema"])
                    .filter(exportTables.target_table == row["target_table"])
                    .one()
                )

                remoteTableID = remoteExportTableID[0]

                # Read the entire export_table row from the source database
                sourceTableDefinition = pd.DataFrame(
                    localSession.query(configSchema.exportTables.__table__).filter(
                        configSchema.exportTables.table_id == row["table_id"]
                    )
                )

                # Table to update with values from import_table source
                remoteTableDefinition = (
                    remoteSession.query(configSchema.exportTables.__table__)
                    .filter(configSchema.exportTables.table_id == remoteTableID)
                    .one()
                )

                # Create dictonary to be used to update the values in import_table on the remote Instance
                updateDict = {}
                for name, values in sourceTableDefinition.iteritems():
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

                    updateDict["%s" % (name)] = value

                # Update the values in import_table on the remote instance
                (
                    remoteSession.query(configSchema.exportTables)
                    .filter(configSchema.exportTables.table_id == remoteTableID)
                    .update(updateDict)
                )
                remoteSession.commit()

                ##################################
                # Update export_colums
                ##################################

                # Read the entire export_columns row from the source database
                sourceAllColumnDefinitions = pd.DataFrame(
                    localSession.query(configSchema.exportColumns.__table__).filter(
                        configSchema.exportColumns.table_id == row["table_id"]
                    )
                )

                for columnIndex, columnRow in sourceAllColumnDefinitions.iterrows():
                    # Check if the column exists on the remote DBImport instance
                    result = (
                        remoteSession.query(exportColumns)
                        .filter(exportColumns.table_id == remoteTableID)
                        .filter(exportColumns.column_name == columnRow["column_name"])
                        .count()
                    )

                    if result == 0:
                        # Create a new row in exportColumns if it doesnt exists
                        newExportColumn = configSchema.exportColumns(
                            table_id=remoteTableID,
                            column_name=columnRow["column_name"],
                            last_update_from_hive=str(
                                columnRow["last_update_from_hive"]
                            ),
                        )
                        remoteSession.add(newExportColumn)
                        remoteSession.commit()

                    # Get the table_id from the table at the remote instance
                    remoteExportColumnID = (
                        remoteSession.query(exportColumns.column_id)
                        .select_from(exportColumns)
                        .filter(exportColumns.table_id == remoteTableID)
                        .filter(exportColumns.column_name == columnRow["column_name"])
                        .one()
                    )

                    remoteColumnID = remoteExportColumnID[0]

                    # Read the entire export_columnis row from the source database
                    sourceColumnDefinition = pd.DataFrame(
                        localSession.query(configSchema.exportColumns.__table__).filter(
                            configSchema.exportColumns.column_id
                            == columnRow["column_id"]
                        )
                    )

                    # Table to update with values from export_columns source
                    remoteColumnDefinition = (
                        remoteSession.query(configSchema.exportColumns.__table__)
                        .filter(configSchema.exportColumns.column_id == remoteColumnID)
                        .one()
                    )

                    # Create dictonary to be used to update the values in export_table on the remote Instance
                    updateDict = {}
                    for name, values in sourceColumnDefinition.iteritems():
                        if name in ("table_id", "column_id", "column_name"):
                            continue

                        value = str(values[0])
                        if value == "None":
                            value = None

                        updateDict["%s" % (name)] = value

                    # Update the values in export_table on the remote instance
                    (
                        remoteSession.query(configSchema.exportColumns)
                        .filter(configSchema.exportColumns.column_id == remoteColumnID)
                        .update(updateDict)
                    )
                    remoteSession.commit()

            remoteSession.close()
        else:
            logging.warning(
                "Connection failed! No data will be copied to instance '%s'"
                % (destination)
            )

        localSession.close()

    def copyImportSchemaToDestinations(
        self,
        tableID=None,
        hiveDB=None,
        hiveTable=None,
        connectionAlias=None,
        copyDAGnoSlave=False,
        deployMode=False,
    ):
        """Copy the schema definitions to the target instances"""
        localSession = self.configDBSession()

        if self.copyDestinations == None:
            if deployMode == False:
                logging.warning(
                    "There are no destination for this table to receive a copy"
                )
            else:
                logging.warning("There are no destination for this deployment")
            return

        if (
            tableID == None
            and hiveDB == None
            and hiveTable == None
            and connectionAlias == None
        ):
            # This happens during a normal import
            tableID = self.import_config.table_id
            hiveDB = self.import_config.Hive_DB
            hiveTable = self.import_config.Hive_Table
            connectionAlias = self.import_config.connection_alias
            printDestination = True
        else:
            # This happens during a "manage --copyAirflowImportDAG" or during deployment.
            # And then the destination is specified in cmd and not needed to be printed
            printDestination = False

        for destAndMethod in self.copyDestinations:
            destination = destAndMethod.split(";")[0]
            method = destAndMethod.split(";")[1]
            if self.connectRemoteDBImportInstance(instance=destination):
                if printDestination == True:
                    logging.info(
                        "Copy schema definitions for %s.%s to instance '%s'"
                        % (hiveDB, hiveTable, destination)
                    )
                else:
                    if deployMode == False:
                        logging.info(
                            "Copy schema definitions for %s.%s" % (hiveDB, hiveTable)
                        )
                    else:
                        logging.info(
                            "Deploying schema definitions for %s.%s"
                            % (hiveDB, hiveTable)
                        )
                remoteSession = self.remoteInstanceConfigDBSession()

                jdbcConnections = aliased(configSchema.jdbcConnections)
                importTables = aliased(configSchema.importTables)
                importColumns = aliased(configSchema.importColumns)
                dbimportInstances = aliased(configSchema.dbimportInstances)

                # Check if if we are going to sync the credentials for this destination
                result = (
                    localSession.query(dbimportInstances.sync_credentials)
                    .select_from(dbimportInstances)
                    .filter(dbimportInstances.name == destination)
                    .one()
                )

                if result[0] == 1:
                    syncCredentials = True
                else:
                    syncCredentials = False

                # Check if the table exists on the remote DBImport instance
                result = (
                    remoteSession.query(importTables)
                    .filter(importTables.hive_db == hiveDB)
                    .filter(importTables.hive_table == hiveTable)
                    .count()
                )

                if result == 0:
                    # Table does not exist in target system. Lets create a skeleton record
                    newImportTable = configSchema.importTables(
                        hive_db=hiveDB,
                        hive_table=hiveTable,
                        dbalias=connectionAlias,
                        source_schema="",
                        source_table="",
                    )
                    remoteSession.add(newImportTable)
                    remoteSession.commit()

                # Get the table_id from the table at the remote instance
                remoteImportTableID = (
                    remoteSession.query(importTables.table_id, importTables.dbalias)
                    .select_from(importTables)
                    .filter(importTables.hive_db == hiveDB)
                    .filter(importTables.hive_table == hiveTable)
                    .one()
                )

                remoteTableID = remoteImportTableID[0]
                jdbcConnection = remoteImportTableID[1]

                ##################################
                # Update jdbc_connections
                ##################################

                self.copyJdbcConnectionToDestination(
                    jdbcConnection=jdbcConnection,
                    deployMode=deployMode,
                    destination=destination,
                )

                ##################################
                # Update import_colums
                ##################################

                # Read the entire import_table row from the source database
                sourceAllColumnDefinitions = pd.DataFrame(
                    localSession.query(configSchema.importColumns.__table__)
                    .filter(configSchema.importColumns.table_id == tableID)
                    .order_by(configSchema.importColumns.column_order)
                )

                targetAllColumnDefinitions = pd.DataFrame(
                    remoteSession.query(configSchema.importColumns.__table__)
                    .filter(configSchema.importColumns.table_id == remoteTableID)
                    .order_by(configSchema.importColumns.column_order)
                )

                if not sourceAllColumnDefinitions.empty:
                    # sourceAllColumnDefinitions might be empty if the table data only exists in import_tables and not in import_columns

                    if targetAllColumnDefinitions.empty:
                        # If the target DF is empty, it means that the table does not exist in the target system. So to be able to continue with the merge, we need the columns
                        # to be presented. So we set them to the same as the sourceDefinition
                        targetAllColumnDefinitions = pd.DataFrame(
                            data=None, columns=sourceAllColumnDefinitions.columns
                        )

                    sourceAllColumnDefinitions.rename(
                        columns={
                            "table_id": "source_table_id",
                            "column_id": "source_column_id",
                        },
                        inplace=True,
                    )
                    targetAllColumnDefinitions.rename(
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
                    columnDifference = pd.merge(
                        sourceAllColumnDefinitions,
                        targetAllColumnDefinitions,
                        on=None,
                        how="outer",
                        indicator="Exist",
                    )
                    # 					print(columnDifference)
                    # 					print("DEBUG 02")
                    columnDifferenceLeftOnly = columnDifference[
                        columnDifference.Exist == "left_only"
                    ]
                    columnDifferenceLeftOnly = columnDifferenceLeftOnly.replace(
                        {np.nan: None}
                    )

                    for columnIndex, columnRow in columnDifferenceLeftOnly.iterrows():
                        sourceColumnName = columnRow["source_column_name"]

                        # Check if column exists in target database
                        if (
                            len(
                                targetAllColumnDefinitions.loc[
                                    targetAllColumnDefinitions["source_column_name"]
                                    == sourceColumnName
                                ]
                            )
                            == 0
                        ):
                            logging.debug(
                                "Source Column Name '%s' does not exists in target"
                                % (sourceColumnName)
                            )
                            newImportColumn = configSchema.importColumns(
                                table_id=remoteTableID,
                                column_name=columnRow["column_name"],
                                hive_db=hiveDB,
                                hive_table=hiveTable,
                                source_column_name=columnRow["source_column_name"],
                                column_type="",
                                source_column_type="",
                                last_update_from_source=str(
                                    columnRow["last_update_from_source"]
                                ),
                            )
                            remoteSession.add(newImportColumn)
                            remoteSession.commit()

                        # Get the table_id from the table at the remote instance
                        remoteImportColumnID = (
                            remoteSession.query(importColumns.column_id)
                            .select_from(importColumns)
                            .filter(importColumns.table_id == remoteTableID)
                            .filter(
                                importColumns.source_column_name
                                == columnRow["source_column_name"]
                            )
                            .one()
                        )

                        remoteColumnID = remoteImportColumnID[0]

                        # Create dictonary to be used to update the values in import_table on the remote Instance
                        updateDict = {}
                        for name, values in columnRow.iteritems():
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

                            updateDict["%s" % (name)] = value

                        # Update the values in import_table on the remote instance
                        (
                            remoteSession.query(configSchema.importColumns)
                            .filter(
                                configSchema.importColumns.column_id == remoteColumnID
                            )
                            .update(updateDict)
                        )
                        remoteSession.commit()

                ##################################
                # Update import_tables
                ##################################

                # Read the entire import_table row from the source database
                sourceTableDefinition = pd.DataFrame(
                    localSession.query(configSchema.importTables.__table__).filter(
                        configSchema.importTables.table_id == tableID
                    )
                )

                # Table to update with values from import_table source
                remoteTableDefinition = (
                    remoteSession.query(configSchema.importTables.__table__)
                    .filter(configSchema.importTables.table_id == remoteTableID)
                    .one()
                )

                # Create dictonary to be used to update the values in import_table on the remote Instance
                updateDict = {}
                jdbcConnection = ""
                for name, values in sourceTableDefinition.iteritems():
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

                    updateDict["%s" % (name)] = value

                if deployMode == False:
                    if copyDAGnoSlave == True:
                        updateDict["copy_slave"] = 0
                        updateDict["copy_finished"] = None
                    else:
                        updateDict["copy_slave"] = 1
                        if method == "Synchronous":
                            updateDict["copy_finished"] = str(
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                            )
                        else:
                            updateDict["copy_finished"] = None
                else:
                    updateDict["copy_slave"] = 0
                    updateDict["copy_finished"] = None

                # Update the values in import_table on the remote instance
                (
                    remoteSession.query(configSchema.importTables)
                    .filter(configSchema.importTables.table_id == remoteTableID)
                    .update(updateDict)
                )
                remoteSession.commit()
                remoteSession.close()

            else:
                logging.warning(
                    "Connection failed! No data will be copied to instance '%s'"
                    % (destination)
                )

        localSession.close()

    def copyJdbcConnectionToDestination(
        self, jdbcConnection, destination, deployMode=False
    ):
        """Copy the JDBC Connection to the target instances"""
        localSession = self.configDBSession()

        if self.copyDestinations == None:
            if deployMode == False:
                logging.warning(
                    "There are no destination for this table to receive a copy"
                )
            else:
                logging.warning("There are no destination for this deployment")
            return

        remoteSession = self.remoteInstanceConfigDBSession()

        jdbcConnections = aliased(configSchema.jdbcConnections)
        dbimportInstances = aliased(configSchema.dbimportInstances)

        # Check if if we are going to sync the credentials for this destination
        result = (
            localSession.query(dbimportInstances.sync_credentials)
            .select_from(dbimportInstances)
            .filter(dbimportInstances.name == destination)
            .one()
        )

        if result[0] == 1:
            syncCredentials = True
        else:
            syncCredentials = False

        # Check if the jdbcConnection exists on the remote DBImport instance
        result = (
            remoteSession.query(jdbcConnections)
            .filter(jdbcConnections.dbalias == jdbcConnection)
            .count()
        )

        if result == 0:
            newJdbcConnection = configSchema.jdbcConnections(
                dbalias=jdbcConnection, jdbc_url=""
            )
            remoteSession.add(newJdbcConnection)
            remoteSession.commit()

        # Read the entire import_table row from the source database
        sourceJdbcConnection = pd.DataFrame(
            localSession.query(configSchema.jdbcConnections.__table__).filter(
                configSchema.jdbcConnections.dbalias == jdbcConnection
            )
        )

        # Table to update with values from import_table source
        remoteJdbcConnection = (
            remoteSession.query(configSchema.jdbcConnections.__table__)
            .filter(configSchema.jdbcConnections.dbalias == jdbcConnection)
            .one()
        )

        # Create dictonary to be used to update the values in import_table on the remote Instance
        updateDict = {}
        for name, values in sourceJdbcConnection.iteritems():
            if name == "dbalias":
                continue

            if syncCredentials == False and name in (
                "credentials",
                "private_key_path",
                "public_key_path",
            ):
                continue

            value = str(values[0])
            if value == "None":
                value = None

            updateDict["%s" % (name)] = value

        # Update the values in jdbc_connections on the remote instance
        (
            remoteSession.query(configSchema.jdbcConnections)
            .filter(configSchema.jdbcConnections.dbalias == jdbcConnection)
            .update(updateDict)
        )
        remoteSession.commit()

        localSession.close()
        remoteSession.close()

    def importCopiedTable(self, localHDFSpath, hiveDB, hiveTable):
        """Import a table from the specified directory"""
        logging.debug("Executing copy_operations.importCopiedTable()")

        self.common_operations.connectToHive(forceSkipTest=True)
        localHDFSpath = (
            (localHDFSpath + "/" + hiveDB + "/" + hiveTable)
            .replace("$", "")
            .replace(" ", "")
        )

        logging.info("Importing table")
        query = "import table `%s`.`%s` from '%s'" % (hiveDB, hiveTable, localHDFSpath)
        try:
            self.common_operations.executeHiveQuery(query)
        except Exception as ex:
            logging.error(ex)
            logging.error("The import failed! The data was not loaded to the table")
            self.remove_temporary_files()
            sys.exit(1)

        logging.debug("Executing copy_operations.importCopiedTable() - Finished")

    def exportTable(self, localHDFSpath, hiveDB, hiveTable):
        """Export the Hive table"""
        logging.debug("Executing copy_operations.exportTable()")

        if localHDFSpath == None:
            logging.error("You need to specify a local HDFS path")
            self.remove_temporary_files()
            sys.exit(1)

        if hiveDB == None:
            hiveDB = self.Hive_DB
        if hiveTable == None:
            hiveTable = self.Hive_Table

        localHDFSpath = (
            (localHDFSpath + "/" + hiveDB + "/" + hiveTable)
            .replace("$", "")
            .replace(" ", "")
        )
        # 		remoteHDFSpath = (remoteHDFSpath + "/"+ hiveDB + "/" + hiveTable).replace('$', '').replace(' ', '')

        logging.info("Deleting local HDFS directory before export")

        hdfsDeleteCommand = ["hdfs", "dfs", "-rm", "-r", "-skipTrash", localHDFSpath]
        sh_session = subprocess.Popen(
            hdfsDeleteCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        hdfsDeleteoutput = ""

        # Print Stdout and stderr while distcp is running
        while sh_session.poll() == None:
            row = sh_session.stdout.readline().decode("utf-8").rstrip()
            if row != "" and "No such file or directory" not in row:
                logging.info(row)
                hdfsDeleteoutput += row + "\n"
                sys.stdout.flush()

        # Print what is left in output after distcp is finished
        for row in sh_session.stdout.readlines():
            row = row.decode("utf-8").rstrip()
            if row != "" and "No such file or directory" not in row:
                logging.info(row)
                hdfsDeleteoutput += row + "\n"
                sys.stdout.flush()

        self.common_operations.connectToHive(forceSkipTest=True)

        logging.info("Exporting table")
        # 		query = "export table `%s`.`%s` to '%s'"%(hiveDB, hiveTable, localHDFSpath)
        query = "export table %s.%s to '%s'" % (hiveDB, hiveTable, localHDFSpath)
        self.common_operations.executeHiveQuery(query)

        logging.debug("Executing copy_operations.exportTable() - Finished")

    def copyTableToFromCluster(
        self,
        remoteInstance,
        localHDFSpath,
        remoteHDFSpath,
        hiveDB,
        hiveTable,
        fromClusterMode,
    ):
        logging.debug("Executing copy_operations.copyTableToFromCluster()")

        if (
            remoteInstance == None
            or self.checkDBImportInstance(instance=remoteInstance) == False
        ):
            logging.error("The specified remote DBImport instance does not exist.")
            self.remove_temporary_files()
            sys.exit(1)

        if localHDFSpath == None or remoteHDFSpath == None:
            logging.error("You need to specify a local and remote HDFS path")
            self.remove_temporary_files()
            sys.exit(1)

        if hiveDB == None:
            hiveDB = self.Hive_DB
        if hiveTable == None:
            hiveTable = self.Hive_Table

        localHDFSpath = (
            (localHDFSpath + "/" + hiveDB + "/" + hiveTable)
            .replace("$", "")
            .replace(" ", "")
        )
        remoteHDFSpath = (
            (remoteHDFSpath + "/" + hiveDB + "/" + hiveTable)
            .replace("$", "")
            .replace(" ", "")
        )

        # Calculate the source and target HDFS directories
        session = self.configDBSession()
        dbimportInstances = aliased(configSchema.dbimportInstances)

        row = (
            session.query(dbimportInstances.hdfs_address)
            .filter(dbimportInstances.name == remoteInstance)
            .one()
        )

        remoteHDFSaddress = row[0]
        localHDFSaddress = self.common_operations.hdfs_address

        if fromClusterMode == True:
            logging.info("Copy table directory from remote cluster")
            HDFSsourcePath = ("%s%s" % (remoteHDFSaddress, remoteHDFSpath),)
            HDFStargetPath = ("%s%s" % (localHDFSaddress, localHDFSpath),)

        else:
            logging.info("Copy table directory to remote cluster")
            HDFSsourcePath = ("%s%s" % (localHDFSaddress, localHDFSpath),)
            HDFStargetPath = ("%s%s" % (remoteHDFSaddress, remoteHDFSpath),)

        logging.debug("HDFSsourcePath = %s" % (HDFSsourcePath))
        logging.debug("HDFStargetPath = %s" % (HDFStargetPath))

        distcpCommand = [
            "hadoop",
            "distcp",
            "-D",
            "mapreduce.job.queuename=%s" % (configuration.get("Sqoop", "yarnqueue")),
            "-overwrite",
            "-delete",
            "%s" % (HDFSsourcePath),
            "%s" % (HDFStargetPath),
        ]

        # Start distcp
        sh_session = subprocess.Popen(
            distcpCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        distCPoutput = ""

        # Print Stdout and stderr while distcp is running
        while sh_session.poll() == None:
            row = sh_session.stdout.readline().decode("utf-8").rstrip()
            if row != "":
                logging.info(row)
                distCPoutput += row + "\n"
                sys.stdout.flush()

        # Print what is left in output after distcp is finished
        for row in sh_session.stdout.readlines():
            row = row.decode("utf-8").rstrip()
            if row != "":
                logging.info(row)
                distCPoutput += row + "\n"
                sys.stdout.flush()

        logging.debug("Executing copy_operations.copyTableToFromCluster() - Finished")
