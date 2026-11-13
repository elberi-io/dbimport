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

import getopt
import logging
import sys

from dbimport.common import constants as constant
from dbimport.operations import copy_operations


def print_header():
    # Font created at http://patorjk.com/software/taag/#p=display&f=Big&t=DBImport%20-%20Deploy
    sys.stdout.write("\u001b[35m")  # Magenta
    sys.stdout.flush()
    print("")
    print(
        "   _____  ____ _____                            _              _____             _              "
    )
    print(
        r"  |  __ \|  _ \_   _|                          | |            |  __ \           | |             "
    )
    print(
        "  | |  | | |_) || |  _ __ ___  _ __   ___  _ __| |_   ______  | |  | | ___ _ __ | | ___  _   _  "
    )
    print(
        r"  | |  | |  _ < | | | '_ ` _ \| '_ \ / _ \| '__| __| |______| | |  | |/ _ \ '_ \| |/ _ \| | | | "
    )
    print(
        "  | |__| | |_) || |_| | | | | | |_) | (_) | |  | |_           | |__| |  __/ |_) | | (_) | |_| | "
    )
    print(
        r"  |_____/|____/_____|_| |_| |_| .__/ \___/|_|   \__|          |_____/ \___| .__/|_|\___/ \__, | "
    )
    print(
        "                              | |                                         | |             __/ | "
    )
    print(
        "                              |_|                                         |_|            |___/  "
    )
    sys.stdout.write("\u001b[0m")  # Reset
    sys.stdout.flush()
    print("")
    print(f"Version: {constant.VERSION}")
    print("")


def print_main_help():
    print_header()
    print("Options:")
    print(
        "  --deployAirflowImportDAG          Deploy an Airflow DAG and all import tables that goes with it"
    )
    print(
        "  --deployAirflowExportDAG          Deploy an Airflow DAG and all export tables that goes with it"
    )
    # 	print ("  --deployImportDatabase            Deploy all import tables that is configured in the database")
    # 	print ("  --deployExportDatabase            Deploy all export tables that is configured in the database")
    # 	print ("  --deployImportTable               Deploy an import table")
    # 	print ("  --deployExportTable               Deploy an export table")
    print("")
    print("General parameters to all commands:")
    print("  --help             Show this help message and exit")
    print(
        "  -v, --debug        Turn on extensive debug output. This will print passwords in cleartext, so use with caution"
    )
    print("  --quiet            Suppress logo and version output")

    sys.exit(1)


def print_deploy_airflow_import_dag_help():
    print_header()
    print(
        "Deploy the DAG definition and all involved databases, tables and connection aliases to a remote DBImport instance"
    )
    print(
        "If the DAG already exists in the remote DBImport instance, all definitions will be updated. Keep in mind that if a table"
    )
    print(
        "exists in the remote DBImport instance, but not in the source, the table wont be deleted from the remote DBImport instance"
    )
    print("")
    print("Required parameters:")
    print("  --airflowDAG=[DAG name]        Name of the DAG to deploy.")
    print(
        "  --deployTarget=[Instance]      Name of the DBImport instance to where you want to deploy"
    )
    print("")
    sys.exit(1)


def print_deploy_airflow_export_dag_help():
    print_header()
    print(
        "Deploy the DAG definition and all involved databases, tables and connection aliases to a remote DBImport instance"
    )
    print(
        "If the DAG already exists in the remote DBImport instance, all definitions will be updated. Keep in mind that if a table"
    )
    print(
        "exists in the remote DBImport instance, but not in the source, the table wont be deleted from the remote DBImport instance"
    )
    print("")
    print("Required parameters:")
    print("  --airflowDAG=[DAG name]        Name of the DAG to deploy.")
    print(
        "  --deployTarget=[Instance]      Name of the DBImport instance to where you want to deploy"
    )
    print("")
    sys.exit(1)


def main(argv):
    try:
        opts, args = getopt.getopt(
            argv,
            "?yvwh:t:a:S:T:",
            [
                "quiet",
                "yes",
                "help",
                "Hive_DB=",
                "hiveDB=",
                "Hive_Table=",
                "hiveTable=",
                "dbAlias=",
                "debug",
                "airflowDAG=",
                "deployTarget=",
                "deployAirflowImportDAG",
                "deployAirflowExportDAG",
                "deployImportDatabase",
                "deployExportDatabase",
                "deployImportTable",
                "deployExportTable",
            ],
        )
    except getopt.GetoptError:
        print_main_help()

    hive_db = None
    hive_table = None
    display_help = False
    operation = None
    logging_level = logging.INFO
    auto_yes_answer = False
    airflow_dag = None
    deploy_target = None
    quiet_mode = False

    if len(opts) == 0:
        print_main_help()

    for opt, arg in opts:
        if opt in ("-?", "--help"):
            display_help = True
        elif opt in ("-y", "--yes"):
            auto_yes_answer = True
        elif opt in ("-v", "--debug"):
            logging_level = logging.DEBUG
        elif opt in ("-h", "--hiveDB", "--Hive_DB"):
            hive_db = arg
        elif opt in ("-t", "--hiveTable", "--Hive_Table"):
            hive_table = arg
        elif opt == "--quiet":
            quiet_mode = True
        elif opt == "--deployTarget":
            deploy_target = str(arg)
        elif opt == "--airflowDAG":
            airflow_dag = str(arg)
        elif opt == "--deployAirflowImportDAG":
            operation = "deployAirflowImportDAG"
        elif opt == "--deployAirflowExportDAG":
            operation = "deployAirflowExportDAG"
    # 		elif opt == "--deployImportDatabase":
    # 			operation = "deployImportDatabase"
    # 		elif opt == "--deployExportDatabase":
    # 			operation = "deployExportDatabase"
    # 		elif opt == "--deployImportTable":
    # 			operation = "deployImportTable"
    # 		elif opt == "--deployExportTable":
    # 			operation = "deployExportTable"

    if operation is None:
        print_main_help()

    if operation == "deployAirflowImportDAG" and (
        airflow_dag is None or deploy_target is None or display_help
    ):
        print_deploy_airflow_import_dag_help()

    if operation == "deployAirflowExportDAG" and (
        airflow_dag is None or deploy_target is None or display_help
    ):
        print_deploy_airflow_export_dag_help()

    if not quiet_mode:
        print_header()

    # Initiate the logging functions with the correct level
    if logging_level == logging.DEBUG:
        logging.basicConfig(
            stream=sys.stdout,
            format="%(levelname)s %(funcName)s - %(message)s",
            level=logging_level,
        )
    else:
        logging.basicConfig(
            stream=sys.stdout, format="%(levelname)s - %(message)s", level=logging_level
        )

    if operation == "deployAirflowImportDAG":
        copy_operation = copy_operations.Operation()
        copy_operation.check_remote_instance_schema(remote_instance=deploy_target)
        copy_operation.copy_airflow_import_dag(
            airflow_dag_name=airflow_dag,
            copy_destination=deploy_target,
            deploy_mode=True,
        )
        copy_operation.remove_temporary_files()

    if operation == "deployAirflowExportDAG":
        copy_operation = copy_operations.Operation()
        copy_operation.check_remote_instance_schema(remote_instance=deploy_target)
        copy_operation.copy_airflow_export_dag(
            airflow_dag_name=airflow_dag,
            copy_destination=deploy_target,
            deploy_mode=True,
        )
        copy_operation.remove_temporary_files()


def run() -> None:
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
