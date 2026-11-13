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
from dbimport.setup import database as database_operations
from dbimport.setup import web_server_operations


def print_help():
    print("Options:")
    print(
        "  --createDB         Creates the database schema in the DBImport configuration database"
    )
    print(
        "  --upgradeDB        Upgrades the configuration database to support a new DBImport version"
    )
    print("")
    print(
        "  --buildHTML        Installs required npm packages and guild HTML files for DBImport UI"
    )
    print("")
    print("  --help             Show this help message and exit")
    print(
        "  -v, --debug        Turn on extensive debug output. This will print passwords in cleartext, so use with caution"
    )
    sys.exit(1)


def main(argv):

    try:
        opts, args = getopt.getopt(
            argv,
            "v",
            [
                "help",
                "createDB",
                "upgradeDB",
                "downgradeDB",
                "createSchemaRst",
                "debug",
                "buildHTML",
            ],
        )
    except getopt.GetoptError:
        print_help()

    version = "0.10"
    hive_db = None
    hive_table = None
    operation = None
    logging_level = logging.INFO
    print_header = True

    if len(opts) == 0:
        print_help()

    for opt, _arg in opts:
        if opt in ("-v", "--debug"):
            logging_level = logging.DEBUG
        elif opt == "--help":
            print_help()
        elif opt == "--createDB":
            operation = "createDB"
        elif opt == "--upgradeDB":
            operation = "upgradeDB"
        elif opt == "--downgradeDB":
            operation = "downgradeDB"
        elif opt == "--buildHTML":
            operation = "buildHTML"
        elif opt == "--createSchemaRst":
            operation = "createSchemaRst"
            print_header = False

    if operation is None:
        print_help()

    if print_header:
        # Font created at http://patorjk.com/software/taag/#p=display&f=Big&t=DBImport%20-%20Setup
        sys.stdout.write("\u001b[35m")  # Magenta
        sys.stdout.flush()
        print("")
        print(
            " _____  ____ _____                            _               _____      _               "
        )
        print(
            r"|  __ \|  _ \_   _|                          | |             / ____|    | |              "
        )
        print(
            "| |  | | |_) || |  _ __ ___  _ __   ___  _ __| |_   ______  | (___   ___| |_ _   _ _ __  "
        )
        print(
            r"| |  | |  _ < | | | '_ ` _ \| '_ \ / _ \| '__| __| |______|  \___ \ / _ \ __| | | | '_ \ "
        )
        print(
            "| |__| | |_) || |_| | | | | | |_) | (_) | |  | |_            ____) |  __/ |_| |_| | |_) |"
        )
        print(
            r"|_____/|____/_____|_| |_| |_| .__/ \___/|_|   \__|          |_____/ \___|\__|\__,_| .__/ "
        )
        print(
            "                            | |                                                   | |    "
        )
        print(
            "                            |_|                                                   |_|    "
        )
        sys.stdout.write("\u001b[0m")  # Reset
        sys.stdout.flush()
        print("")
        print(f"Version: {constant.VERSION}")
        print("")
        print("")

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

    if operation == "createDB":
        db_operations = database_operations.Initialize()
        db_operations.create_db()

    if operation == "upgradeDB":
        db_operations = database_operations.Initialize()
        db_operations.upgrade_db()

    if operation == "downgradeDB":
        db_operations = database_operations.Initialize()
        db_operations.downgradeDB()

    if operation == "createSchemaRst":
        db_operations = database_operations.Initialize()
        db_operations.generate_schema_rst()

    if operation == "buildHTML":
        web_operations = web_server_operations.WebServerOperations()
        web_operations.build_html()


def run() -> None:
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
