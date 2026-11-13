#!/usr/bin/env python3
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
import os
import signal
import sys

from gunicorn import glogging
from gunicorn.app.wsgiapp import WSGIApplication

from dbimport.common import constants as constant
from dbimport.config_reader import configuration

# Kept although unused: importing dbimport.server.rest_server connects to the
# configuration database. Without it, the gunicorn workers would make
# that connection after they fork.
from dbimport.server import rest_server  # noqa: F401


class StandaloneApplication(WSGIApplication):
    def __init__(self, app_uri, options=None):
        self.options = options or {}
        self.app_uri = app_uri
        super().__init__()

    def load_config(self):
        unknown = sorted(key for key in self.options if key not in self.cfg.settings)
        if unknown:
            raise ValueError(f"Unknown gunicorn settings: {', '.join(unknown)}")
        for key, value in self.options.items():
            if value is not None:
                self.cfg.set(key.lower(), value)


def print_help():
    print("Usage: ./server <action> <options>")
    print("Actions:")
    print("  start              Start the DBImport REST server")
    print("  stop               Stop the DBImport REST server")
    print("  restart            Stop and start the DBImport REST server")
    print("  noDaemon           Start the DBImport REST server running in foreground")
    print("  status             Check status of DBImport REST server")
    print("")
    print("Options:")
    print("  --help             Show this help message and exit")
    print("  -v, --debug        Turn on extensive debug output.")
    print("  -V, --version      Displays the version of DBImport")
    sys.exit(1)


def setup_logger(name, logfile, log_format, logging_level, log_propagate=True):
    new_logger = logging.getLogger(name)
    new_logger.propagate = log_propagate
    log_handler = logging.FileHandler(logfile)
    log_handler.setFormatter(logging.Formatter(log_format))
    log_handler.setLevel(logging_level)
    new_logger.addHandler(log_handler)


def main(argv):

    logging_level = logging.INFO
    display_version = False
    daemon_mode = False
    action = ""

    if argv != []:
        try:
            action = sys.argv[1]
            opts, args = getopt.getopt(argv[1:], "vVh", ["version", "help", "debug"])
        except getopt.GetoptError:
            print_help()

        for opt, _arg in opts:
            if opt in ("-h", "--help"):
                print_help()
            elif opt in ("-V", "--version"):
                display_version = True
            elif opt in ("-v", "--debug"):
                logging_level = logging.DEBUG

    if action not in ["start", "stop", "restart", "status", "noDaemon"]:
        print_help()
        sys.exit(0)

    if action in ("start", "restart"):
        daemon_mode = True

    if display_version:
        print(f"DBImport version: {constant.VERSION}")
        sys.exit(0)

    logdir = configuration.get("Server", "logdir")
    pidfile = configuration.get("Server", "pidfile")
    rest_server_address = configuration.get("Server", "restServer_address")
    rest_server_port = configuration.get("Server", "restServer_port")
    rest_server_workers = configuration.get("Server", "restServer_workers")
    rest_server_keyfile = configuration.get("Server", "restServer_keyfile")
    rest_server_certfile = configuration.get("Server", "restServer_certfile")

    if not os.path.isfile(rest_server_keyfile):
        print(
            "The restServer keyfile cant be opened.\n"
            + "Please check the path in the configuration file for settings Server/restServer_keyfile"
        )
        sys.exit(1)

    if not os.path.isfile(rest_server_certfile):
        print(
            "The restServer certfile cant be opened.\n"
            + "Please check the path in the configuration file for settings Server/restServer_certfile"
        )
        sys.exit(1)

    if logging_level == logging.DEBUG:
        log_format = "%(asctime)s %(levelname)s %(name)s %(funcName)s - %(message)s"
    else:
        log_format = "%(asctime)s %(levelname)s - %(message)s"

    if not daemon_mode:
        logging.basicConfig(stream=sys.stdout, format=log_format, level=logging_level)
        log_propagate = True
    else:
        # If we run in daemon mode, we log everything from the root logger to /dev/null.
        logging.basicConfig(
            filename="/dev/null", format=log_format, level=logging_level
        )
        log_propagate = False

    # Setup the loggers
    setup_logger(
        name="server",
        logfile=f"{logdir}/restServer.log",
        log_format=log_format,
        logging_level=logging_level,
        log_propagate=log_propagate,
    )

    glogging.Logger.datefmt = ""
    glogging.Logger.error_fmt = log_format
    glogging.Logger.access_fmt = log_format
    glogging.Logger.syslog_fmt = log_format

    if logging_level == logging.DEBUG:
        gunicorn_debug_level = "debug"
    else:
        gunicorn_debug_level = "info"

    if daemon_mode:
        gunicorn_access_log = f"{logdir}/restServer_access.log"
        gunicorn_error_log = f"{logdir}/restServer.log"
    else:
        gunicorn_access_log = "-"
        gunicorn_error_log = "-"

    options = {
        "bind": f"{rest_server_address}:{rest_server_port}",
        "workers": int(rest_server_workers),
        "worker_class": "uvicorn.workers.UvicornWorker",
        "loglevel": gunicorn_debug_level,
        "accesslog": gunicorn_access_log,
        "errorlog": gunicorn_error_log,
        "daemon": daemon_mode,
        "keyfile": rest_server_keyfile,
        "certfile": rest_server_certfile,
        "control_socket_disable": True,
    }

    if action == "start":
        print("Starting DBImport REST Server")
        StandaloneApplication("dbimport.server.rest_server:app", options).run()

    elif action == "noDaemon":
        StandaloneApplication("dbimport.server.rest_server:app", options).run()

    elif action == "stop":
        print("Stopping DBImport REST Server")
        for line in os.popen(
            'ps ax|grep -E "/restServer restart$|/restServer start$|/restServer noDaemon"|grep -v grep'
        ):
            pid = int(line.split()[0])
            os.kill(pid, signal.SIGTERM)

    elif action == "restart":
        print("Stopping DBImport REST Server")
        current_pid = os.getpid()
        for line in os.popen(
            'ps ax|grep -E "/restServer restart$|/restServer start$|/restServer noDaemon"|grep -v grep'
        ):
            pid = int(line.split()[0])
            if pid == current_pid:
                continue
            os.kill(pid, signal.SIGTERM)

        print("Starting DBImport REST Server")
        StandaloneApplication("dbimport.server.rest_server:app", options).run()

    elif action == "status":
        processes = os.popen('ps ax|grep "/restServer start$"|grep -v grep').read()
        if processes == "":
            print("DBImport REST Server is not running")
            sys.exit(1)
        else:
            print("DBImport REST Server is running")
            sys.exit(0)

    else:
        print_help()


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except getopt.GetoptError:
        print_help()
