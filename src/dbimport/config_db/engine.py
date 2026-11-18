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
"""The connection to the configuration database (ADR-0006)."""

import json
import logging
import threading
from dataclasses import dataclass, field
from functools import cache

from sqlalchemy import URL, Engine, create_engine

from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.settings import Settings, get_settings

TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class Credentials:
    hostname: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)


def credentials(settings: Settings) -> Credentials:
    """The credentials of the configuration file, or, when its user and
    password are empty and an AWS region is set, those of the AWS Secrets
    Manager secret named in it.

    Raises InvalidConfigurationError when neither is configured, and
    botocore's ClientError when the secret cannot be read.
    """
    database = settings.database
    username = database.mysql_username
    password = database.mysql_password.get_secret_value()
    region = settings.aws.region
    if username == "" and password == "" and region is not None:
        if database.aws_secret_name is None:
            raise InvalidConfigurationError(
                "Credentials configuration to configuration database in config "
                "file is incorrect. Either use username/password or a AWS Secret "
                "Manager name"
            )
        import boto3
        from botocore.config import Config

        client = boto3.session.Session().client(
            service_name="secretsmanager",
            region_name=region,
            config=Config(
                connect_timeout=TIMEOUT_SECONDS, read_timeout=TIMEOUT_SECONDS
            ),
        )
        secret = json.loads(
            client.get_secret_value(SecretId=database.aws_secret_name)["SecretString"]
        )
        username = secret["username"]
        password = secret["password"]
    elif username == "" or password == "":
        raise InvalidConfigurationError(
            "Credentials configuration to configuration database in config file "
            "is incorrect. Either use username/password or a AWS Secret Manager "
            "name"
        )
    return Credentials(
        hostname=database.mysql_hostname,
        port=database.mysql_port,
        database=database.mysql_database,
        username=username,
        password=password,
    )


@cache
def configured_credentials() -> Credentials:
    """credentials() of the process's settings, resolved once per process."""
    return credentials(get_settings())


def url(found: Credentials) -> URL:
    return URL.create(
        "mysql+pymysql",
        username=found.username,
        password=found.password,
        host=found.hostname,
        port=found.port,
        database=found.database,
    )


def create(echo: bool = False) -> Engine:
    """A new engine of the configuration database, with its own connection
    pool, for a component that disposes of it on its own."""
    return create_engine(
        url(configured_credentials()),
        echo=echo,
        hide_parameters=True,
        pool_pre_ping=True,
        connect_args={"connect_timeout": TIMEOUT_SECONDS},
    )


_shared: Engine | None = None
_shared_lock = threading.Lock()


def engine() -> Engine:
    """The engine that the config_db classes share in a process; never
    disposed. SQL is echoed when the root logger is at DEBUG level when it
    is created."""
    global _shared
    with _shared_lock:
        if _shared is None:
            _shared = create(echo=logging.getLogger().isEnabledFor(logging.DEBUG))
        return _shared
