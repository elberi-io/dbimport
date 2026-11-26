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
"""The connection to the configuration database of another DBImport
instance, with the credentials stored for it in dbimport_instances."""

from sqlalchemy import Engine

from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.config_db import engine as config_engine
from dbimport.config_db.decryption import Crypto
from dbimport.config_db.repositories.copies import InstanceConnection


def instance_engine(
    connection: InstanceConnection, crypto: Crypto, echo: bool = False
) -> Engine:
    """A new engine of the configuration database of the instance. The
    stored credentials are the user, a space and the password, encrypted
    with the key pair of crypto. Opens and closes one connection to check
    them.

    Raises InvalidConfigurationError when the credentials are missing,
    cannot be decrypted or have no password, and sqlalchemy's DBAPIError
    when the database cannot be reached or refuses them.
    """
    name = connection.name
    if connection.db_credentials is None:
        raise InvalidConfigurationError(
            f"There are no credentials saved in 'dbimport_instances' for {name}"
        )
    try:
        decrypted = crypto.decrypt(connection.db_credentials)
    # ValueError: not base64 (binascii.Error) or of the wrong length;
    # TypeError: the private key file holds a public key.
    except (ValueError, TypeError, InvalidConfigurationError) as err:
        raise InvalidConfigurationError(
            f"Cannot decrypt the credentials saved in 'dbimport_instances' for "
            f"{name}: {err}"
        ) from err
    if decrypted is None:
        raise InvalidConfigurationError(
            f"Cannot decrypt the credentials saved in 'dbimport_instances' for "
            f"{name}. Check the private and public key in the configuration file"
        )
    username, separator, password = decrypted.partition(" ")
    if separator == "":
        raise InvalidConfigurationError(
            f"The credentials saved in 'dbimport_instances' for {name} have no password"
        )
    engine = config_engine.create_for(
        config_engine.Credentials(
            hostname=connection.db_hostname,
            port=connection.db_port,
            database=connection.db_database,
            username=username,
            password=password,
        ),
        echo=echo,
    )
    try:
        engine.connect().close()
    except Exception:
        engine.dispose()
        raise
    return engine
