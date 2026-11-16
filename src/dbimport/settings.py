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
"""The DBImport configuration file as typed settings (ADR-0002).

The INI file is `DBIMPORT_CONFIGFILE`, else
`$DBIMPORT_HOME/conf/dbimport.cfg.dist`, read as configparser always
read it (`%%` is a literal `%`). A blank value of an optional key means
the key is not set. An environment variable `DBIMPORT_<SECTION>__<KEY>`
overrides a value from the file. The file is read and validated on the
first `get_settings()` call; a missing or invalid value ends the program
with one message per problem.
"""

import configparser
import os
import sys
from functools import cache
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    SecretStr,
    ValidationError,
    model_validator,
)
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    SettingsError,
)


def _lower(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


class DatabaseSettings(BaseModel):
    mysql_hostname: str
    mysql_port: int
    mysql_database: str
    mysql_username: str
    mysql_password: SecretStr
    aws_secret_name: str | None = None


class AwsSettings(BaseModel):
    region: str | None = None


class KerberosSettings(BaseModel):
    keytab: str
    principal: str


class MetastoreSettings(BaseModel):
    metastore_type: str
    hive_metastore_alchemy_conn: SecretStr


class HiveSettings(BaseModel):
    servers: str
    kerberos_service_name: str
    kerberos_realm: str
    use_ssl: bool


class ImpalaSettings(BaseModel):
    driver: str | None = None
    class_: str | None = Field(default=None, alias="class")
    address: str | None = None


class SparkSettings(BaseModel):
    spark_major_version: Annotated[int, Field(ge=2, le=4)] = 3
    spark_log_level: str | None = None
    path_append: str
    jar_files: str
    py_files: str
    packages: str | None = None
    master: str
    deploymode: str
    yarnqueue: str
    dynamic_allocation: bool
    executor_memory: str
    hive_library: str
    s3_access_to_ozone: bool = False
    s3_access_key_id: str = ""
    s3_secret_access_key: SecretStr = SecretStr("")

    @model_validator(mode="after")
    def _s3_keys_with_ozone(self) -> "SparkSettings":
        if self.s3_access_to_ozone and not (
            self.s3_access_key_id and self.s3_secret_access_key.get_secret_value()
        ):
            raise ValueError(
                "s3_access_to_ozone = true needs s3_access_key_id and s3_secret_access_key"
            )
        return self


class SqoopSettings(BaseModel):
    yarnqueue: str


class AirflowSettings(BaseModel):
    airflow_mode: Literal["default", "aws_mwaa"] = "default"
    # Only needed for the Airflow database integration, which checks it.
    airflow_alchemy_conn: SecretStr | None = None


class AtlasSettings(BaseModel):
    # Only read when Atlas is used (address set); the defaults are the
    # values the code used before when a key was missing or invalid.
    address: str = ""
    timeout: int = 5
    ssl_verify: bool = True
    ssl_cert_path: str = ""
    authentication_type: (
        Annotated[Literal["kerberos", "username"], BeforeValidator(_lower)] | None
    ) = None
    username: str = ""
    password: SecretStr = SecretStr("")


class CredentialsSettings(BaseModel):
    private_key: str
    public_key: str


class ServerSettings(BaseModel):
    logdir: str
    pidfile: str
    distcp_threads: int
    distcp_separate_logs: bool
    distcp_yarnqueue: str
    atlas_threads: int
    restserver_address: str
    restserver_port: int
    restserver_workers: int
    restserver_keyfile: str
    restserver_certfile: str


class AnonymizationSettings(BaseModel):
    seed: SecretStr


class ConfigFileError(Exception):
    """The configuration file cannot be read; `problems` lists why (no values)."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


class _IniSource(PydanticBaseSettingsSource):
    """Sections and keys of the INI file, both lowercased (keys as configparser does).

    A blank value of a key that has a default is left out, so the default
    applies.
    """

    def __init__(self, settings_cls: type[BaseSettings], path: Path) -> None:
        super().__init__(settings_cls)
        parser = configparser.ConfigParser()
        try:
            with path.open(encoding="utf-8") as config_file:
                parser.read_file(config_file)
        except (OSError, UnicodeDecodeError, configparser.Error) as error:
            raise ConfigFileError([f"{path}: {type(error).__name__}"]) from error
        problems = []
        self._data: dict[str, dict[str, str]] = {}
        for section in parser.sections():
            name = section.lower()
            field = settings_cls.model_fields.get(name)
            model = field.annotation if field else None
            keys = (
                model.model_fields
                if isinstance(model, type) and issubclass(model, BaseModel)
                else {}
            )
            values = {}
            for key in parser[section]:
                try:
                    value = parser.get(section, key)
                except configparser.InterpolationError:
                    problems.append(f"[{section}] {key}: a '%' must be written as '%%'")
                    continue
                if value.strip() == "" and key in keys and not keys[key].is_required():
                    continue
                values[key] = value
            self._data[name] = values
        if problems:
            raise ConfigFileError(problems)

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> tuple[Any, str, bool]:
        return self._data.get(field_name), field_name, False

    def __call__(self) -> dict[str, Any]:
        return dict(self._data)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DBIMPORT_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )
    config_file: Path = Field(exclude=True)

    database: DatabaseSettings
    aws: AwsSettings = AwsSettings()
    kerberos: KerberosSettings
    metastore: MetastoreSettings
    hive: HiveSettings
    impala: ImpalaSettings = ImpalaSettings()
    spark: SparkSettings
    sqoop: SqoopSettings
    airflow: AirflowSettings = AirflowSettings()
    atlas: AtlasSettings = AtlasSettings()
    credentials: CredentialsSettings
    server: ServerSettings
    anonymization: AnonymizationSettings

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Highest priority first: arguments, environment, then the INI file.
        path = init_settings().get("config_file") or config_file_path()
        return (init_settings, env_settings, _IniSource(settings_cls, Path(path)))


def config_file_path() -> Path:
    """`DBIMPORT_CONFIGFILE`, else the template under `DBIMPORT_HOME`; exits if neither works."""
    if "DBIMPORT_HOME" not in os.environ:
        print("Error: System Environment Variable DBIMPORT_HOME is not set")
        sys.exit(1)
    path = Path(
        os.environ.get(
            "DBIMPORT_CONFIGFILE",
            os.path.join(os.environ["DBIMPORT_HOME"], "conf", "dbimport.cfg.dist"),
        )
    )
    if not path.is_file():
        print(f"Error: Configuration file can't be found on '{path}'")
        sys.exit(1)
    return path


# Section names as the configuration file writes them.
SECTIONS = {
    "database": "Database",
    "aws": "AWS",
    "kerberos": "Kerberos",
    "metastore": "Metastore",
    "hive": "Hive",
    "impala": "Impala",
    "spark": "Spark",
    "sqoop": "Sqoop",
    "airflow": "Airflow",
    "atlas": "Atlas",
    "credentials": "Credentials",
    "server": "Server",
    "anonymization": "Anonymization",
}


def _problems(error: ValidationError) -> list[str]:
    # Location and message only: the input value may be a secret.
    lines = []
    for item in error.errors():
        loc = [str(part) for part in item["loc"]]
        section = SECTIONS.get(loc[0], loc[0]) if loc else ""
        key = ".".join(loc[1:])
        lines.append(
            f"[{section}] {key}: {item['msg']}"
            if key
            else f"[{section}]: {item['msg']}"
        )
    return lines


def load_settings(path: Path | None = None) -> Settings:
    """Read and validate the configuration file.

    Raises ConfigFileError, ValidationError, or SettingsError for a value
    from the environment that cannot be parsed.
    """
    # The other fields come from the file and the environment, which mypy
    # does not see; pydantic checks them.
    return Settings(config_file=path or config_file_path())  # type: ignore[call-arg]


@cache
def get_settings() -> Settings:
    """The settings of this process; on invalid configuration prints every problem and exits."""
    try:
        return load_settings()
    except ValidationError as error:
        problems = _problems(error)
    except ConfigFileError as error:
        problems = error.problems
    except SettingsError as error:
        problems = [str(error)]
    print("Error: The configuration file is not valid:")
    for line in problems:
        print(f"  {line}")
    sys.exit(1)
