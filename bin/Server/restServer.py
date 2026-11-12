import json
import logging
from datetime import datetime, timedelta
from typing import Annotated

import bcrypt
import pam
from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from gunicorn.app.wsgiapp import WSGIApplication
from jose import JWTError, jwt

from common import constants as constant
from common.Exceptions import InvalidConfigurationError
from ConfigReader import configuration
from DBImportConfig import common_config
from Schedule import Airflow
from Server import dataModels, restServerCalls

common_config = common_config.Config()

ADMIN_USER = common_config.get_config_value("restserver_admin_user")
AUTHENTICATION_METHOD = common_config.get_config_value(
    "restserver_authentication_method"
)
SECRET_KEY = common_config.get_config_value("restserver_secret_key")
ACCESS_TOKEN_EXPIRE_MINUTES = common_config.get_config_value("restserver_token_ttl")
ALGORITHM = "HS256"
try:
    AWS_REGION = configuration.get("AWS", "region", exit_on_error=False)
except InvalidConfigurationError:
    AWS_REGION = ""


class StandaloneApplication(WSGIApplication):
    def __init__(self, app_uri, options=None):
        self.options = options or {}
        self.app_uri = app_uri
        super().__init__()

    def load_config(self):
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)


def get_password_hash(password):
    if password.startswith("arn:aws:secretsmanager:"):
        # We dont hash "passwords" that links to external repository
        return password
    password_encoded = password.encode("utf-8")
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password=password_encoded, salt=salt)
    return hashed_password.decode("utf-8")


def verify_password(password, hashed_password):
    try:
        password_encoded = password.encode("utf-8")
        hashed_password_encoded = hashed_password.encode("utf-8")
        return bcrypt.checkpw(
            password=password_encoded, hashed_password=hashed_password_encoded
        )
    except ValueError:
        return False


logger = "gunicorn.error"
log = logging.getLogger(logger)

tags_metadata = [
    {"name": "General", "description": "Authentication and general status requests"},
    {"name": "Credentials", "description": "Users and Groups"},
    {"name": "Connections", "description": "Connections to JDBC servers"},
    {
        "name": "Imports",
        "description": "All requests related to Exports, except Airflow",
    },
    {
        "name": "Exports",
        "description": "All requests related to Imports, except Airflow",
    },
    {"name": "Airflow", "description": "Airflow functions"},
    {
        "name": "Configuration",
        "description": "All functions related to configurations and customization",
    },
]

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/oauth2/access_token")
app = FastAPI(openapi_tags=tags_metadata)
db_calls = restServerCalls.DBCalls()
airflow = Airflow.Initialize()
airflow.logger = logger


def get_user(username: str, format_password: bool = True):
    user_dict = db_calls.get_user(username)
    if user_dict == None:
        return

    if format_password:
        if user_dict["password"] == None or not user_dict["password"].startswith(
            "arn:aws:secretsmanager:"
        ):
            user_dict["password"] = "<encrypted>"

    return user_dict


def authenticate_user(username: str, password: str):
    user = get_user(username, format_password=False)
    username = user["username"]
    if not user:
        return False

    if AUTHENTICATION_METHOD == "local":
        if user["password"] == None:
            log.warning(
                "User '%s' made an unsuccessful authentication attempt", username
            )
            return False

        if not user["password"].startswith("arn:aws:secretsmanager:"):
            log.debug("Authenticate with LOCAL")
            if not verify_password(password, user["password"]):
                log.warning(
                    "User '%s' made an unsuccessful authentication attempt", username
                )
                return False
            else:
                log.info("User '%s' authenticated successfully", username)
                return True
        else:
            log.debug("Authenticate with LOCAL using AWS credentials")
            # This user have its password stored in AWS Secrets Manager

            if AWS_REGION == "":
                log.error(
                    "AWS Region is not configured in the configuraton file. This is required in order to user Secrets Manager for user credentials"
                )
                return False

            # 			if "boto3" not in sys.modules:
            # 				# Only load modules if not already loaded. If not deployed in AWS environment, these does not need to be loaded at all
            # 				print("Loading modules")
            import boto3
            from botocore.exceptions import ClientError

            # Create a Secrets Manager client
            session = boto3.session.Session()
            client = session.client(
                service_name="secretsmanager", region_name=AWS_REGION
            )

            try:
                get_secret_value_response = client.get_secret_value(
                    SecretId=user["password"]
                )
            except ClientError as e:
                # For a list of exceptions thrown, see
                # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
                raise e
                return False

            secrets_manager_password = json.loads(
                get_secret_value_response["SecretString"]
            )["password"]

            if password == secrets_manager_password:
                log.info("User '%s' authenticated successfully", username)
                return True
            else:
                log.warning(
                    "User '%s' made an unsuccessful authentication attempt", username
                )
                return False

    elif AUTHENTICATION_METHOD == "pam":
        log.debug("Authenticate with PAM")
        if not pam.authenticate(username, password, service="login"):
            log.warning(
                "User '%s' made an unsuccessful authentication attempt", username
            )
            return False
        else:
            log.info("User '%s' authenticated successfully", username)
            return True

    else:
        print("AUTHENTICATION_METHOD har fel värde")
        return False

    return False


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(
            token, SECRET_KEY, algorithms=[ALGORITHM]
        )  # This also verified that the token isnt expired
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_username = username
    except JWTError:
        raise credentials_exception

    current_user = get_user(token_username)
    if current_user is None:
        raise credentials_exception

    # 	if current_user.disabled:
    if current_user["disabled"]:
        raise HTTPException(status_code=400, detail="User access is disabled")

    return current_user


def create_access_token(data):
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    data.update({"exp": expire})

    encoded_jwt = jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)

    return encoded_jwt


@app.post("/oauth2/access_token", response_model=dataModels.Token, tags=["General"])
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
):
    valid_credentials = authenticate_user(form_data.username, form_data.password)
    if valid_credentials == False:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(data={"sub": form_data.username})
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/status", response_model=dataModels.Status, tags=["General"])
# async def get_restServer_status(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
async def get_server_status():
    return json.loads(json.dumps({"status": "ok", "version": constant.VERSION}))


@app.post("/users/createUser", tags=["Credentials"])
async def create_a_user(
    user_data: dataModels.User,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    user = get_user(user_data.username)

    if user != None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="User already exists"
        )

    if current_user["username"] != ADMIN_USER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not allowed to create users",
        )

    # set the new password in the user object and save it
    user_data.password = get_password_hash(user_data.password)
    db_calls.create_user(user_data)

    return "User created successfully"


@app.get("/users/{user}", response_model=dataModels.User, tags=["Credentials"])
async def get_user_details(
    user: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]
):
    user = get_user(user)
    if user == None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User does not exist"
        )

    return user


@app.post("/users/{user}/changePassword", tags=["Credentials"])
async def change_the_user_password(
    user: str,
    password_data: dataModels.ChangePassword,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    user = get_user(user, format_password=False)

    if AUTHENTICATION_METHOD != "local":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Changing password is only supported when authentication method is 'local'",
        )

    if user == None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User does not exist"
        )

    if current_user["username"] != ADMIN_USER:
        if user["username"] != current_user["username"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You are only allowed to change your own password",
            )

        if verify_password(password_data.old_password, user["password"]) == False:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Current password does not match",
            )

    # set the new password in the user object and save it
    user["password"] = get_password_hash(password_data.new_password)
    db_calls.update_user(
        user, password_changed=True, current_user=current_user["username"]
    )

    return "Password changed successfully"


@app.delete("/users/{user}/delete", tags=["Credentials"])
async def update_user_details(
    user: str,
    user_data: dataModels.ChangeUser,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    username = user
    user = get_user(username, format_password=False)
    if user == None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User does not exist"
        )

    if username == ADMIN_USER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Deleting the defined admin user is not allowed",
        )

    if current_user["username"] != ADMIN_USER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not allowed to delete users",
        )

    db_calls.delete_user(username)

    return "User deleted"


@app.post("/users/{user}/update", response_model=dataModels.User, tags=["Credentials"])
async def update_user_details(  # noqa: F811  # the delete handler above has this name; renaming changes its operationId
    user: str,
    user_data: dataModels.ChangeUser,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    username = user
    user = get_user(username, format_password=False)
    if user == None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User does not exist"
        )

    if current_user["username"] != ADMIN_USER:
        if user["username"] != current_user["username"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You are only allowed to change your own user",
            )

    if user_data.disabled is not None:
        user["disabled"] = user_data.disabled
    if user_data.fullname is not None:
        user["fullname"] = user_data.fullname
    if user_data.department is not None:
        user["department"] = user_data.department
    if user_data.email is not None:
        user["email"] = user_data.email

    db_calls.update_user(
        user, password_changed=False, current_user=current_user["username"]
    )

    user = get_user(
        username
    )  # Need to call it again to make sure the password is encrypted in the returned data
    return user


# @app.get("/config/getJDBCdrivers", response_model=dataModels.JDBCDriver)
@app.get("/config/getJDBCdrivers", tags=["Configuration"])
async def get_all_configured_jdbc_drivers(
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    return db_calls.get_jdbc_drivers()


@app.post("/config/updateJDBCdriver", tags=["Configuration"])
async def change_configuration_for_jdbc_drivers(
    jdbcDriver: dataModels.JDBCDriver,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    # response.status_code = status.HTTP_201_CREATED
    return_msg, response.status_code = db_calls.update_jdbc_driver(
        jdbcDriver, current_user["username"]
    )
    return return_msg


@app.get(
    "/config/getConfig", response_model=dataModels.Configuration, tags=["Configuration"]
)
async def get_global_configuration(
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    return db_calls.get_configuration(current_user["username"])


@app.post("/config/updateConfig", tags=["Configuration"])
async def update_global_configuration(
    configuration: dataModels.Configuration,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.update_configuration(
        configuration, current_user["username"]
    )
    return return_msg


@app.get(
    "/connection", response_model=list[dataModels.Connection], tags=["Connections"]
)
async def get_all_connections(
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    listOnlyName: bool = False,
):
    return db_calls.get_all_connections(listOnlyName)


@app.post("/connection", tags=["Connections"])
async def update_connection(
    connection: dataModels.ConnectionDetails,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.update_connection(
        connection, current_user["username"]
    )
    return return_msg


@app.get(
    "/connection/{connection}",
    response_model=dataModels.ConnectionDetails,
    tags=["Connections"],
)
async def get_connection_details(
    connection: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]
):
    return db_calls.get_connection(connection)


@app.delete("/connection/{connection}", tags=["Connections"])
async def delete_connection(
    connection: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.delete_connection(
        connection, current_user["username"]
    )
    return return_msg


@app.post(
    "/connection/encryptCredentials",
    response_model=dataModels.DefaultResultResponse,
    tags=["Connections"],
)
# @app.post("/connection/encryptCredentials", response_model=dataModels.encryptCredentialResult, tags=["Connections"])
async def encrypt_credentials_and_store_in_connection(
    credentials: dataModels.EncryptCredential,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.encrypted_credentials(
        credentials, current_user["username"]
    )
    return return_msg


@app.post(
    "/connection/generateJDBCconnectionString",
    response_model=dataModels.GeneratedJDBCConnectionString,
    tags=["Connections"],
)
async def generate_a_jdbc_connection_string(
    connectionValues: dataModels.GenerateJDBCConnectionString,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.generate_jdbc_connection_string(
        connectionValues, current_user["username"]
    )
    return return_msg


@app.post(
    "/connection/search",
    response_model=list[dataModels.Connection],
    tags=["Connections"],
)
async def search_connections(
    searchValues: dataModels.ConnectionSearch,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return db_calls.search_connections(searchValues, current_user["username"])


@app.get(
    "/connection/testConnection/{connection}",
    response_model=dataModels.DefaultResultResponse,
    tags=["Connections"],
)
async def test_a_connection(
    connection: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.test_connection(
        connection, current_user["username"]
    )
    return return_msg


@app.get("/import/db", response_model=list[dataModels.ImportDBs], tags=["Imports"])
async def get_all_import_databases(
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    return db_calls.get_all_import_databases()


@app.post(
    "/import/discover/search",
    response_model=list[dataModels.DiscoverImportTable],
    tags=["Imports"],
)
async def discover_new_import_tables(
    data: dataModels.DiscoverImportTableOptions,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.discover_import_tables(
        data, current_user["username"]
    )
    return return_msg


@app.post(
    "/import/discover/add",
    response_model=dataModels.DefaultResultResponse,
    tags=["Imports"],
)
async def add_discovered_import_tables(
    data: list[dataModels.DiscoverImportTable],
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.add_discovered_import_tables(
        data, current_user["username"]
    )
    return return_msg


@app.post(
    "/import/search", response_model=list[dataModels.ImportTable], tags=["Imports"]
)
async def search_import_tables(
    searchValues: dataModels.ImportTableSearch,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return db_calls.search_import_tables(searchValues, current_user["username"])


@app.post(
    "/import/table", response_model=dataModels.DefaultResultResponse, tags=["Imports"]
)
async def multi_record_update_import_table(
    bulkData: dataModels.ImportTableBulkUpdate,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_update_import_table(
        bulkData, current_user["username"]
    )
    return return_msg


@app.delete(
    "/import/table", response_model=dataModels.DefaultResultResponse, tags=["Imports"]
)
async def multi_record_delete_import_table(
    bulkData: list[dataModels.ImportTablePK],
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_delete_import_table(
        bulkData, current_user["username"]
    )
    return return_msg


@app.get(
    "/import/table/{connection}",
    response_model=list[dataModels.ImportTable],
    tags=["Imports"],
)
async def get_import_tables_on_a_connection(
    connection: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]
):
    return db_calls.get_import_tables_on_connection(
        connection, current_user["username"]
    )


@app.get(
    "/import/table/{database}",
    response_model=list[dataModels.ImportTable],
    tags=["Imports"],
)
async def get_import_tables_in_database(
    database: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]
):
    return db_calls.get_import_tables_in_database(database, current_user["username"])


@app.get(
    "/import/table/{database}/{table}",
    response_model=dataModels.ImportTableDetailsRead,
    tags=["Imports"],
)
async def get_import_table_details(
    database: str,
    table: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    includeColumns: bool = True,
):
    return db_calls.get_import_table_details(
        database=database, table=table, include_columns=includeColumns
    )


@app.delete("/import/table/{database}/{table}", tags=["Imports"])
async def delete_import_table(
    database: str,
    table: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.delete_import_table(
        database, table, current_user["username"]
    )
    return return_msg


@app.post(
    "/import/table/{database}/{table}/repair",
    response_model=dataModels.DefaultResultResponse,
    tags=["Imports"],
)
async def repair_import_table(
    database: str,
    table: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.repair_import_table(
        database, table, current_user["username"]
    )
    return return_msg


@app.post(
    "/import/table/{database}/{table}/reset",
    response_model=dataModels.DefaultResultResponse,
    tags=["Imports"],
)
async def reset_import_table(
    database: str,
    table: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.reset_import_table(
        database, table, current_user["username"]
    )
    return return_msg


@app.post("/import/table/{database}/{table}", tags=["Imports"])
async def create_or_update_import_table(
    database: str,
    table: str,
    data: dataModels.ImportTableDetailsWrite,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.update_import_table(
        database, table, data, current_user["username"]
    )
    return return_msg


@app.get(
    "/export/connection",
    response_model=list[dataModels.ExportConnections],
    tags=["Exports"],
)
async def get_all_connections_with_exports(
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    return db_calls.get_export_connections()


@app.post(
    "/export/discover/search",
    response_model=list[dataModels.DiscoverExportTable],
    tags=["Exports"],
)
async def discover_new_export_tables(
    data: dataModels.DiscoverExportTableOptions,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.discover_export_tables(
        data, current_user["username"]
    )
    return return_msg


@app.post(
    "/export/discover/add",
    response_model=dataModels.DefaultResultResponse,
    tags=["Exports"],
)
async def add_discovered_export_tables(
    data: list[dataModels.DiscoverExportTable],
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.add_discovered_export_tables(
        data, current_user["username"]
    )
    return return_msg


@app.post(
    "/export/search", response_model=list[dataModels.ExportTable], tags=["Exports"]
)
async def search_export_tables(
    searchValues: dataModels.ExportTableSearch,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return db_calls.search_export_tables(searchValues, current_user["username"])


@app.post(
    "/export/table", response_model=dataModels.DefaultResultResponse, tags=["Exports"]
)
async def multi_record_update_export_table(
    bulkData: dataModels.ExportTableBulkUpdate,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_update_export_table(
        bulkData, current_user["username"]
    )
    return return_msg


@app.delete(
    "/export/table", response_model=dataModels.DefaultResultResponse, tags=["Exports"]
)
async def multi_record_delete_export_table(
    bulkData: list[dataModels.ExportTablePK],
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_delete_export_table(
        bulkData, current_user["username"]
    )
    return return_msg


@app.get(
    "/export/table/{connection}",
    response_model=list[dataModels.ExportTable],
    tags=["Exports"],
)
async def get_export_tables_on_connection(
    connection: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return db_calls.get_export_tables(
        connection=connection, schema=None, current_user=current_user["username"]
    )


@app.get(
    "/export/table/{connection}/{schema}",
    response_model=list[dataModels.ExportTable],
    tags=["Exports"],
)
async def get_export_tables_on_connection_and_schema(
    connection: str,
    schema: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return db_calls.get_export_tables(
        connection=connection, schema=schema, current_user=current_user["username"]
    )


@app.get(
    "/export/table/{connection}/{schema}/{table}",
    response_model=dataModels.ExportTableDetailsRead,
    tags=["Exports"],
)
async def get_export_table_details(
    connection: str,
    schema: str,
    table: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    includeColumns: bool = True,
):
    return db_calls.get_export_table_details(
        connection=connection,
        schema=schema,
        table=table,
        include_columns=includeColumns,
    )


@app.post("/export/table/{connection}/{schema}/{table}", tags=["Exports"])
async def create_or_update_export_table(
    connection: str,
    schema: str,
    table: str,
    data: dataModels.ExportTableDetailsWrite,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.update_export_table(
        connection, schema, table, data, current_user["username"]
    )
    return return_msg


@app.delete("/export/table/{connection}/{schema}/{table}", tags=["Exports"])
async def delete_export_table(
    connection: str,
    schema: str,
    table: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.delete_export_table(
        connection, schema, table, current_user["username"]
    )
    return return_msg


@app.post(
    "/export/table/{connection}/{schema}/{table}/repair",
    response_model=dataModels.DefaultResultResponse,
    tags=["Exports"],
)
async def repair_export_table(
    connection: str,
    schema: str,
    table: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.repair_export_table(
        connection, schema, table, current_user["username"]
    )
    return return_msg


@app.post(
    "/export/table/{connection}/{schema}/{table}/reset",
    response_model=dataModels.DefaultResultResponse,
    tags=["Exports"],
)
async def reset_export_table(
    connection: str,
    schema: str,
    table: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.reset_export_table(
        connection, schema, table, current_user["username"]
    )
    return return_msg


@app.get(
    "/airflow/dags", response_model=list[dataModels.AirflowAllDAGs], tags=["Airflow"]
)
async def get_all_airflow_dags(
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    return db_calls.get_all_airflow_dags()


@app.get("/airflow/generate_dag", tags=["Airflow"])
async def generate_airflow_dag(
    dagname: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = airflow.generate_dag_from_rest(
        dagname, current_user["username"]
    )
    return return_msg


@app.get(
    "/airflow/dags/import",
    response_model=list[dataModels.AirflowImportDAGs],
    tags=["Airflow"],
)
async def get_import_airflow_dags(
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    return db_calls.get_airflow_import_dags()


@app.post(
    "/airflow/dags/import",
    response_model=dataModels.DefaultResultResponse,
    tags=["Airflow"],
)
async def multi_record_update_import_airflow_dag(
    bulkData: dataModels.AirflowDAGBulkUpdate,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_update_import_airflow_dag(
        bulkData, current_user["username"]
    )
    return return_msg


@app.delete(
    "/airflow/dags/import",
    response_model=dataModels.DefaultResultResponse,
    tags=["Airflow"],
)
async def multi_record_delete_import_airflow_dag(
    bulkData: list[dataModels.AirflowDAGPK],
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_delete_import_airflow_dag(
        bulkData, current_user["username"]
    )
    return return_msg


@app.get(
    "/airflow/dags/import/{dagname}",
    response_model=dataModels.AirflowImportDAG,
    tags=["Airflow"],
)
async def get_import_airflow_dag(
    dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]
):
    return db_calls.get_airflow_import_dag(dagname)


@app.post("/airflow/dags/import/{dagname}", tags=["Airflow"])
async def create_or_update_import_airflow_dag(
    dagname: str,
    airflowDag: dataModels.AirflowImportDAG,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.update_import_airflow_dag(
        dagname, airflowDag, current_user["username"]
    )
    return return_msg


@app.delete("/airflow/dags/import/{dagname}", tags=["Airflow"])
async def delete_import_airflow_dag(
    dagname: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.delete_import_airflow_dag(
        dagname, current_user["username"]
    )
    return return_msg


@app.delete("/airflow/dags/import/{dagname}/{taskname}", tags=["Airflow"])
async def delete_task_from_import_airflow_dag(
    dagname: str,
    taskname: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.delete_task_from_airflow_dag(
        dagname, taskname, current_user["username"]
    )
    return return_msg


@app.get(
    "/airflow/dags/export",
    response_model=list[dataModels.AirflowExportDAGs],
    tags=["Airflow"],
)
async def get_export_airflow_dags(
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    return db_calls.get_airflow_export_dags()


@app.post(
    "/airflow/dags/export",
    response_model=dataModels.DefaultResultResponse,
    tags=["Airflow"],
)
async def multi_record_update_export_airflow_dag(
    bulkData: dataModels.AirflowDAGBulkUpdate,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_update_export_airflow_dag(
        bulkData, current_user["username"]
    )
    return return_msg


@app.delete(
    "/airflow/dags/export",
    response_model=dataModels.DefaultResultResponse,
    tags=["Airflow"],
)
async def multi_record_delete_export_airflow_dag(
    bulkData: list[dataModels.AirflowDAGPK],
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_delete_export_airflow_dag(
        bulkData, current_user["username"]
    )
    return return_msg


@app.get(
    "/airflow/dags/export/{dagname}",
    response_model=dataModels.AirflowExportDAG,
    tags=["Airflow"],
)
async def get_export_airflow_dag(
    dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]
):
    return db_calls.get_airflow_export_dag(dagname)


@app.post("/airflow/dags/export/{dagname}", tags=["Airflow"])
async def create_or_update_export_airflow_dag(
    dagname: str,
    airflowDag: dataModels.AirflowExportDAG,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.update_export_airflow_dag(
        dagname, airflowDag, current_user["username"]
    )
    return return_msg


@app.delete("/airflow/dags/export/{dagname}", tags=["Airflow"])
async def delete_export_airflow_dag(
    dagname: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.delete_export_airflow_dag(
        dagname, current_user["username"]
    )
    return return_msg


@app.delete("/airflow/dags/export/{dagname}/{taskname}", tags=["Airflow"])
async def delete_task_from_export_airflow_dag(
    dagname: str,
    taskname: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.delete_task_from_airflow_dag(
        dagname, taskname, current_user["username"]
    )
    return return_msg


@app.get(
    "/airflow/dags/custom",
    response_model=list[dataModels.AirflowCustomDAGs],
    tags=["Airflow"],
)
async def get_custom_airflow_dags(
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
):
    return db_calls.get_airflow_custom_dags()


@app.post(
    "/airflow/dags/custom",
    response_model=dataModels.DefaultResultResponse,
    tags=["Airflow"],
)
async def multi_record_update_custom_airflow_dag(
    bulkData: dataModels.AirflowDAGBulkUpdate,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_update_custom_airflow_dag(
        bulkData, current_user["username"]
    )
    return return_msg


@app.delete(
    "/airflow/dags/custom",
    response_model=dataModels.DefaultResultResponse,
    tags=["Airflow"],
)
async def multi_record_delete_custom_airflow_dag(
    bulkData: list[dataModels.AirflowDAGPK],
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.bulk_delete_custom_airflow_dag(
        bulkData, current_user["username"]
    )
    return return_msg


@app.get(
    "/airflow/dags/custom/{dagname}",
    response_model=dataModels.AirflowCustomDAG,
    tags=["Airflow"],
)
async def get_custom_airflow_dag(
    dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]
):
    return db_calls.get_airflow_custom_dag(dagname)


@app.post("/airflow/dags/custom/{dagname}", tags=["Airflow"])
async def create_or_update_custom_airflow_dag(
    dagname: str,
    airflowDag: dataModels.AirflowCustomDAG,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.update_custom_airflow_dag(
        dagname, airflowDag, current_user["username"]
    )
    return return_msg


@app.delete("/airflow/dags/custom/{dagname}", tags=["Airflow"])
async def delete_custom_airflow_dag(
    dagname: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.delete_custom_airflow_dag(
        dagname, current_user["username"]
    )
    return return_msg


@app.delete("/airflow/dags/custom/{dagname}/{taskname}", tags=["Airflow"])
async def delete_task_from_custom_airflow_dag(
    dagname: str,
    taskname: str,
    current_user: Annotated[dataModels.User, Depends(get_current_user)],
    response: Response,
):
    return_msg, response.status_code = db_calls.delete_task_from_airflow_dag(
        dagname, taskname, current_user["username"]
    )
    return return_msg
