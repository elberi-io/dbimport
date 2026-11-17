from alembic import context
from sqlalchemy import URL, create_engine, pool

from dbimport.config_db.config_schema import Base
from dbimport.settings import get_settings

config = context.config
target_metadata = Base.metadata


def _url() -> str | URL:
    """The URL that setup passes, with the credentials it resolved;
    otherwise, for the Alembic command line, the user and password of
    the configuration file."""
    url = config.get_main_option("sqlalchemy.url")
    if url:
        return url
    database = get_settings().database
    return URL.create(
        "mysql+pymysql",
        username=database.mysql_username,
        password=database.mysql_password.get_secret_value(),
        host=database.mysql_hostname,
        port=database.mysql_port,
        database=database.mysql_database,
    )


def run_migrations_offline() -> None:
    context.configure(
        url=_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(
        _url(), poolclass=pool.NullPool, connect_args={"connect_timeout": 10}
    )
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
