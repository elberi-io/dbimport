"""Version 0.80.001

Revision ID: 9f02bade6c68
Revises: 401193cd5776
Create Date: 2023-02-10 09:05:05.637761

"""

from alembic import op
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "9f02bade6c68"
down_revision = "401193cd5776"
# down_revision = '0be8afe157ef'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "jdbc_connections_drivers",
        "database_type",
        existing_type=Enum(
            "DB2 AS400",
            "DB2 UDB",
            "MySQL",
            "Oracle",
            "PostgreSQL",
            "Progress DB",
            "SQL Server",
            "MongoDB",
            "CacheDB",
            "Snowflake",
            "Informix",
        ),
        type_=Enum(
            "DB2 AS400",
            "DB2 UDB",
            "MySQL",
            "Oracle",
            "PostgreSQL",
            "Progress DB",
            "SQL Server",
            "MongoDB",
            "CacheDB",
            "Snowflake",
            "Informix",
            "SQL Anywhere",
        ),
        existing_comment="Name of database type.  Name is hardcoded into Python scripts, so use only defined values",
        existing_nullable=False,
    )


def downgrade():
    op.alter_column(
        "jdbc_connections_drivers",
        "database_type",
        existing_type=Enum(
            "DB2 AS400",
            "DB2 UDB",
            "MySQL",
            "Oracle",
            "PostgreSQL",
            "Progress DB",
            "SQL Server",
            "MongoDB",
            "CacheDB",
            "Snowflake",
            "Informix",
            "SQL Anywhere",
        ),
        type_=aEnum(  # noqa: F821  # TODO(#153): aEnum is undefined, downgrade fails
            "DB2 AS400",
            "DB2 UDB",
            "MySQL",
            "Oracle",
            "PostgreSQL",
            "Progress DB",
            "SQL Server",
            "MongoDB",
            "CacheDB",
            "Snowflake",
            "Informix",
        ),
        existing_comment="Name of database type.  Name is hardcoded into Python scripts, so use only defined values",
        existing_nullable=False,
    )
