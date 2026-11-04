"""Version 0.69.001

Revision ID: 82bb45ccaca2
Revises: 4c7ab7d3b46c
Create Date: 2022-04-29 11:13:01.898724

"""

from alembic import op
from sqlalchemy import Enum


# revision identifiers, used by Alembic.
revision = "82bb45ccaca2"
down_revision = "4c7ab7d3b46c"
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
        ),
        existing_comment="Name of database type.  Name is hardcoded into Python scripts, so use only defined values",
        existing_nullable=False,
    )
