"""Version 0.65.005

Revision ID: 670b0bc3eb69
Revises: c5a67d0c7b67
Create Date: 2019-11-22 05:15:41.010129

"""

from alembic import op
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "670b0bc3eb69"
down_revision = "c5a67d0c7b67"
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
        ),
        type_=Enum(
            "DB2 AS400",
            "DB2 UDB",
            "MySQL",
            "Oracle",
            "PostgreSQL",
            "Progress DB",
            "SQL Server",
        ),
        existing_comment="Name of database type.  Name is hardcoded into Python scripts, so use only defined values",
        existing_nullable=False,
    )
