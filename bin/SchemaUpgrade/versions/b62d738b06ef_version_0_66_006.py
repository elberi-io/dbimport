"""Version 0.66.006

Revision ID: b62d738b06ef
Revises: 4368d52462a3
Create Date: 2021-03-12 13:42:25.517636

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "b62d738b06ef"
down_revision = "4368d52462a3"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "import_tables",
        "import_phase_type",
        existing_type=Enum("full", "incr", "oracle_flashback"),
        type_=Enum("full", "incr", "oracle_flashback", "mssql_change_tracking"),
        comment="What method to use for ETL phase",
        existing_server_default=sa.text("'full'"),
        server_default=sa.text("'full'"),
        existing_nullable=True,
    )

    op.alter_column(
        "airflow_tasks",
        "task_config",
        existing_type=mysql.VARCHAR(length=256),
        type_=mysql.VARCHAR(length=512),
        comment="The configuration for the Task. Depends on what Task type it is.",
        existing_nullable=True,
    )


def downgrade():
    op.alter_column(
        "airflow_tasks",
        "task_config",
        existing_type=mysql.VARCHAR(length=512),
        type_=mysql.VARCHAR(length=256),
        comment="The configuration for the Task. Depends on what Task type it is.",
        existing_nullable=True,
    )

    op.alter_column(
        "import_tables",
        "import_phase_type",
        existing_type=Enum("full", "incr", "oracle_flashback", "mssql_change_tracking"),
        type_=Enum("full", "incr", "oracle_flashback"),
        comment="What method to use for ETL phase",
        existing_server_default=sa.text("'full'"),
        server_default=sa.text("'full'"),
        existing_nullable=True,
    )
