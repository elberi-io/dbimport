"""Version 0.80.007

Revision ID: 769cc11bdff9
Revises: 188b85533fda
Create Date: 2023-10-24 06:00:12.565042

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum


# revision identifiers, used by Alembic.
revision = "769cc11bdff9"
down_revision = "188b85533fda"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "concurrency",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
        ),
    )
    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "concurrency",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
        ),
    )
    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "concurrency",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
        ),
    )
    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "concurrency",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
        ),
    )


def downgrade():
    op.drop_column("airflow_import_dags", "concurrency")
    op.drop_column("airflow_export_dags", "concurrency")
    op.drop_column("airflow_custom_dags", "concurrency")
    op.drop_column("airflow_etl_dags", "concurrency")
