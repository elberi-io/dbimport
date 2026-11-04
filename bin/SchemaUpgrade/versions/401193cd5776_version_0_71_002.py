"""Version 0.71.002

Revision ID: 401193cd5776
Revises: 0cb3d65c99b0
Create Date: 2022-10-28 04:58:09.301467

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum


# revision identifiers, used by Alembic.
revision = "401193cd5776"
down_revision = "0cb3d65c99b0"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "tags",
            sa.String(length=256),
            nullable=True,
            comment="Comma seperated list of Airflow tags that will be set on the Dag",
        ),
    )
    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "tags",
            sa.String(length=256),
            nullable=True,
            comment="Comma seperated list of Airflow tags that will be set on the Dag",
        ),
    )
    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "tags",
            sa.String(length=256),
            nullable=True,
            comment="Comma seperated list of Airflow tags that will be set on the Dag",
        ),
    )
    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "tags",
            sa.String(length=256),
            nullable=True,
            comment="Comma seperated list of Airflow tags that will be set on the Dag",
        ),
    )

    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "sla_warning_time",
            sa.Time(),
            nullable=True,
            comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
        ),
    )
    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "sla_warning_time",
            sa.Time(),
            nullable=True,
            comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
        ),
    )
    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "sla_warning_time",
            sa.Time(),
            nullable=True,
            comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
        ),
    )
    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "sla_warning_time",
            sa.Time(),
            nullable=True,
            comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
        ),
    )


def downgrade():
    op.drop_column("airflow_import_dags", "sla_warning_time")
    op.drop_column("airflow_export_dags", "sla_warning_time")
    op.drop_column("airflow_etl_dags", "sla_warning_time")
    op.drop_column("airflow_custom_dags", "sla_warning_time")

    op.drop_column("airflow_import_dags", "tags")
    op.drop_column("airflow_export_dags", "tags")
    op.drop_column("airflow_etl_dags", "tags")
    op.drop_column("airflow_custom_dags", "tags")
