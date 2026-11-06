"""Version 0.68.002

Revision ID: 2714c9baba79
Revises: a2b6a63b2932
Create Date: 2021-06-15 11:42:55.702844

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2714c9baba79"
down_revision = "a2b6a63b2932"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "timezone",
            sa.String(length=64),
            nullable=True,
            comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
        ),
    )
    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "timezone",
            sa.String(length=64),
            nullable=True,
            comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
        ),
    )
    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "timezone",
            sa.String(length=64),
            nullable=True,
            comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
        ),
    )
    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "timezone",
            sa.String(length=64),
            nullable=True,
            comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
        ),
    )


def downgrade():
    op.drop_column("airflow_import_dags", "timezone")
    op.drop_column("airflow_export_dags", "timezone")
    op.drop_column("airflow_etl_dags", "timezone")
    op.drop_column("airflow_custom_dags", "timezone")
