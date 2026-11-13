"""Version 0.80.005

Revision ID: cb96befeaba7
Revises: 67a2742fc1fb
Create Date: 2023-10-05 05:39:31.920366

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "cb96befeaba7"
down_revision = "67a2742fc1fb"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "yarn_statistics",
        sa.Column(
            "yarn_application_id",
            sa.String(length=64),
            nullable=False,
            comment="Name of the application in Yarn",
        ),
        sa.Column(
            "operation_type",
            mysql.ENUM("import", "export"),
            nullable=False,
            comment="What kind of DBImport job it is",
        ),
        sa.Column(
            "operation_tool",
            mysql.ENUM("hive", "spark", "sqoop"),
            nullable=False,
            comment="The tool or operation that was used",
        ),
        sa.Column(
            "operation_timestamp",
            sa.DateTime(),
            nullable=False,
            comment="The timestamp for the complete operation. Can be used for grouping together all applications for a specific operation, i.e a complete import/export",
        ),
        sa.Column(
            "hive_db",
            sa.String(length=255),
            nullable=True,
            comment="Name of the Hive DB. Used for imports",
        ),
        sa.Column(
            "hive_table",
            sa.String(length=255),
            nullable=True,
            comment="Name of the Hive Table. Used for imports",
        ),
        sa.Column(
            "dbalias",
            sa.String(length=255),
            nullable=True,
            comment="Name of the database alias. Used for exports",
        ),
        sa.Column(
            "target_schema",
            sa.String(length=255),
            nullable=True,
            comment="Name of the target schema. Used for exports",
        ),
        sa.Column(
            "target_table",
            sa.String(length=255),
            nullable=True,
            comment="Name for the target table. Used for exports",
        ),
        sa.Column(
            "application_start",
            sa.DateTime(),
            nullable=False,
            comment="Timestamp when the operation started",
        ),
        sa.Column(
            "application_stop",
            sa.DateTime(),
            nullable=True,
            comment="Timestampe when the operation was completed",
        ),
        sa.Column(
            "yarn_containers_total",
            sa.Integer,
            nullable=True,
            comment="Number of containers used. For spark and sqoop, this will be the maximum that was used during the operation",
        ),
        sa.Column(
            "yarn_containers_failed",
            sa.Integer,
            nullable=True,
            comment="Number of contaners that failed",
        ),
        sa.Column(
            "yarn_containers_killed",
            sa.Integer,
            nullable=True,
            comment="Number of contaners that was killed",
        ),
        sa.PrimaryKeyConstraint("yarn_application_id", "application_start"),
        comment="Statistics about Yarn applications that DBImport launches. Used for rough resource estimations used and connection between Yarn application ID and DBImport job",
    )

    op.create_index(
        "hive_db_hive_table_last_update_from_source",
        "yarn_statistics",
        ["hive_db", "hive_table", "operation_timestamp"],
        unique=False,
    )
    op.create_index(
        "dbalias_target_schema_target_table_last_update_from_source",
        "yarn_statistics",
        ["dbalias", "target_schema", "target_table", "operation_timestamp"],
        unique=False,
    )


def downgrade():
    op.drop_table("yarn_statistics")
