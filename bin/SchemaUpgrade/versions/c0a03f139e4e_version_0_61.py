"""Version 0.61

Revision ID: c0a03f139e4e
Revises: c0c3337bb44a
Create Date: 2019-09-10 04:16:01.322893

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "c0a03f139e4e"
down_revision = "c0c3337bb44a"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "jdbc_connections",
        sa.Column(
            "create_foreign_keys",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="1 = Create foreign keys, 0 = Dont create foreign keys",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "create_foreign_keys",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="-1 (default) = Get information from jdbc_connections table",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "airflow_notes",
            sa.Text(),
            nullable=True,
            comment="Documentation that will be available in Airflow. Markdown syntax",
        ),
    )
    op.add_column(
        "export_tables",
        sa.Column(
            "airflow_notes",
            sa.Text(),
            nullable=True,
            comment="Documentation that will be available in Airflow. Markdown syntax",
        ),
    )
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "airflow_notes",
            sa.Text(),
            nullable=True,
            comment="Documentation that will be available in Airflow. Markdown syntax",
        ),
    )
    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "airflow_notes",
            sa.Text(),
            nullable=True,
            comment="Documentation that will be available in Airflow. Markdown syntax",
        ),
    )
    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "airflow_notes",
            sa.Text(),
            nullable=True,
            comment="Documentation that will be available in Airflow. Markdown syntax",
        ),
    )
    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "airflow_notes",
            sa.Text(),
            nullable=True,
            comment="Documentation that will be available in Airflow. Markdown syntax",
        ),
    )

    op.create_table(
        "copy_async_status",
        sa.Column(
            "table_id",
            mysql.INTEGER(display_width=11),
            nullable=False,
            comment="Reference to import_table.table_id",
        ),
        sa.Column(
            "hive_db", sa.String(length=256), nullable=False, comment="Hive Database"
        ),
        sa.Column(
            "hive_table",
            sa.String(length=256),
            nullable=False,
            comment="Hive Table to copy",
        ),
        sa.Column(
            "destination",
            sa.String(length=32),
            nullable=False,
            comment="DBImport instances to copy the imported data to",
        ),
        sa.Column(
            "copy_status",
            mysql.TINYINT(display_width=4),
            nullable=False,
            server_default=sa.text("'0'"),
            comment="Status of the copy operation",
        ),
        sa.Column(
            "last_status_update",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
            comment="Last time the server changed progress on this copy",
        ),
        sa.PrimaryKeyConstraint("table_id", "destination"),
        # 	sa.ForeignKeyConstraint(['destination'], ['dbimport_instances.name'], ),
        comment="The status table for asynchronous copy between DBImport instances.",
    )


# 	create_index('IX_copy_async_status_table_id', 'copy_async_status', ['table_id'], unique=False)


def downgrade():
    op.drop_column("import_tables", "create_foreign_keys")
    op.drop_column("jdbc_connections", "create_foreign_keys")
    op.drop_column("import_tables", "airflow_notes")
    op.drop_column("export_tables", "airflow_notes")
    op.drop_column("airflow_import_dags", "airflow_notes")
    op.drop_column("airflow_export_dags", "airflow_notes")
    op.drop_column("airflow_etl_dags", "airflow_notes")
    op.drop_column("airflow_custom_dags", "airflow_notes")

    op.drop_table("copy_async_status")
