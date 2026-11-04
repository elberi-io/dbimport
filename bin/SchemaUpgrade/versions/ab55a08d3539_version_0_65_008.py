"""Version 0.65.008

Revision ID: ab55a08d3539
Revises: 7b0431792061
Create Date: 2020-03-18 06:04:22.110170

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "ab55a08d3539"
down_revision = "7b0431792061"
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
        ),
        existing_comment="Name of database type.  Name is hardcoded into Python scripts, so use only defined values",
        existing_nullable=False,
    )

    op.execute("""
		ALTER TABLE `airflow_import_dags`
		CHANGE COLUMN `filter_hive` `filter_hive` VARCHAR(1024) NOT NULL COMMENT 'Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE' 
		""")

    op.drop_column("airflow_import_dags", "filter_hive_db_OLD")
    op.drop_column("airflow_import_dags", "filter_hive_table_OLD")


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

    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "filter_hive_table_OLD",
            sa.String(length=256),
            nullable=True,
            comment="NOT USED: Filter string for HIVE_TABLE in import_tables",
        ),
    )
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "filter_hive_db_OLD",
            sa.String(length=256),
            nullable=True,
            comment="NOT USED: Filter string for HIVE_DB in import_tables",
        ),
    )

    op.execute("""
		ALTER TABLE `airflow_import_dags`
		CHANGE COLUMN `filter_hive` `filter_hive` VARCHAR(256) NOT NULL COMMENT 'Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE' 
		""")
