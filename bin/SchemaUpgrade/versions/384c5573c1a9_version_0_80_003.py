"""Version 0.80.003

Revision ID: 384c5573c1a9
Revises: 0be8afe157ef
Create Date: 2023-04-19 10:17:31.029563

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "384c5573c1a9"
down_revision = "0be8afe157ef"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
		ALTER TABLE `airflow_import_dags`
		CHANGE COLUMN `filter_hive` `filter_hive` VARCHAR(16384) NOT NULL COMMENT 'Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE' 
		""")


def downgrade():
    op.execute("""
		ALTER TABLE `airflow_import_dags`
		CHANGE COLUMN `filter_hive` `filter_hive` VARCHAR(1024) NOT NULL COMMENT 'Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE' 
		""")
