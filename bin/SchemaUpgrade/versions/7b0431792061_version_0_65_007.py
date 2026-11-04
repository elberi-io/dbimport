"""Version 0.65.007

Revision ID: 7b0431792061
Revises: e40a43899b6e
Create Date: 2020-02-06 08:17:44.154498

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "7b0431792061"
down_revision = "e40a43899b6e"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
	ALTER TABLE `airflow_custom_dags`
	CHANGE COLUMN `schedule_interval` `schedule_interval` VARCHAR(32) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag' AFTER `dag_name`
	""")

    op.execute("""
	ALTER TABLE `airflow_etl_dags`
	CHANGE COLUMN `schedule_interval` `schedule_interval` VARCHAR(32) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag' AFTER `dag_name`;
	""")

    op.execute("""
	ALTER TABLE `airflow_export_dags`
	CHANGE COLUMN `schedule_interval` `schedule_interval` VARCHAR(32) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag' AFTER `dag_name`;
	""")


def downgrade():
    op.execute("""
	ALTER TABLE `airflow_custom_dags`
	CHANGE COLUMN `schedule_interval` `schedule_interval` VARCHAR(20) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag' AFTER `dag_name`
	""")

    op.execute("""
	ALTER TABLE `airflow_etl_dags`
	CHANGE COLUMN `schedule_interval` `schedule_interval` VARCHAR(20) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag' AFTER `dag_name`;
	""")

    op.execute("""
	ALTER TABLE `airflow_export_dags`
	CHANGE COLUMN `schedule_interval` `schedule_interval` VARCHAR(20) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag' AFTER `dag_name`;
	""")
