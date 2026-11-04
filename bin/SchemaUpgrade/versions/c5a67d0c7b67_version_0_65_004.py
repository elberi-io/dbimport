"""Version 0.65.004

Revision ID: c5a67d0c7b67
Revises: 353931adb42e
Create Date: 2019-11-21 11:56:54.364889

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "c5a67d0c7b67"
down_revision = "353931adb42e"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
	ALTER TABLE `export_tables`
	CHANGE COLUMN `incr_minvalue` `incr_minvalue` VARCHAR(32) NULL DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually' AFTER `incr_validation_method`,
	CHANGE COLUMN `incr_maxvalue` `incr_maxvalue` VARCHAR(32) NULL DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually' AFTER `incr_minvalue`,
	CHANGE COLUMN `incr_minvalue_pending` `incr_minvalue_pending` VARCHAR(32) NULL DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually' AFTER `incr_maxvalue`,
	CHANGE COLUMN `incr_maxvalue_pending` `incr_maxvalue_pending` VARCHAR(32) NULL DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually' AFTER `incr_minvalue_pending`
	""")


def downgrade():
    op.execute("""
	ALTER TABLE `export_tables`
	CHANGE COLUMN `incr_minvalue` `incr_minvalue` VARCHAR(25) NULL DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually' AFTER `incr_validation_method`,
	CHANGE COLUMN `incr_maxvalue` `incr_maxvalue` VARCHAR(25) NULL DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually' AFTER `incr_minvalue`,
	CHANGE COLUMN `incr_minvalue_pending` `incr_minvalue_pending` VARCHAR(25) NULL DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually' AFTER `incr_maxvalue`,
	CHANGE COLUMN `incr_maxvalue_pending` `incr_maxvalue_pending` VARCHAR(25) NULL DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually' AFTER `incr_minvalue_pending`
	""")
