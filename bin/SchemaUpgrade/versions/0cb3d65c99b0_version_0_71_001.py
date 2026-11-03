"""Version 0.71.001

Revision ID: 0cb3d65c99b0
Revises: 77c42a5a9f39
Create Date: 2022-09-06 03:42:09.790022

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum



# revision identifiers, used by Alembic.
revision = '0cb3d65c99b0'
down_revision = '77c42a5a9f39'
branch_labels = None
depends_on = None


def upgrade():
	op.alter_column('jdbc_connections_drivers', 'database_type',
		existing_type=Enum('DB2 AS400', 'DB2 UDB', 'MySQL', 'Oracle', 'PostgreSQL', 'Progress DB', 'SQL Server', 'MongoDB', 'CacheDB', 'Snowflake'),
		type_=Enum('DB2 AS400', 'DB2 UDB', 'MySQL', 'Oracle', 'PostgreSQL', 'Progress DB', 'SQL Server', 'MongoDB', 'CacheDB', 'Snowflake', 'Informix'),
		existing_comment='Name of database type.  Name is hardcoded into Python scripts, so use only defined values',
		existing_nullable=False)

	op.alter_column('import_tables', 'etl_phase_type',
		existing_type=Enum('truncate_insert','insert','merge','merge_history_audit','none','external'),
		comment='What method to use for ETL phase',
		existing_nullable=True,
		server_default=sa.text("'truncate_insert'"))

def downgrade():
	op.alter_column('jdbc_connections_drivers', 'database_type',
		existing_type=Enum('DB2 AS400', 'DB2 UDB', 'MySQL', 'Oracle', 'PostgreSQL', 'Progress DB', 'SQL Server', 'MongoDB', 'CacheDB', 'Snowflake', 'Informix'),
		type_=aEnum('DB2 AS400', 'DB2 UDB', 'MySQL', 'Oracle', 'PostgreSQL', 'Progress DB', 'SQL Server', 'MongoDB', 'CacheDB', 'Snowflake'),  # noqa: F821  # TODO(#153): aEnum is undefined, downgrade fails
		existing_comment='Name of database type.  Name is hardcoded into Python scripts, so use only defined values',
		existing_nullable=False)

