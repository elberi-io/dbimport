"""Add hive_javaheap to export_tables

Revision ID: 5d9908c8cf55
Revises:
Create Date: 2019-06-10 12:00:55.102094

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5d9908c8cf55"
down_revision = "1375ea17a2b0"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("export_tables", sa.Column("hive_javaheap", sa.BigInteger))


def downgrade():
    op.drop_column("export_tables", "hive_javaheap")
