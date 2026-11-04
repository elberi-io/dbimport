"""Version 0.63.001

Revision ID: 0ac155cc2d5e
Revises: 6a9d20321d65
Create Date: 2019-10-18 11:26:19.828818

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "0ac155cc2d5e"
down_revision = "6a9d20321d65"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "dbimport_instances",
        sa.Column(
            "sync_credentials",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="0 = Credentials wont be synced, 1 = The credentials information will be synced to the other cluster",
        ),
    )
    op.add_column(
        "import_columns",
        sa.Column(
            "sqoop_column_type_override",
            sa.String(length=256),
            nullable=True,
            comment="Set the --map-column-java field to a fixed value and not calculated by DBImport",
        ),
    )


def downgrade():
    op.drop_column("dbimport_instances", "sync_credentials")
    op.drop_column("import_columns", "sqoop_column_type_override")
