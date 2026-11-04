"""Version 0.68.001

Revision ID: a2b6a63b2932
Revises: b62d738b06ef
Create Date: 2021-05-25 09:05:30.442042

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "a2b6a63b2932"
down_revision = "b62d738b06ef"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "metadata_import",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="1 = Run only getSchema and getRowCount, 0 = Run a normal import",
        ),
    )


def downgrade():
    op.drop_column("airflow_import_dags", "metadata_import")
