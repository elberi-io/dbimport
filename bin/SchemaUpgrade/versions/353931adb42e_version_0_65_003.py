"""Version 0.65.003

Revision ID: 353931adb42e
Revises: ef699aba8ba4
Create Date: 2019-11-12 08:01:03.126448

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "353931adb42e"
down_revision = "ef699aba8ba4"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "jdbc_connections",
        sa.Column(
            "atlas_discovery",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="1 = Discover tables/views on this connection, 0 = Dont use Atlas discovery on this connection",
        ),
    )
    op.add_column(
        "jdbc_connections",
        sa.Column(
            "atlas_include_filter",
            sa.String(length=256),
            nullable=True,
            comment="Include filter for Atlas discovery",
        ),
    )
    op.add_column(
        "jdbc_connections",
        sa.Column(
            "atlas_exclude_filter",
            sa.String(length=256),
            nullable=True,
            comment="Exclude filter for Atlas discovery",
        ),
    )
    op.add_column(
        "jdbc_connections",
        sa.Column("atlas_last_discovery", sa.DateTime(), nullable=True),
    )


def downgrade():
    op.drop_column("jdbc_connections", "atlas_last_discovery")
    op.drop_column("jdbc_connections", "atlas_exclude_filter")
    op.drop_column("jdbc_connections", "atlas_include_filter")
    op.drop_column("jdbc_connections", "atlas_discovery")
