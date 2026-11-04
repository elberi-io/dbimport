"""Version 0.62

Revision ID: 6a9d20321d65
Revises: c0a03f139e4e
Create Date: 2019-09-16 04:27:00.831817

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6a9d20321d65"
down_revision = "c0a03f139e4e"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "copy_async_status",
        sa.Column(
            "failures",
            sa.Integer,
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Number of failures on current state",
        ),
    )
    op.add_column(
        "copy_async_status",
        sa.Column(
            "hdfs_source_path",
            sa.String(length=768),
            nullable=False,
            comment="HDFS path to copy from",
        ),
    )
    op.add_column(
        "copy_async_status",
        sa.Column(
            "hdfs_target_path",
            sa.String(length=768),
            nullable=False,
            comment="HDFS path to copy to",
        ),
    )


def downgrade():
    op.drop_column("copy_async_status", "failures")
    op.drop_column("copy_async_status", "hdfs_source_path")
    op.drop_column("copy_async_status", "hdfs_target_path")
