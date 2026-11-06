"""Version 0.68.004

Revision ID: d0f3519de94f
Revises: 414ee1e3a8a1
Create Date: 2021-08-20 04:07:06.869412

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d0f3519de94f"
down_revision = "414ee1e3a8a1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "jdbc_connections",
        sa.Column(
            "timewindow_timezone",
            sa.String(length=64),
            nullable=True,
            comment="Timezone used for timewindow_start and timewindow_stop columns. Use full text timezone, example Europe/Stockholm.",
        ),
    )

    # Change order of the column
    op.execute(
        "ALTER TABLE `jdbc_connections` CHANGE COLUMN `timewindow_timezone` `timewindow_timezone` VARCHAR(64) NULL COMMENT 'Timezone used for timewindow_start and timewindow_stop columns. Use full text timezone, example Europe/Stockholm.' AFTER `timewindow_stop`"
    )


def downgrade():
    op.drop_column("jdbc_connections", "timewindow_timezone")
