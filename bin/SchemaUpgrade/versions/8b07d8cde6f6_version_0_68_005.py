"""Version 0.68.005

Revision ID: 8b07d8cde6f6
Revises: d0f3519de94f
Create Date: 2021-09-17 03:12:54.428183

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "8b07d8cde6f6"
down_revision = "d0f3519de94f"
branch_labels = None
depends_on = None


def upgrade():
    op.rename_table("json_to_rest", "json_to_send")
    op.add_column(
        "json_to_send",
        sa.Column(
            "destination",
            sa.String(length=8),
            nullable=False,
            comment="The destination where to send the json.",
        ),
    )

    # Change order of the column
    op.execute(
        "ALTER TABLE `json_to_send` CHANGE COLUMN `destination` `destination` VARCHAR(8) NOT NULL COMMENT 'The destination where to send the json.' AFTER `status`"
    )


def downgrade():
    op.drop_column("json_to_send", "destination")
    op.rename_table("json_to_send", "json_to_rest")
