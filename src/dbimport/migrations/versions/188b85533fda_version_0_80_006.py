"""Version 0.80.006

Revision ID: 188b85533fda
Revises: cb96befeaba7
Create Date: 2023-10-17 06:45:15.368389

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "188b85533fda"
down_revision = "cb96befeaba7"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("PRIMARY", "yarn_statistics", type_="primary")
    op.create_index(
        "yarn_application_id", "yarn_statistics", ["yarn_application_id"], unique=False
    )


def downgrade():
    op.drop_index("yarn_application_id", table_name="yarn_statistics")
    op.create_primary_key(
        "PRIMARY", "yarn_statistics", ["yarn_application_id", "application_start"]
    )
