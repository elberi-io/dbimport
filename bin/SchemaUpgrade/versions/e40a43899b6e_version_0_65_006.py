"""Version 0.65.006

Revision ID: e40a43899b6e
Revises: 670b0bc3eb69
Create Date: 2019-12-05 05:41:42.942886

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e40a43899b6e"
down_revision = "670b0bc3eb69"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "jdbc_connections_environments",
        sa.Column(
            "environment",
            sa.String(length=32),
            nullable=False,
            comment="Name of the Environment type",
        ),
        sa.Column(
            "description",
            sa.String(length=256),
            nullable=True,
            comment="Description of the Environment type",
        ),
        sa.PrimaryKeyConstraint("environment"),
        comment="Environments types. Used in jdbc_connections table to define what kind of connection type it is",
    )

    op.add_column(
        "jdbc_connections",
        sa.Column(
            "environment",
            sa.String(length=32),
            nullable=True,
            comment="Name of the Environment type",
        ),
    )
    op.create_foreign_key(
        "FK_jdbc_connections__jdbc_connections_environments",
        "jdbc_connections",
        "jdbc_connections_environments",
        ["environment"],
        ["environment"],
    )


def downgrade():
    op.execute(
        "ALTER TABLE `jdbc_connections` DROP FOREIGN KEY `FK_jdbc_connections__jdbc_connections_environments`"
    )
    op.drop_column("jdbc_connections", "environment")
    op.drop_table("jdbc_connections_environments")
