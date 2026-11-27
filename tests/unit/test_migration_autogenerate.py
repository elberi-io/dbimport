"""What autogenerate writes for the ORM's own column types."""

import sqlalchemy as sa
from alembic.autogenerate import render_python_code
from alembic.operations import ops

from dbimport.config_db.config_schema import UTCDateTime
from dbimport.migrations.autogenerate import render_item


def test_autogenerate_writes_a_utc_column_as_datetime() -> None:
    upgrade = ops.UpgradeOps(
        ops=[ops.AddColumnOp("json_to_send", sa.Column("sent", UTCDateTime()))]
    )

    code = render_python_code(upgrade, render_item=render_item)

    assert "sa.Column('sent', sa.DateTime()" in code
    assert "UTCDateTime" not in code
