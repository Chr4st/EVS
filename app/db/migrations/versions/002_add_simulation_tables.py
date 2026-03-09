"""add simulation_runs and load_timeseries tables

Revision ID: 002
Revises: 001
Create Date: 2026-03-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: str | None = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "simulation_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scenario_name", sa.Text(), nullable=False),
        sa.Column("policy_name", sa.Text(), nullable=False),
        sa.Column("sessions_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("peak_load_kw", sa.Float(), nullable=False, server_default="0"),
        sa.Column("total_energy_kwh", sa.Float(), nullable=False, server_default="0"),
        sa.Column("completion_rate", sa.Float(), nullable=False, server_default="0"),
        sa.Column("average_load_kw", sa.Float(), nullable=False, server_default="0"),
        sa.Column("load_factor", sa.Float(), nullable=False, server_default="0"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "load_timeseries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "run_id",
            sa.Integer(),
            sa.ForeignKey("simulation_runs.id"),
            nullable=False,
        ),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("station_id", sa.Text(), nullable=True),
        sa.Column("load_kw", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_load_timeseries_run_id", "load_timeseries", ["run_id"])
    op.create_index("ix_load_timeseries_timestamp", "load_timeseries", ["timestamp"])
    op.create_index(
        "ix_load_timeseries_run_timestamp",
        "load_timeseries",
        ["run_id", "timestamp"],
    )


def downgrade() -> None:
    op.drop_index("ix_load_timeseries_run_timestamp", table_name="load_timeseries")
    op.drop_index("ix_load_timeseries_timestamp", table_name="load_timeseries")
    op.drop_index("ix_load_timeseries_run_id", table_name="load_timeseries")
    op.drop_table("load_timeseries")
    op.drop_table("simulation_runs")
