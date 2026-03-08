"""create charging_sessions and ingestion_runs tables

Revision ID: 001
Revises:
Create Date: 2026-03-08
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "charging_sessions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("source_dataset", sa.Text(), nullable=False),
        sa.Column("source_record_id", sa.Text(), nullable=True),
        sa.Column("station_id", sa.Text(), nullable=True),
        sa.Column("port_id", sa.Text(), nullable=True),
        sa.Column("vehicle_id", sa.Text(), nullable=True),
        sa.Column("arrival_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("departure_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("energy_requested_kwh", sa.Float(), nullable=True),
        sa.Column("energy_delivered_kwh", sa.Float(), nullable=True),
        sa.Column("max_charge_rate_kw", sa.Float(), nullable=True),
        sa.Column("average_charge_rate_kw", sa.Float(), nullable=True),
        sa.Column("session_duration_minutes", sa.Integer(), nullable=False),
        sa.Column("charging_duration_minutes", sa.Integer(), nullable=True),
        sa.Column("is_valid", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "validation_errors",
            postgresql.JSONB(),
            nullable=False,
            server_default="[]",
        ),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id"),
    )
    op.create_index("ix_charging_sessions_session_id", "charging_sessions", ["session_id"])
    op.create_index("ix_charging_sessions_source_dataset", "charging_sessions", ["source_dataset"])
    op.create_index("ix_charging_sessions_arrival_ts", "charging_sessions", ["arrival_ts"])
    op.create_index("ix_charging_sessions_departure_ts", "charging_sessions", ["departure_ts"])
    op.create_index("ix_charging_sessions_station_id", "charging_sessions", ["station_id"])
    op.create_index("ix_charging_sessions_is_valid", "charging_sessions", ["is_valid"])
    op.create_index(
        "ix_charging_sessions_source_arrival",
        "charging_sessions",
        ["source_dataset", "arrival_ts"],
    )

    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_dataset", sa.Text(), nullable=False),
        sa.Column("source_path", sa.Text(), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="running"),
        sa.Column("records_seen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_inserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_invalid", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("ingestion_runs")
    op.drop_index("ix_charging_sessions_source_arrival", table_name="charging_sessions")
    op.drop_index("ix_charging_sessions_is_valid", table_name="charging_sessions")
    op.drop_index("ix_charging_sessions_station_id", table_name="charging_sessions")
    op.drop_index("ix_charging_sessions_departure_ts", table_name="charging_sessions")
    op.drop_index("ix_charging_sessions_arrival_ts", table_name="charging_sessions")
    op.drop_index("ix_charging_sessions_source_dataset", table_name="charging_sessions")
    op.drop_index("ix_charging_sessions_session_id", table_name="charging_sessions")
    op.drop_table("charging_sessions")
