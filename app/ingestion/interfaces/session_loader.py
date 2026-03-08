from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

import polars as pl


class SessionLoader(ABC):
    """Abstract interface for loading EV charging session data from any source."""

    @property
    @abstractmethod
    def dataset_name(self) -> str:
        """Unique identifier for this dataset source."""

    @abstractmethod
    def load_raw(self, source_path: str) -> pl.LazyFrame:
        """Load raw data from source into a Polars LazyFrame."""

    @abstractmethod
    def normalize(self, raw: pl.LazyFrame) -> pl.LazyFrame:
        """Map dataset-specific columns to the canonical schema columns.

        Returns a LazyFrame with columns matching ChargingSessionCreate fields.
        Must include: session_id, source_dataset, arrival_ts, departure_ts,
        session_duration_minutes, raw_payload.
        """

    def load_and_normalize(self, source_path: str) -> pl.LazyFrame:
        """Convenience: load then normalize."""
        raw = self.load_raw(source_path)
        return self.normalize(raw)

    def iter_batches(
        self, source_path: str, batch_size: int = 5000
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield batches of normalized records as dicts for persistence."""
        lf = self.load_and_normalize(source_path)
        df = lf.collect()
        for offset in range(0, len(df), batch_size):
            batch = df.slice(offset, batch_size)
            yield batch.to_dicts()
