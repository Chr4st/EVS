"""Tests for loader registry."""

import pytest

from app.ingestion.loaders import get_loader
from app.ingestion.loaders.acn_loader import AcnLoader
from app.ingestion.loaders.urbanev_loader import UrbanevLoader


class TestGetLoader:
    def test_acn(self) -> None:
        loader = get_loader("acn")
        assert isinstance(loader, AcnLoader)

    def test_urbanev(self) -> None:
        loader = get_loader("urbanev")
        assert isinstance(loader, UrbanevLoader)

    def test_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown dataset"):
            get_loader("nonexistent")
