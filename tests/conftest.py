from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def acn_sample_path() -> Path:
    return FIXTURES_DIR / "acn_sample.json"


@pytest.fixture
def urbanev_sample_path() -> Path:
    return FIXTURES_DIR / "urbanev_sample.csv"
