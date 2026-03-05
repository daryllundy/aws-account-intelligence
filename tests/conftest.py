from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from aws_account_intelligence.config import get_settings


@pytest.fixture(autouse=True)
def test_env(tmp_path) -> Iterator[None]:
    os.environ["DATABASE_URL"] = f"sqlite+pysqlite:///{tmp_path / 'test.db'}"
    os.environ["AAI_DATA_SOURCE"] = "fixtures"
    os.environ["AAI_OUTPUT_DIR"] = str(tmp_path / ".aai-output")
    os.environ["AAI_AWS_REGIONS"] = "us-west-2,us-east-1"
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
