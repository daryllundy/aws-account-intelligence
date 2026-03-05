from __future__ import annotations

from aws_account_intelligence.collectors.aws import AwsCollector
from aws_account_intelligence.collectors.fixtures import FixtureCollector


def get_collector(data_source: str):
    if data_source == "fixtures":
        return FixtureCollector()
    if data_source == "aws":
        return AwsCollector()
    raise ValueError(f"Unsupported data source: {data_source}")
