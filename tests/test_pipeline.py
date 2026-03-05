from __future__ import annotations

from aws_account_intelligence.config import get_settings
from aws_account_intelligence.collectors.base import DiscoveryBundle, ScanWarning
from aws_account_intelligence.pipeline import ScanPipeline
from aws_account_intelligence.storage import Database


def test_scan_pipeline_persists_snapshot() -> None:
    settings = get_settings()
    database = Database(settings.database_url)
    database.create_all()

    pipeline = ScanPipeline(settings, database)
    scan = pipeline.run()

    services = database.list_service_records(scan.scan_run_id)
    costs = database.list_cost_attributions(scan.scan_run_id)
    edges = database.list_dependency_edges(scan.scan_run_id)

    assert scan.status == "completed"
    assert len(services) >= 7
    assert any(cost.resource_id == "unattributed" for cost in costs)
    assert any(edge.evidence_source == "apigateway.integration" for edge in edges)
    assert scan.summary["warnings"] == []
    assert scan.summary["warning_count"] == 0
    cost_summary = pipeline.costs(scan.scan_run_id)
    assert cost_summary.cost_freshness_at is not None


def test_scan_pipeline_persists_structured_warnings(monkeypatch) -> None:
    settings = get_settings()
    database = Database(settings.database_url)
    database.create_all()

    class WarningCollector:
        def load(self, scan_run_id: str) -> DiscoveryBundle:
            return DiscoveryBundle(
                services=[],
                costs=[],
                warnings=[
                    ScanWarning(
                        stage="discovery",
                        service="ec2",
                        region="us-west-2",
                        code="ACCESS_DENIED",
                        message="Missing DescribeInstances permission.",
                    )
                ],
            )

    monkeypatch.setattr("aws_account_intelligence.pipeline.runner.get_collector", lambda _: WarningCollector())

    scan = ScanPipeline(settings, database).run()

    assert scan.summary["warning_count"] == 1
    assert scan.summary["warnings"][0] == {
        "stage": "discovery",
        "service": "ec2",
        "region": "us-west-2",
        "code": "ACCESS_DENIED",
        "message": "Missing DescribeInstances permission.",
    }
