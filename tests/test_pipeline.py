from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

from aws_account_intelligence.config import get_settings
from aws_account_intelligence.collectors.base import DiscoveryBundle, ScanWarning
from aws_account_intelligence.models import AttributionMethod, CostAttribution, CostPoint, ResourceStatus, ServiceRecord
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
    assert cost_summary.cost_freshness_status == "FRESH"
    assert cost_summary.cost_freshness_age_hours is not None

    audit_files = list((settings.output_dir / "audit").glob("*.jsonl"))
    assert audit_files
    records = [json.loads(line) for line in audit_files[0].read_text().splitlines()]
    assert any(record["event_type"] == "scan_run_started" for record in records)
    assert any(record["event_type"] == "scan_run_completed" for record in records)


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


def test_scan_pipeline_persists_delta_report_between_snapshots(monkeypatch) -> None:
    settings = get_settings()
    database = Database(settings.database_url)
    database.create_all()
    now = datetime.now(UTC)

    first_bundle = DiscoveryBundle(
        services=[
            ServiceRecord(
                resource_id="arn:aws:lambda:us-west-2:123:function:orders",
                arn="arn:aws:lambda:us-west-2:123:function:orders",
                resource_type="AWS::Lambda::Function",
                service_name="lambda",
                region="us-west-2",
                account_id="123",
                status=ResourceStatus.ACTIVE,
                last_seen_at=now,
                scan_run_id="placeholder",
                metadata={},
            )
        ],
        costs=[
            CostAttribution(
                resource_id="arn:aws:lambda:us-west-2:123:function:orders",
                scan_run_id="placeholder",
                daily_costs=[CostPoint(date=now.date(), amount_usd=1.0)],
                projected_monthly_cost_usd=10.0,
                mtd_cost_usd=2.0,
                attribution_method=AttributionMethod.DIRECT,
                confidence=1.0,
            )
        ],
        warnings=[],
    )
    second_bundle = DiscoveryBundle(
        services=[
            ServiceRecord(
                resource_id="arn:aws:lambda:us-west-2:123:function:orders",
                arn="arn:aws:lambda:us-west-2:123:function:orders",
                resource_type="AWS::Lambda::Function",
                service_name="lambda",
                region="us-west-2",
                account_id="123",
                status=ResourceStatus.ACTIVE,
                last_seen_at=now,
                scan_run_id="placeholder",
                metadata={},
            ),
            ServiceRecord(
                resource_id="arn:aws:sqs:us-west-2:123:orders-queue",
                arn="arn:aws:sqs:us-west-2:123:orders-queue",
                resource_type="AWS::SQS::Queue",
                service_name="sqs",
                region="us-west-2",
                account_id="123",
                status=ResourceStatus.ACTIVE,
                last_seen_at=now,
                scan_run_id="placeholder",
                metadata={},
            ),
        ],
        costs=[
            CostAttribution(
                resource_id="arn:aws:lambda:us-west-2:123:function:orders",
                scan_run_id="placeholder",
                daily_costs=[CostPoint(date=now.date(), amount_usd=2.0)],
                projected_monthly_cost_usd=18.0,
                mtd_cost_usd=4.0,
                attribution_method=AttributionMethod.DIRECT,
                confidence=1.0,
            ),
            CostAttribution(
                resource_id="arn:aws:sqs:us-west-2:123:orders-queue",
                scan_run_id="placeholder",
                daily_costs=[CostPoint(date=now.date(), amount_usd=0.5)],
                projected_monthly_cost_usd=4.0,
                mtd_cost_usd=0.8,
                attribution_method=AttributionMethod.DIRECT,
                confidence=1.0,
            ),
        ],
        warnings=[],
    )

    class SequenceCollector:
        def __init__(self) -> None:
            self.calls = 0

        def load(self, scan_run_id: str) -> DiscoveryBundle:
            self.calls += 1
            bundle = first_bundle if self.calls == 1 else second_bundle
            services = [item.model_copy(update={"scan_run_id": scan_run_id}) for item in bundle.services]
            costs = [item.model_copy(update={"scan_run_id": scan_run_id}) for item in bundle.costs]
            return DiscoveryBundle(services=services, costs=costs, warnings=bundle.warnings)

    collector = SequenceCollector()
    monkeypatch.setattr("aws_account_intelligence.pipeline.runner.get_collector", lambda _: collector)

    pipeline = ScanPipeline(settings, database)
    first_scan = pipeline.run()
    second_scan = pipeline.run()
    delta = pipeline.delta(second_scan.scan_run_id)

    assert first_scan.summary["delta"]["added"] == 0
    assert delta.baseline_scan_run_id == first_scan.scan_run_id
    assert len(delta.added_resources) == 1
    assert delta.added_resources[0].resource_id.endswith("orders-queue")
    assert len(delta.cost_changes) == 1
    assert delta.cost_changes[0].current_value == 18.0
    assert second_scan.summary["delta"] == {
        "baseline_scan_run_id": first_scan.scan_run_id,
        "added": 1,
        "removed": 0,
        "cost_changed": 1,
    }


def test_run_due_schedules_executes_pending_schedule() -> None:
    settings = get_settings()
    database = Database(settings.database_url)
    database.create_all()
    pipeline = ScanPipeline(settings, database)

    schedule = pipeline.create_schedule(name="nightly", interval_hours=1)
    due_schedule = schedule.model_copy(update={"next_run_at": datetime.now(UTC) - timedelta(minutes=5)})
    database.save_schedule(due_schedule)

    results = pipeline.run_due_schedules(now=datetime.now(UTC))
    schedules = pipeline.list_schedules()

    assert len(results) == 1
    assert results[0]["name"] == "nightly"
    assert schedules[0].last_run_at is not None


def test_scan_pipeline_benchmark_writes_audit_record() -> None:
    settings = get_settings()
    database = Database(settings.database_url)
    database.create_all()
    pipeline = ScanPipeline(settings, database)

    report = pipeline.benchmark(runs=2)

    assert report["runs"] == 2
    assert len(report["durations_seconds"]) == 2
    audit_files = list((settings.output_dir / "audit").glob("*.jsonl"))
    records = [json.loads(line) for line in audit_files[0].read_text().splitlines()]
    assert any(record["event_type"] == "scan_benchmark_completed" for record in records)
