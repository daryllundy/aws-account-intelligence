from __future__ import annotations

from aws_account_intelligence.analysis.impact import ImpactAnalyzer
from aws_account_intelligence.collectors.fixtures import FixtureCollector
from aws_account_intelligence.collectors.aws import AwsCollector
from aws_account_intelligence.analysis.dependency_graph import DependencyGraphBuilder
from aws_account_intelligence.models import EdgeType, RiskLevel
from aws_account_intelligence.config import Settings
from tests.test_aws_collector import FakeSession


def test_impact_analysis_flags_lambda_shutdown_risk() -> None:
    bundle = FixtureCollector().load("scan-1")
    edges = DependencyGraphBuilder().build(bundle.services, "scan-1")
    costs = {item.resource_id: item.projected_monthly_cost_usd for item in bundle.costs}

    report = ImpactAnalyzer().analyze(
        scan_run_id="scan-1",
        target_resource_id="arn:aws:lambda:us-west-2:123456789012:function:process-orders",
        services=bundle.services,
        costs_by_resource=costs,
        edges=edges,
    )

    direct = {item.resource_id for item in report.direct_dependents}
    assert "arn:aws:apigateway:us-west-2::/restapis/orders-api" in direct
    assert report.risk_score in {RiskLevel.MEDIUM, RiskLevel.CRITICAL}
    assert report.estimated_monthly_savings_usd > 0


def test_impact_analysis_low_risk_for_isolated_bucket() -> None:
    bundle = FixtureCollector().load("scan-1")
    edges = DependencyGraphBuilder().build(bundle.services, "scan-1")
    costs = {item.resource_id: item.projected_monthly_cost_usd for item in bundle.costs}

    report = ImpactAnalyzer().analyze(
        scan_run_id="scan-1",
        target_resource_id="arn:aws:s3:::orders-artifacts",
        services=bundle.services,
        costs_by_resource=costs,
        edges=edges,
    )

    assert report.risk_score is RiskLevel.LOW
    assert report.direct_dependents == []


def test_dependency_graph_includes_config_and_iam_edges_from_live_collector_metadata() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2")
    bundle = AwsCollector(settings=settings, session=FakeSession()).load("scan-aws-deps")

    edges = DependencyGraphBuilder().build(bundle.services, "scan-aws-deps")

    assert any(
        edge.edge_type is EdgeType.CONFIG
        and edge.from_resource_id.endswith("process-orders-us-west-2")
        and edge.to_resource_id.endswith("orders-db-us-west-2")
        for edge in edges
    )
    assert any(
        edge.edge_type is EdgeType.IAM
        and edge.from_resource_id.endswith("process-orders-us-west-2")
        and edge.to_resource_id.endswith("cluster/orders-us-west-2")
        for edge in edges
    )
