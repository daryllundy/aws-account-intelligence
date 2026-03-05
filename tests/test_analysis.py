from __future__ import annotations

from aws_account_intelligence.analysis.impact import ImpactAnalyzer
from aws_account_intelligence.collectors.fixtures import FixtureCollector
from aws_account_intelligence.analysis.dependency_graph import DependencyGraphBuilder
from aws_account_intelligence.models import RiskLevel


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
