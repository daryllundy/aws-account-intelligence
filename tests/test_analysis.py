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
    assert report.risk_score is RiskLevel.MEDIUM
    assert report.estimated_monthly_savings_usd > 0
    assert report.risk_factors == [
        "target_in_production",
        "direct_dependents:1",
        "critical_direct_dependents:1",
        "high_confidence_dependency_paths",
    ]
    direct_api = next(item for item in report.direct_dependents if item.service_name == "apigateway")
    assert direct_api.path_depth == 1
    assert direct_api.dependency_path == [
        "arn:aws:apigateway:us-west-2::/restapis/orders-api",
        "arn:aws:lambda:us-west-2:123456789012:function:process-orders",
    ]
    assert direct_api.is_critical is True
    assert "production_environment" in direct_api.criticality_reasons


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
    assert report.transitive_dependents == []
    assert report.risk_factors == ["target_in_production"]


def test_dependency_graph_includes_config_iam_cloudtrail_and_xray_edges_from_live_collector_metadata() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2")
    bundle = AwsCollector(settings=settings, session=FakeSession()).load("scan-aws-deps")

    edges = DependencyGraphBuilder().build(bundle.services, "scan-aws-deps")

    assert any(
        edge.edge_type is EdgeType.CONFIG
        and edge.from_resource_id.endswith("process-orders-us-west-2")
        and edge.to_resource_id.endswith("orders-db-us-west-2")
        for edge in edges
    )


def test_impact_analysis_reports_transitive_dependency_chains() -> None:
    bundle = FixtureCollector().load("scan-1")
    edges = DependencyGraphBuilder().build(bundle.services, "scan-1")
    costs = {item.resource_id: item.projected_monthly_cost_usd for item in bundle.costs}

    report = ImpactAnalyzer().analyze(
        scan_run_id="scan-1",
        target_resource_id="arn:aws:sqs:us-west-2:123456789012:orders-queue",
        services=bundle.services,
        costs_by_resource=costs,
        edges=edges,
    )

    assert report.risk_score is RiskLevel.HIGH
    assert "max_dependency_depth:2" in report.risk_factors
    direct_lambda = next(item for item in report.direct_dependents if item.service_name == "lambda")
    assert direct_lambda.dependency_path == [
        "arn:aws:lambda:us-west-2:123456789012:function:process-orders",
        "arn:aws:sqs:us-west-2:123456789012:orders-queue",
    ]

    transitive_api = next(item for item in report.transitive_dependents if item.service_name == "apigateway")
    assert transitive_api.path_depth == 2
    assert transitive_api.dependency_path == [
        "arn:aws:apigateway:us-west-2::/restapis/orders-api",
        "arn:aws:lambda:us-west-2:123456789012:function:process-orders",
        "arn:aws:sqs:us-west-2:123456789012:orders-queue",
    ]
    assert transitive_api.is_critical is True


def test_impact_analysis_marks_critical_target_with_dependents_as_critical() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2")
    bundle = AwsCollector(settings=settings, session=FakeSession()).load("scan-aws-impact")
    edges = DependencyGraphBuilder().build(bundle.services, "scan-aws-impact")
    costs = {item.resource_id: item.projected_monthly_cost_usd for item in bundle.costs}

    report = ImpactAnalyzer().analyze(
        scan_run_id="scan-aws-impact",
        target_resource_id="arn:aws:rds:us-west-2:123456789012:db:orders-db-us-west-2",
        services=bundle.services,
        costs_by_resource=costs,
        edges=edges,
    )

    assert report.risk_score is RiskLevel.CRITICAL
    assert "target_marked_critical" in report.risk_factors
    assert report.direct_dependents
    assert "target is tagged as critical or production" in report.rationale
    assert any(
        edge.edge_type is EdgeType.IAM
        and edge.from_resource_id.endswith("process-orders-us-west-2")
        and edge.to_resource_id.endswith("cluster/orders-us-west-2")
        for edge in edges
    )
    assert any(
        edge.edge_type is EdgeType.INVOCATION
        and edge.evidence_source == "cloudtrail.lookup_events"
        and edge.from_resource_id.endswith("process-orders-us-west-2")
        and edge.to_resource_id.endswith("orders-db-us-west-2")
        for edge in edges
    )
    assert any(
        edge.edge_type is EdgeType.DATA_FLOW
        and edge.evidence_source == "xray.service_graph"
        and edge.from_resource_id.endswith("process-orders-us-west-2")
        and edge.to_resource_id.endswith("orders-db-us-west-2")
        for edge in edges
    )
