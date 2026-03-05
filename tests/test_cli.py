from __future__ import annotations

import json

from typer.testing import CliRunner

from aws_account_intelligence.cli.main import app

runner = CliRunner()


def test_cli_scan_run_and_inventory_list() -> None:
    scan_result = runner.invoke(app, ["scan", "run", "--output", "json"])
    assert scan_result.exit_code == 0, scan_result.stdout
    scan_payload = json.loads(scan_result.stdout)

    inventory_result = runner.invoke(app, ["inventory", "list", "--scan-run-id", scan_payload["scan_run_id"], "--output", "json"])
    assert inventory_result.exit_code == 0, inventory_result.stdout
    inventory_payload = json.loads(inventory_result.stdout)

    assert inventory_payload["scan"]["scan_run_id"] == scan_payload["scan_run_id"]
    assert len(inventory_payload["services"]) >= 7
    assert scan_payload["summary"]["warnings"] == []
    assert scan_payload["summary"]["warning_count"] == 0


def test_cli_impact_analyze() -> None:
    scan_result = runner.invoke(app, ["scan", "run", "--output", "json"])
    scan_payload = json.loads(scan_result.stdout)

    impact_result = runner.invoke(
        app,
        [
            "impact",
            "analyze",
            "--scan-run-id",
            scan_payload["scan_run_id"],
            "--resource",
            "arn:aws:lambda:us-west-2:123456789012:function:process-orders",
            "--output",
            "json",
        ],
    )
    assert impact_result.exit_code == 0, impact_result.stdout
    impact_payload = json.loads(impact_result.stdout)
    assert impact_payload["risk_score"] in {"MEDIUM", "CRITICAL"}


def test_cli_scan_status_includes_warning_summary(monkeypatch) -> None:
    from aws_account_intelligence.collectors.base import DiscoveryBundle, ScanWarning

    class WarningCollector:
        def load(self, scan_run_id: str) -> DiscoveryBundle:
            return DiscoveryBundle(
                services=[],
                costs=[],
                warnings=[
                    ScanWarning(
                        stage="discovery",
                        service="rds",
                        region="us-east-1",
                        code="THROTTLED",
                        message="Collector skipped after repeated throttling.",
                    )
                ],
            )

    monkeypatch.setattr("aws_account_intelligence.pipeline.runner.get_collector", lambda _: WarningCollector())

    scan_result = runner.invoke(app, ["scan", "run", "--output", "json"])
    assert scan_result.exit_code == 0, scan_result.stdout
    scan_payload = json.loads(scan_result.stdout)

    status_result = runner.invoke(app, ["scan", "status", "--scan-run-id", scan_payload["scan_run_id"], "--output", "json"])
    assert status_result.exit_code == 0, status_result.stdout
    status_payload = json.loads(status_result.stdout)

    assert status_payload["summary"]["warning_count"] == 1
    assert status_payload["summary"]["warnings"][0]["service"] == "rds"
