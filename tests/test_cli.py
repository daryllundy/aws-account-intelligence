from __future__ import annotations

import json
from pathlib import Path

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


def test_cli_schedule_create_list_run_due() -> None:
    create_result = runner.invoke(app, ["schedule", "create", "nightly", "--interval-hours", "1", "--output", "json"])
    assert create_result.exit_code == 0, create_result.stdout
    schedule_payload = json.loads(create_result.stdout)
    assert schedule_payload["name"] == "nightly"

    list_result = runner.invoke(app, ["schedule", "list", "--output", "json"])
    assert list_result.exit_code == 0, list_result.stdout
    list_payload = json.loads(list_result.stdout)
    assert any(item["name"] == "nightly" for item in list_payload)


def test_cli_scan_delta() -> None:
    scan_result = runner.invoke(app, ["scan", "run", "--output", "json"])
    assert scan_result.exit_code == 0, scan_result.stdout
    scan_payload = json.loads(scan_result.stdout)

    delta_result = runner.invoke(app, ["scan", "delta", "--scan-run-id", scan_payload["scan_run_id"], "--output", "json"])
    assert delta_result.exit_code == 0, delta_result.stdout
    delta_payload = json.loads(delta_result.stdout)
    assert delta_payload["scan_run_id"] == scan_payload["scan_run_id"]


def test_cli_report_export_formats(tmp_path: Path) -> None:
    scan_result = runner.invoke(app, ["scan", "run", "--output", "json"])
    assert scan_result.exit_code == 0, scan_result.stdout
    scan_payload = json.loads(scan_result.stdout)

    targets = {
        "json": tmp_path / "report.json",
        "csv": tmp_path / "report.csv",
        "pdf": tmp_path / "report.pdf",
        "slack": tmp_path / "report-slack.txt",
        "email": tmp_path / "report-email.txt",
    }

    for format_name, target in targets.items():
        result = runner.invoke(
            app,
            [
                "report",
                "export",
                "--scan-run-id",
                scan_payload["scan_run_id"],
                "--format",
                format_name,
                "--destination",
                str(target),
            ],
        )
        assert result.exit_code == 0, result.stdout
        assert target.exists()
        assert target.stat().st_size > 0

    assert json.loads(targets["json"].read_text())["scan"]["scan_run_id"] == scan_payload["scan_run_id"]
    assert "resource_id" in targets["csv"].read_text()
    assert targets["pdf"].read_bytes().startswith(b"%PDF")
    assert "AWS Account Intelligence" in targets["slack"].read_text()
    assert "Subject: AWS Account Intelligence Snapshot" in targets["email"].read_text()


def test_cli_account_summary() -> None:
    scan_result = runner.invoke(app, ["scan", "run", "--output", "json"])
    assert scan_result.exit_code == 0, scan_result.stdout

    summary_result = runner.invoke(app, ["account", "summary", "--latest", "--output", "json"])
    assert summary_result.exit_code == 0, summary_result.stdout
    payload = json.loads(summary_result.stdout)
    assert payload[0]["account_id"] == "123456789012"


def test_cli_scan_benchmark() -> None:
    result = runner.invoke(app, ["scan", "benchmark", "--runs", "2", "--output", "json"])

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["runs"] == 2
    assert len(payload["durations_seconds"]) == 2
