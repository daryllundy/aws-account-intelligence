from __future__ import annotations

from fastapi.testclient import TestClient

from aws_account_intelligence.cli.main import create_api_app
from aws_account_intelligence.config import get_settings
from aws_account_intelligence.pipeline import ScanPipeline
from aws_account_intelligence.storage import Database


def _seed_scan() -> str:
    settings = get_settings()
    database = Database(settings.database_url)
    database.create_all()
    scan = ScanPipeline(settings, database).run()
    return scan.scan_run_id


def test_api_exposes_scan_inventory_cost_graph_and_impact_endpoints() -> None:
    scan_run_id = _seed_scan()
    client = TestClient(create_api_app())

    latest = client.get("/scans/latest")
    assert latest.status_code == 200
    assert latest.json()["scan_run_id"] == scan_run_id

    scans = client.get("/scans")
    assert scans.status_code == 200
    assert scans.json()[0]["scan_run_id"] == scan_run_id

    inventory = client.get("/inventory", params={"scan_run_id": scan_run_id, "service_name": "lambda"})
    assert inventory.status_code == 200
    assert len(inventory.json()["services"]) == 1
    assert inventory.json()["services"][0]["service_name"] == "lambda"

    costs = client.get("/costs/summary", params={"scan_run_id": scan_run_id})
    assert costs.status_code == 200
    assert costs.json()["total_projected_monthly_cost_usd"] > 0
    assert costs.json()["cost_freshness_at"] is not None

    graph = client.get("/graph", params={"scan_run_id": scan_run_id, "edge_type": "event"})
    assert graph.status_code == 200
    assert graph.json()["adjacency"]
    first_edge = next(iter(graph.json()["adjacency"].values()))[0]
    assert first_edge["edge_type"] == "EVENT"

    impact = client.get(
        "/impact",
        params={
            "scan_run_id": scan_run_id,
            "resource": "arn:aws:lambda:us-west-2:123456789012:function:process-orders",
        },
    )
    assert impact.status_code == 200
    assert impact.json()["risk_score"] in {"MEDIUM", "HIGH", "CRITICAL"}
    assert impact.json()["risk_factors"]


def test_api_inventory_supports_search_and_status_filters() -> None:
    scan_run_id = _seed_scan()
    client = TestClient(create_api_app())

    response = client.get(
        "/inventory",
        params={"scan_run_id": scan_run_id, "status": "active", "search": "orders"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["services"]
    assert all(item["status"] == "ACTIVE" for item in payload["services"])
    assert all(
        "orders" in item["resource_id"].lower()
        or "orders" in item["arn"].lower()
        or any("orders" in value.lower() for value in item["tags"].values())
        for item in payload["services"]
    )


def test_api_returns_404_for_unknown_scan_or_resource() -> None:
    client = TestClient(create_api_app())

    scan_response = client.get("/scans/not-a-scan")
    assert scan_response.status_code == 404

    scan_run_id = _seed_scan()
    impact_response = client.get("/impact", params={"scan_run_id": scan_run_id, "resource": "missing"})
    assert impact_response.status_code == 404


def test_dashboard_route_serves_mvp_ui() -> None:
    _seed_scan()
    client = TestClient(create_api_app())

    response = client.get("/dashboard")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Filterable Resource Ledger" in response.text
    assert "Shutdown Consequences" in response.text
    assert "loadDashboard" in response.text
