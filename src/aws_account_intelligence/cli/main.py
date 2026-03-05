from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Annotated

import typer
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse

from aws_account_intelligence.analysis.dependency_graph import DependencyGraphBuilder
from aws_account_intelligence.analysis.impact import ImpactAnalyzer
from aws_account_intelligence.config import get_settings
from aws_account_intelligence.iam_validation import IamValidator
from aws_account_intelligence.models import IamValidationResult
from aws_account_intelligence.pipeline import ScanPipeline
from aws_account_intelligence.reporting import ReportExporter
from aws_account_intelligence.storage import Database
from aws_account_intelligence.web.dashboard import render_dashboard_html

app = typer.Typer(help="AWS Account Intelligence CLI")
scan_app = typer.Typer()
inventory_app = typer.Typer()
cost_app = typer.Typer()
graph_app = typer.Typer()
impact_app = typer.Typer()
iam_app = typer.Typer()
api_app = typer.Typer()
schedule_app = typer.Typer()
report_app = typer.Typer()

app.add_typer(scan_app, name="scan")
app.add_typer(inventory_app, name="inventory")
app.add_typer(cost_app, name="cost")
app.add_typer(graph_app, name="graph")
app.add_typer(impact_app, name="impact")
app.add_typer(iam_app, name="iam")
app.add_typer(api_app, name="api")
app.add_typer(schedule_app, name="schedule")
app.add_typer(report_app, name="report")


@app.callback()
def main() -> None:
    pass


def _services() -> tuple[Database, ScanPipeline]:
    settings = get_settings()
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    database = Database(settings.database_url)
    database.create_all()
    return database, ScanPipeline(settings, database)


def _resolve_scan_id(database: Database, scan_run_id: str | None, latest: bool) -> str:
    if scan_run_id:
        return scan_run_id
    if latest:
        scan = database.get_latest_scan_run()
        if scan is None:
            raise typer.BadParameter("No scan runs found.")
        return scan.scan_run_id
    raise typer.BadParameter("Provide --scan-run-id or --latest.")


def _emit(payload, output: str, csv_path: Path | None = None) -> None:
    if output == "json":
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    if output == "csv":
        if csv_path is None:
            raise typer.BadParameter("CSV output requires --csv-path.")
        _write_csv(payload, csv_path)
        typer.echo(str(csv_path))
        return
    if output == "table":
        typer.echo(_to_table(payload))
        return
    raise typer.BadParameter(f"Unsupported output format: {output}")


def _write_csv(payload, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, dict) and "services" in payload:
        rows = payload["services"]
    elif isinstance(payload, dict) and "costs" in payload:
        rows = payload["costs"]
    else:
        rows = payload if isinstance(payload, list) else [payload]
    if not rows:
        rows = [{}]
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=sorted(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _to_table(payload) -> str:
    if isinstance(payload, dict) and "services" in payload:
        rows = payload["services"]
        if not rows:
            return "No services found"
        headers = ["service_name", "resource_type", "region", "status", "resource_id"]
    elif isinstance(payload, dict) and "costs" in payload:
        rows = payload["costs"]
        if not rows:
            return "No costs found"
        headers = ["resource_id", "mtd_cost_usd", "projected_monthly_cost_usd", "attribution_method", "confidence"]
    else:
        return json.dumps(payload, indent=2, default=str)
    lines = [" | ".join(headers)]
    lines.append(" | ".join(["---"] * len(headers)))
    for row in rows:
        lines.append(" | ".join(str(row.get(header, "")) for header in headers))
    return "\n".join(lines)


@scan_app.command("run")
def scan_run(output: str = "json") -> None:
    _, pipeline = _services()
    scan = pipeline.run()
    _emit(scan.model_dump(mode="json"), output)


@scan_app.command("status")
def scan_status(scan_run_id: str | None = None, latest: bool = True, output: str = "json") -> None:
    database, _ = _services()
    resolved = _resolve_scan_id(database, scan_run_id, latest)
    scan = database.get_scan_run(resolved)
    _emit(scan.model_dump(mode="json"), output)


@inventory_app.command("list")
def inventory_list(
    scan_run_id: str | None = None,
    latest: bool = True,
    output: str = "table",
    csv_path: Path | None = None,
) -> None:
    database, pipeline = _services()
    resolved = _resolve_scan_id(database, scan_run_id, latest)
    response = pipeline.inventory(resolved)
    _emit(response.model_dump(mode="json"), output, csv_path)


@cost_app.command("summary")
def cost_summary(
    scan_run_id: str | None = None,
    latest: bool = True,
    output: str = "json",
    csv_path: Path | None = None,
) -> None:
    database, pipeline = _services()
    resolved = _resolve_scan_id(database, scan_run_id, latest)
    response = pipeline.costs(resolved)
    _emit(response.model_dump(mode="json"), output, csv_path)


@graph_app.command("export")
def graph_export(scan_run_id: str | None = None, latest: bool = True, output: str = "json") -> None:
    database, _ = _services()
    resolved = _resolve_scan_id(database, scan_run_id, latest)
    scan = database.get_scan_run(resolved)
    edges = database.list_dependency_edges(resolved)
    response = DependencyGraphBuilder().export(scan, edges)
    _emit(response.model_dump(mode="json"), output)


@impact_app.command("analyze")
def impact_analyze(
    resource: Annotated[str, typer.Option("--resource", help="Resource ARN or normalized resource ID")],
    scan_run_id: str | None = None,
    latest: bool = True,
    output: str = "json",
) -> None:
    database, _ = _services()
    resolved = _resolve_scan_id(database, scan_run_id, latest)
    services = database.list_service_records(resolved)
    costs = {item.resource_id: item.projected_monthly_cost_usd for item in database.list_cost_attributions(resolved)}
    edges = database.list_dependency_edges(resolved)
    report = ImpactAnalyzer().analyze(resolved, resource, services, costs, edges)
    _emit(report.model_dump(mode="json"), output)


@scan_app.command("delta")
def scan_delta(scan_run_id: str | None = None, latest: bool = True, output: str = "json") -> None:
    database, pipeline = _services()
    resolved = _resolve_scan_id(database, scan_run_id, latest)
    report = pipeline.delta(resolved)
    _emit(report.model_dump(mode="json"), output)


@iam_app.command("validate")
def iam_validate(output: str = "json") -> None:
    result: IamValidationResult = IamValidator().validate()
    _emit(result.model_dump(mode="json"), output)


@schedule_app.command("create")
def schedule_create(name: str, interval_hours: int = 24, output: str = "json") -> None:
    _, pipeline = _services()
    schedule = pipeline.create_schedule(name=name, interval_hours=interval_hours)
    _emit(schedule.model_dump(mode="json"), output)


@schedule_app.command("list")
def schedule_list(output: str = "json") -> None:
    _, pipeline = _services()
    schedules = [schedule.model_dump(mode="json") for schedule in pipeline.list_schedules()]
    _emit(schedules, output)


@schedule_app.command("run-due")
def schedule_run_due(output: str = "json") -> None:
    _, pipeline = _services()
    result = pipeline.run_due_schedules()
    _emit(result, output)


@report_app.command("export")
def report_export(
    format_name: Annotated[str, typer.Option("--format", help="json, csv, pdf, slack, or email")],
    scan_run_id: str | None = None,
    latest: bool = True,
    destination: Path | None = None,
) -> None:
    database, pipeline = _services()
    settings = get_settings()
    resolved = _resolve_scan_id(database, scan_run_id, latest)
    exporter = ReportExporter(database=database, pipeline=pipeline, output_dir=settings.output_dir)
    target = exporter.export(resolved, format_name=format_name, destination=destination)
    typer.echo(str(target))


@api_app.command("serve")
def api_serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    import uvicorn

    uvicorn.run(create_api_app(), host=host, port=port)


def create_api_app() -> FastAPI:
    api = FastAPI(title="AWS Account Intelligence API", version="0.1.0")

    @api.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @api.get("/", response_class=HTMLResponse)
    @api.get("/dashboard", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        return HTMLResponse(render_dashboard_html())

    @api.get("/scans")
    def list_scans(limit: int = Query(default=20, ge=1, le=100)) -> JSONResponse:
        database, _ = _services()
        scans = database.list_scan_runs(limit=limit)
        return JSONResponse([scan.model_dump(mode="json") for scan in scans])

    @api.get("/scans/latest")
    def latest_scan() -> JSONResponse:
        database, _ = _services()
        scan = database.get_latest_scan_run()
        if scan is None:
            return JSONResponse(status_code=404, content={"detail": "No scans found"})
        return JSONResponse(scan.model_dump(mode="json"))

    @api.get("/scans/{scan_run_id}")
    def scan_detail(scan_run_id: str) -> JSONResponse:
        database, _ = _services()
        scan = database.get_scan_run(scan_run_id)
        if scan is None:
            raise HTTPException(status_code=404, detail=f"Unknown scan run: {scan_run_id}")
        return JSONResponse(scan.model_dump(mode="json"))

    @api.get("/inventory")
    def inventory(
        scan_run_id: str | None = None,
        latest: bool = True,
        service_name: str | None = None,
        region: str | None = None,
        status: str | None = None,
        search: str | None = None,
    ) -> JSONResponse:
        database, pipeline = _services()
        resolved = _resolve_api_scan_id(database, scan_run_id, latest)
        response = pipeline.inventory(resolved)
        services = response.services
        if service_name:
            services = [item for item in services if item.service_name == service_name]
        if region:
            services = [item for item in services if item.region == region]
        if status:
            status_normalized = status.upper()
            services = [item for item in services if item.status.value == status_normalized]
        if search:
            needle = search.lower()
            services = [
                item
                for item in services
                if needle in item.resource_id.lower()
                or needle in item.arn.lower()
                or any(needle in value.lower() for value in item.tags.values())
            ]
        payload = response.model_copy(update={"services": services})
        return JSONResponse(payload.model_dump(mode="json"))

    @api.get("/costs/summary")
    def api_cost_summary(scan_run_id: str | None = None, latest: bool = True) -> JSONResponse:
        database, pipeline = _services()
        resolved = _resolve_api_scan_id(database, scan_run_id, latest)
        response = pipeline.costs(resolved)
        return JSONResponse(response.model_dump(mode="json"))

    @api.get("/scans/{scan_run_id}/delta")
    def api_scan_delta(scan_run_id: str) -> JSONResponse:
        database, pipeline = _services()
        if database.get_scan_run(scan_run_id) is None:
            raise HTTPException(status_code=404, detail=f"Unknown scan run: {scan_run_id}")
        response = pipeline.delta(scan_run_id)
        return JSONResponse(response.model_dump(mode="json"))

    @api.get("/graph")
    def graph_export_api(
        scan_run_id: str | None = None,
        latest: bool = True,
        edge_type: str | None = None,
        resource_id: str | None = None,
    ) -> JSONResponse:
        database, _ = _services()
        resolved = _resolve_api_scan_id(database, scan_run_id, latest)
        scan = database.get_scan_run(resolved)
        if scan is None:
            raise HTTPException(status_code=404, detail=f"Unknown scan run: {resolved}")
        edges = database.list_dependency_edges(resolved)
        if edge_type:
            edges = [edge for edge in edges if edge.edge_type.value == edge_type.upper()]
        if resource_id:
            edges = [edge for edge in edges if resource_id in {edge.from_resource_id, edge.to_resource_id}]
        response = DependencyGraphBuilder().export(scan, edges)
        return JSONResponse(response.model_dump(mode="json"))

    @api.get("/impact")
    def impact(
        resource: str,
        scan_run_id: str | None = None,
        latest: bool = True,
    ) -> JSONResponse:
        database, _ = _services()
        resolved = _resolve_api_scan_id(database, scan_run_id, latest)
        services = database.list_service_records(resolved)
        if resource not in {service.resource_id for service in services}:
            raise HTTPException(status_code=404, detail=f"Unknown resource: {resource}")
        costs = {item.resource_id: item.projected_monthly_cost_usd for item in database.list_cost_attributions(resolved)}
        edges = database.list_dependency_edges(resolved)
        report = ImpactAnalyzer().analyze(resolved, resource, services, costs, edges)
        return JSONResponse(report.model_dump(mode="json"))

    @api.get("/schedules")
    def schedules() -> JSONResponse:
        _, pipeline = _services()
        return JSONResponse([schedule.model_dump(mode="json") for schedule in pipeline.list_schedules()])

    return api


def _resolve_api_scan_id(database: Database, scan_run_id: str | None, latest: bool) -> str:
    try:
        return _resolve_scan_id(database, scan_run_id, latest)
    except typer.BadParameter as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
