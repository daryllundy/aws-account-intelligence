from __future__ import annotations

from datetime import UTC, datetime, timedelta
from time import perf_counter
from uuid import uuid4

from aws_account_intelligence.audit import AuditLogger
from aws_account_intelligence.analysis.dependency_graph import DependencyGraphBuilder
from aws_account_intelligence.collectors.factory import get_collector
from aws_account_intelligence.config import Settings
from aws_account_intelligence.models import (
    CostSummaryResponse,
    InventoryListResponse,
    ScanDeltaChange,
    ScanDeltaReport,
    ScanRun,
    ScanSchedule,
    ScheduleStatus,
)
from aws_account_intelligence.storage import Database


class ScanPipeline:
    def __init__(self, settings: Settings, database: Database):
        self.settings = settings
        self.database = database
        self.graph_builder = DependencyGraphBuilder()
        self.audit = AuditLogger(settings.output_dir)

    def run(self) -> ScanRun:
        scan_run = ScanRun(
            scan_run_id=str(uuid4()),
            started_at=datetime.now(UTC),
            status="running",
            data_source=self.settings.data_source,
            regions=self.settings.region_list,
        )
        self.database.upsert_scan_run(scan_run)
        self.audit.emit(
            "scan_run_started",
            {
                "scan_run_id": scan_run.scan_run_id,
                "data_source": scan_run.data_source,
                "regions": scan_run.regions,
            },
        )
        baseline_scan = self.database.get_latest_completed_scan_run(exclude_scan_run_id=scan_run.scan_run_id)

        collector = get_collector(self.settings.data_source)
        bundle = collector.load(scan_run.scan_run_id)
        edges = self.graph_builder.build(bundle.services, scan_run.scan_run_id)

        self.database.save_service_records(bundle.services)
        self.database.save_cost_attributions(bundle.costs)
        self.database.save_dependency_edges(edges)
        delta_report = self._build_delta_report(scan_run.scan_run_id, bundle.services, bundle.costs, baseline_scan)
        self.database.save_delta_report(delta_report)

        scan_run.completed_at = datetime.now(UTC)
        scan_run.status = "completed"
        scan_run.resource_count = len(bundle.services)
        scan_run.edge_count = len(edges)
        scan_run.summary = {
            "services_by_type": _count_by(bundle.services, key=lambda item: item.service_name),
            "cost_total_mtd_usd": round(sum(cost.mtd_cost_usd for cost in bundle.costs), 2),
            "warnings": [
                {
                    "stage": warning.stage,
                    "service": warning.service,
                    "region": warning.region,
                    "code": warning.code,
                    "message": warning.message,
                }
                for warning in bundle.warnings
            ],
            "warning_count": len(bundle.warnings),
            "delta": {
                "baseline_scan_run_id": delta_report.baseline_scan_run_id,
                "added": len(delta_report.added_resources),
                "removed": len(delta_report.removed_resources),
                "cost_changed": len(delta_report.cost_changes),
            },
        }
        self.database.upsert_scan_run(scan_run)
        self.audit.emit(
            "scan_run_completed",
            {
                "scan_run_id": scan_run.scan_run_id,
                "resource_count": scan_run.resource_count,
                "edge_count": scan_run.edge_count,
                "warning_count": scan_run.summary["warning_count"],
                "delta": scan_run.summary["delta"],
            },
        )
        return scan_run

    def inventory(self, scan_run_id: str) -> InventoryListResponse:
        scan = self._load_scan(scan_run_id)
        return InventoryListResponse(scan=scan, services=self.database.list_service_records(scan_run_id))

    def costs(self, scan_run_id: str) -> CostSummaryResponse:
        scan = self._load_scan(scan_run_id)
        costs = self.database.list_cost_attributions(scan_run_id)
        unattributed = next((cost.mtd_cost_usd for cost in costs if cost.resource_id == "unattributed"), 0.0)
        return CostSummaryResponse(
            scan=scan,
            total_mtd_cost_usd=round(sum(cost.mtd_cost_usd for cost in costs), 2),
            total_projected_monthly_cost_usd=round(sum(cost.projected_monthly_cost_usd for cost in costs), 2),
            unattributed_cost_usd=round(unattributed, 2),
            cost_freshness_at=scan.completed_at,
            cost_freshness_age_hours=_freshness_age_hours(scan.completed_at),
            cost_freshness_status=_freshness_status(scan.completed_at),
            costs=costs,
        )

    def _load_scan(self, scan_run_id: str) -> ScanRun:
        scan = self.database.get_scan_run(scan_run_id)
        if scan is None:
            raise ValueError(f"Unknown scan run: {scan_run_id}")
        return scan

    def delta(self, scan_run_id: str) -> ScanDeltaReport:
        report = self.database.get_delta_report(scan_run_id)
        if report is None:
            raise ValueError(f"No delta report found for scan: {scan_run_id}")
        return report

    def create_schedule(self, name: str, interval_hours: int) -> ScanSchedule:
        now = datetime.now(UTC)
        schedule = ScanSchedule(
            schedule_id=str(uuid4()),
            name=name,
            interval_hours=interval_hours,
            status=ScheduleStatus.ACTIVE,
            next_run_at=now + timedelta(hours=interval_hours),
            last_run_at=None,
            regions=self.settings.region_list,
            data_source=self.settings.data_source,
        )
        self.database.save_schedule(schedule)
        return schedule

    def list_schedules(self) -> list[ScanSchedule]:
        return self.database.list_schedules()

    def run_due_schedules(self, now: datetime | None = None) -> list[dict[str, str]]:
        current = now or datetime.now(UTC)
        results: list[dict[str, str]] = []
        for schedule in self.database.get_due_schedules(current):
            scan = self.run()
            next_run_at = current + timedelta(hours=schedule.interval_hours)
            updated = schedule.model_copy(update={"last_run_at": current, "next_run_at": next_run_at})
            self.database.save_schedule(updated)
            results.append(
                {
                    "schedule_id": schedule.schedule_id,
                    "name": schedule.name,
                    "scan_run_id": scan.scan_run_id,
                }
            )
        return results

    def benchmark(self, runs: int = 3) -> dict[str, object]:
        durations = []
        for _ in range(runs):
            started = perf_counter()
            scan = self.run()
            durations.append(round(perf_counter() - started, 4))
        report = {
            "runs": runs,
            "durations_seconds": durations,
            "avg_duration_seconds": round(sum(durations) / len(durations), 4) if durations else 0.0,
            "max_duration_seconds": max(durations) if durations else 0.0,
            "latest_scan_run_id": scan.scan_run_id if durations else None,
        }
        self.audit.emit("scan_benchmark_completed", report)
        return report

    def _build_delta_report(
        self,
        scan_run_id: str,
        services,
        costs,
        baseline_scan: ScanRun | None,
    ) -> ScanDeltaReport:
        if baseline_scan is None:
            return ScanDeltaReport(scan_run_id=scan_run_id, baseline_scan_run_id=None)

        current_services = {service.resource_id: service for service in services}
        current_costs = {cost.resource_id: cost.projected_monthly_cost_usd for cost in costs}
        prior_services = {service.resource_id: service for service in self.database.list_service_records(baseline_scan.scan_run_id)}
        prior_costs = {
            cost.resource_id: cost.projected_monthly_cost_usd
            for cost in self.database.list_cost_attributions(baseline_scan.scan_run_id)
        }

        added = [
            ScanDeltaChange(
                resource_id=service.resource_id,
                service_name=service.service_name,
                change_type="ADDED",
                current_value=current_costs.get(service.resource_id),
                summary=f"{service.service_name} resource was added since the previous scan.",
            )
            for resource_id, service in current_services.items()
            if resource_id not in prior_services
        ]
        removed = [
            ScanDeltaChange(
                resource_id=service.resource_id,
                service_name=service.service_name,
                change_type="REMOVED",
                prior_value=prior_costs.get(service.resource_id),
                summary=f"{service.service_name} resource is no longer present in the latest scan.",
            )
            for resource_id, service in prior_services.items()
            if resource_id not in current_services
        ]
        cost_changes = []
        for resource_id, service in current_services.items():
            if resource_id not in prior_costs or resource_id not in current_costs:
                continue
            prior_value = round(prior_costs[resource_id], 2)
            current_value = round(current_costs[resource_id], 2)
            if abs(current_value - prior_value) < 0.01:
                continue
            cost_changes.append(
                ScanDeltaChange(
                    resource_id=resource_id,
                    service_name=service.service_name,
                    change_type="COST_CHANGED",
                    prior_value=prior_value,
                    current_value=current_value,
                    summary=f"Projected monthly cost changed by {round(current_value - prior_value, 2):.2f} USD.",
                )
            )

        return ScanDeltaReport(
            scan_run_id=scan_run_id,
            baseline_scan_run_id=baseline_scan.scan_run_id,
            added_resources=sorted(added, key=lambda item: item.resource_id),
            removed_resources=sorted(removed, key=lambda item: item.resource_id),
            cost_changes=sorted(cost_changes, key=lambda item: item.resource_id),
        )


def _count_by(items: list, key):
    counts: dict[str, int] = {}
    for item in items:
        name = key(item)
        counts[name] = counts.get(name, 0) + 1
    return counts


def _freshness_age_hours(completed_at: datetime | None) -> float | None:
    if completed_at is None:
        return None
    return round((datetime.now(UTC) - completed_at).total_seconds() / 3600, 2)


def _freshness_status(completed_at: datetime | None) -> str:
    age_hours = _freshness_age_hours(completed_at)
    if age_hours is None:
        return "UNKNOWN"
    if age_hours <= 24:
        return "FRESH"
    return "STALE"
