from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from aws_account_intelligence.analysis.dependency_graph import DependencyGraphBuilder
from aws_account_intelligence.collectors.factory import get_collector
from aws_account_intelligence.config import Settings
from aws_account_intelligence.models import CostSummaryResponse, InventoryListResponse, ScanRun
from aws_account_intelligence.storage import Database


class ScanPipeline:
    def __init__(self, settings: Settings, database: Database):
        self.settings = settings
        self.database = database
        self.graph_builder = DependencyGraphBuilder()

    def run(self) -> ScanRun:
        scan_run = ScanRun(
            scan_run_id=str(uuid4()),
            started_at=datetime.now(UTC),
            status="running",
            data_source=self.settings.data_source,
            regions=self.settings.region_list,
        )
        self.database.upsert_scan_run(scan_run)

        collector = get_collector(self.settings.data_source)
        bundle = collector.load(scan_run.scan_run_id)
        edges = self.graph_builder.build(bundle.services, scan_run.scan_run_id)

        self.database.save_service_records(bundle.services)
        self.database.save_cost_attributions(bundle.costs)
        self.database.save_dependency_edges(edges)

        scan_run.completed_at = datetime.now(UTC)
        scan_run.status = "completed"
        scan_run.resource_count = len(bundle.services)
        scan_run.edge_count = len(edges)
        scan_run.summary = {
            "services_by_type": _count_by(bundle.services, key=lambda item: item.service_name),
            "cost_total_mtd_usd": round(sum(cost.mtd_cost_usd for cost in bundle.costs), 2),
        }
        self.database.upsert_scan_run(scan_run)
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
            costs=costs,
        )

    def _load_scan(self, scan_run_id: str) -> ScanRun:
        scan = self.database.get_scan_run(scan_run_id)
        if scan is None:
            raise ValueError(f"Unknown scan run: {scan_run_id}")
        return scan


def _count_by(items: list, key):
    counts: dict[str, int] = {}
    for item in items:
        name = key(item)
        counts[name] = counts.get(name, 0) + 1
    return counts
