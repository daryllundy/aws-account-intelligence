from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from importlib import resources

from aws_account_intelligence.models import AttributionMethod, CostAttribution, CostPoint, ResourceStatus, ServiceRecord

from .base import DiscoveryBundle


class FixtureCollector:
    def load(self, scan_run_id: str) -> DiscoveryBundle:
        raw = resources.files("aws_account_intelligence.fixtures").joinpath("sample_account.json").read_text()
        payload = json.loads(raw)
        now = datetime.now(UTC)
        services = [
            ServiceRecord(
                resource_id=item["resource_id"],
                arn=item["arn"],
                resource_type=item["resource_type"],
                service_name=item["service_name"],
                region=item["region"],
                account_id=payload["account_id"],
                tags=item.get("tags", {}),
                status=ResourceStatus.ACTIVE,
                last_seen_at=now,
                scan_run_id=scan_run_id,
                metadata=item.get("metadata", {}),
            )
            for item in payload["resources"]
        ]
        costs = []
        for entry in payload["costs"]:
            mtd = float(entry["mtd_cost_usd"])
            daily_avg = round(mtd / 5, 2)
            daily_costs = [
                CostPoint(date=date.today() - timedelta(days=offset), amount_usd=daily_avg)
                for offset in range(5)
            ]
            costs.append(
                CostAttribution(
                    resource_id=entry["resource_id"],
                    scan_run_id=scan_run_id,
                    daily_costs=daily_costs,
                    mtd_cost_usd=mtd,
                    projected_monthly_cost_usd=float(entry["projected_monthly_cost_usd"]),
                    prior_30_day_cost_usd=float(entry["prior_30_day_cost_usd"]),
                    trend_delta_usd=round(mtd - float(entry["prior_30_day_cost_usd"]), 2),
                    attribution_method=AttributionMethod(entry["attribution_method"]),
                    confidence=float(entry["confidence"]),
                )
            )
        return DiscoveryBundle(services=services, costs=costs, warnings=[])
