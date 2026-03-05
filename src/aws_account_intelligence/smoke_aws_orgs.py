from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from aws_account_intelligence.config import get_settings
from aws_account_intelligence.pipeline import ScanPipeline
from aws_account_intelligence.storage import Database


def run_smoke_test() -> int:
    if os.environ.get("AAI_DATA_SOURCE") != "aws_orgs":
        print("Smoke test requires AAI_DATA_SOURCE=aws_orgs.", file=sys.stderr)
        return 2

    os.environ.setdefault(
        "DATABASE_URL",
        f"sqlite+pysqlite:///{Path('.aai-output') / 'smoke_aws_orgs.db'}",
    )
    get_settings.cache_clear()
    settings = get_settings()
    settings.output_dir.mkdir(parents=True, exist_ok=True)

    session = boto3.session.Session()

    try:
        caller = session.client("sts").get_caller_identity()
    except (ClientError, BotoCoreError) as exc:
        print(f"Failed to resolve caller identity: {exc}", file=sys.stderr)
        return 2

    try:
        accounts = _list_accounts(session, limit=min(settings.aws_org_account_limit, 5))
    except ClientError as exc:
        error = exc.response.get("Error", {})
        if error.get("Code") == "AccessDeniedException":
            print(
                "Missing organizations:ListAccounts on the management/scanner identity. "
                "Grant Organizations read access before running aws_orgs scans.",
                file=sys.stderr,
            )
            return 2
        print(f"Organizations access failed: {error.get('Message', str(exc))}", file=sys.stderr)
        return 2
    except BotoCoreError as exc:
        print(f"Organizations access failed: {exc}", file=sys.stderr)
        return 2

    if not accounts:
        print("No active AWS Organization accounts were returned.", file=sys.stderr)
        return 2

    database = Database(settings.database_url)
    database.create_all()
    pipeline = ScanPipeline(settings, database)

    try:
        scan = pipeline.run()
    except Exception as exc:
        message = str(exc)
        if "AssumeRole" in message or "assume_role" in message:
            print(
                "Org scan failed while assuming member-account roles. "
                "Deploy the member role and verify sts:AssumeRole from the management/scanner identity.",
                file=sys.stderr,
            )
            return 2
        print(f"Org scan failed: {message}", file=sys.stderr)
        return 2

    services = database.list_service_records(scan.scan_run_id)
    if not services:
        print("Org scan completed but returned no inventory records.", file=sys.stderr)
        return 2

    account_summary = _account_summary(database, scan.scan_run_id)
    if not account_summary:
        print("Account summary returned no rows after the org scan.", file=sys.stderr)
        return 2

    account_ids = {service.account_id for service in services if service.account_id}
    if not account_ids:
        print("Inventory rows did not contain account IDs.", file=sys.stderr)
        return 2

    payload = {
        "status": "ok",
        "scan_run_id": scan.scan_run_id,
        "caller_account": caller["Account"],
        "caller_arn": caller["Arn"],
        "org_accounts_checked": len(accounts),
        "discovered_inventory_accounts": sorted(account_ids),
        "account_summary_rows": len(account_summary),
        "warning_count": scan.summary.get("warning_count", 0),
    }
    print(json.dumps(payload, indent=2))
    return 0


def _list_accounts(session: boto3.session.Session, limit: int) -> list[dict[str, Any]]:
    paginator = session.client("organizations").get_paginator("list_accounts")
    results: list[dict[str, Any]] = []
    for page in paginator.paginate():
        for account in page.get("Accounts", []):
            if account.get("Status") != "ACTIVE":
                continue
            results.append(account)
            if len(results) >= limit:
                return results
    return results


def _account_summary(database: Database, scan_run_id: str) -> list[dict[str, Any]]:
    services = database.list_service_records(scan_run_id)
    costs = {item.resource_id: item.projected_monthly_cost_usd for item in database.list_cost_attributions(scan_run_id)}
    summary: dict[str, dict[str, Any]] = {}
    for service in services:
        account = summary.setdefault(
            service.account_id,
            {
                "account_id": service.account_id,
                "account_name": service.metadata.get("account_name"),
                "resource_count": 0,
                "projected_monthly_cost_usd": 0.0,
                "regions": set(),
            },
        )
        account["resource_count"] += 1
        account["projected_monthly_cost_usd"] = round(
            account["projected_monthly_cost_usd"] + costs.get(service.resource_id, 0.0),
            2,
        )
        account["regions"].add(service.region)
    return [{**item, "regions": sorted(item["regions"])} for item in summary.values()]


if __name__ == "__main__":
    raise SystemExit(run_smoke_test())
