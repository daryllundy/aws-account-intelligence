from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from aws_account_intelligence.collectors.aws import AwsCollector
from aws_account_intelligence.collectors.base import CollectorError, DiscoveryBundle, ScanWarning
from aws_account_intelligence.config import Settings, get_settings


class OrganizationsCollector:
    def __init__(
        self,
        settings: Settings | None = None,
        session: boto3.session.Session | None = None,
        account_session_factory: Callable[[str], boto3.session.Session] | None = None,
    ):
        self.settings = settings or get_settings()
        self.session = session or boto3.session.Session()
        self.account_session_factory = account_session_factory or self._assume_role_session

    def load(self, scan_run_id: str) -> DiscoveryBundle:
        accounts = self._list_active_accounts()
        services = []
        costs = []
        warnings: list[ScanWarning] = []

        max_workers = max(1, min(len(accounts), self.settings.aws_region_concurrency))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._collect_account, account, scan_run_id): account for account in accounts}
            for future in as_completed(futures):
                account = futures[future]
                try:
                    bundle = future.result()
                except CollectorError as exc:
                    warnings.append(
                        ScanWarning(
                            stage="org_account_scan",
                            service="organizations",
                            region=None,
                            code="ACCOUNT_SCAN_FAILED",
                            message=f"{account['id']} ({account['name']}): {exc}",
                        )
                    )
                    continue
                services.extend(bundle.services)
                costs.extend(bundle.costs)
                for warning in bundle.warnings:
                    warnings.append(
                        ScanWarning(
                            stage=warning.stage,
                            service=warning.service,
                            region=warning.region,
                            code=warning.code,
                            message=f"{account['id']} ({account['name']}): {warning.message}",
                        )
                    )
        return DiscoveryBundle(services=services, costs=costs, warnings=warnings)

    def _collect_account(self, account: dict[str, str], scan_run_id: str) -> DiscoveryBundle:
        session = self.account_session_factory(account["id"])
        collector = AwsCollector(settings=self.settings, session=session)
        bundle = collector.load(scan_run_id)
        for service in bundle.services:
            service.metadata["account_name"] = account["name"]
        return bundle

    def _list_active_accounts(self) -> list[dict[str, str]]:
        client = self.session.client("organizations")
        try:
            paginator = client.get_paginator("list_accounts")
            accounts = []
            for page in paginator.paginate():
                for account in page.get("Accounts", []):
                    if account.get("Status") != "ACTIVE":
                        continue
                    accounts.append({"id": account["Id"], "name": account["Name"]})
                    if len(accounts) >= self.settings.aws_org_account_limit:
                        return accounts
            return accounts
        except ClientError as exc:
            message = exc.response.get("Error", {}).get("Message", str(exc))
            raise CollectorError(f"Organizations discovery failed: {message}") from exc
        except BotoCoreError as exc:
            raise CollectorError(f"Organizations discovery failed: {exc}") from exc

    def _assume_role_session(self, account_id: str) -> boto3.session.Session:
        role_arn = f"arn:aws:iam::{account_id}:role/{self.settings.aws_org_role_name}"
        sts = self.session.client("sts")
        response = sts.assume_role(RoleArn=role_arn, RoleSessionName="aws-account-intelligence")
        credentials = response["Credentials"]
        return boto3.session.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
