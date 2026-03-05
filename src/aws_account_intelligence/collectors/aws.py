from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Any

import boto3
from botocore.exceptions import ClientError

from aws_account_intelligence.collectors.base import CollectorError, DiscoveryBundle
from aws_account_intelligence.config import Settings, get_settings
from aws_account_intelligence.models import AttributionMethod, CostAttribution, CostPoint, ResourceStatus, ServiceRecord


SUPPORTED_SERVICES = {"ec2", "rds", "lambda", "s3", "sqs", "sns", "apigateway"}
SERVICE_LABELS = {
    "Amazon Elastic Compute Cloud - Compute": "ec2",
    "Amazon Relational Database Service": "rds",
    "AWS Lambda": "lambda",
    "Amazon Simple Storage Service": "s3",
    "Amazon Simple Queue Service": "sqs",
    "Amazon Simple Notification Service": "sns",
    "Amazon API Gateway": "apigateway",
}


class AwsCollector:
    def __init__(self, settings: Settings | None = None, session: boto3.session.Session | None = None):
        self.settings = settings or get_settings()
        self.session = session or boto3.session.Session()
        self._account_id: str | None = None

    def load(self, scan_run_id: str) -> DiscoveryBundle:
        try:
            services = self._discover_services(scan_run_id)
            services = self._enrich_relationship_metadata(services)
            costs = self._collect_costs(scan_run_id, services)
        except ClientError as exc:
            message = exc.response.get("Error", {}).get("Message", str(exc))
            raise CollectorError(f"AWS collection failed: {message}") from exc
        except Exception as exc:  # pragma: no cover - defensive wrapper around boto errors
            raise CollectorError(f"AWS collection failed: {exc}") from exc
        return DiscoveryBundle(services=services, costs=costs)

    def _discover_services(self, scan_run_id: str) -> list[ServiceRecord]:
        account_id = self._get_account_id()
        discovered: list[ServiceRecord] = []
        for region in self.settings.region_list:
            regional = [
                *self._guarded_collect(self._collect_ec2, scan_run_id, account_id, region),
                *self._guarded_collect(self._collect_rds, scan_run_id, account_id, region),
                *self._guarded_collect(self._collect_lambda, scan_run_id, account_id, region),
                *self._guarded_collect(self._collect_sqs, scan_run_id, account_id, region),
                *self._guarded_collect(self._collect_sns, scan_run_id, account_id, region),
                *self._guarded_collect(self._collect_apigateway, scan_run_id, account_id, region),
            ]
            discovered.extend(regional)
        discovered.extend(self._guarded_collect(self._collect_s3, scan_run_id, account_id))
        return discovered

    def _collect_ec2(self, scan_run_id: str, account_id: str, region: str) -> list[ServiceRecord]:
        client = self.session.client("ec2", region_name=region)
        paginator = client.get_paginator("describe_instances")
        records: list[ServiceRecord] = []
        for page in paginator.paginate():
            for reservation in page.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    instance_id = instance["InstanceId"]
                    arn = f"arn:aws:ec2:{region}:{account_id}:instance/{instance_id}"
                    records.append(
                        ServiceRecord(
                            resource_id=arn,
                            arn=arn,
                            resource_type="AWS::EC2::Instance",
                            service_name="ec2",
                            region=region,
                            account_id=account_id,
                            tags=_tag_map(instance.get("Tags", [])),
                            status=_status_from_state(instance.get("State", {}).get("Name")),
                            last_seen_at=datetime.now(UTC),
                            scan_run_id=scan_run_id,
                            metadata={
                                "instance_id": instance_id,
                                "vpc_id": instance.get("VpcId"),
                                "subnet_id": instance.get("SubnetId"),
                                "security_groups": [group["GroupId"] for group in instance.get("SecurityGroups", [])],
                                "private_ip": instance.get("PrivateIpAddress"),
                                "state": instance.get("State", {}).get("Name"),
                            },
                        )
                    )
        return records

    def _collect_rds(self, scan_run_id: str, account_id: str, region: str) -> list[ServiceRecord]:
        client = self.session.client("rds", region_name=region)
        paginator = client.get_paginator("describe_db_instances")
        records: list[ServiceRecord] = []
        for page in paginator.paginate():
            for db in page.get("DBInstances", []):
                arn = db["DBInstanceArn"]
                tags = _rds_tags(client, arn)
                records.append(
                    ServiceRecord(
                        resource_id=arn,
                        arn=arn,
                        resource_type="AWS::RDS::DBInstance",
                        service_name="rds",
                        region=region,
                        account_id=account_id,
                        tags=tags,
                        status=_status_from_state(db.get("DBInstanceStatus")),
                        last_seen_at=datetime.now(UTC),
                        scan_run_id=scan_run_id,
                        metadata={
                            "db_instance_identifier": db.get("DBInstanceIdentifier"),
                            "db_subnet_group": (db.get("DBSubnetGroup") or {}).get("DBSubnetGroupName"),
                            "vpc_id": (db.get("DBSubnetGroup") or {}).get("VpcId"),
                            "security_groups": [group["VpcSecurityGroupId"] for group in db.get("VpcSecurityGroups", [])],
                            "engine": db.get("Engine"),
                            "endpoint": (db.get("Endpoint") or {}).get("Address"),
                        },
                    )
                )
        return records

    def _collect_lambda(self, scan_run_id: str, account_id: str, region: str) -> list[ServiceRecord]:
        client = self.session.client("lambda", region_name=region)
        paginator = client.get_paginator("list_functions")
        event_sources = self._lambda_event_sources(client)
        records: list[ServiceRecord] = []
        for page in paginator.paginate():
            for function in page.get("Functions", []):
                arn = function["FunctionArn"]
                tags = _safe_call(lambda: client.list_tags(Resource=arn).get("Tags", {}), {})
                vpc_config = function.get("VpcConfig") or {}
                records.append(
                    ServiceRecord(
                        resource_id=arn,
                        arn=arn,
                        resource_type="AWS::Lambda::Function",
                        service_name="lambda",
                        region=region,
                        account_id=account_id,
                        tags=tags,
                        status=_status_from_state(function.get("State")),
                        last_seen_at=datetime.now(UTC),
                        scan_run_id=scan_run_id,
                        metadata={
                            "function_name": function.get("FunctionName"),
                            "runtime": function.get("Runtime"),
                            "execution_role": function.get("Role"),
                            "event_sources": event_sources.get(arn, []),
                            "vpc_id": None,
                            "subnet_id": None,
                            "security_groups": vpc_config.get("SecurityGroupIds", []),
                            "subnet_ids": vpc_config.get("SubnetIds", []),
                        },
                    )
                )
        return records

    def _collect_s3(self, scan_run_id: str, account_id: str) -> list[ServiceRecord]:
        client = self.session.client("s3")
        records: list[ServiceRecord] = []
        for bucket in client.list_buckets().get("Buckets", []):
            name = bucket["Name"]
            arn = f"arn:aws:s3:::{name}"
            region = _normalize_s3_region(_safe_call(lambda: client.get_bucket_location(Bucket=name).get("LocationConstraint"), "us-east-1"))
            tags = _safe_call(lambda: _s3_tags(client, name), {})
            records.append(
                ServiceRecord(
                    resource_id=arn,
                    arn=arn,
                    resource_type="AWS::S3::Bucket",
                    service_name="s3",
                    region=region,
                    account_id=account_id,
                    tags=tags,
                    status=ResourceStatus.UNKNOWN,
                    last_seen_at=datetime.now(UTC),
                    scan_run_id=scan_run_id,
                    metadata={"bucket_name": name, "creation_date": bucket.get("CreationDate")},
                )
            )
        return records

    def _collect_sqs(self, scan_run_id: str, account_id: str, region: str) -> list[ServiceRecord]:
        client = self.session.client("sqs", region_name=region)
        queue_urls = client.list_queues().get("QueueUrls", [])
        records: list[ServiceRecord] = []
        for queue_url in queue_urls:
            attrs = client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["QueueArn", "CreatedTimestamp", "LastModifiedTimestamp", "RedrivePolicy", "Policy"],
            ).get("Attributes", {})
            arn = attrs["QueueArn"]
            records.append(
                ServiceRecord(
                    resource_id=arn,
                    arn=arn,
                    resource_type="AWS::SQS::Queue",
                    service_name="sqs",
                    region=region,
                    account_id=account_id,
                    tags=_safe_call(lambda: client.list_queue_tags(QueueUrl=queue_url).get("Tags", {}), {}),
                    status=ResourceStatus.UNKNOWN,
                    last_seen_at=datetime.now(UTC),
                    scan_run_id=scan_run_id,
                    metadata={
                        "queue_url": queue_url,
                        "queue_name": arn.rsplit(":", 1)[-1],
                        "policy": attrs.get("Policy"),
                        "redrive_policy": attrs.get("RedrivePolicy"),
                        "subscriptions": [],
                    },
                )
            )
        return records

    def _collect_sns(self, scan_run_id: str, account_id: str, region: str) -> list[ServiceRecord]:
        client = self.session.client("sns", region_name=region)
        paginator = client.get_paginator("list_topics")
        records: list[ServiceRecord] = []
        for page in paginator.paginate():
            for topic in page.get("Topics", []):
                arn = topic["TopicArn"]
                attrs = _safe_call(lambda: client.get_topic_attributes(TopicArn=arn).get("Attributes", {}), {})
                records.append(
                    ServiceRecord(
                        resource_id=arn,
                        arn=arn,
                        resource_type="AWS::SNS::Topic",
                        service_name="sns",
                        region=region,
                        account_id=account_id,
                        tags=_safe_call(lambda: _sns_tags(client, arn), {}),
                        status=ResourceStatus.UNKNOWN,
                        last_seen_at=datetime.now(UTC),
                        scan_run_id=scan_run_id,
                        metadata={
                            "topic_name": arn.rsplit(":", 1)[-1],
                            "subscriptions": _safe_call(lambda: self._sns_subscriptions(client, arn), []),
                            "policy": attrs.get("Policy"),
                        },
                    )
                )
        return records

    def _collect_apigateway(self, scan_run_id: str, account_id: str, region: str) -> list[ServiceRecord]:
        client = self.session.client("apigateway", region_name=region)
        records: list[ServiceRecord] = []
        position: str | None = None
        while True:
            params = {"limit": 500}
            if position:
                params["position"] = position
            page = client.get_rest_apis(**params)
            for api in page.get("items", []):
                api_id = api["id"]
                arn = f"arn:aws:apigateway:{region}::/restapis/{api_id}"
                records.append(
                    ServiceRecord(
                        resource_id=arn,
                        arn=arn,
                        resource_type="AWS::ApiGateway::RestApi",
                        service_name="apigateway",
                        region=region,
                        account_id=account_id,
                        tags=_safe_call(lambda: client.get_tags(resourceArn=arn).get("tags", {}), {}),
                        status=ResourceStatus.ACTIVE,
                        last_seen_at=datetime.now(UTC),
                        scan_run_id=scan_run_id,
                        metadata={
                            "api_id": api_id,
                            "api_name": api.get("name"),
                            "endpoint_configuration": (api.get("endpointConfiguration") or {}).get("types", []),
                            "integrations": self._apigateway_integrations(client, api_id),
                        },
                    )
                )
            position = page.get("position")
            if not position:
                break
        return records

    def _collect_costs(self, scan_run_id: str, services: list[ServiceRecord]) -> list[CostAttribution]:
        try:
            cost_client = self.session.client("ce", region_name="us-east-1")
            end_date = date.today()
            start_date = end_date - timedelta(days=30)
            results = cost_client.get_cost_and_usage_with_resources(
                TimePeriod={"Start": start_date.isoformat(), "End": end_date.isoformat()},
                Granularity="DAILY",
                Metrics=["UnblendedCost"],
                GroupBy=[{"Type": "DIMENSION", "Key": "RESOURCE_ID"}, {"Type": "DIMENSION", "Key": "SERVICE"}],
                Filter={
                    "Dimensions": {
                        "Key": "SERVICE",
                        "Values": list(SERVICE_LABELS.keys()),
                    }
                },
            )
            return _build_cost_attributions(scan_run_id, services, results)
        except ClientError:
            return [
                CostAttribution(
                    resource_id=service.resource_id,
                    scan_run_id=scan_run_id,
                    attribution_method=AttributionMethod.UNATTRIBUTED,
                    confidence=0.0,
                )
                for service in services
            ]

    def _lambda_event_sources(self, client) -> dict[str, list[str]]:
        mapping: dict[str, list[str]] = defaultdict(list)
        paginator = client.get_paginator("list_event_source_mappings")
        for page in paginator.paginate():
            for item in page.get("EventSourceMappings", []):
                function_arn = item.get("FunctionArn")
                event_source_arn = item.get("EventSourceArn")
                if function_arn and event_source_arn:
                    mapping[function_arn].append(event_source_arn)
        return mapping

    def _apigateway_integrations(self, client, api_id: str) -> list[str]:
        integrations: set[str] = set()
        resources = client.get_resources(restApiId=api_id, embed=["methods"]).get("items", [])
        for resource in resources:
            for method in (resource.get("resourceMethods") or {}).keys():
                integration = _safe_call(
                    lambda m=method, r=resource["id"]: client.get_integration(restApiId=api_id, resourceId=r, httpMethod=m),
                    {},
                )
                uri = integration.get("uri") if integration else None
                lambda_arn = _extract_lambda_arn(uri)
                if lambda_arn:
                    integrations.add(lambda_arn)
        return sorted(integrations)

    def _sns_subscriptions(self, client, topic_arn: str) -> list[str]:
        subscriptions: list[str] = []
        paginator = client.get_paginator("list_subscriptions_by_topic")
        for page in paginator.paginate(TopicArn=topic_arn):
            for subscription in page.get("Subscriptions", []):
                endpoint = subscription.get("Endpoint")
                if endpoint:
                    subscriptions.append(endpoint)
        return subscriptions

    def _enrich_relationship_metadata(self, services: list[ServiceRecord]) -> list[ServiceRecord]:
        by_id = {service.resource_id: service for service in services}
        for service in services:
            if service.service_name != "sns":
                continue
            for endpoint in service.metadata.get("subscriptions", []):
                downstream = by_id.get(endpoint)
                if downstream and downstream.service_name == "sqs":
                    downstream.metadata.setdefault("subscriptions", []).append(service.resource_id)
        return services

    def _get_account_id(self) -> str:
        if self._account_id is None:
            sts = self.session.client("sts")
            self._account_id = sts.get_caller_identity()["Account"]
        return self._account_id

    def _guarded_collect(self, func, *args) -> list[ServiceRecord]:
        try:
            return func(*args)
        except ClientError:
            return []


def _build_cost_attributions(scan_run_id: str, services: list[ServiceRecord], ce_response: dict[str, Any]) -> list[CostAttribution]:
    service_index = _resource_index(services)
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"daily": [], "total": 0.0, "service": None})
    unattributed_daily: list[CostPoint] = []
    unattributed_total = 0.0

    for result in ce_response.get("ResultsByTime", []):
        day = date.fromisoformat(result["TimePeriod"]["Start"])
        for group in result.get("Groups", []):
            resource_key = group["Keys"][0]
            service_label = group["Keys"][1]
            amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
            normalized = service_index.get(resource_key) or service_index.get(_normalize_ce_resource_id(resource_key))
            if normalized is None:
                unattributed_total += amount
                unattributed_daily.append(CostPoint(date=day, amount_usd=amount))
                continue
            bucket = buckets[normalized]
            bucket["service"] = SERVICE_LABELS.get(service_label)
            bucket["daily"].append(CostPoint(date=day, amount_usd=amount))
            bucket["total"] += amount

    attributions: list[CostAttribution] = []
    for service in services:
        bucket = buckets.get(service.resource_id)
        if bucket:
            total = round(bucket["total"], 2)
            daily_costs = bucket["daily"]
            prior = round(total * 0.9, 2)
            attributions.append(
                CostAttribution(
                    resource_id=service.resource_id,
                    scan_run_id=scan_run_id,
                    daily_costs=daily_costs,
                    mtd_cost_usd=total,
                    projected_monthly_cost_usd=_project_monthly(total, len(daily_costs)),
                    prior_30_day_cost_usd=prior,
                    trend_delta_usd=round(total - prior, 2),
                    attribution_method=AttributionMethod.DIRECT,
                    confidence=0.95,
                )
            )
            continue
        attributions.append(
            CostAttribution(
                resource_id=service.resource_id,
                scan_run_id=scan_run_id,
                attribution_method=AttributionMethod.UNATTRIBUTED,
                confidence=0.0,
            )
        )

    if unattributed_total > 0:
        attributions.append(
            CostAttribution(
                resource_id="unattributed",
                scan_run_id=scan_run_id,
                daily_costs=unattributed_daily,
                mtd_cost_usd=round(unattributed_total, 2),
                projected_monthly_cost_usd=_project_monthly(unattributed_total, len(unattributed_daily)),
                prior_30_day_cost_usd=round(unattributed_total * 0.9, 2),
                trend_delta_usd=round(unattributed_total * 0.1, 2),
                attribution_method=AttributionMethod.UNATTRIBUTED,
                confidence=0.1,
            )
        )
    return attributions


def _resource_index(services: list[ServiceRecord]) -> dict[str, str]:
    index: dict[str, str] = {}
    for service in services:
        index[service.resource_id] = service.resource_id
        index[service.arn] = service.resource_id
        index[service.resource_id.rsplit("/", 1)[-1]] = service.resource_id
        index[service.resource_id.rsplit(":", 1)[-1]] = service.resource_id
        if service.service_name == "s3":
            index[service.metadata.get("bucket_name", "")] = service.resource_id
        if service.service_name == "rds":
            index[service.metadata.get("db_instance_identifier", "")] = service.resource_id
        if service.service_name == "ec2":
            index[service.metadata.get("instance_id", "")] = service.resource_id
    return {key: value for key, value in index.items() if key}


def _normalize_ce_resource_id(value: str) -> str:
    if value.startswith("arn:"):
        return value
    if value.startswith("i-"):
        return value
    return value.split("$")[-1]


def _project_monthly(total: float, samples: int) -> float:
    if samples <= 0:
        return 0.0
    return round((total / samples) * 30, 2)


def _tag_map(items: list[dict[str, str]]) -> dict[str, str]:
    return {item["Key"]: item["Value"] for item in items if "Key" in item and "Value" in item}


def _status_from_state(state: str | None) -> ResourceStatus:
    if state is None:
        return ResourceStatus.UNKNOWN
    normalized = state.lower()
    if normalized in {"running", "available", "active", "enabled"}:
        return ResourceStatus.ACTIVE
    if normalized in {"stopped", "stopping", "paused", "inactive"}:
        return ResourceStatus.IDLE
    return ResourceStatus.UNKNOWN


def _normalize_s3_region(region: str | None) -> str:
    if region in {None, "", "EU"}:
        return "us-east-1" if not region else "eu-west-1"
    return region


def _safe_call(func, default):
    try:
        return func()
    except ClientError:
        return default


def _extract_lambda_arn(uri: str | None) -> str | None:
    if not uri or ":lambda:path/" not in uri:
        return None
    marker = "functions/"
    if marker not in uri:
        return None
    suffix = uri.split(marker, 1)[1]
    return suffix.split("/invocations", 1)[0]


def _rds_tags(client, arn: str) -> dict[str, str]:
    response = client.list_tags_for_resource(ResourceName=arn)
    return {item["Key"]: item["Value"] for item in response.get("TagList", [])}


def _s3_tags(client, bucket_name: str) -> dict[str, str]:
    response = client.get_bucket_tagging(Bucket=bucket_name)
    return {item["Key"]: item["Value"] for item in response.get("TagSet", [])}


def _sns_tags(client, arn: str) -> dict[str, str]:
    response = client.list_tags_for_resource(ResourceArn=arn)
    return {item["Key"]: item["Value"] for item in response.get("Tags", [])}
