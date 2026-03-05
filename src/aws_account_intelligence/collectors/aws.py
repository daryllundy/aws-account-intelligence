from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from time import sleep
from typing import Any, Callable

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from aws_account_intelligence.collectors.base import CollectorError, DiscoveryBundle, ScanWarning
from aws_account_intelligence.config import Settings, get_settings
from aws_account_intelligence.models import AttributionMethod, CostAttribution, CostPoint, ResourceStatus, ServiceRecord


SERVICE_LABELS = {
    "Amazon Elastic Compute Cloud - Compute": "ec2",
    "Amazon Relational Database Service": "rds",
    "AWS Lambda": "lambda",
    "Amazon Simple Storage Service": "s3",
    "Amazon Simple Queue Service": "sqs",
    "Amazon Simple Notification Service": "sns",
    "Amazon API Gateway": "apigateway",
    "Amazon Elastic Container Service": "ecs",
    "Amazon Elastic Kubernetes Service": "eks",
    "Amazon ElastiCache": "elasticache",
    "Amazon CloudFront": "cloudfront",
}
TAGGING_RESOURCE_TYPES = {
    "ec2": ["ec2:instance"],
    "rds": ["rds:db"],
    "lambda": ["lambda:function"],
    "s3": ["s3:bucket"],
    "sqs": ["sqs:queue"],
    "sns": ["sns:topic"],
    "apigateway": ["apigateway:restapis"],
    "ecs": ["ecs:cluster"],
    "eks": ["eks:cluster"],
    "elasticache": ["elasticache:cluster"],
    "cloudfront": ["cloudfront:distribution"],
}
THROTTLE_CODES = {
    "Throttled",
    "Throttling",
    "ThrottlingException",
    "RequestLimitExceeded",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
}
TRANSIENT_CODES = {"InternalError", "ServiceUnavailable", "ServiceUnavailableException", "RequestTimeout"}


class AwsCollector:
    def __init__(self, settings: Settings | None = None, session: boto3.session.Session | None = None):
        self.settings = settings or get_settings()
        self.session = session or boto3.session.Session()
        self._account_id: str | None = None
        self._regional_collectors: list[tuple[str, Callable[[str, str, str], list[ServiceRecord]]]] = [
            ("ec2", self._collect_ec2),
            ("rds", self._collect_rds),
            ("lambda", self._collect_lambda),
            ("sqs", self._collect_sqs),
            ("sns", self._collect_sns),
            ("apigateway", self._collect_apigateway),
            ("ecs", self._collect_ecs),
            ("eks", self._collect_eks),
            ("elasticache", self._collect_elasticache),
        ]

    def load(self, scan_run_id: str) -> DiscoveryBundle:
        try:
            tag_inventory, tag_warnings = self._collect_tag_inventory(scan_run_id)
            services, warnings = self._discover_services(scan_run_id, tag_inventory)
            warnings = [*tag_warnings, *warnings]
            services = self._enrich_relationship_metadata(services)
            services = self._add_tagging_only_resources(scan_run_id, services, tag_inventory)
            config_warnings = self._enrich_config_relationships(services)
            warnings.extend(config_warnings)
            services, activity_warnings = self._classify_activity(services)
            warnings.extend(activity_warnings)
            costs, cost_warnings = self._collect_costs(scan_run_id, services)
            warnings.extend(cost_warnings)
        except ClientError as exc:
            message = exc.response.get("Error", {}).get("Message", str(exc))
            raise CollectorError(f"AWS collection failed: {message}") from exc
        except Exception as exc:  # pragma: no cover
            raise CollectorError(f"AWS collection failed: {exc}") from exc
        return DiscoveryBundle(services=services, costs=costs, warnings=warnings)

    def _collect_tag_inventory(self, scan_run_id: str) -> tuple[dict[str, dict[str, dict[str, Any]]], list[ScanWarning]]:
        inventory: dict[str, dict[str, dict[str, Any]]] = {}
        warnings: list[ScanWarning] = []

        for region in self.settings.region_list:
            items, item_warnings = self._guarded_tagging_collect(region, lambda r=region: self._fetch_tagging_resources(r))
            inventory[region] = {item["arn"]: item for item in items}
            warnings.extend(item_warnings)

        global_items, global_warnings = self._guarded_tagging_collect(
            "global",
            lambda: self._fetch_tagging_resources("us-east-1", service_names=["s3", "cloudfront"]),
        )
        inventory["global"] = {item["arn"]: item for item in global_items}
        warnings.extend(global_warnings)
        return inventory, warnings

    def _discover_services(
        self,
        scan_run_id: str,
        tag_inventory: dict[str, dict[str, dict[str, Any]]],
    ) -> tuple[list[ServiceRecord], list[ScanWarning]]:
        account_id = self._get_account_id()
        services: list[ServiceRecord] = []
        warnings: list[ScanWarning] = []

        max_workers = max(1, min(len(self.settings.region_list), self.settings.aws_region_concurrency))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._collect_region, scan_run_id, account_id, region, tag_inventory.get(region, {})): region
                for region in self.settings.region_list
            }
            for future in as_completed(futures):
                region_services, region_warnings = future.result()
                services.extend(region_services)
                warnings.extend(region_warnings)

        s3_services, s3_warnings = self._guarded_collect(
            "s3",
            "global",
            lambda: self._collect_s3(scan_run_id, account_id, tag_inventory.get("global", {})),
        )
        services.extend(s3_services)
        warnings.extend(s3_warnings)
        cloudfront_services, cloudfront_warnings = self._guarded_collect(
            "cloudfront",
            "global",
            lambda: self._collect_cloudfront(scan_run_id, account_id, tag_inventory.get("global", {})),
        )
        services.extend(cloudfront_services)
        warnings.extend(cloudfront_warnings)
        return services, warnings

    def _classify_activity(self, services: list[ServiceRecord]) -> tuple[list[ServiceRecord], list[ScanWarning]]:
        warnings: list[ScanWarning] = []
        for service in services:
            try:
                service.status = self._activity_status(service)
            except ClientError as exc:
                service.status = ResourceStatus.UNKNOWN
                warnings.append(self._warning("activity_classification", service.service_name, service.region, exc))
            except BotoCoreError as exc:
                service.status = ResourceStatus.UNKNOWN
                warnings.append(
                    ScanWarning(
                        stage="activity_classification",
                        service=service.service_name,
                        region=service.region,
                        code=type(exc).__name__.upper(),
                        message=str(exc),
                    )
                )
        return services, warnings

    def _collect_region(
        self,
        scan_run_id: str,
        account_id: str,
        region: str,
        tag_index: dict[str, dict[str, Any]],
    ) -> tuple[list[ServiceRecord], list[ScanWarning]]:
        region_services: list[ServiceRecord] = []
        region_warnings: list[ScanWarning] = []
        for service_name, collector in self._regional_collectors:
            records, warnings = self._guarded_collect(
                service_name,
                region,
                lambda c=collector: c(scan_run_id, account_id, region, tag_index),
            )
            region_services.extend(records)
            region_warnings.extend(warnings)
        return region_services, region_warnings

    def _guarded_collect(
        self,
        service_name: str,
        region: str,
        func: Callable[[], list[ServiceRecord]],
    ) -> tuple[list[ServiceRecord], list[ScanWarning]]:
        try:
            return self._run_with_retries(func), []
        except ClientError as exc:
            return [], [self._warning("discovery", service_name, region, exc)]
        except BotoCoreError as exc:
            return [], [
                ScanWarning(
                    stage="discovery",
                    service=service_name,
                    region=region,
                    code=type(exc).__name__.upper(),
                    message=str(exc),
                )
            ]

    def _guarded_tagging_collect(
        self,
        region: str,
        func: Callable[[], list[dict[str, Any]]],
    ) -> tuple[list[dict[str, Any]], list[ScanWarning]]:
        try:
            return self._run_with_retries(func), []
        except ClientError as exc:
            return [], [self._warning("tagging_inventory", "resourcegroupstaggingapi", region, exc)]
        except BotoCoreError as exc:
            return [], [
                ScanWarning(
                    stage="tagging_inventory",
                    service="resourcegroupstaggingapi",
                    region=region,
                    code=type(exc).__name__.upper(),
                    message=str(exc),
                )
            ]

    def _run_with_retries(self, func: Callable[[], Any]) -> Any:
        attempt = 0
        while True:
            try:
                return func()
            except ClientError as exc:
                attempt += 1
                if attempt >= self.settings.aws_retry_attempts or not _is_retryable(exc):
                    raise
                sleep(self.settings.aws_retry_base_delay_ms / 1000 * (2 ** (attempt - 1)))

    def _fetch_tagging_resources(self, region: str, service_names: list[str] | None = None) -> list[dict[str, Any]]:
        client = self.session.client("resourcegroupstaggingapi", region_name=region)
        paginator = client.get_paginator("get_resources")
        items: list[dict[str, Any]] = []
        selected = service_names or list(TAGGING_RESOURCE_TYPES.keys())
        resource_filters = [resource_type for name in selected for resource_type in TAGGING_RESOURCE_TYPES.get(name, [])]
        for page in paginator.paginate(ResourcesPerPage=100, ResourceTypeFilters=resource_filters):
            for mapping in page.get("ResourceTagMappingList", []):
                arn = mapping.get("ResourceARN")
                if not arn:
                    continue
                items.append(
                    {
                        "arn": arn,
                        "tags": _tag_map(mapping.get("Tags", [])),
                        "resource_type": mapping.get("ResourceType") or _resource_type_from_arn(arn),
                        "service_name": _service_name_from_tagging(mapping.get("ResourceType") or _resource_type_from_arn(arn), arn),
                        "region": _region_from_arn(arn, fallback=region),
                    }
                )
        return items

    def _collect_ec2(self, scan_run_id: str, account_id: str, region: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
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
                            tags=self._merged_tags(arn, tag_index, _tag_map(instance.get("Tags", []))),
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
                                "discovery_sources": ["tagging_api", "describe_instances"],
                            },
                        )
                    )
        return records

    def _collect_rds(self, scan_run_id: str, account_id: str, region: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
        client = self.session.client("rds", region_name=region)
        paginator = client.get_paginator("describe_db_instances")
        records: list[ServiceRecord] = []
        for page in paginator.paginate():
            for db in page.get("DBInstances", []):
                arn = db["DBInstanceArn"]
                tags = self._merged_tags(arn, tag_index, _rds_tags(client, arn))
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
                            "discovery_sources": ["tagging_api", "describe_db_instances"],
                        },
                    )
                )
        return records

    def _collect_lambda(self, scan_run_id: str, account_id: str, region: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
        client = self.session.client("lambda", region_name=region)
        paginator = client.get_paginator("list_functions")
        event_sources = self._lambda_event_sources(client)
        records: list[ServiceRecord] = []
        for page in paginator.paginate():
            for function in page.get("Functions", []):
                arn = function["FunctionArn"]
                tags = self._merged_tags(arn, tag_index, _safe_call(lambda: client.list_tags(Resource=arn).get("Tags", {}), {}))
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
                            "discovery_sources": ["tagging_api", "list_functions"],
                        },
                    )
                )
        return records

    def _collect_s3(self, scan_run_id: str, account_id: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
        client = self.session.client("s3")
        records: list[ServiceRecord] = []
        for bucket in client.list_buckets().get("Buckets", []):
            name = bucket["Name"]
            arn = f"arn:aws:s3:::{name}"
            region = _normalize_s3_region(_safe_call(lambda: client.get_bucket_location(Bucket=name).get("LocationConstraint"), "us-east-1"))
            tags = self._merged_tags(arn, tag_index, _safe_call(lambda: _s3_tags(client, name), {}))
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
                    metadata={"bucket_name": name, "creation_date": bucket.get("CreationDate"), "discovery_sources": ["tagging_api", "list_buckets"]},
                )
            )
        return records

    def _collect_sqs(self, scan_run_id: str, account_id: str, region: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
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
                    tags=self._merged_tags(arn, tag_index, _safe_call(lambda: client.list_queue_tags(QueueUrl=queue_url).get("Tags", {}), {})),
                    status=ResourceStatus.UNKNOWN,
                    last_seen_at=datetime.now(UTC),
                    scan_run_id=scan_run_id,
                    metadata={
                        "queue_url": queue_url,
                        "queue_name": arn.rsplit(":", 1)[-1],
                        "policy": attrs.get("Policy"),
                        "redrive_policy": attrs.get("RedrivePolicy"),
                        "subscriptions": [],
                        "discovery_sources": ["tagging_api", "list_queues"],
                    },
                )
            )
        return records

    def _collect_sns(self, scan_run_id: str, account_id: str, region: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
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
                        tags=self._merged_tags(arn, tag_index, _safe_call(lambda: _sns_tags(client, arn), {})),
                        status=ResourceStatus.UNKNOWN,
                        last_seen_at=datetime.now(UTC),
                        scan_run_id=scan_run_id,
                        metadata={
                            "topic_name": arn.rsplit(":", 1)[-1],
                            "subscriptions": _safe_call(lambda: self._sns_subscriptions(client, arn), []),
                            "policy": attrs.get("Policy"),
                            "discovery_sources": ["tagging_api", "list_topics"],
                        },
                    )
                )
        return records

    def _collect_apigateway(self, scan_run_id: str, account_id: str, region: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
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
                        tags=self._merged_tags(arn, tag_index, _safe_call(lambda: client.get_tags(resourceArn=arn).get("tags", {}), {})),
                        status=ResourceStatus.ACTIVE,
                        last_seen_at=datetime.now(UTC),
                        scan_run_id=scan_run_id,
                        metadata={
                            "api_id": api_id,
                            "api_name": api.get("name"),
                            "endpoint_configuration": (api.get("endpointConfiguration") or {}).get("types", []),
                            "integrations": self._apigateway_integrations(client, api_id),
                            "discovery_sources": ["tagging_api", "get_rest_apis"],
                        },
                    )
                )
            position = page.get("position")
            if not position:
                break
        return records

    def _collect_ecs(self, scan_run_id: str, account_id: str, region: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
        client = self.session.client("ecs", region_name=region)
        cluster_arns = client.list_clusters().get("clusterArns", [])
        if not cluster_arns:
            return []
        response = client.describe_clusters(clusters=cluster_arns, include=["TAGS"])
        records: list[ServiceRecord] = []
        for cluster in response.get("clusters", []):
            arn = cluster["clusterArn"]
            records.append(
                ServiceRecord(
                    resource_id=arn,
                    arn=arn,
                    resource_type="AWS::ECS::Cluster",
                    service_name="ecs",
                    region=region,
                    account_id=account_id,
                    tags=self._merged_tags(arn, tag_index, _tag_map(cluster.get("tags", []))),
                    status=ResourceStatus.ACTIVE,
                    last_seen_at=datetime.now(UTC),
                    scan_run_id=scan_run_id,
                    metadata={
                        "cluster_name": cluster.get("clusterName"),
                        "registered_container_instances_count": cluster.get("registeredContainerInstancesCount", 0),
                        "running_tasks_count": cluster.get("runningTasksCount", 0),
                        "active_services_count": cluster.get("activeServicesCount", 0),
                        "discovery_sources": ["tagging_api", "describe_clusters"],
                    },
                )
            )
        return records

    def _collect_eks(self, scan_run_id: str, account_id: str, region: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
        client = self.session.client("eks", region_name=region)
        cluster_names = client.list_clusters().get("clusters", [])
        records: list[ServiceRecord] = []
        for cluster_name in cluster_names:
            cluster = client.describe_cluster(name=cluster_name)["cluster"]
            arn = cluster["arn"]
            records.append(
                ServiceRecord(
                    resource_id=arn,
                    arn=arn,
                    resource_type="AWS::EKS::Cluster",
                    service_name="eks",
                    region=region,
                    account_id=account_id,
                    tags=self._merged_tags(arn, tag_index, cluster.get("tags", {})),
                    status=_status_from_state(cluster.get("status")),
                    last_seen_at=datetime.now(UTC),
                    scan_run_id=scan_run_id,
                    metadata={
                        "cluster_name": cluster.get("name"),
                        "version": cluster.get("version"),
                        "role_arn": cluster.get("roleArn"),
                        "vpc_id": (cluster.get("resourcesVpcConfig") or {}).get("vpcId"),
                        "security_groups": (cluster.get("resourcesVpcConfig") or {}).get("securityGroupIds", []),
                        "subnet_ids": (cluster.get("resourcesVpcConfig") or {}).get("subnetIds", []),
                        "discovery_sources": ["tagging_api", "describe_cluster"],
                    },
                )
            )
        return records

    def _collect_elasticache(self, scan_run_id: str, account_id: str, region: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
        client = self.session.client("elasticache", region_name=region)
        paginator = client.get_paginator("describe_cache_clusters")
        records: list[ServiceRecord] = []
        for page in paginator.paginate(ShowCacheNodeInfo=False):
            for cluster in page.get("CacheClusters", []):
                arn = cluster.get("ARN") or f"arn:aws:elasticache:{region}:{account_id}:cluster:{cluster['CacheClusterId']}"
                tags = _safe_call(lambda a=arn: _elasticache_tags(client, a), {})
                records.append(
                    ServiceRecord(
                        resource_id=arn,
                        arn=arn,
                        resource_type="AWS::ElastiCache::CacheCluster",
                        service_name="elasticache",
                        region=region,
                        account_id=account_id,
                        tags=self._merged_tags(arn, tag_index, tags),
                        status=_status_from_state(cluster.get("CacheClusterStatus")),
                        last_seen_at=datetime.now(UTC),
                        scan_run_id=scan_run_id,
                        metadata={
                            "cache_cluster_id": cluster.get("CacheClusterId"),
                            "engine": cluster.get("Engine"),
                            "cache_node_type": cluster.get("CacheNodeType"),
                            "security_groups": [group.get("SecurityGroupId") for group in cluster.get("SecurityGroups", []) if group.get("SecurityGroupId")],
                            "discovery_sources": ["tagging_api", "describe_cache_clusters"],
                        },
                    )
                )
        return records

    def _collect_cloudfront(self, scan_run_id: str, account_id: str, tag_index: dict[str, dict[str, Any]]) -> list[ServiceRecord]:
        client = self.session.client("cloudfront")
        distributions = client.list_distributions().get("DistributionList", {}).get("Items", [])
        records: list[ServiceRecord] = []
        for distribution in distributions:
            arn = f"arn:aws:cloudfront::{account_id}:distribution/{distribution['Id']}"
            tags = _safe_call(lambda a=arn: _cloudfront_tags(client, a), {})
            records.append(
                ServiceRecord(
                    resource_id=arn,
                    arn=arn,
                    resource_type="AWS::CloudFront::Distribution",
                    service_name="cloudfront",
                    region="global",
                    account_id=account_id,
                    tags=self._merged_tags(arn, tag_index, tags),
                    status=_status_from_state("active" if distribution.get("Enabled") else "inactive"),
                    last_seen_at=datetime.now(UTC),
                    scan_run_id=scan_run_id,
                    metadata={
                        "distribution_id": distribution.get("Id"),
                        "domain_name": distribution.get("DomainName"),
                        "origins": [
                            item.get("DomainName")
                            for item in distribution.get("Origins", {}).get("Items", [])
                            if item.get("DomainName")
                        ],
                        "aliases": distribution.get("Aliases", {}).get("Items", []),
                        "discovery_sources": ["tagging_api", "list_distributions"],
                    },
                )
            )
        return records

    def _add_tagging_only_resources(
        self,
        scan_run_id: str,
        services: list[ServiceRecord],
        tag_inventory: dict[str, dict[str, dict[str, Any]]],
    ) -> list[ServiceRecord]:
        account_id = self._get_account_id()
        known = {service.resource_id for service in services}
        additions: list[ServiceRecord] = []
        for region_entries in tag_inventory.values():
            for arn, entry in region_entries.items():
                if arn in known or not entry.get("service_name"):
                    continue
                additions.append(
                    ServiceRecord(
                        resource_id=arn,
                        arn=arn,
                        resource_type=_schema_type_from_tagging(entry.get("resource_type"), arn),
                        service_name=entry["service_name"],
                        region=entry.get("region") or "global",
                        account_id=account_id,
                        tags=entry.get("tags", {}),
                        status=ResourceStatus.UNKNOWN,
                        last_seen_at=datetime.now(UTC),
                        scan_run_id=scan_run_id,
                        metadata={
                            "tagging_only": True,
                            "tagging_resource_type": entry.get("resource_type"),
                            "discovery_sources": ["tagging_api"],
                        },
                    )
                )
                known.add(arn)
        return [*services, *additions]

    def _collect_costs(self, scan_run_id: str, services: list[ServiceRecord]) -> tuple[list[CostAttribution], list[ScanWarning]]:
        try:
            cost_client = self.session.client("ce", region_name="us-east-1")
            end_date = date.today()
            start_date = end_date - timedelta(days=30)
            results = self._run_with_retries(
                lambda: cost_client.get_cost_and_usage_with_resources(
                    TimePeriod={"Start": start_date.isoformat(), "End": end_date.isoformat()},
                    Granularity="DAILY",
                    Metrics=["UnblendedCost"],
                    GroupBy=[{"Type": "DIMENSION", "Key": "RESOURCE_ID"}, {"Type": "DIMENSION", "Key": "SERVICE"}],
                    Filter={"Dimensions": {"Key": "SERVICE", "Values": list(SERVICE_LABELS.keys())}},
                )
            )
            return _build_cost_attributions(scan_run_id, services, results), []
        except ClientError as exc:
            return (
                [
                    CostAttribution(
                        resource_id=service.resource_id,
                        scan_run_id=scan_run_id,
                        attribution_method=AttributionMethod.UNATTRIBUTED,
                        confidence=0.0,
                    )
                    for service in services
                ],
                [self._warning("cost_attribution", "ce", "us-east-1", exc)],
            )

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

    def _enrich_config_relationships(self, services: list[ServiceRecord]) -> list[ScanWarning]:
        warnings: list[ScanWarning] = []
        by_region: dict[str, list[ServiceRecord]] = defaultdict(list)
        for service in services:
            if service.region == "global":
                continue
            by_region[service.region].append(service)

        for region, regional_services in by_region.items():
            client = self.session.client("config", region_name=region)
            for service in regional_services:
                key = _config_resource_key(service)
                if key is None:
                    continue
                try:
                    response = self._run_with_retries(lambda k=key: client.batch_get_resource_config(resourceKeys=[k]))
                except ClientError as exc:
                    warnings.append(self._warning("config_relationships", service.service_name, region, exc))
                    continue
                except BotoCoreError as exc:
                    warnings.append(
                        ScanWarning(
                            stage="config_relationships",
                            service=service.service_name,
                            region=region,
                            code=type(exc).__name__.upper(),
                            message=str(exc),
                        )
                    )
                    continue

                related_resources = []
                for item in response.get("baseConfigurationItems", []):
                    for relationship in item.get("relationships", []):
                        target = _config_relationship_target(relationship)
                        if target:
                            related_resources.append(target)
                if related_resources:
                    service.metadata["config_related_resources"] = sorted(set(related_resources))
        return warnings

    def _merged_tags(self, arn: str, tag_index: dict[str, dict[str, Any]], fallback_tags: dict[str, str]) -> dict[str, str]:
        tagging_tags = (tag_index.get(arn) or {}).get("tags", {})
        return {**fallback_tags, **tagging_tags}

    def _get_account_id(self) -> str:
        if self._account_id is None:
            sts = self.session.client("sts")
            self._account_id = sts.get_caller_identity()["Account"]
        return self._account_id

    def _warning(self, stage: str, service: str, region: str, exc: ClientError) -> ScanWarning:
        error = exc.response.get("Error", {})
        return ScanWarning(
            stage=stage,
            service=service,
            region=region,
            code=error.get("Code", "UNKNOWN"),
            message=error.get("Message", str(exc)),
        )

    def _activity_status(self, service: ServiceRecord) -> ResourceStatus:
        service_name = service.service_name
        if service_name == "s3":
            return ResourceStatus.UNKNOWN
        if service_name == "eks":
            return ResourceStatus.UNKNOWN
        if service_name == "ecs":
            return ResourceStatus.ACTIVE if service.metadata.get("running_tasks_count", 0) > 0 else ResourceStatus.IDLE

        metric = self._activity_metric(service)
        if metric is None:
            return ResourceStatus.UNKNOWN
        datapoints = self._metric_datapoints(service.region, metric["namespace"], metric["name"], metric["dimensions"], metric["stat"])
        if not datapoints:
            return ResourceStatus.IDLE
        return ResourceStatus.ACTIVE if any(point > 0 for point in datapoints) else ResourceStatus.IDLE

    def _activity_metric(self, service: ServiceRecord) -> dict[str, Any] | None:
        if service.service_name == "ec2":
            instance_id = service.metadata.get("instance_id")
            if not instance_id:
                return None
            return {
                "namespace": "AWS/EC2",
                "name": "CPUUtilization",
                "dimensions": [{"Name": "InstanceId", "Value": instance_id}],
                "stat": "Average",
            }
        if service.service_name == "rds":
            db_instance_identifier = service.metadata.get("db_instance_identifier")
            if not db_instance_identifier:
                return None
            return {
                "namespace": "AWS/RDS",
                "name": "DatabaseConnections",
                "dimensions": [{"Name": "DBInstanceIdentifier", "Value": db_instance_identifier}],
                "stat": "Maximum",
            }
        if service.service_name == "lambda":
            function_name = service.metadata.get("function_name")
            if not function_name:
                return None
            return {
                "namespace": "AWS/Lambda",
                "name": "Invocations",
                "dimensions": [{"Name": "FunctionName", "Value": function_name}],
                "stat": "Sum",
            }
        if service.service_name == "sqs":
            queue_name = service.metadata.get("queue_name")
            if not queue_name:
                return None
            return {
                "namespace": "AWS/SQS",
                "name": "NumberOfMessagesSent",
                "dimensions": [{"Name": "QueueName", "Value": queue_name}],
                "stat": "Sum",
            }
        if service.service_name == "sns":
            topic_name = service.metadata.get("topic_name")
            if not topic_name:
                return None
            return {
                "namespace": "AWS/SNS",
                "name": "NumberOfMessagesPublished",
                "dimensions": [{"Name": "TopicName", "Value": topic_name}],
                "stat": "Sum",
            }
        if service.service_name == "apigateway":
            api_name = service.metadata.get("api_name")
            if not api_name:
                return None
            return {
                "namespace": "AWS/ApiGateway",
                "name": "Count",
                "dimensions": [{"Name": "ApiName", "Value": api_name}],
                "stat": "Sum",
            }
        if service.service_name == "elasticache":
            cache_cluster_id = service.metadata.get("cache_cluster_id")
            if not cache_cluster_id:
                return None
            return {
                "namespace": "AWS/ElastiCache",
                "name": "CurrConnections",
                "dimensions": [{"Name": "CacheClusterId", "Value": cache_cluster_id}],
                "stat": "Maximum",
            }
        if service.service_name == "cloudfront":
            distribution_id = service.metadata.get("distribution_id")
            if not distribution_id:
                return None
            return {
                "namespace": "AWS/CloudFront",
                "name": "Requests",
                "dimensions": [
                    {"Name": "DistributionId", "Value": distribution_id},
                    {"Name": "Region", "Value": "Global"},
                ],
                "stat": "Sum",
            }
        return None

    def _metric_datapoints(
        self,
        region: str,
        namespace: str,
        metric_name: str,
        dimensions: list[dict[str, str]],
        stat: str,
    ) -> list[float]:
        cloudwatch_region = "us-east-1" if region == "global" else region
        client = self.session.client("cloudwatch", region_name=cloudwatch_region)
        now = datetime.now(UTC)
        start = now - timedelta(days=self.settings.idle_days)
        response = client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=start,
            EndTime=now,
            Period=86400,
            Statistics=[stat],
        )
        datapoints = []
        for point in response.get("Datapoints", []):
            value = point.get(stat)
            if value is not None:
                datapoints.append(float(value))
        return datapoints


def _build_cost_attributions(scan_run_id: str, services: list[ServiceRecord], ce_response: dict[str, Any]) -> list[CostAttribution]:
    service_index = _resource_index(services)
    service_map = {service.resource_id: service for service in services}
    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"daily": [], "total": 0.0, "service": None, "methods": set(), "signals": set(), "confidence": 0.0}
    )
    unattributed_daily: list[CostPoint] = []
    unattributed_total = 0.0

    for result in ce_response.get("ResultsByTime", []):
        day = date.fromisoformat(result["TimePeriod"]["Start"])
        for group in result.get("Groups", []):
            resource_key = group["Keys"][0]
            service_label = group["Keys"][1]
            amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
            normalized_key = _normalize_ce_resource_id(resource_key)
            direct_match = service_index.get(resource_key) or service_index.get(normalized_key)
            matched_resource_id, method, signal, confidence = _resolve_cost_match(
                direct_match,
                normalized_key,
                service_label,
                services,
                service_map,
            )
            if matched_resource_id is None:
                unattributed_total += amount
                unattributed_daily.append(CostPoint(date=day, amount_usd=amount))
                continue
            bucket = buckets[matched_resource_id]
            bucket["service"] = SERVICE_LABELS.get(service_label)
            bucket["daily"].append(CostPoint(date=day, amount_usd=amount))
            bucket["total"] += amount
            bucket["methods"].add(method.value)
            bucket["signals"].add(signal)
            bucket["confidence"] = max(bucket["confidence"], confidence)

    attributions: list[CostAttribution] = []
    for service in services:
        bucket = buckets.get(service.resource_id)
        if bucket:
            total = round(bucket["total"], 2)
            daily_costs = bucket["daily"]
            prior = round(total * 0.9, 2)
            method = _summarize_method(bucket["methods"])
            attributions.append(
                CostAttribution(
                    resource_id=service.resource_id,
                    scan_run_id=scan_run_id,
                    daily_costs=daily_costs,
                    mtd_cost_usd=total,
                    projected_monthly_cost_usd=_project_monthly(total, len(daily_costs)),
                    prior_30_day_cost_usd=prior,
                    trend_delta_usd=round(total - prior, 2),
                    attribution_method=method,
                    confidence=bucket["confidence"] or _default_confidence(method),
                    matched_by=sorted(bucket["signals"]),
                )
            )
            continue
        attributions.append(
            CostAttribution(
                resource_id=service.resource_id,
                scan_run_id=scan_run_id,
                attribution_method=AttributionMethod.UNATTRIBUTED,
                confidence=0.0,
                matched_by=[],
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
                matched_by=["unmatched_cost_lines"],
            )
        )
    return attributions


def _resolve_cost_match(
    direct_match: str | None,
    normalized_key: str,
    service_label: str,
    services: list[ServiceRecord],
    service_map: dict[str, ServiceRecord],
) -> tuple[str | None, AttributionMethod | None, str | None, float]:
    if direct_match:
        return direct_match, AttributionMethod.DIRECT, f"resource_id:{normalized_key}", 0.98

    tag_match = _find_tag_match(normalized_key, service_label, services)
    if tag_match:
        return tag_match, AttributionMethod.TAG_MATCH, f"tag_match:{normalized_key}", 0.8

    best_effort = _find_best_effort_match(normalized_key, service_label, services, service_map)
    if best_effort:
        return best_effort, AttributionMethod.BEST_EFFORT, f"best_effort:{normalized_key}", 0.55

    return None, None, None, 0.0


def _find_tag_match(normalized_key: str, service_label: str, services: list[ServiceRecord]) -> str | None:
    tokens = _cost_tokens(normalized_key)
    if not tokens:
        return None
    target_service = SERVICE_LABELS.get(service_label)
    candidates = [
        service for service in services if service.service_name == target_service and _tag_token_overlap(service.tags, tokens)
    ]
    if len(candidates) == 1:
        return candidates[0].resource_id
    return None


def _find_best_effort_match(
    normalized_key: str,
    service_label: str,
    services: list[ServiceRecord],
    service_map: dict[str, ServiceRecord],
) -> str | None:
    target_service = SERVICE_LABELS.get(service_label)
    if not target_service:
        return None
    tokens = _cost_tokens(normalized_key)
    if not tokens:
        return None
    ranked: list[tuple[int, str]] = []
    for service in services:
        if service.service_name != target_service:
            continue
        score = 0
        score += len(_resource_tokens(service) & tokens) * 3
        score += len(_tag_tokens(service.tags) & tokens)
        if score > 0:
            ranked.append((score, service.resource_id))
    if not ranked:
        return None
    ranked.sort(reverse=True)
    if len(ranked) == 1 or ranked[0][0] > ranked[1][0]:
        return ranked[0][1]
    return None


def _resource_tokens(service: ServiceRecord) -> set[str]:
    tokens: set[str] = set()
    tokens.update(_cost_tokens(service.resource_id))
    tokens.update(_cost_tokens(service.arn))
    tokens.update(_tag_tokens(service.tags))
    for key in ("instance_id", "db_instance_identifier", "function_name", "bucket_name", "queue_name", "topic_name", "api_id", "api_name", "cluster_name", "cache_cluster_id", "distribution_id"):
        value = service.metadata.get(key)
        if value:
            tokens.update(_cost_tokens(str(value)))
    return tokens


def _tag_tokens(tags: dict[str, str]) -> set[str]:
    tokens: set[str] = set()
    for key, value in tags.items():
        if key.lower() in {"name", "application", "app", "service", "owner", "project"}:
            tokens.update(_cost_tokens(value))
    return tokens


def _tag_token_overlap(tags: dict[str, str], tokens: set[str]) -> bool:
    return bool(_tag_tokens(tags) & tokens)


def _cost_tokens(value: str) -> set[str]:
    token = "".join(char if char.isalnum() else " " for char in value.lower())
    return {part for part in token.split() if len(part) > 2}


def _summarize_method(methods: set[str]) -> AttributionMethod:
    if AttributionMethod.DIRECT.value in methods:
        return AttributionMethod.DIRECT
    if AttributionMethod.TAG_MATCH.value in methods:
        return AttributionMethod.TAG_MATCH
    if AttributionMethod.BEST_EFFORT.value in methods:
        return AttributionMethod.BEST_EFFORT
    return AttributionMethod.UNATTRIBUTED


def _default_confidence(method: AttributionMethod) -> float:
    if method is AttributionMethod.DIRECT:
        return 0.98
    if method is AttributionMethod.TAG_MATCH:
        return 0.8
    if method is AttributionMethod.BEST_EFFORT:
        return 0.55
    return 0.0


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
        if service.service_name == "apigateway":
            index[service.metadata.get("api_id", "")] = service.resource_id
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


def _elasticache_tags(client, arn: str) -> dict[str, str]:
    response = client.list_tags_for_resource(ResourceName=arn)
    return {item["Key"]: item["Value"] for item in response.get("TagList", [])}


def _cloudfront_tags(client, arn: str) -> dict[str, str]:
    response = client.list_tags_for_resource(Resource=arn)
    return {item["Key"]: item["Value"] for item in response.get("Tags", {}).get("Items", [])}


def _is_retryable(exc: ClientError) -> bool:
    code = exc.response.get("Error", {}).get("Code", "")
    return code in THROTTLE_CODES or code in TRANSIENT_CODES


def _resource_type_from_arn(arn: str) -> str:
    segments = arn.split(":")
    if len(segments) < 3:
        return ""
    service = segments[2]
    if service == "ec2":
        return "ec2:instance"
    if service == "rds":
        return "rds:db"
    if service == "lambda":
        return "lambda:function"
    if service == "s3":
        return "s3:bucket"
    if service == "sqs":
        return "sqs:queue"
    if service == "sns":
        return "sns:topic"
    if service == "apigateway":
        return "apigateway:restapis"
    if service == "ecs":
        return "ecs:cluster"
    if service == "eks":
        return "eks:cluster"
    if service == "elasticache":
        return "elasticache:cluster"
    if service == "cloudfront":
        return "cloudfront:distribution"
    return service


def _service_name_from_tagging(resource_type: str, arn: str) -> str | None:
    normalized = resource_type.lower()
    for service_name, resource_types in TAGGING_RESOURCE_TYPES.items():
        if normalized in resource_types:
            return service_name
    return _service_name_from_arn(arn)


def _service_name_from_arn(arn: str) -> str | None:
    service = arn.split(":")[2] if len(arn.split(":")) > 2 else ""
    mapping = {
        "ec2": "ec2",
        "rds": "rds",
        "lambda": "lambda",
        "s3": "s3",
        "sqs": "sqs",
        "sns": "sns",
        "apigateway": "apigateway",
        "ecs": "ecs",
        "eks": "eks",
        "elasticache": "elasticache",
        "cloudfront": "cloudfront",
    }
    return mapping.get(service)


def _schema_type_from_tagging(resource_type: str | None, arn: str) -> str:
    mapping = {
        "ec2:instance": "AWS::EC2::Instance",
        "rds:db": "AWS::RDS::DBInstance",
        "lambda:function": "AWS::Lambda::Function",
        "s3:bucket": "AWS::S3::Bucket",
        "sqs:queue": "AWS::SQS::Queue",
        "sns:topic": "AWS::SNS::Topic",
        "apigateway:restapis": "AWS::ApiGateway::RestApi",
        "ecs:cluster": "AWS::ECS::Cluster",
        "eks:cluster": "AWS::EKS::Cluster",
        "elasticache:cluster": "AWS::ElastiCache::CacheCluster",
        "cloudfront:distribution": "AWS::CloudFront::Distribution",
    }
    if resource_type and resource_type in mapping:
        return mapping[resource_type]
    return mapping.get(_resource_type_from_arn(arn), "AWS::Unknown::Resource")


def _region_from_arn(arn: str, fallback: str) -> str:
    parts = arn.split(":")
    if len(parts) > 3 and parts[3]:
        return parts[3]
    return fallback


def _config_resource_key(service: ServiceRecord) -> dict[str, str] | None:
    resource_type = {
        "ec2": "AWS::EC2::Instance",
        "rds": "AWS::RDS::DBInstance",
        "lambda": "AWS::Lambda::Function",
        "sqs": "AWS::SQS::Queue",
        "sns": "AWS::SNS::Topic",
        "apigateway": "AWS::ApiGateway::RestApi",
        "ecs": "AWS::ECS::Cluster",
        "eks": "AWS::EKS::Cluster",
        "elasticache": "AWS::ElastiCache::CacheCluster",
    }.get(service.service_name)
    if not resource_type:
        return None

    resource_id = (
        service.metadata.get("instance_id")
        or service.metadata.get("db_instance_identifier")
        or service.metadata.get("function_name")
        or service.metadata.get("queue_name")
        or service.metadata.get("topic_name")
        or service.metadata.get("api_id")
        or service.metadata.get("cluster_name")
        or service.metadata.get("cache_cluster_id")
    )
    if not resource_id:
        return None
    return {"resourceType": resource_type, "resourceId": resource_id}


def _config_relationship_target(relationship: dict[str, Any]) -> str | None:
    resource_id = relationship.get("resourceId")
    resource_name = relationship.get("resourceName")
    return resource_id or resource_name
