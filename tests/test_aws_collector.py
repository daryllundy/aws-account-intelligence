from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from botocore.exceptions import ClientError

from aws_account_intelligence.collectors.aws import AwsCollector
from aws_account_intelligence.config import Settings


class FakePaginator:
    def __init__(self, pages, failure=None):
        self.pages = pages
        self.failure = failure

    def paginate(self, **kwargs):
        if self.failure:
            raise self.failure
        return iter(self.pages)


class FakeStsClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        pass

    def get_caller_identity(self):
        return {"Account": "123456789012"}


class FakeTaggingClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name
        self.failures = failures if failures is not None else {}

    def get_paginator(self, name):
        assert name == "get_resources"
        failure = self.failures.get((self.region_name, "tagging"))
        if failure:
            return FakePaginator([], failure=failure)
        return FakePaginator([{"ResourceTagMappingList": _tagging_items_for_region(self.region_name)}])


class FakeEc2Client:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name
        self.counters = counters if counters is not None else defaultdict(int)
        self.failures = failures if failures is not None else {}

    def get_paginator(self, name):
        assert name == "describe_instances"
        failure = self.failures.get((self.region_name, "ec2"))
        if failure:
            return FakePaginator([], failure=failure)
        if self.failures.get((self.region_name, "ec2_throttle_once")) and self.counters[(self.region_name, "ec2")] == 0:
            self.counters[(self.region_name, "ec2")] += 1
            return FakePaginator([], failure=_client_error("ThrottlingException", "rate exceeded", "DescribeInstances"))
        self.counters[(self.region_name, "ec2")] += 1
        return FakePaginator([
            {
                "Reservations": [
                    {
                        "Instances": [
                            {
                                "InstanceId": f"i-{self.region_name}",
                                "State": {"Name": "running"},
                                "VpcId": "vpc-main",
                                "SubnetId": "subnet-app",
                                "SecurityGroups": [{"GroupId": "sg-web"}, {"GroupId": "sg-db-client"}],
                                "PrivateIpAddress": "10.0.1.15",
                                "Tags": [{"Key": "Environment", "Value": "runtime"}],
                            }
                        ]
                    }
                ]
            }
        ])


class FakeRdsClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def get_paginator(self, name):
        assert name == "describe_db_instances"
        return FakePaginator([
            {
                "DBInstances": [
                    {
                        "DBInstanceArn": f"arn:aws:rds:{self.region_name}:123456789012:db:orders-db-{self.region_name}",
                        "DBInstanceIdentifier": f"orders-db-{self.region_name}",
                        "DBInstanceStatus": "available",
                        "VpcSecurityGroups": [{"VpcSecurityGroupId": "sg-db"}],
                        "DBSubnetGroup": {"DBSubnetGroupName": "db-subnet", "VpcId": "vpc-main"},
                        "Engine": "postgres",
                        "Endpoint": {"Address": "orders-db.example.local"},
                    }
                ]
            }
        ])

    def list_tags_for_resource(self, ResourceName):
        return {"TagList": [{"Key": "Environment", "Value": "runtime"}, {"Key": "Critical", "Value": "true"}]}


class FakeLambdaClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def get_paginator(self, name):
        if name == "list_functions":
            return FakePaginator([
                {
                    "Functions": [
                        {
                            "FunctionArn": f"arn:aws:lambda:{self.region_name}:123456789012:function:process-orders-{self.region_name}",
                            "FunctionName": f"process-orders-{self.region_name}",
                            "Runtime": "python3.12",
                            "Role": "arn:aws:iam::123456789012:role/orders-lambda-role",
                            "State": "Active",
                            "VpcConfig": {"SecurityGroupIds": ["sg-web"], "SubnetIds": ["subnet-app"]},
                        }
                    ]
                }
            ])
        assert name == "list_event_source_mappings"
        return FakePaginator([
            {
                "EventSourceMappings": [
                    {
                        "FunctionArn": f"arn:aws:lambda:{self.region_name}:123456789012:function:process-orders-{self.region_name}",
                        "EventSourceArn": f"arn:aws:sqs:{self.region_name}:123456789012:orders-queue-{self.region_name}",
                    }
                ]
            }
        ])

    def list_tags(self, Resource):
        return {"Tags": {"Environment": "runtime", "Application": "orders"}}


class FakeS3Client:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        pass

    def list_buckets(self):
        return {"Buckets": [{"Name": "orders-artifacts", "CreationDate": datetime.now(UTC)}]}

    def get_bucket_location(self, Bucket):
        return {"LocationConstraint": "us-west-2"}

    def get_bucket_tagging(self, Bucket):
        return {"TagSet": [{"Key": "Environment", "Value": "runtime"}]}


class FakeSqsClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def list_queues(self):
        return {"QueueUrls": [f"https://sqs.{self.region_name}.amazonaws.com/123456789012/orders-queue-{self.region_name}"]}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        return {"Attributes": {"QueueArn": f"arn:aws:sqs:{self.region_name}:123456789012:orders-queue-{self.region_name}", "Policy": "{}", "RedrivePolicy": "{}"}}

    def list_queue_tags(self, QueueUrl):
        return {"Tags": {"Environment": "runtime"}}


class FakeSnsClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def get_topic_attributes(self, TopicArn):
        return {"Attributes": {"Policy": "{}"}}

    def list_tags_for_resource(self, ResourceArn):
        return {"Tags": [{"Key": "Environment", "Value": "runtime"}]}

    def get_paginator(self, name):
        if name == "list_topics":
            return FakePaginator([{"Topics": [{"TopicArn": f"arn:aws:sns:{self.region_name}:123456789012:orders-topic-{self.region_name}"}]}])
        assert name == "list_subscriptions_by_topic"
        return FakePaginator([{"Subscriptions": [{"Endpoint": f"arn:aws:sqs:{self.region_name}:123456789012:orders-queue-{self.region_name}"}]}])


class FakeApiGatewayClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def get_rest_apis(self, **kwargs):
        return {"items": [{"id": f"orders-api-{self.region_name}", "name": "orders-api", "endpointConfiguration": {"types": ["REGIONAL"]}}]}

    def get_tags(self, resourceArn):
        return {"tags": {"Environment": "runtime"}}

    def get_resources(self, restApiId, embed):
        return {"items": [{"id": "root", "resourceMethods": {"POST": {}}}]}

    def get_integration(self, restApiId, resourceId, httpMethod):
        return {"uri": f"arn:aws:apigateway:{self.region_name}:lambda:path/2015-03-31/functions/arn:aws:lambda:{self.region_name}:123456789012:function:process-orders-{self.region_name}/invocations"}


class FakeEcsClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def list_clusters(self):
        return {"clusterArns": [f"arn:aws:ecs:{self.region_name}:123456789012:cluster/orders-{self.region_name}"]}

    def describe_clusters(self, clusters, include):
        return {"clusters": [{"clusterArn": f"arn:aws:ecs:{self.region_name}:123456789012:cluster/orders-{self.region_name}", "clusterName": f"orders-{self.region_name}", "registeredContainerInstancesCount": 2, "runningTasksCount": 3, "activeServicesCount": 1, "tags": [{"Key": "Environment", "Value": "runtime"}]}]}


class FakeEksClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def list_clusters(self):
        return {"clusters": [f"orders-{self.region_name}"]}

    def describe_cluster(self, name):
        return {"cluster": {"arn": f"arn:aws:eks:{self.region_name}:123456789012:cluster/{name}", "name": name, "version": "1.31", "roleArn": "arn:aws:iam::123456789012:role/eks-cluster", "status": "ACTIVE", "tags": {"Environment": "runtime"}, "resourcesVpcConfig": {"vpcId": "vpc-main", "securityGroupIds": ["sg-eks"], "subnetIds": ["subnet-app", "subnet-data"]}}}


class FakeElastiCacheClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def get_paginator(self, name):
        assert name == "describe_cache_clusters"
        return FakePaginator([{"CacheClusters": [{"ARN": f"arn:aws:elasticache:{self.region_name}:123456789012:cluster:orders-cache-{self.region_name}", "CacheClusterId": f"orders-cache-{self.region_name}", "CacheClusterStatus": "available", "Engine": "redis", "CacheNodeType": "cache.t4g.small", "SecurityGroups": [{"SecurityGroupId": "sg-cache"}]}]}])

    def list_tags_for_resource(self, ResourceName):
        return {"TagList": [{"Key": "Environment", "Value": "runtime"}]}


class FakeCloudFrontClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        pass

    def list_distributions(self):
        return {"DistributionList": {"Items": [{"Id": "DIST123", "DomainName": "d111111abcdef8.cloudfront.net", "Enabled": True, "Origins": {"Items": [{"DomainName": "orders-artifacts.s3.amazonaws.com"}]}, "Aliases": {"Items": ["cdn.example.com"]}}]}}

    def list_tags_for_resource(self, Resource):
        return {"Tags": {"Items": [{"Key": "Environment", "Value": "runtime"}]}}


class FakeCloudWatchClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def get_metric_statistics(self, Namespace, MetricName, Dimensions, StartTime, EndTime, Period, Statistics):
        key = (Namespace, MetricName, tuple((item["Name"], item["Value"]) for item in Dimensions))
        datapoints = {
            ("AWS/EC2", "CPUUtilization", (("InstanceId", "i-us-west-2"),)): [{"Average": 3.0}],
            ("AWS/RDS", "DatabaseConnections", (("DBInstanceIdentifier", "orders-db-us-west-2"),)): [{"Maximum": 1.0}],
            ("AWS/Lambda", "Invocations", (("FunctionName", "process-orders-us-west-2"),)): [{"Sum": 12.0}],
            ("AWS/SQS", "NumberOfMessagesSent", (("QueueName", "orders-queue-us-west-2"),)): [{"Sum": 0.0}],
            ("AWS/SNS", "NumberOfMessagesPublished", (("TopicName", "orders-topic-us-west-2"),)): [{"Sum": 0.0}],
            ("AWS/ApiGateway", "Count", (("ApiName", "orders-api"),)): [{"Sum": 5.0}],
            ("AWS/ElastiCache", "CurrConnections", (("CacheClusterId", "orders-cache-us-west-2"),)): [{"Maximum": 0.0}],
            ("AWS/CloudFront", "Requests", (("DistributionId", "DIST123"), ("Region", "Global"))): [{"Sum": 7.0}],
        }
        return {"Datapoints": datapoints.get(key, [])}


class FakeCeClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.failures = failures if failures is not None else {}
        self.counters = counters if counters is not None else defaultdict(int)

    def get_cost_and_usage_with_resources(self, **kwargs):
        if self.failures.get(("global", "ce_throttle_once")) and self.counters[("global", "ce")] == 0:
            self.counters[("global", "ce")] += 1
            raise _client_error("ThrottlingException", "rate exceeded", "GetCostAndUsageWithResources")
        self.counters[("global", "ce")] += 1
        return {
            "ResultsByTime": [
                {
                    "TimePeriod": {"Start": "2026-03-01", "End": "2026-03-02"},
                    "Groups": [
                        {"Keys": ["i-us-west-2", "Amazon Elastic Compute Cloud - Compute"], "Metrics": {"UnblendedCost": {"Amount": "3.5"}}},
                        {"Keys": ["orders-db-us-west-2", "Amazon Relational Database Service"], "Metrics": {"UnblendedCost": {"Amount": "5.0"}}},
                        {"Keys": ["arn:aws:lambda:us-west-2:123456789012:function:process-orders-us-west-2", "AWS Lambda"], "Metrics": {"UnblendedCost": {"Amount": "0.9"}}},
                        {"Keys": ["arn:aws:s3:::orders-artifacts", "Amazon Simple Storage Service"], "Metrics": {"UnblendedCost": {"Amount": "0.6"}}},
                        {"Keys": ["arn:aws:sqs:us-west-2:123456789012:orders-queue-us-west-2", "Amazon Simple Queue Service"], "Metrics": {"UnblendedCost": {"Amount": "0.2"}}},
                        {"Keys": ["arn:aws:sns:us-west-2:123456789012:orders-topic-us-west-2", "Amazon Simple Notification Service"], "Metrics": {"UnblendedCost": {"Amount": "0.1"}}},
                        {"Keys": ["orders-api-us-west-2", "Amazon API Gateway"], "Metrics": {"UnblendedCost": {"Amount": "0.4"}}},
                        {"Keys": ["mystery-resource", "AWS Lambda"], "Metrics": {"UnblendedCost": {"Amount": "1.2"}}},
                    ],
                }
            ]
        }


class FakeSession:
    def __init__(self, failures=None):
        self.failures = failures or {}
        self.counters = defaultdict(int)

    def client(self, service_name, region_name=None):
        mapping = {
            "sts": FakeStsClient,
            "resourcegroupstaggingapi": FakeTaggingClient,
            "ec2": FakeEc2Client,
            "rds": FakeRdsClient,
            "lambda": FakeLambdaClient,
            "s3": FakeS3Client,
            "sqs": FakeSqsClient,
            "sns": FakeSnsClient,
            "apigateway": FakeApiGatewayClient,
            "ecs": FakeEcsClient,
            "eks": FakeEksClient,
            "elasticache": FakeElastiCacheClient,
            "cloudfront": FakeCloudFrontClient,
            "cloudwatch": FakeCloudWatchClient,
            "ce": FakeCeClient,
        }
        return mapping[service_name](region_name=region_name, counters=self.counters, failures=self.failures)


def test_aws_collector_uses_tagging_as_primary_inventory_source() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2")
    collector = AwsCollector(settings=settings, session=FakeSession())

    bundle = collector.load("scan-aws-1")

    service_names = {service.service_name for service in bundle.services}
    assert service_names == {"ec2", "rds", "lambda", "s3", "sqs", "sns", "apigateway", "ecs", "eks", "elasticache", "cloudfront"}

    ec2_service = next(service for service in bundle.services if service.service_name == "ec2")
    assert ec2_service.tags["Environment"] == "tagging"
    assert ec2_service.tags["Owner"] == "platform"
    assert "tagging_api" in ec2_service.metadata["discovery_sources"]

    lambda_service = next(service for service in bundle.services if service.service_name == "lambda")
    assert lambda_service.metadata["event_sources"] == ["arn:aws:sqs:us-west-2:123456789012:orders-queue-us-west-2"]

    queue_service = next(service for service in bundle.services if service.service_name == "sqs")
    assert queue_service.metadata["subscriptions"] == ["arn:aws:sns:us-west-2:123456789012:orders-topic-us-west-2"]

    api_service = next(service for service in bundle.services if service.service_name == "apigateway")
    assert api_service.metadata["integrations"] == ["arn:aws:lambda:us-west-2:123456789012:function:process-orders-us-west-2"]

    assert any(service.service_name == "ecs" for service in bundle.services)
    assert any(service.service_name == "eks" for service in bundle.services)
    assert any(service.service_name == "elasticache" for service in bundle.services)
    assert any(service.service_name == "cloudfront" for service in bundle.services)

    unattributed = next(cost for cost in bundle.costs if cost.resource_id == "unattributed")
    assert unattributed.mtd_cost_usd == 1.2
    assert bundle.warnings == []

    assert next(service for service in bundle.services if service.service_name == "ec2").status.value == "ACTIVE"
    assert next(service for service in bundle.services if service.service_name == "rds").status.value == "ACTIVE"
    assert next(service for service in bundle.services if service.service_name == "lambda").status.value == "ACTIVE"
    assert next(service for service in bundle.services if service.service_name == "sqs").status.value == "IDLE"
    assert next(service for service in bundle.services if service.service_name == "sns").status.value == "IDLE"
    assert next(service for service in bundle.services if service.service_name == "apigateway").status.value == "ACTIVE"
    assert next(service for service in bundle.services if service.service_name == "elasticache").status.value == "IDLE"
    assert next(service for service in bundle.services if service.service_name == "cloudfront").status.value == "ACTIVE"
    assert next(service for service in bundle.services if service.service_name == "s3").status.value == "UNKNOWN"
    assert next(service for service in bundle.services if service.service_name == "eks").status.value == "UNKNOWN"


def test_aws_collector_adds_tagging_only_supported_resources() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2")
    collector = AwsCollector(settings=settings, session=FakeSession())

    bundle = collector.load("scan-tagging-only")

    tagging_only = [service for service in bundle.services if service.metadata.get("tagging_only")]
    assert len(tagging_only) == 1
    assert tagging_only[0].service_name == "sqs"
    assert tagging_only[0].resource_id == "arn:aws:sqs:us-west-2:123456789012:orders-queue-shadow-us-west-2"


def test_aws_collector_warns_when_tagging_inventory_fails() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2")
    collector = AwsCollector(settings=settings, session=FakeSession({("us-west-2", "tagging"): _client_error("AccessDeniedException", "denied", "GetResources")}))

    bundle = collector.load("scan-tagging-warning")

    assert any(warning.stage == "tagging_inventory" and warning.service == "resourcegroupstaggingapi" for warning in bundle.warnings)
    assert any(service.service_name == "ec2" for service in bundle.services)


def test_aws_collector_retries_throttled_region_call() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2", aws_retry_attempts=3, aws_retry_base_delay_ms=0)
    session = FakeSession({("us-west-2", "ec2_throttle_once"): True})
    collector = AwsCollector(settings=settings, session=session)

    bundle = collector.load("scan-aws-retry")

    assert any(service.service_name == "ec2" for service in bundle.services)
    assert session.counters[("us-west-2", "ec2")] == 2
    assert not any(warning.service == "ec2" for warning in bundle.warnings)


def test_aws_collector_surfaces_partial_region_failure_as_warning() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2,us-east-1", aws_region_concurrency=2, aws_retry_attempts=1, aws_retry_base_delay_ms=0)
    session = FakeSession({("us-east-1", "ec2"): _client_error("AccessDeniedException", "denied", "DescribeInstances")})
    collector = AwsCollector(settings=settings, session=session)

    bundle = collector.load("scan-aws-warning")

    assert any(service.region == "us-west-2" and service.service_name == "ec2" for service in bundle.services)
    assert any(warning.service == "ec2" and warning.region == "us-east-1" and warning.stage == "discovery" for warning in bundle.warnings)


def _tagging_items_for_region(region: str | None):
    if region == "us-west-2":
        return [
            {"ResourceARN": "arn:aws:ec2:us-west-2:123456789012:instance/i-us-west-2", "ResourceType": "ec2:instance", "Tags": [{"Key": "Environment", "Value": "tagging"}, {"Key": "Owner", "Value": "platform"}]},
            {"ResourceARN": "arn:aws:rds:us-west-2:123456789012:db:orders-db-us-west-2", "ResourceType": "rds:db", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
            {"ResourceARN": "arn:aws:lambda:us-west-2:123456789012:function:process-orders-us-west-2", "ResourceType": "lambda:function", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
            {"ResourceARN": "arn:aws:sqs:us-west-2:123456789012:orders-queue-us-west-2", "ResourceType": "sqs:queue", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
            {"ResourceARN": "arn:aws:sns:us-west-2:123456789012:orders-topic-us-west-2", "ResourceType": "sns:topic", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
            {"ResourceARN": "arn:aws:apigateway:us-west-2::/restapis/orders-api-us-west-2", "ResourceType": "apigateway:restapis", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
            {"ResourceARN": "arn:aws:ecs:us-west-2:123456789012:cluster/orders-us-west-2", "ResourceType": "ecs:cluster", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
            {"ResourceARN": "arn:aws:eks:us-west-2:123456789012:cluster/orders-us-west-2", "ResourceType": "eks:cluster", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
            {"ResourceARN": "arn:aws:elasticache:us-west-2:123456789012:cluster:orders-cache-us-west-2", "ResourceType": "elasticache:cluster", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
            {"ResourceARN": "arn:aws:sqs:us-west-2:123456789012:orders-queue-shadow-us-west-2", "ResourceType": "sqs:queue", "Tags": [{"Key": "Environment", "Value": "tagging"}, {"Key": "Shadow", "Value": "true"}]},
        ]
    return [
        {"ResourceARN": "arn:aws:s3:::orders-artifacts", "ResourceType": "s3:bucket", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
        {"ResourceARN": "arn:aws:cloudfront::123456789012:distribution/DIST123", "ResourceType": "cloudfront:distribution", "Tags": [{"Key": "Environment", "Value": "tagging"}]},
    ]


def _client_error(code: str, message: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, operation)
