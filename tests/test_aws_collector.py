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
        return FakePaginator(
            [{"Reservations": [{"Instances": [{"InstanceId": f"i-{self.region_name or 'global'}", "State": {"Name": "running"}, "VpcId": "vpc-main", "SubnetId": "subnet-app", "SecurityGroups": [{"GroupId": "sg-web"}, {"GroupId": "sg-db-client"}], "PrivateIpAddress": "10.0.1.15", "Tags": [{"Key": "Environment", "Value": "prod"}]}]}]}]
        )


class FakeRdsClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def get_paginator(self, name):
        assert name == "describe_db_instances"
        return FakePaginator([{"DBInstances": [{"DBInstanceArn": f"arn:aws:rds:{self.region_name}:123456789012:db:orders-db-{self.region_name}", "DBInstanceIdentifier": f"orders-db-{self.region_name}", "DBInstanceStatus": "available", "VpcSecurityGroups": [{"VpcSecurityGroupId": "sg-db"}], "DBSubnetGroup": {"DBSubnetGroupName": "db-subnet", "VpcId": "vpc-main"}, "Engine": "postgres", "Endpoint": {"Address": "orders-db.example.local"}}]}])

    def list_tags_for_resource(self, ResourceName):
        return {"TagList": [{"Key": "Environment", "Value": "prod"}, {"Key": "Critical", "Value": "true"}]}


class FakeLambdaClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def get_paginator(self, name):
        if name == "list_functions":
            return FakePaginator([{"Functions": [{"FunctionArn": f"arn:aws:lambda:{self.region_name}:123456789012:function:process-orders-{self.region_name}", "FunctionName": f"process-orders-{self.region_name}", "Runtime": "python3.12", "Role": "arn:aws:iam::123456789012:role/orders-lambda-role", "State": "Active", "VpcConfig": {"SecurityGroupIds": ["sg-web"], "SubnetIds": ["subnet-app"]}}]}])
        assert name == "list_event_source_mappings"
        return FakePaginator([{"EventSourceMappings": [{"FunctionArn": f"arn:aws:lambda:{self.region_name}:123456789012:function:process-orders-{self.region_name}", "EventSourceArn": f"arn:aws:sqs:{self.region_name}:123456789012:orders-queue-{self.region_name}"}]}])

    def list_tags(self, Resource):
        return {"Tags": {"Environment": "prod", "Application": "orders"}}


class FakeS3Client:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        pass

    def list_buckets(self):
        return {"Buckets": [{"Name": "orders-artifacts", "CreationDate": datetime.now(UTC)}]}

    def get_bucket_location(self, Bucket):
        return {"LocationConstraint": "us-west-2"}

    def get_bucket_tagging(self, Bucket):
        return {"TagSet": [{"Key": "Environment", "Value": "prod"}]}


class FakeSqsClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def list_queues(self):
        return {"QueueUrls": [f"https://sqs.{self.region_name}.amazonaws.com/123456789012/orders-queue-{self.region_name}"]}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        return {"Attributes": {"QueueArn": f"arn:aws:sqs:{self.region_name}:123456789012:orders-queue-{self.region_name}", "Policy": "{}", "RedrivePolicy": "{}"}}

    def list_queue_tags(self, QueueUrl):
        return {"Tags": {"Environment": "prod"}}


class FakeSnsClient:
    def __init__(self, region_name: str | None = None, counters=None, failures=None):
        self.region_name = region_name

    def get_topic_attributes(self, TopicArn):
        return {"Attributes": {"Policy": "{}"}}

    def list_tags_for_resource(self, ResourceArn):
        return {"Tags": [{"Key": "Environment", "Value": "prod"}]}

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
        return {"tags": {"Environment": "prod"}}

    def get_resources(self, restApiId, embed):
        return {"items": [{"id": "root", "resourceMethods": {"POST": {}}}]}

    def get_integration(self, restApiId, resourceId, httpMethod):
        return {"uri": f"arn:aws:apigateway:{self.region_name}:lambda:path/2015-03-31/functions/arn:aws:lambda:{self.region_name}:123456789012:function:process-orders-{self.region_name}/invocations"}


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
            "ec2": FakeEc2Client,
            "rds": FakeRdsClient,
            "lambda": FakeLambdaClient,
            "s3": FakeS3Client,
            "sqs": FakeSqsClient,
            "sns": FakeSnsClient,
            "apigateway": FakeApiGatewayClient,
            "ce": FakeCeClient,
        }
        return mapping[service_name](region_name=region_name, counters=self.counters, failures=self.failures)


def test_aws_collector_discovers_supported_services_and_costs() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2")
    collector = AwsCollector(settings=settings, session=FakeSession())

    bundle = collector.load("scan-aws-1")

    service_names = {service.service_name for service in bundle.services}
    assert service_names == {"ec2", "rds", "lambda", "s3", "sqs", "sns", "apigateway"}

    lambda_service = next(service for service in bundle.services if service.service_name == "lambda")
    assert lambda_service.metadata["event_sources"] == ["arn:aws:sqs:us-west-2:123456789012:orders-queue-us-west-2"]

    queue_service = next(service for service in bundle.services if service.service_name == "sqs")
    assert queue_service.metadata["subscriptions"] == ["arn:aws:sns:us-west-2:123456789012:orders-topic-us-west-2"]

    api_service = next(service for service in bundle.services if service.service_name == "apigateway")
    assert api_service.metadata["integrations"] == ["arn:aws:lambda:us-west-2:123456789012:function:process-orders-us-west-2"]

    unattributed = next(cost for cost in bundle.costs if cost.resource_id == "unattributed")
    assert unattributed.mtd_cost_usd == 1.2
    assert bundle.warnings == []
    assert any(cost.resource_id == lambda_service.resource_id and cost.mtd_cost_usd == 0.9 for cost in bundle.costs)


def test_aws_collector_retries_throttled_region_call() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2", aws_retry_attempts=3, aws_retry_base_delay_ms=0)
    session = FakeSession({("us-west-2", "ec2_throttle_once"): True})
    collector = AwsCollector(settings=settings, session=session)

    bundle = collector.load("scan-aws-retry")

    assert any(service.service_name == "ec2" for service in bundle.services)
    assert session.counters[("us-west-2", "ec2")] == 2
    assert bundle.warnings == []


def test_aws_collector_surfaces_partial_region_failure_as_warning() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2,us-east-1", aws_region_concurrency=2, aws_retry_attempts=1, aws_retry_base_delay_ms=0)
    session = FakeSession({("us-east-1", "ec2"): _client_error("AccessDeniedException", "denied", "DescribeInstances")})
    collector = AwsCollector(settings=settings, session=session)

    bundle = collector.load("scan-aws-warning")

    assert any(service.region == "us-west-2" and service.service_name == "ec2" for service in bundle.services)
    assert any(warning.service == "ec2" and warning.region == "us-east-1" and warning.stage == "discovery" for warning in bundle.warnings)


def _client_error(code: str, message: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, operation)
