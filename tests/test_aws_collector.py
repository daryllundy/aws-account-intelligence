from __future__ import annotations

from datetime import UTC, datetime

from aws_account_intelligence.collectors.aws import AwsCollector
from aws_account_intelligence.config import Settings


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kwargs):
        return iter(self.pages)


class FakeStsClient:
    def get_caller_identity(self):
        return {"Account": "123456789012"}


class FakeEc2Client:
    def get_paginator(self, name):
        assert name == "describe_instances"
        return FakePaginator(
            [
                {
                    "Reservations": [
                        {
                            "Instances": [
                                {
                                    "InstanceId": "i-web123",
                                    "State": {"Name": "running"},
                                    "VpcId": "vpc-main",
                                    "SubnetId": "subnet-app",
                                    "SecurityGroups": [{"GroupId": "sg-web"}, {"GroupId": "sg-db-client"}],
                                    "PrivateIpAddress": "10.0.1.15",
                                    "Tags": [{"Key": "Environment", "Value": "prod"}],
                                }
                            ]
                        }
                    ]
                }
            ]
        )


class FakeRdsClient:
    def get_paginator(self, name):
        assert name == "describe_db_instances"
        return FakePaginator(
            [
                {
                    "DBInstances": [
                        {
                            "DBInstanceArn": "arn:aws:rds:us-west-2:123456789012:db:orders-db",
                            "DBInstanceIdentifier": "orders-db",
                            "DBInstanceStatus": "available",
                            "VpcSecurityGroups": [{"VpcSecurityGroupId": "sg-db"}],
                            "DBSubnetGroup": {"DBSubnetGroupName": "db-subnet", "VpcId": "vpc-main"},
                            "Engine": "postgres",
                            "Endpoint": {"Address": "orders-db.example.local"},
                        }
                    ]
                }
            ]
        )

    def list_tags_for_resource(self, ResourceName):
        return {"TagList": [{"Key": "Environment", "Value": "prod"}, {"Key": "Critical", "Value": "true"}]}


class FakeLambdaClient:
    def get_paginator(self, name):
        if name == "list_functions":
            return FakePaginator(
                [
                    {
                        "Functions": [
                            {
                                "FunctionArn": "arn:aws:lambda:us-west-2:123456789012:function:process-orders",
                                "FunctionName": "process-orders",
                                "Runtime": "python3.12",
                                "Role": "arn:aws:iam::123456789012:role/orders-lambda-role",
                                "State": "Active",
                                "VpcConfig": {"SecurityGroupIds": ["sg-web"], "SubnetIds": ["subnet-app"]},
                            }
                        ]
                    }
                ]
            )
        assert name == "list_event_source_mappings"
        return FakePaginator(
            [
                {
                    "EventSourceMappings": [
                        {
                            "FunctionArn": "arn:aws:lambda:us-west-2:123456789012:function:process-orders",
                            "EventSourceArn": "arn:aws:sqs:us-west-2:123456789012:orders-queue",
                        }
                    ]
                }
            ]
        )

    def list_tags(self, Resource):
        return {"Tags": {"Environment": "prod", "Application": "orders"}}


class FakeS3Client:
    def list_buckets(self):
        return {"Buckets": [{"Name": "orders-artifacts", "CreationDate": datetime.now(UTC)}]}

    def get_bucket_location(self, Bucket):
        return {"LocationConstraint": "us-west-2"}

    def get_bucket_tagging(self, Bucket):
        return {"TagSet": [{"Key": "Environment", "Value": "prod"}]}


class FakeSqsClient:
    def list_queues(self):
        return {"QueueUrls": ["https://sqs.us-west-2.amazonaws.com/123456789012/orders-queue"]}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        return {
            "Attributes": {
                "QueueArn": "arn:aws:sqs:us-west-2:123456789012:orders-queue",
                "Policy": "{}",
                "RedrivePolicy": "{}",
            }
        }

    def list_queue_tags(self, QueueUrl):
        return {"Tags": {"Environment": "prod"}}


class FakeSnsClient:
    def get_topic_attributes(self, TopicArn):
        return {"Attributes": {"Policy": "{}"}}

    def list_tags_for_resource(self, ResourceArn):
        return {"Tags": [{"Key": "Environment", "Value": "prod"}]}

    def get_paginator(self, name):
        if name == "list_topics":
            return FakePaginator([{"Topics": [{"TopicArn": "arn:aws:sns:us-west-2:123456789012:orders-topic"}]}])
        assert name == "list_subscriptions_by_topic"
        return FakePaginator([{"Subscriptions": [{"Endpoint": "arn:aws:sqs:us-west-2:123456789012:orders-queue"}]}])


class FakeApiGatewayClient:
    def get_rest_apis(self, **kwargs):
        return {"items": [{"id": "orders-api", "name": "orders-api", "endpointConfiguration": {"types": ["REGIONAL"]}}]}

    def get_tags(self, resourceArn):
        return {"tags": {"Environment": "prod"}}

    def get_resources(self, restApiId, embed):
        return {"items": [{"id": "root", "resourceMethods": {"POST": {}}}]}

    def get_integration(self, restApiId, resourceId, httpMethod):
        return {
            "uri": "arn:aws:apigateway:us-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:us-west-2:123456789012:function:process-orders/invocations"
        }


class FakeCeClient:
    def get_cost_and_usage_with_resources(self, **kwargs):
        return {
            "ResultsByTime": [
                {
                    "TimePeriod": {"Start": "2026-03-01", "End": "2026-03-02"},
                    "Groups": [
                        {
                            "Keys": ["i-web123", "Amazon Elastic Compute Cloud - Compute"],
                            "Metrics": {"UnblendedCost": {"Amount": "3.5"}},
                        },
                        {
                            "Keys": ["orders-db", "Amazon Relational Database Service"],
                            "Metrics": {"UnblendedCost": {"Amount": "5.0"}},
                        },
                        {
                            "Keys": ["arn:aws:lambda:us-west-2:123456789012:function:process-orders", "AWS Lambda"],
                            "Metrics": {"UnblendedCost": {"Amount": "0.9"}},
                        },
                        {
                            "Keys": ["arn:aws:s3:::orders-artifacts", "Amazon Simple Storage Service"],
                            "Metrics": {"UnblendedCost": {"Amount": "0.6"}},
                        },
                        {
                            "Keys": ["arn:aws:sqs:us-west-2:123456789012:orders-queue", "Amazon Simple Queue Service"],
                            "Metrics": {"UnblendedCost": {"Amount": "0.2"}},
                        },
                        {
                            "Keys": ["arn:aws:sns:us-west-2:123456789012:orders-topic", "Amazon Simple Notification Service"],
                            "Metrics": {"UnblendedCost": {"Amount": "0.1"}},
                        },
                        {
                            "Keys": ["orders-api", "Amazon API Gateway"],
                            "Metrics": {"UnblendedCost": {"Amount": "0.4"}},
                        },
                        {
                            "Keys": ["mystery-resource", "AWS Lambda"],
                            "Metrics": {"UnblendedCost": {"Amount": "1.2"}},
                        },
                    ],
                }
            ]
        }


class FakeSession:
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
        return mapping[service_name]()


def test_aws_collector_discovers_supported_services_and_costs() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///:memory:", data_source="aws", aws_regions="us-west-2")
    collector = AwsCollector(settings=settings, session=FakeSession())

    bundle = collector.load("scan-aws-1")

    service_names = {service.service_name for service in bundle.services}
    assert service_names == {"ec2", "rds", "lambda", "s3", "sqs", "sns", "apigateway"}

    lambda_service = next(service for service in bundle.services if service.service_name == "lambda")
    assert lambda_service.metadata["event_sources"] == ["arn:aws:sqs:us-west-2:123456789012:orders-queue"]

    queue_service = next(service for service in bundle.services if service.service_name == "sqs")
    assert queue_service.metadata["subscriptions"] == ["arn:aws:sns:us-west-2:123456789012:orders-topic"]

    api_service = next(service for service in bundle.services if service.service_name == "apigateway")
    assert api_service.metadata["integrations"] == ["arn:aws:lambda:us-west-2:123456789012:function:process-orders"]

    unattributed = next(cost for cost in bundle.costs if cost.resource_id == "unattributed")
    assert unattributed.mtd_cost_usd == 1.2
    assert any(cost.resource_id == lambda_service.resource_id and cost.mtd_cost_usd == 0.9 for cost in bundle.costs)
