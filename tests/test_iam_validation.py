from __future__ import annotations

import json

from botocore.exceptions import ClientError, NoCredentialsError
from typer.testing import CliRunner

from aws_account_intelligence.cli.main import app
from aws_account_intelligence.iam_validation import IamValidator


class FakeClient:
    def __init__(self, name: str, failure_map: dict[str, str] | None = None):
        self.name = name
        self.failure_map = failure_map or {}

    def _maybe_fail(self, operation: str):
        code = self.failure_map.get(f"{self.name}:{operation}")
        if code == "NoCredentials":
            raise NoCredentialsError()
        if code:
            raise ClientError({"Error": {"Code": code, "Message": f"{operation} denied"}}, operation)

    def get_caller_identity(self):
        self._maybe_fail("GetCallerIdentity")
        return {"Account": "123456789012", "Arn": "arn:aws:iam::123456789012:user/test"}

    def get_resources(self, **kwargs):
        self._maybe_fail("GetResources")
        return {"ResourceTagMappingList": []}

    def describe_configuration_recorders(self):
        self._maybe_fail("DescribeConfigurationRecorders")
        return {"ConfigurationRecorders": []}

    def list_discovered_resources(self, **kwargs):
        self._maybe_fail("ListDiscoveredResources")
        return {"resourceIdentifiers": []}

    def get_cost_and_usage(self, **kwargs):
        self._maybe_fail("GetCostAndUsage")
        return {"ResultsByTime": []}

    def describe_instances(self, **kwargs):
        self._maybe_fail("DescribeInstances")
        return {"Reservations": []}

    def describe_db_instances(self, **kwargs):
        self._maybe_fail("DescribeDBInstances")
        return {"DBInstances": []}

    def list_functions(self, **kwargs):
        self._maybe_fail("ListFunctions")
        return {"Functions": []}

    def list_buckets(self):
        self._maybe_fail("ListBuckets")
        return {"Buckets": []}

    def list_queues(self, **kwargs):
        self._maybe_fail("ListQueues")
        return {"QueueUrls": []}

    def list_topics(self):
        self._maybe_fail("ListTopics")
        return {"Topics": []}

    def get_rest_apis(self, **kwargs):
        self._maybe_fail("GetRestApis")
        return {"items": []}

    def lookup_events(self, **kwargs):
        self._maybe_fail("LookupEvents")
        return {"Events": []}


class FakeSession:
    def __init__(self, failure_map: dict[str, str] | None = None):
        self.failure_map = failure_map or {}

    def client(self, service_name: str, region_name: str | None = None):
        service_alias = {
            "resourcegroupstaggingapi": "tagging",
            "config": "config",
            "ce": "ce",
            "ec2": "ec2",
            "rds": "rds",
            "lambda": "lambda",
            "s3": "s3",
            "sqs": "sqs",
            "sns": "sns",
            "apigateway": "apigateway",
            "cloudtrail": "cloudtrail",
            "sts": "sts",
        }[service_name]
        return FakeClient(service_alias, self.failure_map)


runner = CliRunner()


def test_iam_validator_reports_success() -> None:
    result = IamValidator(session=FakeSession()).validate()

    assert result.ok is True
    assert result.missing_permissions == []
    assert result.details["ec2:DescribeInstances"] == "ok"


def test_iam_validator_reports_missing_permissions() -> None:
    result = IamValidator(
        session=FakeSession(
            {
                "ce:GetCostAndUsage": "AccessDeniedException",
                "apigateway:GetRestApis": "AccessDeniedException",
            }
        )
    ).validate()

    assert result.ok is False
    assert "ce:GetCostAndUsage" in result.missing_permissions
    assert "apigateway:GET" in result.missing_permissions


def test_iam_validator_reports_missing_credentials() -> None:
    result = IamValidator(session=FakeSession({"sts:GetCallerIdentity": "NoCredentials"})).validate()

    assert result.ok is False
    assert result.missing_permissions == ["AWS credentials not found"]


def test_cli_iam_validate_uses_real_validator(monkeypatch) -> None:
    class StubValidator:
        def validate(self):
            from aws_account_intelligence.models import IamValidationResult

            return IamValidationResult(
                ok=False,
                checked_permissions=["sts:GetCallerIdentity"],
                missing_permissions=["ce:GetCostAndUsage"],
                details={"sts:GetCallerIdentity": "ok"},
            )

    monkeypatch.setattr("aws_account_intelligence.cli.main.IamValidator", lambda: StubValidator())

    result = runner.invoke(app, ["iam", "validate", "--output", "json"])
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["missing_permissions"] == ["ce:GetCostAndUsage"]
