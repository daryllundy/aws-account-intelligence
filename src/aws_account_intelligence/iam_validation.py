from __future__ import annotations

from collections import OrderedDict
from datetime import date, timedelta
from typing import Any, Callable

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from aws_account_intelligence.models import IamValidationResult


Probe = Callable[[boto3.session.Session], dict[str, Any] | None]


class IamValidator:
    def __init__(self, session: boto3.session.Session | None = None):
        self.session = session or boto3.session.Session()
        self.probes: OrderedDict[str, Probe] = OrderedDict(
            [
                ("sts:GetCallerIdentity", _probe_sts),
                ("tag:GetResources", _probe_tagging),
                ("config:DescribeConfigurationRecorders", _probe_config_describe),
                ("config:ListDiscoveredResources", _probe_config_list),
                ("ce:GetCostAndUsage", _probe_cost_explorer),
                ("ec2:DescribeInstances", _probe_ec2),
                ("rds:DescribeDBInstances", _probe_rds),
                ("lambda:ListFunctions", _probe_lambda),
                ("s3:ListAllMyBuckets", _probe_s3),
                ("sqs:ListQueues", _probe_sqs),
                ("sns:ListTopics", _probe_sns),
                ("apigateway:GET", _probe_apigateway),
                ("cloudtrail:LookupEvents", _probe_cloudtrail),
            ]
        )

    def validate(self) -> IamValidationResult:
        missing_permissions: list[str] = []
        details: dict[str, Any] = {}
        credential_error = self._credentials_status()
        if credential_error:
            return IamValidationResult(
                ok=False,
                checked_permissions=list(self.probes.keys()),
                missing_permissions=[credential_error],
                details={"credentials": credential_error},
            )

        for permission, probe in self.probes.items():
            try:
                response = probe(self.session)
                details[permission] = "ok"
                if response:
                    details[f"{permission}:detail"] = response
            except ClientError as exc:
                if _is_access_denied(exc):
                    missing_permissions.append(permission)
                    details[permission] = _error_message(exc)
                else:
                    details[permission] = f"non-permission error: {_error_message(exc)}"
            except (BotoCoreError, RuntimeError) as exc:
                details[permission] = f"non-permission error: {exc}"

        return IamValidationResult(
            ok=not missing_permissions,
            checked_permissions=list(self.probes.keys()),
            missing_permissions=missing_permissions,
            details=details,
        )

    def _credentials_status(self) -> str | None:
        try:
            self.session.client("sts").get_caller_identity()
        except NoCredentialsError:
            return "AWS credentials not found"
        except ClientError as exc:
            if _is_access_denied(exc):
                return "sts:GetCallerIdentity"
            return _error_message(exc)
        except BotoCoreError as exc:
            return str(exc)
        return None


def _probe_sts(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("sts").get_caller_identity()


def _probe_tagging(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("resourcegroupstaggingapi", region_name="us-east-1").get_resources(ResourcesPerPage=1)


def _probe_config_describe(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("config", region_name="us-east-1").describe_configuration_recorders()


def _probe_config_list(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("config", region_name="us-east-1").list_discovered_resources(resourceType="AWS::EC2::Instance", limit=1)


def _probe_cost_explorer(session: boto3.session.Session) -> dict[str, Any]:
    end_date = date.today()
    start_date = end_date - timedelta(days=1)
    return session.client("ce", region_name="us-east-1").get_cost_and_usage(
        TimePeriod={"Start": start_date.isoformat(), "End": end_date.isoformat()},
        Granularity="DAILY",
        Metrics=["UnblendedCost"],
    )


def _probe_ec2(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("ec2", region_name="us-east-1").describe_instances(MaxResults=1)


def _probe_rds(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("rds", region_name="us-east-1").describe_db_instances(MaxRecords=20)


def _probe_lambda(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("lambda", region_name="us-east-1").list_functions(MaxItems=1)


def _probe_s3(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("s3").list_buckets()


def _probe_sqs(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("sqs", region_name="us-east-1").list_queues(MaxResults=1)


def _probe_sns(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("sns", region_name="us-east-1").list_topics()


def _probe_apigateway(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("apigateway", region_name="us-east-1").get_rest_apis(limit=1)


def _probe_cloudtrail(session: boto3.session.Session) -> dict[str, Any]:
    return session.client("cloudtrail", region_name="us-east-1").lookup_events(MaxResults=1)


def _is_access_denied(exc: ClientError) -> bool:
    code = exc.response.get("Error", {}).get("Code", "")
    return code in {"AccessDenied", "AccessDeniedException", "UnauthorizedOperation", "Client.UnauthorizedOperation"}


def _error_message(exc: ClientError) -> str:
    error = exc.response.get("Error", {})
    code = error.get("Code", "Unknown")
    message = error.get("Message", "Unknown error")
    return f"{code}: {message}"
