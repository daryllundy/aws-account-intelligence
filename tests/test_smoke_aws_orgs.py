from __future__ import annotations

from botocore.exceptions import ClientError

from aws_account_intelligence import smoke_aws_orgs


class FakeStsClient:
    def get_caller_identity(self):
        return {
            "Account": "628743727012",
            "Arn": "arn:aws:iam::628743727012:user/daryl-cli",
        }


class FakeOrganizationsClient:
    def get_paginator(self, name):
        assert name == "list_accounts"
        raise _client_error("AccessDeniedException", "denied", "ListAccounts")


class FakeSession:
    def client(self, service_name):
        if service_name == "sts":
            return FakeStsClient()
        if service_name == "organizations":
            return FakeOrganizationsClient()
        raise AssertionError(service_name)


def test_smoke_test_requires_aws_orgs_data_source(monkeypatch, capsys) -> None:
    monkeypatch.delenv("AAI_DATA_SOURCE", raising=False)

    exit_code = smoke_aws_orgs.run_smoke_test()

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "AAI_DATA_SOURCE=aws_orgs" in captured.err


def test_smoke_test_fails_fast_when_org_permissions_are_missing(monkeypatch, capsys) -> None:
    monkeypatch.setenv("AAI_DATA_SOURCE", "aws_orgs")
    monkeypatch.setattr(smoke_aws_orgs.boto3.session, "Session", lambda: FakeSession())
    smoke_aws_orgs.get_settings.cache_clear()

    exit_code = smoke_aws_orgs.run_smoke_test()

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "organizations:ListAccounts" in captured.err


def _client_error(code: str, message: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, operation)
