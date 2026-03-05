from __future__ import annotations

from datetime import UTC, datetime

from aws_account_intelligence.models import DependencyEdge, EdgeType, ResourceStatus, ServiceRecord


def test_service_record_schema_round_trip() -> None:
    record = ServiceRecord(
        resource_id="r1",
        arn="arn:aws:s3:::bucket",
        resource_type="AWS::S3::Bucket",
        service_name="s3",
        region="us-west-2",
        account_id="123456789012",
        tags={"Environment": "prod"},
        status=ResourceStatus.ACTIVE,
        last_seen_at=datetime.now(UTC),
        scan_run_id="scan-1",
        metadata={"k": "v"},
    )

    payload = record.model_dump(mode="json")
    parsed = ServiceRecord.model_validate(payload)

    assert parsed.resource_id == "r1"
    assert parsed.status is ResourceStatus.ACTIVE
    assert parsed.metadata["k"] == "v"


def test_dependency_edge_schema_preserves_enums() -> None:
    edge = DependencyEdge(
        from_resource_id="a",
        to_resource_id="b",
        scan_run_id="scan-1",
        edge_type=EdgeType.EVENT,
        evidence_source="lambda.event_source_mapping",
        confidence=0.9,
        rationale="test",
    )

    assert edge.edge_type is EdgeType.EVENT
