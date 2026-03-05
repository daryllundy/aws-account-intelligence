from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, date, datetime
from typing import Iterator

from sqlalchemy import JSON, DateTime, Float, Integer, String, Text, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from aws_account_intelligence.models import CostAttribution, CostPoint, DependencyEdge, ScanRun, ServiceRecord


class Base(DeclarativeBase):
    pass


class ScanRunRow(Base):
    __tablename__ = "scan_runs"

    scan_run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16))
    data_source: Mapped[str] = mapped_column(String(32))
    regions_json: Mapped[str] = mapped_column(Text)
    resource_count: Mapped[int] = mapped_column(Integer, default=0)
    edge_count: Mapped[int] = mapped_column(Integer, default=0)
    summary: Mapped[dict] = mapped_column(JSON, default=dict)


class ServiceRecordRow(Base):
    __tablename__ = "service_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    resource_id: Mapped[str] = mapped_column(String(512), index=True)
    arn: Mapped[str] = mapped_column(String(1024))
    resource_type: Mapped[str] = mapped_column(String(128))
    service_name: Mapped[str] = mapped_column(String(64), index=True)
    region: Mapped[str] = mapped_column(String(32), index=True)
    account_id: Mapped[str] = mapped_column(String(32), index=True)
    tags: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(16))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    scan_run_id: Mapped[str] = mapped_column(String(64), index=True)
    resource_metadata: Mapped[dict] = mapped_column("metadata", JSON, default=dict)


class CostAttributionRow(Base):
    __tablename__ = "cost_attributions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    resource_id: Mapped[str] = mapped_column(String(512), index=True)
    scan_run_id: Mapped[str] = mapped_column(String(64), index=True)
    daily_costs: Mapped[list] = mapped_column(JSON, default=list)
    mtd_cost_usd: Mapped[float] = mapped_column(Float)
    projected_monthly_cost_usd: Mapped[float] = mapped_column(Float)
    prior_30_day_cost_usd: Mapped[float] = mapped_column(Float)
    trend_delta_usd: Mapped[float] = mapped_column(Float)
    attribution_method: Mapped[str] = mapped_column(String(32))
    confidence: Mapped[float] = mapped_column(Float)
    currency: Mapped[str] = mapped_column(String(8), default="USD")


class DependencyEdgeRow(Base):
    __tablename__ = "dependency_edges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    from_resource_id: Mapped[str] = mapped_column(String(512), index=True)
    to_resource_id: Mapped[str] = mapped_column(String(512), index=True)
    scan_run_id: Mapped[str] = mapped_column(String(64), index=True)
    edge_type: Mapped[str] = mapped_column(String(32))
    evidence_source: Mapped[str] = mapped_column(String(128))
    confidence: Mapped[float] = mapped_column(Float)
    rationale: Mapped[str] = mapped_column(Text)
    resource_metadata: Mapped[dict] = mapped_column("metadata", JSON, default=dict)


class Database:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, future=True)
        self._session_factory = sessionmaker(self.engine, expire_on_commit=False, class_=Session)

    def create_all(self) -> None:
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_scan_run(self, scan_run: ScanRun) -> None:
        with self.session() as session:
            row = session.get(ScanRunRow, scan_run.scan_run_id)
            payload = {
                "scan_run_id": scan_run.scan_run_id,
                "started_at": scan_run.started_at,
                "completed_at": scan_run.completed_at,
                "status": scan_run.status,
                "data_source": scan_run.data_source,
                "regions_json": json.dumps(scan_run.regions),
                "resource_count": scan_run.resource_count,
                "edge_count": scan_run.edge_count,
                "summary": scan_run.summary,
            }
            if row is None:
                session.add(ScanRunRow(**payload))
            else:
                for key, value in payload.items():
                    setattr(row, key, value)

    def save_service_records(self, records: list[ServiceRecord]) -> None:
        with self.session() as session:
            for record in records:
                session.add(
                    ServiceRecordRow(
                        resource_id=record.resource_id,
                        arn=record.arn,
                        resource_type=record.resource_type,
                        service_name=record.service_name,
                        region=record.region,
                        account_id=record.account_id,
                        tags=record.tags,
                        status=record.status.value,
                        last_seen_at=record.last_seen_at,
                        scan_run_id=record.scan_run_id,
                        resource_metadata=record.metadata,
                    )
                )

    def save_cost_attributions(self, costs: list[CostAttribution]) -> None:
        with self.session() as session:
            for cost in costs:
                session.add(
                    CostAttributionRow(
                        resource_id=cost.resource_id,
                        scan_run_id=cost.scan_run_id,
                        daily_costs=[point.model_dump(mode="json") for point in cost.daily_costs],
                        mtd_cost_usd=cost.mtd_cost_usd,
                        projected_monthly_cost_usd=cost.projected_monthly_cost_usd,
                        prior_30_day_cost_usd=cost.prior_30_day_cost_usd,
                        trend_delta_usd=cost.trend_delta_usd,
                        attribution_method=cost.attribution_method.value,
                        confidence=cost.confidence,
                        currency=cost.currency,
                    )
                )

    def save_dependency_edges(self, edges: list[DependencyEdge]) -> None:
        with self.session() as session:
            for edge in edges:
                session.add(
                    DependencyEdgeRow(
                        from_resource_id=edge.from_resource_id,
                        to_resource_id=edge.to_resource_id,
                        scan_run_id=edge.scan_run_id,
                        edge_type=edge.edge_type.value,
                        evidence_source=edge.evidence_source,
                        confidence=edge.confidence,
                        rationale=edge.rationale,
                        resource_metadata=edge.metadata,
                    )
                )

    def get_scan_run(self, scan_run_id: str) -> ScanRun | None:
        with self.session() as session:
            row = session.get(ScanRunRow, scan_run_id)
            return _scan_run_from_row(row) if row else None

    def get_latest_scan_run(self) -> ScanRun | None:
        with self.session() as session:
            row = session.scalars(select(ScanRunRow).order_by(ScanRunRow.started_at.desc())).first()
            return _scan_run_from_row(row) if row else None

    def list_service_records(self, scan_run_id: str) -> list[ServiceRecord]:
        with self.session() as session:
            rows = session.scalars(select(ServiceRecordRow).where(ServiceRecordRow.scan_run_id == scan_run_id)).all()
            return [_service_record_from_row(row) for row in rows]

    def list_cost_attributions(self, scan_run_id: str) -> list[CostAttribution]:
        with self.session() as session:
            rows = session.scalars(select(CostAttributionRow).where(CostAttributionRow.scan_run_id == scan_run_id)).all()
            return [_cost_from_row(row) for row in rows]

    def list_dependency_edges(self, scan_run_id: str) -> list[DependencyEdge]:
        with self.session() as session:
            rows = session.scalars(select(DependencyEdgeRow).where(DependencyEdgeRow.scan_run_id == scan_run_id)).all()
            return [_edge_from_row(row) for row in rows]


def _scan_run_from_row(row: ScanRunRow) -> ScanRun:
    return ScanRun(
        scan_run_id=row.scan_run_id,
        started_at=_ensure_utc(row.started_at),
        completed_at=_ensure_utc(row.completed_at) if row.completed_at else None,
        status=row.status,
        data_source=row.data_source,
        regions=json.loads(row.regions_json),
        resource_count=row.resource_count,
        edge_count=row.edge_count,
        summary=row.summary or {},
    )


def _service_record_from_row(row: ServiceRecordRow) -> ServiceRecord:
    return ServiceRecord(
        resource_id=row.resource_id,
        arn=row.arn,
        resource_type=row.resource_type,
        service_name=row.service_name,
        region=row.region,
        account_id=row.account_id,
        tags=row.tags or {},
        status=row.status,
        last_seen_at=_ensure_utc(row.last_seen_at),
        scan_run_id=row.scan_run_id,
        metadata=row.resource_metadata or {},
    )


def _cost_from_row(row: CostAttributionRow) -> CostAttribution:
    return CostAttribution(
        resource_id=row.resource_id,
        scan_run_id=row.scan_run_id,
        daily_costs=[CostPoint(date=_coerce_date(point["date"]), amount_usd=point["amount_usd"]) for point in row.daily_costs],
        mtd_cost_usd=row.mtd_cost_usd,
        projected_monthly_cost_usd=row.projected_monthly_cost_usd,
        prior_30_day_cost_usd=row.prior_30_day_cost_usd,
        trend_delta_usd=row.trend_delta_usd,
        attribution_method=row.attribution_method,
        confidence=row.confidence,
        currency=row.currency,
    )


def _edge_from_row(row: DependencyEdgeRow) -> DependencyEdge:
    return DependencyEdge(
        from_resource_id=row.from_resource_id,
        to_resource_id=row.to_resource_id,
        scan_run_id=row.scan_run_id,
        edge_type=row.edge_type,
        evidence_source=row.evidence_source,
        confidence=row.confidence,
        rationale=row.rationale,
        metadata=row.resource_metadata or {},
    )


def _coerce_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
