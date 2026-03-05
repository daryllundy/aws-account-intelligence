from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class ResourceStatus(str, Enum):
    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
    UNKNOWN = "UNKNOWN"


class AttributionMethod(str, Enum):
    DIRECT = "DIRECT"
    TAG_MATCH = "TAG_MATCH"
    BEST_EFFORT = "BEST_EFFORT"
    UNATTRIBUTED = "UNATTRIBUTED"


class EdgeType(str, Enum):
    NETWORK = "NETWORK"
    IAM = "IAM"
    EVENT = "EVENT"
    INVOCATION = "INVOCATION"
    DATA_FLOW = "DATA_FLOW"
    CONFIG = "CONFIG"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ServiceRecord(BaseModel):
    resource_id: str
    arn: str
    resource_type: str
    service_name: str
    region: str
    account_id: str
    tags: dict[str, str] = Field(default_factory=dict)
    status: ResourceStatus = ResourceStatus.UNKNOWN
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    scan_run_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class CostPoint(BaseModel):
    date: date
    amount_usd: float


class CostAttribution(BaseModel):
    resource_id: str
    scan_run_id: str
    daily_costs: list[CostPoint] = Field(default_factory=list)
    mtd_cost_usd: float = 0.0
    projected_monthly_cost_usd: float = 0.0
    prior_30_day_cost_usd: float = 0.0
    trend_delta_usd: float = 0.0
    attribution_method: AttributionMethod = AttributionMethod.UNATTRIBUTED
    confidence: float = 0.0
    currency: Literal["USD"] = "USD"
    matched_by: list[str] = Field(default_factory=list)


class DependencyEdge(BaseModel):
    from_resource_id: str
    to_resource_id: str
    scan_run_id: str
    edge_type: EdgeType
    evidence_source: str
    confidence: float
    rationale: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ScanRun(BaseModel):
    scan_run_id: str
    started_at: datetime
    completed_at: datetime | None = None
    status: Literal["running", "completed", "failed"]
    data_source: str
    regions: list[str]
    resource_count: int = 0
    edge_count: int = 0
    summary: dict[str, Any] = Field(default_factory=dict)


class DependentNode(BaseModel):
    resource_id: str
    service_name: str
    edge_type: EdgeType | None = None
    confidence: float | None = None
    rationale: str | None = None
    path_depth: int = 1
    dependency_path: list[str] = Field(default_factory=list)
    is_critical: bool = False
    criticality_reasons: list[str] = Field(default_factory=list)


class ImpactReport(BaseModel):
    target_resource_id: str
    scan_run_id: str
    direct_dependents: list[DependentNode]
    transitive_dependents: list[DependentNode]
    estimated_monthly_savings_usd: float
    risk_score: RiskLevel
    rationale: str
    risk_factors: list[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class InventoryListResponse(BaseModel):
    scan: ScanRun
    services: list[ServiceRecord]


class CostSummaryResponse(BaseModel):
    scan: ScanRun
    total_mtd_cost_usd: float
    total_projected_monthly_cost_usd: float
    unattributed_cost_usd: float
    cost_freshness_at: datetime | None = None
    costs: list[CostAttribution]


class GraphExportResponse(BaseModel):
    scan: ScanRun
    adjacency: dict[str, list[DependencyEdge]]


class IamValidationResult(BaseModel):
    ok: bool
    checked_permissions: list[str]
    missing_permissions: list[str]
    details: dict[str, Any] = Field(default_factory=dict)
