from __future__ import annotations

from dataclasses import dataclass, field

from aws_account_intelligence.models import CostAttribution, ServiceRecord


@dataclass(slots=True)
class ScanWarning:
    stage: str
    service: str
    region: str | None
    code: str
    message: str


@dataclass(slots=True)
class DiscoveryBundle:
    services: list[ServiceRecord]
    costs: list[CostAttribution]
    warnings: list[ScanWarning] = field(default_factory=list)


class CollectorError(RuntimeError):
    pass
