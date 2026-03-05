from __future__ import annotations

from dataclasses import dataclass

from aws_account_intelligence.models import CostAttribution, ServiceRecord


@dataclass(slots=True)
class DiscoveryBundle:
    services: list[ServiceRecord]
    costs: list[CostAttribution]


class CollectorError(RuntimeError):
    pass
