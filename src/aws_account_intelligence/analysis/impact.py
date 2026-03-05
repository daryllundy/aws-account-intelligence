from __future__ import annotations

from collections import deque

from aws_account_intelligence.models import DependentNode, DependencyEdge, ImpactReport, RiskLevel, ServiceRecord

from .dependency_graph import DependencyGraphBuilder


class ImpactAnalyzer:
    def __init__(self) -> None:
        self.graph_builder = DependencyGraphBuilder()

    def analyze(
        self,
        scan_run_id: str,
        target_resource_id: str,
        services: list[ServiceRecord],
        costs_by_resource: dict[str, float],
        edges: list[DependencyEdge],
    ) -> ImpactReport:
        graph = self.graph_builder.to_networkx(edges)
        service_map = {service.resource_id: service for service in services}
        reverse_graph = graph.reverse(copy=True)

        direct_predecessors = list(reverse_graph.neighbors(target_resource_id)) if target_resource_id in reverse_graph else []
        direct_nodes = [self._node(resource_id, service_map, graph.get_edge_data(resource_id, target_resource_id)) for resource_id in direct_predecessors]

        seen = set()
        transitive_nodes: list[DependentNode] = []
        queue = deque((resource_id, target_resource_id) for resource_id in direct_predecessors)
        while queue:
            current, dependency = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            edge = graph.get_edge_data(current, dependency)
            transitive_nodes.append(self._node(current, service_map, edge))
            for parent in reverse_graph.neighbors(current):
                queue.append((parent, current))

        target_cost = costs_by_resource.get(target_resource_id, 0.0)
        risk = self._risk_level(target_resource_id, service_map, direct_nodes, transitive_nodes)
        rationale = self._rationale(risk, direct_nodes, transitive_nodes)
        return ImpactReport(
            target_resource_id=target_resource_id,
            scan_run_id=scan_run_id,
            direct_dependents=direct_nodes,
            transitive_dependents=transitive_nodes,
            estimated_monthly_savings_usd=round(target_cost, 2),
            risk_score=risk,
            rationale=rationale,
        )

    def _node(self, resource_id: str, service_map: dict[str, ServiceRecord], edge_data: dict | None) -> DependentNode:
        service = service_map[resource_id]
        return DependentNode(
            resource_id=resource_id,
            service_name=service.service_name,
            edge_type=edge_data and edge_data.get("edge_type"),
            confidence=edge_data and edge_data.get("confidence"),
            rationale=edge_data and edge_data.get("rationale"),
        )

    def _risk_level(
        self,
        target_resource_id: str,
        service_map: dict[str, ServiceRecord],
        direct_nodes: list[DependentNode],
        transitive_nodes: list[DependentNode],
    ) -> RiskLevel:
        target = service_map.get(target_resource_id)
        critical = target and (
            target.tags.get("Critical", "false").lower() == "true"
            or target.tags.get("Environment", "").lower() in {"prod", "production"}
        )
        if critical and direct_nodes:
            return RiskLevel.CRITICAL
        if len(transitive_nodes) >= 3:
            return RiskLevel.HIGH
        if direct_nodes:
            return RiskLevel.MEDIUM
        return RiskLevel.LOW

    def _rationale(
        self,
        risk: RiskLevel,
        direct_nodes: list[DependentNode],
        transitive_nodes: list[DependentNode],
    ) -> str:
        if risk is RiskLevel.CRITICAL:
            return "Production-tagged target has direct dependents and should be treated as a critical shutdown risk."
        if risk is RiskLevel.HIGH:
            return f"Target has {len(transitive_nodes)} transitive dependents across the current graph snapshot."
        if risk is RiskLevel.MEDIUM:
            return f"Target has {len(direct_nodes)} direct dependents in the current graph snapshot."
        return "No dependents were identified in the current graph snapshot."
