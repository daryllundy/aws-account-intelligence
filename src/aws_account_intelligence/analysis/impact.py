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
        direct_nodes = [
            self._node(
                resource_id=resource_id,
                service_map=service_map,
                edge_data=graph.get_edge_data(resource_id, target_resource_id),
                dependency_path=[resource_id, target_resource_id],
            )
            for resource_id in direct_predecessors
        ]

        seen_depths: dict[str, int] = {}
        transitive_nodes: list[DependentNode] = []
        queue = deque((resource_id, [target_resource_id, resource_id]) for resource_id in direct_predecessors)
        while queue:
            current, reverse_path = queue.popleft()
            for parent in reverse_graph.neighbors(current):
                reverse_parent_path = [*reverse_path, parent]
                path_depth = len(reverse_parent_path) - 1
                previous_depth = seen_depths.get(parent)
                if previous_depth is not None and previous_depth <= path_depth:
                    continue
                seen_depths[parent] = path_depth
                dependency_path = list(reversed(reverse_parent_path))
                transitive_nodes.append(
                    self._node(
                        resource_id=parent,
                        service_map=service_map,
                        edge_data=graph.get_edge_data(parent, current),
                        dependency_path=dependency_path,
                    )
                )
                queue.append((parent, reverse_parent_path))

        transitive_nodes.sort(key=lambda node: (node.path_depth, node.resource_id))
        target_cost = costs_by_resource.get(target_resource_id, 0.0)
        risk, risk_factors = self._risk_level(target_resource_id, service_map, direct_nodes, transitive_nodes)
        rationale = self._rationale(target_resource_id, service_map, direct_nodes, transitive_nodes, risk_factors)
        return ImpactReport(
            target_resource_id=target_resource_id,
            scan_run_id=scan_run_id,
            direct_dependents=direct_nodes,
            transitive_dependents=transitive_nodes,
            estimated_monthly_savings_usd=round(target_cost, 2),
            risk_score=risk,
            rationale=rationale,
            risk_factors=risk_factors,
        )

    def _node(
        self,
        resource_id: str,
        service_map: dict[str, ServiceRecord],
        edge_data: dict | None,
        dependency_path: list[str],
    ) -> DependentNode:
        service = service_map[resource_id]
        is_critical, reasons = _criticality(service)
        return DependentNode(
            resource_id=resource_id,
            service_name=service.service_name,
            edge_type=edge_data and edge_data.get("edge_type"),
            confidence=edge_data and edge_data.get("confidence"),
            rationale=edge_data and edge_data.get("rationale"),
            path_depth=max(len(dependency_path) - 1, 1),
            dependency_path=dependency_path,
            is_critical=is_critical,
            criticality_reasons=reasons,
        )

    def _risk_level(
        self,
        target_resource_id: str,
        service_map: dict[str, ServiceRecord],
        direct_nodes: list[DependentNode],
        transitive_nodes: list[DependentNode],
    ) -> tuple[RiskLevel, list[str]]:
        target = service_map.get(target_resource_id)
        target_reasons = _criticality(target)[1] if target else []
        target_has_explicit_criticality = any(reason in {"critical_tag", "critical_tier"} for reason in target_reasons)
        target_in_production = "production_environment" in target_reasons
        direct_critical = sum(1 for node in direct_nodes if node.is_critical)
        transitive_critical = sum(1 for node in transitive_nodes if node.is_critical)
        max_depth = max([node.path_depth for node in [*direct_nodes, *transitive_nodes]], default=0)
        max_confidence = max(
            [node.confidence for node in [*direct_nodes, *transitive_nodes] if node.confidence is not None],
            default=0.0,
        )

        score = 0
        risk_factors: list[str] = []

        if target_has_explicit_criticality:
            score += 4
            risk_factors.append("target_marked_critical")
        elif target_in_production:
            score += 1
            risk_factors.append("target_in_production")
        if direct_nodes:
            score += 3 if len(direct_nodes) >= 3 else 2
            risk_factors.append(f"direct_dependents:{len(direct_nodes)}")
        if transitive_nodes:
            score += 2 if len(transitive_nodes) >= 3 else 1
            risk_factors.append(f"transitive_dependents:{len(transitive_nodes)}")
        if max_depth >= 2:
            score += 3 if max_depth >= 3 else 2
            risk_factors.append(f"max_dependency_depth:{max_depth}")
        if direct_critical:
            risk_factors.append(f"critical_direct_dependents:{direct_critical}")
        if transitive_critical:
            risk_factors.append(f"critical_transitive_dependents:{transitive_critical}")
        if max_confidence >= 0.85:
            score += 1
            risk_factors.append("high_confidence_dependency_paths")

        if score >= 9:
            return RiskLevel.CRITICAL, risk_factors
        if score >= 6:
            return RiskLevel.HIGH, risk_factors
        if score >= 3:
            return RiskLevel.MEDIUM, risk_factors
        return RiskLevel.LOW, risk_factors

    def _rationale(
        self,
        target_resource_id: str,
        service_map: dict[str, ServiceRecord],
        direct_nodes: list[DependentNode],
        transitive_nodes: list[DependentNode],
        risk_factors: list[str],
    ) -> str:
        target = service_map.get(target_resource_id)
        target_name = target.service_name if target else target_resource_id
        direct_summary = f"{len(direct_nodes)} direct"
        transitive_summary = f"{len(transitive_nodes)} transitive"
        max_depth = max([node.path_depth for node in [*direct_nodes, *transitive_nodes]], default=0)
        critical_dependents = [node for node in [*direct_nodes, *transitive_nodes] if node.is_critical]

        details = [f"{target_name} has {direct_summary} and {transitive_summary} dependents"]
        if max_depth:
            details.append(f"dependency chains reach depth {max_depth}")
        if critical_dependents:
            critical_services = ", ".join(sorted({node.service_name for node in critical_dependents}))
            details.append(f"critical dependents include {critical_services}")
        if "target_marked_critical" in risk_factors:
            details.append("target is tagged as critical or production")
        elif "target_in_production" in risk_factors:
            details.append("target runs in production")
        return ". ".join(details) + "."


def _criticality(service: ServiceRecord) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if service.tags.get("Critical", "false").lower() == "true":
        reasons.append("critical_tag")
    environment = service.tags.get("Environment", "").lower()
    if environment in {"prod", "production"}:
        reasons.append("production_environment")
    tier = service.tags.get("Tier", "").lower()
    if tier in {"critical", "tier-0", "tier0"}:
        reasons.append("critical_tier")
    return bool(reasons), reasons
