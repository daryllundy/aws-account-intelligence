from __future__ import annotations

from collections import defaultdict

import networkx as nx

from aws_account_intelligence.models import DependencyEdge, EdgeType, GraphExportResponse, ServiceRecord


class DependencyGraphBuilder:
    def build(self, services: list[ServiceRecord], scan_run_id: str) -> list[DependencyEdge]:
        edges: list[DependencyEdge] = []
        by_id = {service.resource_id: service for service in services}

        for service in services:
            metadata = service.metadata
            for integration in metadata.get("integrations", []):
                if integration in by_id:
                    edges.append(
                        DependencyEdge(
                            from_resource_id=service.resource_id,
                            to_resource_id=integration,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.INVOCATION,
                            evidence_source="apigateway.integration",
                            confidence=0.95,
                            rationale="API Gateway depends on the downstream compute integration to serve requests.",
                        )
                    )
            for source in metadata.get("event_sources", []):
                if source in by_id:
                    edges.append(
                        DependencyEdge(
                            from_resource_id=service.resource_id,
                            to_resource_id=source,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.EVENT,
                            evidence_source="lambda.event_source_mapping",
                            confidence=0.92,
                            rationale="Lambda function depends on the configured upstream event source.",
                        )
                    )
            for topic in metadata.get("subscriptions", []):
                if topic in by_id:
                    edges.append(
                        DependencyEdge(
                            from_resource_id=service.resource_id,
                            to_resource_id=topic,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.EVENT,
                            evidence_source="sqs.subscription",
                            confidence=0.88,
                            rationale="Queue depends on the upstream SNS topic that fans out messages.",
                        )
                    )

        edges.extend(self._infer_network_edges(services, scan_run_id))
        return _dedupe(edges)

    def export(self, scan, edges: list[DependencyEdge]) -> GraphExportResponse:
        adjacency: dict[str, list[DependencyEdge]] = defaultdict(list)
        for edge in edges:
            adjacency[edge.from_resource_id].append(edge)
        return GraphExportResponse(scan=scan, adjacency=dict(adjacency))

    def to_networkx(self, edges: list[DependencyEdge]) -> nx.DiGraph:
        graph = nx.DiGraph()
        for edge in edges:
            graph.add_edge(
                edge.from_resource_id,
                edge.to_resource_id,
                edge_type=edge.edge_type.value,
                evidence_source=edge.evidence_source,
                confidence=edge.confidence,
                rationale=edge.rationale,
            )
        return graph

    def _infer_network_edges(self, services: list[ServiceRecord], scan_run_id: str) -> list[DependencyEdge]:
        edges: list[DependencyEdge] = []
        for source in services:
            source_sgs = set(source.metadata.get("security_groups", []))
            if not source_sgs:
                continue
            for target in services:
                if source.resource_id == target.resource_id:
                    continue
                target_sgs = set(target.metadata.get("security_groups", []))
                if source.metadata.get("vpc_id") and source.metadata.get("vpc_id") == target.metadata.get("vpc_id"):
                    if source_sgs & target_sgs or {"sg-db-client", "sg-db"}.issubset(source_sgs | target_sgs):
                        edges.append(
                            DependencyEdge(
                                from_resource_id=source.resource_id,
                                to_resource_id=target.resource_id,
                                scan_run_id=scan_run_id,
                                edge_type=EdgeType.NETWORK,
                                evidence_source="vpc.security_group_overlap",
                                confidence=0.72,
                                rationale="Source resource appears to depend on the target through shared VPC and security group topology.",
                            )
                        )
        return edges


def _dedupe(edges: list[DependencyEdge]) -> list[DependencyEdge]:
    unique: dict[tuple[str, str, str], DependencyEdge] = {}
    for edge in edges:
        key = (edge.from_resource_id, edge.to_resource_id, edge.edge_type.value)
        existing = unique.get(key)
        if existing is None or edge.confidence > existing.confidence:
            unique[key] = edge
    return list(unique.values())
