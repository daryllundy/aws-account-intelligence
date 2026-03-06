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
            for target_group_arn in metadata.get("target_group_arns", []):
                if target_group_arn in by_id:
                    edges.append(
                        DependencyEdge(
                            from_resource_id=service.resource_id,
                            to_resource_id=target_group_arn,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.CONFIG,
                            evidence_source="ecs.service_load_balancer",
                            confidence=0.9,
                            rationale="ECS service is configured to register targets in this target group.",
                        )
                    )
            for load_balancer_arn in metadata.get("load_balancer_arns", []):
                if load_balancer_arn in by_id and service.resource_type == "AWS::ElasticLoadBalancingV2::TargetGroup":
                    edges.append(
                        DependencyEdge(
                            from_resource_id=service.resource_id,
                            to_resource_id=load_balancer_arn,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.CONFIG,
                            evidence_source="elbv2.target_group_attachment",
                            confidence=0.9,
                            rationale="Target group is attached to this load balancer.",
                        )
                    )
            for repository_arn in metadata.get("ecr_repository_arns", []):
                if repository_arn in by_id:
                    edges.append(
                        DependencyEdge(
                            from_resource_id=service.resource_id,
                            to_resource_id=repository_arn,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.CONFIG,
                            evidence_source="ecs.task_definition_image",
                            confidence=0.85,
                            rationale="ECS service task definition references a container image from this ECR repository.",
                        )
                    )
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
            for related in metadata.get("config_related_resources", []):
                target = _resolve_target_resource_id(related, by_id)
                if target:
                    edges.append(
                        DependencyEdge(
                            from_resource_id=service.resource_id,
                            to_resource_id=target,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.CONFIG,
                            evidence_source="aws_config.relationship",
                            confidence=0.84,
                            rationale="AWS Config reported a resource relationship between these resources.",
                        )
                    )
            for related in metadata.get("cloudtrail_related_resources", []):
                target = _resolve_target_resource_id(related, by_id)
                if target:
                    edges.append(
                        DependencyEdge(
                            from_resource_id=service.resource_id,
                            to_resource_id=target,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.INVOCATION,
                            evidence_source="cloudtrail.lookup_events",
                            confidence=0.67,
                            rationale="CloudTrail events indicate these resources interact through recent API activity.",
                        )
                    )
            for related in metadata.get("xray_related_resources", []):
                target = _resolve_target_resource_id(related, by_id)
                if target:
                    edges.append(
                        DependencyEdge(
                            from_resource_id=service.resource_id,
                            to_resource_id=target,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.DATA_FLOW,
                            evidence_source="xray.service_graph",
                            confidence=0.78,
                            rationale="X-Ray service graph indicates request/data flow between these resources.",
                        )
                    )

        edges.extend(self._infer_network_edges(services, scan_run_id))
        edges.extend(self._infer_iam_edges(services, scan_run_id))
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
        by_id = {service.resource_id: service for service in services}
        for source in services:
            for related in _direct_network_targets(source):
                target = _resolve_target_resource_id(related, by_id)
                if target and target != source.resource_id:
                    edges.append(
                        DependencyEdge(
                            from_resource_id=source.resource_id,
                            to_resource_id=target,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.NETWORK,
                            evidence_source="network.direct_attachment",
                            confidence=0.93,
                            rationale="Resource is directly attached to this network resource through its configuration.",
                        )
                    )
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

    def _infer_iam_edges(self, services: list[ServiceRecord], scan_run_id: str) -> list[DependencyEdge]:
        role_index: dict[str, list[ServiceRecord]] = defaultdict(list)
        for service in services:
            role_arn = service.metadata.get("execution_role") or service.metadata.get("role_arn")
            if role_arn:
                role_index[role_arn].append(service)

        edges: list[DependencyEdge] = []
        for role_arn, bound_services in role_index.items():
            if len(bound_services) < 2:
                continue
            for source in bound_services:
                for target in bound_services:
                    if source.resource_id == target.resource_id:
                        continue
                    edges.append(
                        DependencyEdge(
                            from_resource_id=source.resource_id,
                            to_resource_id=target.resource_id,
                            scan_run_id=scan_run_id,
                            edge_type=EdgeType.IAM,
                            evidence_source="iam.shared_role_binding",
                            confidence=0.6,
                            rationale=f"Resources share IAM role binding {role_arn}.",
                        )
                    )
        return edges


def _resolve_target_resource_id(related: str, by_id: dict[str, ServiceRecord]) -> str | None:
    if related in by_id:
        return related
    prioritized = _resolve_structured_identifier(related, by_id)
    if prioritized:
        return prioritized
    for resource_id, service in by_id.items():
        if related in {
            resource_id,
            service.arn,
            service.metadata.get("instance_id"),
            service.metadata.get("db_instance_identifier"),
            service.metadata.get("function_name"),
            service.metadata.get("queue_name"),
            service.metadata.get("topic_name"),
            service.metadata.get("api_id"),
            service.metadata.get("service_name"),
            service.metadata.get("cluster_name"),
            service.metadata.get("cache_cluster_id"),
            service.metadata.get("distribution_id"),
            service.metadata.get("vpc_id"),
            service.metadata.get("subnet_id"),
            service.metadata.get("security_group_id"),
            service.metadata.get("group_name"),
            service.metadata.get("target_group_name"),
            service.metadata.get("load_balancer_name"),
            service.metadata.get("repository_name"),
            service.metadata.get("repository_uri"),
        }:
            return resource_id
    return None


def _direct_network_targets(service: ServiceRecord) -> list[str]:
    related: list[str] = []
    if service.metadata.get("vpc_id"):
        related.append(service.metadata["vpc_id"])
    if service.metadata.get("subnet_id"):
        related.append(service.metadata["subnet_id"])
    related.extend(service.metadata.get("subnet_ids", []))
    related.extend(service.metadata.get("security_groups", []))
    return related


def _resolve_structured_identifier(related: str, by_id: dict[str, ServiceRecord]) -> str | None:
    if related.startswith("vpc-"):
        for resource_id, service in by_id.items():
            if service.resource_type == "AWS::EC2::VPC" and service.metadata.get("vpc_id") == related:
                return resource_id
    if related.startswith("subnet-"):
        for resource_id, service in by_id.items():
            if service.resource_type == "AWS::EC2::Subnet" and service.metadata.get("subnet_id") == related:
                return resource_id
    if related.startswith("sg-"):
        for resource_id, service in by_id.items():
            if service.resource_type == "AWS::EC2::SecurityGroup" and service.metadata.get("security_group_id") == related:
                return resource_id
    return None


def _dedupe(edges: list[DependencyEdge]) -> list[DependencyEdge]:
    unique: dict[tuple[str, str, str], DependencyEdge] = {}
    for edge in edges:
        key = (edge.from_resource_id, edge.to_resource_id, edge.edge_type.value)
        existing = unique.get(key)
        if existing is None or edge.confidence > existing.confidence:
            unique[key] = edge
    return list(unique.values())
