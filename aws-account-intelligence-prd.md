**PRODUCT REQUIREMENTS DOCUMENT**

**AWS Account Intelligence Tool**

*Service Discovery · Cost Visibility · Dependency Mapping · Safe Shutdown Analysis*

Version 1.0  ·  March 2026

**Status: Draft for Review**

# **1\. Executive Summary**

Cloud environments grow organically and, without active governance, accumulate services that are forgotten, duplicated, or loosely coupled in ways that make decommissioning risky. This document defines requirements for the AWS Account Intelligence Tool — a read-only scanning and analysis platform that gives engineering and FinOps teams a single, authoritative answer to three questions:

* What is actively running in our AWS account(s)?

* What does each service cost, and what is the aggregate cloud spend?

* If I shut down Service X, what else breaks?

The tool will integrate with AWS APIs to enumerate resources, pull cost data, and construct a dependency graph. An impact analyzer will let engineers safely evaluate decommission decisions before acting. The initial release targets a single AWS account; multi-account (AWS Organizations) support follows in the same major version.

# **2\. Problem Statement & Context**

## **2.1  Pain Points**

* No unified view of active services across regions — teams rely on tribal knowledge or manual console browsing.

* Cost visibility is siloed: billing data exists in Cost Explorer but is not correlated with resource inventory, making waste hard to identify.

* Service interdependencies are undocumented. Decommissioning decisions are made cautiously (or avoided) because engineers cannot confidently assess downstream blast radius.

* Time spent on manual audits — often spreadsheet-driven — is high and error-prone, with results going stale within days.

## **2.2  Opportunity**

AWS provides rich APIs (Resource Groups Tagging, Config, Cost Explorer, CloudTrail) that, when combined, enable automated, accurate, and continuously updated inventory and dependency mapping. A purpose-built tool surfacing this data in an actionable format would directly reduce cloud waste and accelerate safe infrastructure changes.

# **3\. Goals & Success Metrics**

## **3.1  Goals**

* Provide a complete, accurate inventory of all active AWS resources across all regions within a single scan.

* Surface per-service and aggregate cost data with ≤24-hour freshness, enabling data-driven cost governance.

* Generate a dependency graph precise enough to identify transitive service relationships and flag shutdown risks.

* Reduce time-to-insight for decommission decisions from days to minutes.

## **3.2  Success Metrics**

* 95%+ of active chargeable resources enumerated in a scan (validated against billing line items).

* Dependency graph correctly identifies ≥90% of documented service relationships in acceptance test environments.

* Shutdown impact analysis reduces unplanned outages caused by decommission decisions by ≥80% (tracked post-launch).

* Engineer self-reported time savings: ≥4 hours/month per user on audit and cost review tasks.

# **4\. Target Users & Personas**

The tool is designed for technical practitioners responsible for infrastructure operations, cost management, and architectural decisions in AWS environments.

| Persona | Role | Primary Need |
| :---- | :---- | :---- |
| Cloud Ops Engineer | Manages AWS infra day-to-day | Quickly identify unused or orphaned resources |
| FinOps Analyst | Owns cloud cost management | Attribute spend accurately; find waste |
| Security Engineer | Reviews infra posture | Map blast radius before decommission |
| Platform Lead | Owns architecture decisions | Understand service coupling before changes |

# **5\. Feature Requirements**

## **5.1  Feature Summary**

| \# | Feature | Description | Priority |
| :---- | :---- | :---- | :---- |
| F-01 | Service Inventory Scan | Enumerate all active AWS services via API/SDK across all regions; normalize into a unified service registry | P0 |
| F-02 | Cost Attribution | Pull line-item cost data from AWS Cost Explorer and Billing APIs; map to service records in the registry | P0 |
| F-03 | Dependency Graph Engine | Build a directed graph of service-to-service relationships using resource tags, VPC/subnet membership, IAM role bindings, and API call traces | P0 |
| F-04 | Shutdown Impact Analyzer | Given a target service, traverse the dependency graph to return: direct dependents, transitive dependents, estimated cost savings, and risk score | P0 |
| F-05 | Interactive Dashboard | Web or CLI view with filterable service list, cost roll-ups by tag/account/region, and graph visualization | P1 |
| F-06 | Scheduled Refresh | Configurable scan cadence (daily/weekly); delta reports showing new, removed, or cost-changed services | P1 |
| F-07 | Export & Reporting | Export inventory \+ costs \+ dependency map to CSV, JSON, and PDF; Slack/email digest option | P2 |
| F-08 | Multi-Account Support | Scan across AWS Organizations using cross-account IAM roles; aggregate results with per-account drill-down | P2 |

## **5.2  Feature Detail**

### **F-01  Service Inventory Scan**

* Use AWS Resource Groups Tagging API as the primary enumeration layer, supplemented by per-service Describe\* calls for services not fully covered by tagging.

* Normalize results into a unified Service Record schema: {resource\_id, resource\_type, service\_name, region, account\_id, tags, status, last\_seen\_at}.

* Mark resources as ACTIVE, IDLE (0 invocations/connections in last 30 days), or UNKNOWN when activity data is unavailable.

* Support filtering by region, service type, tag key/value, and account.

### **F-02  Cost Attribution**

* Query AWS Cost Explorer GetCostAndUsage API with daily granularity for the trailing 30 days.

* Join cost line items to Service Records using resource ARN, resource ID, and tag matching.

* Expose: daily cost, MTD cost, projected monthly cost, and cost trend (delta vs. prior 30 days).

* Handle untagged resources — surface them in an 'Unattributed Cost' bucket with best-effort resource matching.

### **F-03  Dependency Graph Engine**

* Build a directed graph G(V, E) where V \= service resources and E \= detected dependency relationships.

* Edge detection sources: AWS Config resource relationships, VPC/subnet co-membership, Security Group ingress/egress rules, IAM role trust relationships, Lambda trigger event sources, and CloudTrail API call patterns.

* Classify edges by type: NETWORK, IAM, EVENT, INVOCATION, DATA\_FLOW.

* Expose graph as both a visual rendering (D3/Cytoscape in the dashboard) and a queryable API (JSON adjacency list).

### **F-04  Shutdown Impact Analyzer**

* Accept a target resource ARN or ID as input.

* Return: (1) direct dependents, (2) transitive dependents via BFS/DFS graph traversal, (3) estimated monthly cost savings from shutdown, (4) an impact risk score (Low / Medium / High / Critical) based on number and criticality of dependents.

* Flag services tagged with business-critical or production environment tags as elevated risk.

* Output as structured JSON (CLI) and as a formatted impact report card (dashboard).

# **6\. Scope**

| In Scope (v1.0) | Out of Scope (Future) |
| :---- | :---- |
| EC2, RDS, Lambda, S3, ECS/EKS, ElastiCache, SNS, SQS, API Gateway, CloudFront | Third-party SaaS costs (Datadog, etc.) |
| Cost Explorer \+ Billing API cost pull | Automated shutdown execution (read-only tool) |
| VPC, Security Group, IAM dependency edges | Budget alerts or anomaly detection |
| Single-account and AWS Org multi-account | GCP / Azure cross-cloud support |
| CLI \+ web dashboard output modes | Real-time streaming event ingestion |

# **7\. Technical Approach & AWS API Surface**

## **7.1  Required AWS APIs**

| AWS API / SDK | Purpose | IAM Permission Required |
| :---- | :---- | :---- |
| Resource Groups Tagging API | Cross-service resource enumeration | tag:GetResources |
| AWS Config | Resource relationships & compliance | config:Describe\*, config:List\* |
| Cost Explorer API | Line-item cost attribution | ce:GetCostAndUsage |
| AWS Organizations API | Multi-account discovery | organizations:List\*, organizations:Describe\* |
| CloudTrail / X-Ray | API call-based dependency inference | cloudtrail:LookupEvents, xray:GetServiceGraph |
| EC2 / VPC APIs | Network topology & security groups | ec2:Describe\* |

## **7.2  IAM Permission Principle**

The tool operates with a read-only IAM role. No write, delete, start, or stop permissions are granted. A managed IAM policy document will be provided as part of the tool's deployment package, following the principle of least privilege. Cross-account access is implemented via IAM role assumption (sts:AssumeRole) with a trust policy scoped to the tool's execution role.

## **7.3  Architecture Overview**

* Scanner Layer: Python-based scan orchestrator using Boto3; parallelized per-region workers.

* Data Layer: PostgreSQL or DynamoDB for the service registry; Neo4j (or NetworkX in-memory) for the dependency graph.

* API Layer: FastAPI or equivalent; serves inventory, cost, and graph query endpoints.

* Presentation Layer: React dashboard (optional); first-class CLI for pipeline integration.

* Scheduler: AWS EventBridge or cron-driven Lambda for automated periodic scans.

# **8\. Non-Functional Requirements**

| Category | Requirement | Target Metric |
| :---- | :---- | :---- |
| Performance | Full account scan completes within acceptable time | \< 10 min for ≤500 resources |
| Security | Read-only IAM permissions; no resource mutation | Zero write permissions in policy |
| Reliability | Graceful degradation when APIs are throttled | Exponential backoff, partial results returned |
| Accuracy | Cost data freshness | ≤ 24hr lag from Cost Explorer |
| Scalability | Handle large organizations | ≥50 accounts, ≥10k resources |
| Auditability | Log all API calls made by the tool | Structured JSON scan logs retained 90 days |

# **9\. Milestones & Phased Delivery**

| M\# | Milestone | Deliverables | Target |
| :---- | :---- | :---- | :---- |
| M1 | Foundation | IAM role design, API scaffolding, data models, repo setup | Week 2 |
| M2 | Core Scan | F-01: Service inventory scan across all regions \+ F-02: Cost attribution | Week 5 |
| M3 | Dependency Graph | F-03: Dependency graph engine \+ F-04: Shutdown impact analyzer (CLI) | Week 9 |
| M4 | Dashboard MVP | F-05: Web dashboard \+ F-06: Scheduled refresh | Week 13 |
| M5 | GA Release | F-07: Export/reporting \+ F-08: Multi-account \+ hardening \+ docs | Week 17 |

# **10\. Risks & Mitigations**

| Risk | Severity | Mitigation |
| :---- | :---- | :---- |
| AWS API rate limiting during full scan | Medium | Implement exponential backoff \+ parallel scan with concurrency caps |
| Incomplete dependency graph (unlabeled resources) | High | Require tagging policy; supplement with CloudTrail call graph analysis |
| Cost Explorer data lag (up to 24hr) | Low | Surface data timestamp in UI; add staleness warning badge |
| Cross-account IAM trust misconfiguration | High | Provide Terraform/CDK IAM role template; validate permissions pre-scan |
| Tool misused to trigger unintended shutdowns | Critical | Read-only enforcement in IAM; separate decommission workflow out of scope v1 |

# **11\. Open Questions**

* Should the tool support Terraform state file import as an additional dependency-mapping signal, in addition to live API discovery?

* What is the preferred data store for the dependency graph — an embedded solution (NetworkX) for simplicity or a dedicated graph DB (Neo4j/Neptune) for scale?

* Is real-time Slack/PagerDuty alerting on cost threshold breaches in scope for v1 or deferred?

* Who owns the IAM role provisioning process — the tool's deployment scripts, or the organization's IaC platform team?

* Should the idle resource classification threshold (30 days) be configurable per service type?

# **12\. Glossary**

**Service Record:** Normalized data object representing a single AWS resource in the tool's inventory.

**Dependency Graph:** Directed graph where nodes are AWS resources and edges represent detected dependency relationships.

**Shutdown Impact Score:** Composite risk rating (Low/Medium/High/Critical) returned by the Shutdown Impact Analyzer for a target resource.

**Idle Resource:** A resource that has had zero meaningful activity (invocations, connections, reads/writes) in the past 30 days.

**Edge Type:** Classification of a dependency relationship: NETWORK, IAM, EVENT, INVOCATION, or DATA\_FLOW.

**FinOps:** Financial Operations — the practice of bringing financial accountability to cloud spend.