# AWS Account Intelligence

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![uv](https://img.shields.io/badge/env-uv-6A4CFF)
![FastAPI](https://img.shields.io/badge/api-FastAPI-009688)
![Single Account](https://img.shields.io/badge/focus-single--account-2F855A)

CLI-first AWS inventory, cost, dependency, and shutdown-impact tooling for a single AWS account. It includes a shared CLI, API, and dashboard, with AWS Organizations support kept as an optional advanced path.

## Highlights

- Snapshot-based scans with persisted inventory, costs, dependency edges, schedules, and deltas
- CLI, FastAPI, and dashboard surfaces built on shared schemas
- Report export formats: `json`, `csv`, `pdf`, `slack`, `email`
- Audit logs and benchmark output under `.aai-output/`
- Terraform reference templates for single-account and optional org-scanning IAM setup

## Quick Start

### 1. Start Postgres

```bash
docker compose up -d postgres
```

### 2. Install dependencies

```bash
uv sync --extra dev
```

### 3. Configure environment

```bash
cp .env.example .env
```

By default the project runs against fixture data. For the main supported path, set `AAI_DATA_SOURCE=aws` for a single AWS account. `AAI_DATA_SOURCE=aws_orgs` remains available as an optional advanced mode.

### 4. Run a scan

```bash
export AAI_DATA_SOURCE=aws
uv run aws-account-intel scan run --output json
```

## Common Commands

### Scan and inspect

```bash
uv run aws-account-intel scan run --output json
uv run aws-account-intel scan status --latest --output json
uv run aws-account-intel scan delta --latest --output json
```

### Inventory and impact

```bash
uv run aws-account-intel inventory list --latest --output table
uv run aws-account-intel cost summary --latest --output json
uv run aws-account-intel graph export --latest --output json
uv run aws-account-intel impact analyze --latest --resource arn:aws:lambda:us-west-2:123456789012:function:process-orders --output json
```

### Scheduling and reporting

```bash
uv run aws-account-intel schedule create nightly --interval-hours 24 --output json
uv run aws-account-intel report export --latest --format pdf
uv run aws-account-intel iam validate --output json
```

## API and Dashboard

Start the API:

```bash
uv run aws-account-intel api serve --host 127.0.0.1 --port 8000
```

Dashboard:

- [http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard)

Key routes:

- `GET /health`
- `GET /scans`
- `GET /scans/latest`
- `GET /inventory`
- `GET /costs/summary`
- `GET /graph`
- `GET /impact`
- `GET /accounts/summary`
- `GET /schedules`

## Reporting

Reports are written to `.aai-output/reports/` unless `--destination` is provided.

- `json`: full machine-readable snapshot bundle
- `csv`: flattened resource, cost, and risk rows
- `pdf`: operator-friendly summary report
- `slack`: plain-text digest
- `email`: plain-text digest with subject line and top risks

## Terraform

Reference Terraform lives under [`terraform/`](./terraform):

- `terraform/`: single-account role
- `terraform/org-member/`: optional member-account cross-account read-only role
- `terraform/org-management/`: optional management/scanner policy for org discovery and assume-role

See [terraform/README.md](/Users/daryl/work/aws-service-scanner/terraform/README.md) for the single-account default path and the optional org-scanning path.

## Optional Advanced: AWS Organizations

Use this only if you later move the repo into an AWS Organization-backed environment.

Required environment:

```bash
export AAI_DATA_SOURCE=aws_orgs
export AAI_AWS_ORG_ROLE_NAME=aws-account-intelligence-readonly
export AAI_AWS_ORG_ACCOUNT_LIMIT=25
```

Prerequisites:

- management/scanner identity with `organizations:ListAccounts`
- management/scanner identity with `sts:AssumeRole` into the member role
- member accounts with the cross-account role deployed

Org smoke test:

```bash
uv run python scripts/smoke_aws_orgs.py
```

## Development Notes

- Uses `DATABASE_URL` for SQLAlchemy connectivity
- Defaults to `postgresql+psycopg://postgres:postgres@localhost:5432/aws_account_intelligence`
- `AAI_DATA_SOURCE=fixtures` provides deterministic local data
- Tests run against SQLite plus mocked AWS fixtures
- The org smoke test falls back to a local SQLite file under `.aai-output/` if `DATABASE_URL` is unset

## Limitations

- Collector coverage is strongest for EC2, VPC/subnet/security-group, RDS, Lambda, S3, SQS, SNS, API Gateway, ECS, EKS, ElastiCache, ECR, and CloudFront
- Cost attribution remains best-effort even with stronger matching and freshness metadata
- Dependency edges are evidence-backed and may still be incomplete where AWS APIs lack direct linkage
- Idle classification still uses a fixed 30-day threshold
