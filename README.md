# AWS Account Intelligence

CLI-first AWS account inventory, cost attribution, dependency mapping, reporting, and shutdown impact analysis for a single AWS account, with optional advanced support for AWS Organizations.

## What the repo includes

- Snapshot-based scan pipeline with persisted inventory, costs, dependency edges, schedules, and deltas
- CLI, FastAPI, and dashboard surfaces built on shared schemas
- Report export formats: `json`, `csv`, `pdf`, `slack`, `email`
- Optional AWS Organizations collector with per-account summaries and drill-down filters
- Audit logging and benchmark tooling under `.aai-output/`
- Terraform reference templates for single-account and org-scanning IAM setup

## Quick start

### 1. Start Postgres

```bash
docker compose up -d postgres
```

### 2. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

### 3. Configure environment

```bash
cp .env.example .env
```

By default the project runs against fixture data. For the primary supported path in this repo, use `AAI_DATA_SOURCE=aws` for your single AWS account. `AAI_DATA_SOURCE=aws_orgs` remains available as an optional advanced path for future multi-account use.

### 4. Run a scan

```bash
export AAI_DATA_SOURCE=aws
aws-account-intel scan run --output json
```

## CLI overview

### Scan

```bash
aws-account-intel scan run --output json
aws-account-intel scan status --latest --output json
aws-account-intel scan delta --latest --output json
aws-account-intel scan benchmark --runs 3 --output json
```

### Inventory, cost, graph, and impact

```bash
aws-account-intel inventory list --latest --output table
aws-account-intel inventory list --latest --account-id 123456789012 --output json
aws-account-intel cost summary --latest --output json
aws-account-intel cost summary --latest --account-id 123456789012 --output json
aws-account-intel graph export --latest --output json
aws-account-intel impact analyze --latest --resource arn:aws:lambda:us-west-2:123456789012:function:process-orders --output json
aws-account-intel account summary --latest --output json
```

### Scheduling

```bash
aws-account-intel schedule create nightly --interval-hours 24 --output json
aws-account-intel schedule list --output json
aws-account-intel schedule run-due --output json
```

### Reporting

```bash
aws-account-intel report export --latest --format json
aws-account-intel report export --latest --format csv
aws-account-intel report export --latest --format pdf
aws-account-intel report export --latest --format slack
aws-account-intel report export --latest --format email
```

### IAM validation

```bash
aws-account-intel iam validate --output json
```

## Supported operating model

This repo is optimized for:

- one human user
- one AWS account
- one operator-owned CLI/API/dashboard workflow

Production-ready for this repo means:

- single-account scans work with `AAI_DATA_SOURCE=aws`
- reporting works
- dashboard and API work
- audit logs and benchmark output work
- IAM validation works for one account

`aws_orgs` is kept in the repo, but it is not required for personal single-account use.

## API and dashboard

Start the API:

```bash
aws-account-intel api serve --host 127.0.0.1 --port 8000
```

Dashboard:

- [http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard)

Key routes:

- `GET /health`
- `GET /`
- `GET /dashboard`
- `GET /scans`
- `GET /scans/latest`
- `GET /scans/{scan_run_id}`
- `GET /scans/{scan_run_id}/delta`
- `GET /inventory`
- `GET /costs/summary`
- `GET /graph`
- `GET /impact`
- `GET /accounts/summary`
- `GET /schedules`

## Reporting outputs

Reports are written to `.aai-output/reports/` unless `--destination` is provided.

- `json`: full machine-readable snapshot bundle
- `csv`: flattened resource + cost + risk rows
- `pdf`: operator-friendly summary report
- `slack`: plain-text digest suitable for paste/send
- `email`: plain-text email digest with subject line and top risks

## Audit logs and benchmark output

- Audit logs are written to `.aai-output/audit/*.jsonl`
- Events currently include:
  - `scan_run_started`
  - `scan_run_completed`
  - `scan_benchmark_completed`
  - `api_request`
- Cost summaries expose:
  - `cost_freshness_at`
  - `cost_freshness_age_hours`
  - `cost_freshness_status`
- `scan benchmark` is intended to produce repeatable timing artifacts before larger deployments or collector changes

## Optional advanced: AWS Organizations scans

Use this only if you later move the repo into an AWS Organization-backed environment.

To use `AAI_DATA_SOURCE=aws_orgs`:

- the management/scanner identity must have `organizations:ListAccounts`
- the management/scanner identity must have `sts:AssumeRole` into the member role
- member accounts must have the cross-account role deployed
- the runtime must be configured with:

```bash
export AAI_DATA_SOURCE=aws_orgs
export AAI_AWS_ORG_ROLE_NAME=aws-account-intelligence-readonly
export AAI_AWS_ORG_ACCOUNT_LIMIT=25
```

### Current verified state on this machine

The current local AWS CLI identity resolves successfully, but the current AWS account is not operating inside an AWS Organization. That means `aws_orgs` is not part of the normal workflow for this repo right now.

## Optional advanced: org smoke test

Run the checked-in smoke test only after IAM and Terraform setup is complete:

```bash
python scripts/smoke_aws_orgs.py
```

The smoke test:

- verifies caller identity
- verifies Organizations account discovery
- runs a lightweight real org scan
- verifies account summary data exists
- verifies inventory rows contain account IDs
- exits non-zero with clear prerequisite errors for:
  - missing Organizations access
  - missing member role deployment
  - assume-role failures
  - empty scan results

The script is read-only and manual by design. It is not intended for CI, and its failure is not a blocker in a single-account environment.

## Terraform

Reference Terraform lives under [`terraform/`](./terraform):

- `terraform/`: single-account role
- `terraform/org-member/`: optional member-account cross-account read-only role
- `terraform/org-management/`: optional management/scanner policy for org discovery and assume-role

See [terraform/README.md](/Users/daryl/work/aws-service-scanner/terraform/README.md) for the single-account default path and the optional org-scanning path.

## Local development notes

- Uses `DATABASE_URL` for SQLAlchemy connectivity
- Defaults to `postgresql+psycopg://postgres:postgres@localhost:5432/aws_account_intelligence`
- `AAI_DATA_SOURCE=fixtures` provides deterministic local data
- tests run against SQLite plus mocked AWS fixtures
- the org smoke test falls back to a local SQLite file under `.aai-output/` if `DATABASE_URL` is unset

## Scan limitations

- collector coverage is strongest for EC2, RDS, Lambda, S3, SQS, SNS, API Gateway, ECS, EKS, ElastiCache, and CloudFront
- cost attribution remains best-effort even with stronger matching and freshness metadata
- dependency edges are evidence-backed and may still be incomplete where AWS APIs lack direct linkage
- idle classification still uses a fixed 30-day threshold
