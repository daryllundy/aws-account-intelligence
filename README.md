# AWS Account Intelligence

CLI-first AWS account inventory, cost attribution, dependency mapping, and shutdown impact analysis for a single AWS account.

## What this repo includes

- Snapshot-based scan pipeline
- Shared output schemas for CLI and future API use
- PostgreSQL-backed persistence via SQLAlchemy
- NetworkX dependency graph analysis
- Fixture-backed example scans so contributors can work without AWS credentials
- Terraform templates for read-only IAM setup

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

By default the project runs against local fixture data. Set `AAI_DATA_SOURCE=aws` and valid AWS credentials when you are ready to scan a real account.

### 4. Run a scan

```bash
aws-account-intel scan run --output json
```

### 5. Explore results

```bash
aws-account-intel inventory list --latest --output table
aws-account-intel cost summary --latest --output json
aws-account-intel impact analyze --latest --resource arn:aws:lambda:us-west-2:123456789012:function:process-orders
aws-account-intel graph export --latest --output json
```

## CLI commands

- `scan run`
- `scan status`
- `inventory list`
- `cost summary`
- `graph export`
- `impact analyze --resource <id>`
- `iam validate`

## Local development

- Uses `DATABASE_URL` for SQLAlchemy connectivity
- Defaults to `postgresql+psycopg://postgres:postgres@localhost:5432/aws_account_intelligence`
- `AAI_DATA_SOURCE=fixtures` provides deterministic local data
- Tests run entirely against SQLite in-memory plus mocked fixtures

## Scan limitations

- v1 collector coverage prioritizes EC2, RDS, Lambda, S3, SQS, SNS, API Gateway
- Cost attribution is best-effort and includes explicit confidence metadata
- Dependency edges are evidence-backed and may be incomplete where AWS APIs lack direct linkage
- Idle classification currently uses a fixed 30-day threshold

## IAM

Reference Terraform lives under [`terraform/`](./terraform). The tool is read-only by design and includes an `iam validate` command for permission preflight checks.
