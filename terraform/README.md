# Terraform IAM Templates

This directory contains three Terraform entrypoints:

- `terraform/`
  Single-account read-only role for local or single-account scans.
- `terraform/org-member/`
  Optional cross-account read-only role deployed into each AWS Organization member account.
- `terraform/org-management/`
  Optional management-account policy for the scanner identity that discovers accounts and assumes the member role.

## Single-account usage

Use the root module when you are scanning one account directly.

```bash
cd terraform
terraform init
terraform apply -var='principal_arn=arn:aws:iam::123456789012:role/my-execution-role'
```

Output:

- `role_arn`

## Optional advanced: AWS Organizations usage

The `aws_orgs` data source requires both a management-account policy and a member-account role. This path is only relevant if you later use this repo in an AWS Organization-backed environment.

### 1. Deploy the member-account role

Apply this module in each member account that should be scanned.

```bash
cd terraform/org-member
terraform init
terraform apply -var='management_principal_arn=arn:aws:iam::123456789012:user/daryl-cli'
```

Outputs:

- `role_arn`
- `role_name`
- `policy_arn`

Default role name:

- `aws-account-intelligence-readonly`

### 2. Grant management-account scanner permissions

Apply this module in the management/scanner account.

```bash
cd terraform/org-management
terraform init
terraform apply -var='member_account_ids=["111122223333","444455556666"]'
```

Outputs:

- `policy_arn`
- `member_role_name`
- `assume_role_resources`

This module grants:

- `organizations:Describe*`
- `organizations:List*`
- `sts:AssumeRole` into the member role

### 3. Configure the tool

```bash
export AAI_DATA_SOURCE=aws_orgs
export AAI_AWS_ORG_ROLE_NAME=aws-account-intelligence-readonly
```

### 4. Run the org smoke test

```bash
uv run python scripts/smoke_aws_orgs.py
```

## Rollout order

1. Apply `terraform/org-member` in member accounts.
2. Apply `terraform/org-management` in the management/scanner account.
3. Set `AAI_DATA_SOURCE=aws_orgs`.
4. Run `uv run python scripts/smoke_aws_orgs.py`.

If you are using this repo as a personal single-account tool, you can ignore the `org-member` and `org-management` templates entirely.
