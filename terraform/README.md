# Terraform IAM Template

This directory provides a reference read-only IAM role for AWS Account Intelligence.

## Usage

```bash
terraform init
terraform apply -var='principal_arn=arn:aws:iam::123456789012:role/my-execution-role'
```

The resulting role grants read-only access to the AWS APIs targeted by the current MVP.
