terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

variable "tool_role_name" {
  type        = string
  description = "Name of the org member role for AWS Account Intelligence"
  default     = "aws-account-intelligence-readonly"
}

variable "management_principal_arn" {
  type        = string
  description = "Management/scanner principal allowed to assume the member-account role"
}

locals {
  readonly_actions = [
    "tag:GetResources",
    "config:Describe*",
    "config:List*",
    "ce:GetCostAndUsage",
    "ec2:Describe*",
    "rds:Describe*",
    "lambda:Get*",
    "lambda:List*",
    "s3:GetBucket*",
    "s3:List*",
    "sqs:GetQueueAttributes",
    "sqs:ListQueues",
    "sns:GetTopicAttributes",
    "sns:List*",
    "apigateway:GET",
    "cloudtrail:LookupEvents",
    "xray:GetServiceGraph",
    "organizations:Describe*",
    "organizations:List*",
    "sts:GetCallerIdentity"
  ]
}

resource "aws_iam_policy" "readonly" {
  name        = "${var.tool_role_name}-policy"
  description = "Read-only policy for AWS Account Intelligence org member scans"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = local.readonly_actions
      Resource = "*"
    }]
  })
}

resource "aws_iam_role" "tool" {
  name = var.tool_role_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = var.management_principal_arn
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "attach_readonly" {
  role       = aws_iam_role.tool.name
  policy_arn = aws_iam_policy.readonly.arn
}

output "role_arn" {
  value = aws_iam_role.tool.arn
}

output "role_name" {
  value = aws_iam_role.tool.name
}

output "policy_arn" {
  value = aws_iam_policy.readonly.arn
}
