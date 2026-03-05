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
  description = "Name of the read-only role for AWS Account Intelligence"
  default     = "aws-account-intelligence-readonly"
}

variable "principal_arn" {
  type        = string
  description = "Principal allowed to assume the read-only role"
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
  description = "Read-only policy for AWS Account Intelligence"
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
        AWS = var.principal_arn
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
