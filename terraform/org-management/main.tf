terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

variable "policy_name" {
  type        = string
  description = "Name of the management-account policy for AWS Account Intelligence org scanning"
  default     = "aws-account-intelligence-org-scanner"
}

variable "member_role_name" {
  type        = string
  description = "Role name to assume in member accounts"
  default     = "aws-account-intelligence-readonly"
}

variable "member_account_ids" {
  type        = list(string)
  description = "Optional explicit list of member account IDs to scope sts:AssumeRole"
  default     = []
}

variable "principal_arn" {
  type        = string
  description = "Optional principal ARN to attach the management policy to directly"
  default     = null
}

locals {
  assume_role_resources = length(var.member_account_ids) > 0 ? [
    for account_id in var.member_account_ids : "arn:aws:iam::${account_id}:role/${var.member_role_name}"
  ] : ["arn:aws:iam::*:role/${var.member_role_name}"]
}

resource "aws_iam_policy" "org_scanner" {
  name        = var.policy_name
  description = "Management-account policy for AWS Account Intelligence org scanning"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "organizations:Describe*",
          "organizations:List*",
          "sts:GetCallerIdentity"
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["sts:AssumeRole"]
        Resource = local.assume_role_resources
      }
    ]
  })
}

output "policy_arn" {
  value = aws_iam_policy.org_scanner.arn
}

output "member_role_name" {
  value = var.member_role_name
}

output "assume_role_resources" {
  value = local.assume_role_resources
}
