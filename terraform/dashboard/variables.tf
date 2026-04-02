variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "openai_api_key" {
  type      = string
  sensitive = true
}

variable "subnet_ids" {
  description = "Subnets for ECS Fargate tasks"
  type        = list(string)
}

variable "vpc_id" {
  description = "VPC ID for the security group"
  type        = string
}

variable "secrets_repo_name" {
  description = "Name of the AWS Secrets Manager secret containing database credentials"
  type        = string
  sensitive   = true
}
