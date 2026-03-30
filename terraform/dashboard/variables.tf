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