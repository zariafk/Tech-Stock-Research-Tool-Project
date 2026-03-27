variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for the security group"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for the RDS subnet group"
  type        = list(string)
}

variable "openai_api_key" {
  type      = string
  sensitive = true
}

variable "db_name" {
  type        = string
  description = "Name of the RDS database"
}

variable "db_username" {
  type        = string
  description = "Username for the RDS database"
}

variable "db_password" {
  type        = string
  description = "Password for the RDS database"
  sensitive   = true
}
