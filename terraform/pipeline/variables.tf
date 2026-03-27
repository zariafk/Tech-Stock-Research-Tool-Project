variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "openai_api_key" {
  type      = string
  sensitive = true
}

