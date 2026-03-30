variable "vpc_id" {
    description = "VPC ID for Chroma resources"
    type        = string
}

variable "subnet_ids" {
    description = "Subnets for ECS/EFS"
    type        = list(string)
}