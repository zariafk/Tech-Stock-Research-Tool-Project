variable "vpc_id" {
    description = "VPC ID for Chroma resources"
    type        = string
}

variable "subnet_ids" {
    description = "Subnets for ECS/EFS"
    type        = list(string)
}

variable "ecs_cluster_name" {
    description = "Existing ECS cluster name"
    type        = string
}

variable "chroma_container_image" {
    description = "Docker image for Chroma"
    type        = string
}

variable "chroma_host" {
    description = "Chroma public host"
    type        = string
}

variable "secret_name" {
    description = "Name of the Secrets Manager secret for Chroma API key"
    type        = string
}