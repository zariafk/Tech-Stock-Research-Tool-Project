# Terraform configuration for AWS CloudWatch Logs for Chroma ECS service
resource "aws_cloudwatch_log_group" "chroma_logs" {
    name              = "/ecs/c22-stocksiphon-chroma"
    retention_in_days = 14
}