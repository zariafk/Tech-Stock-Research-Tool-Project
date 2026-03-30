# IAM roles and policies for RAG service Lambdas

# IAM role for ingest Lambda function - needs basic execution permissions
resource "aws_iam_role" "ingest_lambda_role" {
    name = "c22-stocksiphon-rag-ingest-lambda-role"

    assume_role_policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            Effect = "Allow"
            Principal = {
                Service = "lambda.amazonaws.com"
            }
            Action = "sts:AssumeRole"
        }]
    })
}

# Attach AWSLambdaBasicExecutionRole policy to ingest Lambda role
resource "aws_iam_role_policy_attachment" "ingest_lambda_basic_execution" {
    role       = aws_iam_role.ingest_lambda_role.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# IAM role for query Lambda function - needs basic execution permissions
resource "aws_iam_role" "query_lambda_role" {
    name = "c22-stocksiphon-rag-query-lambda-role"

    assume_role_policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            Effect = "Allow"
            Principal = {
                Service = "lambda.amazonaws.com"
            }
            Action = "sts:AssumeRole"
        }]
    })
}

# Attach AWSLambdaBasicExecutionRole policy to query Lambda role
resource "aws_iam_role_policy_attachment" "query_lambda_basic_execution" {
    role       = aws_iam_role.query_lambda_role.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Define IAM policies to allow Lambdas to access Secrets Manager secrets
data "aws_secretsmanager_secret" "trade_research_tool_secrets" {
    name = "c22-trade-research-tool-secrets"
}

# Define inline policies for Lambdas to allow them to read the necessary secrets from Secrets Manager
resource "aws_iam_role_policy" "ingest_lambda_secrets_policy" {
    name = "c22-stocksiphon-rag-ingest-secrets-policy"
    role = aws_iam_role.ingest_lambda_role.id

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            Effect = "Allow"
            Action = [
                "secretsmanager:GetSecretValue"
            ]
            Resource = data.aws_secretsmanager_secret.trade_research_tool_secrets.arn
        }]
    })
}

# Similar policy for query Lambda
resource "aws_iam_role_policy" "query_lambda_secrets_policy" {
    name = "c22-stocksiphon-rag-query-secrets-policy"
    role = aws_iam_role.query_lambda_role.id

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            Effect = "Allow"
            Action = [
                "secretsmanager:GetSecretValue"
            ]
            Resource = data.aws_secretsmanager_secret.trade_research_tool_secrets.arn
        }]
    })
}

# IAM role for ECS task execution for Chroma service - needs permissions to pull container images and write logs
resource "aws_iam_role" "chroma_task_execution_role" {
    name = "c22-stocksiphon-chroma-task-execution-role"

    assume_role_policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            Effect = "Allow"
            Principal = {
                Service = "ecs-tasks.amazonaws.com"
            }
            Action = "sts:AssumeRole"
        }]
    })
}

# Attach AmazonECSTaskExecutionRolePolicy to ECS task execution role
resource "aws_iam_role_policy_attachment" "chroma_task_execution_role_policy" {
    role       = aws_iam_role.chroma_task_execution_role.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}