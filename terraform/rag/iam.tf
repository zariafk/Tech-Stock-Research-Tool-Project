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