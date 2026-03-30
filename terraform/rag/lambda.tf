# Terraform configuration for AWS Lambda functions used in the RAG service

# Defines aws account information
data "aws_caller_identity" "current" {}

# Ingest Lambda function - responsible for ingesting and processing data, and storing it in the vector database
resource "aws_lambda_function" "ingest_lambda" {
    function_name = "c22-stocksiphon-rag-ingest-lambda"
    role          = aws_iam_role.ingest_lambda_role.arn
    package_type  = "Image"
    image_uri     = "${data.aws_caller_identity.current.account_id}.dkr.ecr.eu-west-2.amazonaws.com/c22-stocksiphon-ingest-lambda:latest"
    timeout       = 300
    memory_size   = 1024

    environment {
        variables = {
            SECRET_NAME = var.secret_name
            CHROMA_HOST = var.chroma_host
        }
    }
}

# Query Lambda function - responsible for handling user queries, retrieving relevant data, and generating responses using the LLM
resource "aws_lambda_function" "query_lambda" {
    function_name = "c22-stocksiphon-rag-query-lambda"
    role          = aws_iam_role.query_lambda_role.arn
    package_type  = "Image"
    image_uri     = "${data.aws_caller_identity.current.account_id}.dkr.ecr.eu-west-2.amazonaws.com/c22-stocksiphon-query-lambda:latest"
    timeout       = 60
    memory_size   = 512

    environment {
        variables = {
            SECRET_NAME = var.secret_name
            CHROMA_HOST = var.chroma_host
        }
    }
}