resource "aws_lambda_function" "c22_stocksiphon_rss_lambda" {
  function_name = "c22-stocksiphon-rss-lambda"
  role          = aws_iam_role.c22_stocksiphon_lambda_role.arn
  image_uri     = "${aws_ecr_repository.c22_stocksiphon_rss_ecr.repository_url}:latest"
  package_type  = "Image"
  timeout       = 600
}

resource "aws_lambda_function" "c22_stocksiphon_alpaca_lambda" {
  function_name = "c22-stocksiphon-alpaca-lambda"
  role          = aws_iam_role.c22_stocksiphon_lambda_role.arn
  image_uri     = "${aws_ecr_repository.c22_stocksiphon_alpaca_ecr.repository_url}:latest"
  package_type  = "Image"
  timeout       = 600
}

resource "aws_lambda_function" "c22_stocksiphon_reddit_lambda" {
  function_name = "c22-stocksiphon-reddit-lambda"
  role          = aws_iam_role.c22_stocksiphon_lambda_role.arn
  image_uri     = "${aws_ecr_repository.c22_stocksiphon_reddit_ecr.repository_url}:latest"
  package_type  = "Image"
  timeout       = 600
}

resource "aws_iam_role" "c22_stocksiphon_lambda_role" {
  name = "c22-stocksiphon-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "c22_stocksiphon_lambda_policy" {
  role       = aws_iam_role.c22_stocksiphon_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "c22_stocksiphon_lambda_ecr_policy" {
  role       = aws_iam_role.c22_stocksiphon_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role_policy" "invoke_rag_ingest_lambda" {
  name = "c22-stocksiphon-invoke-rag-ingest"
  role = aws_iam_role.c22_stocksiphon_lambda_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = "arn:aws:lambda:eu-west-2:129033205317:function:c22-stocksiphon-rag-ingest-lambda"
      }
    ]
  })
}

data "aws_secretsmanager_secret" "c22_trade_research_tool_secrets" {
  name = "c22-trade-research-tool-secrets"
}

resource "aws_iam_role_policy" "c22_stocksiphon_lambda_secrets_policy" {
  name = "c22-stocksiphon-lambda-secrets-policy"
  role = aws_iam_role.c22_stocksiphon_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = data.aws_secretsmanager_secret.c22_trade_research_tool_secrets.arn
    }]
  })
}

