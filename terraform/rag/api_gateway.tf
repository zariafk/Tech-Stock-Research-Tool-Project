# API gateway configuration for RAG service
resource "aws_apigatewayv2_api" "query_api" {
    name          = "c22-stocksiphon-query-api"
    protocol_type = "HTTP"
}

# API Gateway integration with Query Lambda function
resource "aws_apigatewayv2_integration" "query_lambda_integration" {
    api_id                 = aws_apigatewayv2_api.query_api.id
    integration_type       = "AWS_PROXY"
    integration_uri        = aws_lambda_function.query_lambda.invoke_arn
    payload_format_version = "2.0"
}

# Define API Gateway route to trigger Query Lambda function
resource "aws_apigatewayv2_route" "query_route" {
    api_id    = aws_apigatewayv2_api.query_api.id
    route_key = "POST /query"
    target    = "integrations/${aws_apigatewayv2_integration.query_lambda_integration.id}"
}

# Define API Gateway stage
resource "aws_apigatewayv2_stage" "query_stage" {
    api_id      = aws_apigatewayv2_api.query_api.id
    name        = "$default"
    auto_deploy = true
}

# Output the API endpoint URL
resource "aws_lambda_permission" "allow_apigw_invoke_query_lambda" {
    statement_id  = "AllowExecutionFromAPIGateway"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.query_lambda.function_name
    principal     = "apigateway.amazonaws.com"
    source_arn    = "${aws_apigatewayv2_api.query_api.execution_arn}/*/*"
}