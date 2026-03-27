
resource "aws_iam_role" "c22_stocksiphon_scheduler_role" {
  name = "c22-stocksiphon-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "scheduler.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "c22_stocksiphon_scheduler_lambda_policy" {
  name = "c22-stocksiphon-scheduler-lambda-policy"
  role = aws_iam_role.c22_stocksiphon_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["lambda:InvokeFunction"]
      Resource = [
        aws_lambda_function.c22_stocksiphon_rss_lambda.arn,
        aws_lambda_function.c22_stocksiphon_alpaca_lambda.arn,
        aws_lambda_function.c22_stocksiphon_reddit_lambda.arn
      ]
    }]
  })
}

resource "aws_scheduler_schedule" "c22_stocksiphon_rss_schedule" {
  name                = "c22-stocksiphon-rss-schedule"
  schedule_expression = "cron(0/20 * * * ? *)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.c22_stocksiphon_rss_lambda.arn
    role_arn = aws_iam_role.c22_stocksiphon_scheduler_role.arn
  }
}

resource "aws_scheduler_schedule" "c22_stocksiphon_alpaca_schedule" {
  name                = "c22-stocksiphon-alpaca-schedule"
  schedule_expression = "cron(0/20 * * * ? *)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.c22_stocksiphon_alpaca_lambda.arn
    role_arn = aws_iam_role.c22_stocksiphon_scheduler_role.arn
  }
}

resource "aws_scheduler_schedule" "c22_stocksiphon_reddit_schedule" {
  name                = "c22-stocksiphon-reddit-schedule"
  schedule_expression = "cron(0/20 * * * ? *)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.c22_stocksiphon_reddit_lambda.arn
    role_arn = aws_iam_role.c22_stocksiphon_scheduler_role.arn
  }
}
