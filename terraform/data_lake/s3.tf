terraform {
  backend "s3" {
    bucket = "c22-tsrt-terraform-state"
    key    = "stocksiphon/data_lake/terraform.tfstate"
    region = var.aws_region
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "c22_stocksiphon_s3_bucket" {
  bucket = "c22-stocksiphon-s3-bucket"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bucket_encryption" {
  bucket = aws_s3_bucket.c22_stocksiphon_s3_bucket.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_glue_catalog_database" "c22_stocksiphon_glue_database" {
  name = "c22_stocksiphon_glue_database"
}

resource "aws_iam_role" "c22_stocksiphon_glue_crawler_role" {
  name = "c22_stocksiphon_glue_crawler_role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "c22_stocksiphon_glue_service_policy" {
  role       = aws_iam_role.c22_stocksiphon_glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "c22_stocksiphon_s3_read_policy" {
  role       = aws_iam_role.c22_stocksiphon_glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_glue_crawler" "c22_stocksiphon_glue_crawler" {
  name          = "c22-stocksiphon-glue-crawler"
  role          = aws_iam_role.c22_stocksiphon_glue_crawler_role.arn
  database_name = aws_glue_catalog_database.c22_stocksiphon_glue_database.name

  s3_target {
    path = "s3://${aws_s3_bucket.c22_stocksiphon_s3_bucket.id}/"
  }
}

resource "aws_secretsmanager_secret" "c22_stocksiphon_api_keys" {
  name = "c22-stocksiphon-api-keys"
}

resource "aws_secretsmanager_secret_version" "c22_stocksiphon_api_keys_version" {
  secret_id = aws_secretsmanager_secret.c22_stocksiphon_api_keys.id

  secret_string = jsonencode({
    openai_api_key       = var.openai_api_key
  })
}
