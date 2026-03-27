terraform {
  backend "s3" {
    bucket  = "c22-tsrt-terraform-state" 
    key     = "global/secrets_repository/terraform.tfstate"
    region  = "eu-west-2"
    encrypt = true
  }
}

resource "aws_secretsmanager_secret" "reddit_pipeline" {
  name        = "reddit-pipeline/s3-credentials"
  description = "S3 credentials and bucket name for the Reddit ETL pipeline"
}