
terraform {
  required_providers {
        aws = {
            source  = "hashicorp/aws"
            version = "~> 5.0"
        }
  }

  backend "s3" {
    bucket = "c22-tsrt-terraform-state"
    key    = "stocksiphon/pipeline/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_ecr_repository" "c22_stocksiphon_rss_ecr" {
  name = "c22-stocksiphon-rss-ecr"
}

resource "aws_ecr_repository" "c22_stocksiphon_alpaca_ecr" {
  name = "c22-stocksiphon-alpaca-ecr"
}

resource "aws_ecr_repository" "c22_stocksiphon_reddit_ecr" {
  name = "c22-stocksiphon-reddit-ecr"
}
