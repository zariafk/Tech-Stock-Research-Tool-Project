terraform {
  backend "s3" {
    bucket = "c22-tsrt-terraform-state"
    key    = "stocksiphon/database/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_db_subnet_group" "c22_stocksiphon_rds_subnet_group" {
  name       = "c22-stocksiphon-rds-subnet-group"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "c22_stocksiphon_rds_sg" {
  name   = "c22-stocksiphon-rds-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "c22_stocksiphon_rds" {
  identifier        = "c22-stocksiphon-rds"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = "db.t3.micro"
  allocated_storage = 20

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  publicly_accessible    = true
  skip_final_snapshot    = true
  db_subnet_group_name   = aws_db_subnet_group.c22_stocksiphon_rds_subnet_group.name
  vpc_security_group_ids = [aws_security_group.c22_stocksiphon_rds_sg.id]
}

