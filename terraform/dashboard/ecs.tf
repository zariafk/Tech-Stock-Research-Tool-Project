terraform {
  backend "s3" {
    bucket = "c22-tsrt-terraform-state"
    key    = "stocksiphon/dashboard/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_ecs_cluster" "c22_stocksiphon_cluster" {
  name = "c22-stocksiphon-cluster"
}

resource "aws_ecr_repository" "c22_stocksiphon_dashboard" {
  name = "c22-stocksiphon-dashboard"
}

resource "aws_iam_role" "c22_stocksiphon_ecs_execution_role" {
  name = "c22-stocksiphon-ecs-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "c22_stocksiphon_ecs_execution_policy" {
  role       = aws_iam_role.c22_stocksiphon_ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "c22_stocksiphon_dashboard_task_role" {
  name = "c22-stocksiphon-dashboard-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "c22_stocksiphon_dashboard_task_policy" {
  name = "c22-stocksiphon-dashboard-task-policy"
  role = aws_iam_role.c22_stocksiphon_dashboard_task_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:GetBucketLocation", "s3:ListBucket"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_security_group" "c22_stocksiphon_dashboard_sg" {
  name   = "c22-stocksiphon-dashboard-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 8501
    to_port     = 8501
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

resource "aws_cloudwatch_log_group" "c22_stocksiphon_dashboard_logs" {
  name              = "/ecs/c22-stocksiphon-dashboard"
  retention_in_days = 30
}

resource "aws_ecs_task_definition" "c22_stocksiphon_dashboard_task" {
  family                   = "c22-stocksiphon-dashboard-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.c22_stocksiphon_ecs_execution_role.arn
  task_role_arn            = aws_iam_role.c22_stocksiphon_dashboard_task_role.arn

  container_definitions = jsonencode([{
    name  = "stocksiphon-dashboard"
    image = "${aws_ecr_repository.c22_stocksiphon_dashboard.repository_url}:latest"
    portMappings = [{
      containerPort = 8501
      hostPort      = 8501
    }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/c22-stocksiphon-dashboard"
        "awslogs-region"        = "eu-west-2"
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])
}

resource "aws_ecs_service" "c22_stocksiphon_dashboard_service" {
  name            = "c22-stocksiphon-dashboard-service"
  cluster         = aws_ecs_cluster.c22_stocksiphon_cluster.id
  task_definition = aws_ecs_task_definition.c22_stocksiphon_dashboard_task.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [aws_security_group.c22_stocksiphon_dashboard_sg.id]
    assign_public_ip = true
  }
}

