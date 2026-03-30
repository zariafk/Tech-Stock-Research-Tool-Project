resource "aws_ecs_task_definition" "chroma_task" {
    family                   = "c22-stocksiphon-chroma"
    requires_compatibilities = ["FARGATE"]
    network_mode             = "awsvpc"
    cpu                      = "512"
    memory                   = "1024"
    execution_role_arn       = aws_iam_role.chroma_task_execution_role.arn

    container_definitions = jsonencode([
        {
            name      = "chroma"
            image     = var.chroma_container_image
            essential = true

            portMappings = [{
                containerPort = 8000
                hostPort      = 8000
                protocol      = "tcp"
            }]

            mountPoints = [{
                sourceVolume  = "chroma-storage"
                containerPath = "/chroma"
                readOnly      = false
            }]

            logConfiguration = {
                logDriver = "awslogs"
                options = {
                    awslogs-group         = aws_cloudwatch_log_group.chroma_logs.name
                    awslogs-region        = "eu-west-2"
                    awslogs-stream-prefix = "chroma"
                }
            }
        }
    ])

    volume {
        name = "chroma-storage"

        efs_volume_configuration {
            file_system_id = aws_efs_file_system.chroma_efs.id

            transit_encryption = "ENABLED"
        }
    }
}