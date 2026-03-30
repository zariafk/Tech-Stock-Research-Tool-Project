# Security group for Chroma ECS service
resource "aws_security_group" "chroma_service_sg" {
    name        = "c22-stocksiphon-chroma-service-sg"
    description = "Security group for Chroma ECS service"
    vpc_id      = var.vpc_id

    ingress {
        description = "Allow Chroma HTTP"
        from_port   = 8000
        to_port     = 8000
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags = {
        Name = "c22-stocksiphon-chroma-service-sg"
    }
}

# Security group for EFS mount targets
resource "aws_security_group" "efs_sg" {
    name        = "c22-stocksiphon-chroma-sg"
    description = "Security group for Chroma ECS service"
    vpc_id      = var.vpc_id

    ingress {
        description     = "Allow NFS from Chroma ECS service"
        from_port       = 2049
        to_port         = 2049
        protocol        = "tcp"
        security_groups = [aws_security_group.chroma_service_sg.id]
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags = {
        Name = "c22-stocksiphon-chroma-sg"
    }
}