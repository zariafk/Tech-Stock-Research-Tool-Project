resource "aws_security_group" "chroma_sg" {
    name        = "c22-stocksiphon-chroma-sg"
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
        Name = "c22-stocksiphon-chroma-sg"
    }
}