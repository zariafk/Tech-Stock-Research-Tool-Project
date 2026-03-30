# Terraform configuration for AWS EFS for Chroma ECS service
resource "aws_efs_file_system" "chroma_efs" {
    creation_token = "c22-stocksiphon-chroma-efs"

    tags = {
        Name = "c22-stocksiphon-chroma-efs"
    }
}

# Create EFS mount targets in each subnet
resource "aws_efs_mount_target" "chroma_efs_mounts" {
    for_each = toset(var.subnet_ids)
    
    file_system_id  = aws_efs_file_system.chroma_efs.id
    subnet_id       = each.value
    security_groups = [aws_security_group.efs_sg.id]
}