resource "aws_efs_file_system" "chroma_efs" {
    creation_token = "c22-stocksiphon-chroma-efs"

    tags = {
        Name = "c22-stocksiphon-chroma-efs"
    }
}

resource "aws_efs_mount_target" "chroma_efs_mount_a" {
    for_each = toset(var.subnet_ids)
    
    file_system_id  = aws_efs_file_system.chroma_efs.id
    subnet_id       = each.value
    security_groups = [aws_security_group.chroma_sg.id]
}

resource "aws_efs_mount_target" "chroma_efs_mount_b" {
    for_each = toset(var.subnet_ids)
    
    file_system_id  = aws_efs_file_system.chroma_efs.id
    subnet_id       = each.value
    security_groups = [aws_security_group.chroma_sg.id]
}

resource "aws_efs_mount_target" "chroma_efs_mount_c" {
    for_each = toset(var.subnet_ids)
    
    file_system_id  = aws_efs_file_system.chroma_efs.id
    subnet_id       = each.value
    security_groups = [aws_security_group.chroma_sg.id]
}