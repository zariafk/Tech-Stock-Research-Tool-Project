# 1. The S3 Bucket
resource "aws_s3_bucket" "terraform_state" {
  bucket = "c22-lito-lmnh-state-bucket" 

  # Prevent accidental deletion of this bucket
  lifecycle {
    prevent_destroy = true
  }
}

# 2. Enable Versioning (Crucial for state recovery)
resource "aws_s3_bucket_versioning" "enabled" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# 3. Enable Server-Side Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "default" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}