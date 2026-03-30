# Terraform State & Modules

## Shared State Bucket

All `.tfstate` files are stored in a shared S3 bucket so the team works from a single source of truth. The bucket `c22-lito-lmnh-state-bucket` is already live in `eu-west-2` with versioning and AES-256 encryption. **Do not delete or modify it.**

### Setup

Add this block to the top of your Terraform config, replacing `<your-project-path>` with a unique path (e.g. `dashboard`, `envs/prod`, `pipeline`):

```hcl
terraform {
  backend "s3" {
    bucket  = "c22-lito-lmnh-state-bucket"
    key     = "global/<your-project-path>/terraform.tfstate"
    region  = "eu-west-2"
    encrypt = true
  }
}
```

**Every project must have a unique `key`** — duplicate keys will overwrite each other's state. Then run `terraform init`.

---

## Terraform Modules

A module is a reusable folder of `.tf` files — like a function in programming. You define infrastructure once and call it with different parameters wherever you need it. This keeps things consistent, readable, and DRY.

### Project Structure

```
project/
├── main.tf          # Calls your modules
├── variables.tf     # Root-level inputs
├── outputs.tf       # Root-level outputs
├── terraform.tf     # Backend config & providers
└── modules/
    └── s3-bucket/
        ├── main.tf      # Resources
        ├── variables.tf # Inputs
        └── outputs.tf   # Exposed values
```

Every module has three files: `main.tf`, `variables.tf`, `outputs.tf`.

### Example: S3 Bucket Module

**`modules/s3-bucket/variables.tf`** — define inputs:
```hcl
variable "bucket_name" {
  type = string
}
variable "enable_versioning" {
  type    = bool
  default = true
}
```

**`modules/s3-bucket/main.tf`** — define resources using those inputs:
```hcl
resource "aws_s3_bucket" "this" {
  bucket = var.bucket_name
}
resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
```

**`modules/s3-bucket/outputs.tf`** — expose values for other resources:
```hcl
output "bucket_arn" {
  value = aws_s3_bucket.this.arn
}
```

**Root `main.tf`** — call the module:
```hcl
module "data_bucket" {
  source            = "./modules/s3-bucket"
  bucket_name       = "c22-my-data-bucket"
  enable_versioning = true
}
```

Reference outputs elsewhere with `module.data_bucket.bucket_arn`.

---

## Team Rules

1. Use a **unique `key` path** in the S3 backend for each project.
2. **Don't hardcode** values that change between environments — use variables.
3. **One module per logical component** (e.g. a bucket + its encryption + versioning).
4. Run `terraform init` after pulling changes that add or modify modules.
5. Always run `terraform plan` before `terraform apply` — review before you deploy.
