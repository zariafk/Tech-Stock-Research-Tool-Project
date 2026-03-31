# Terraform Infrastructure

## Overview

This folder contains all Terraform configuration for the **StockSiphon** (Tech Stock Research Tool) project. It provisions the full AWS infrastructure across six independent modules, each managing a distinct layer of the system:

| Module | Purpose |
|---|---|
| `state_bucket/` | Bootstraps the shared S3 bucket used to store all remote Terraform state |
| `secrets_repository/` | Creates the AWS Secrets Manager secret shell used by all pipelines |
| `database/` | PostgreSQL 16 RDS instance (the central data store) |
| `pipeline/` | Three Lambda functions (RSS, Alpaca, Reddit) + ECR repos + EventBridge schedules |
| `rag/` | Full RAG stack — ChromaDB on ECS Fargate (with EFS), ingest/query Lambdas, API Gateway |
| `dashboard/` | Streamlit dashboard on ECS Fargate |
| `terraform_template/` | Copy-paste template for adding new modules |

All modules deploy to `eu-west-2` (London) under the `c22-stocksiphon-` naming prefix.

---

## What You Need

### Tools

- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.0
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) configured with valid credentials
- [Docker](https://docs.docker.com/get-docker/) (required to build and push container images before deploying pipeline/rag/dashboard)

### AWS Credentials

Configure the AWS CLI with credentials that have permissions to manage ECS, ECR, Lambda, RDS, S3, EFS, API Gateway, Secrets Manager, IAM, EventBridge, and CloudWatch:

```bash
aws configure
# AWS Access Key ID:     <your-key>
# AWS Secret Access Key: <your-secret>
# Default region:        eu-west-2
# Default output format: json
```

### Secrets

All runtime secrets are pulled from AWS Secrets Manager under the secret name `c22-trade-research-tool-secrets`. The secret values must be populated out-of-band (i.e. manually via the AWS console or CLI) before the pipelines or RAG service will function. Sensitive Terraform variables (`db_password`, `openai_api_key`) are passed via `terraform.tfvars` — never commit these files.

### Shared State Bucket

Remote state for all modules is stored in `c22-tsrt-terraform-state` (S3, AES-256, versioning enabled). This bucket is bootstrapped once by `state_bucket/` and must exist before any other module can be initialised. **Do not delete it.**

Each module uses a unique state key under `stocksiphon/<module>/terraform.tfstate`.

---

## How to Deploy

Container images must be built and pushed to ECR before Terraform provisions the Lambda or ECS resources that reference them. The general flow for each module is:

### 1. Build and push the Docker image (pipeline, rag, dashboard)

```bash
# Authenticate Docker with ECR
aws ecr get-login-password --region eu-west-2 | \
  docker login --username AWS --password-stdin \
  129033205317.dkr.ecr.eu-west-2.amazonaws.com

# Build the image (run from the relevant source folder)
docker build -t <ecr-repo-name> .

# Tag and push
docker tag <ecr-repo-name>:latest \
  129033205317.dkr.ecr.eu-west-2.amazonaws.com/<ecr-repo-name>:latest

docker push \
  129033205317.dkr.ecr.eu-west-2.amazonaws.com/<ecr-repo-name>:latest
```

ECR repository names follow the pattern `c22-stocksiphon-{rss,alpaca,reddit,rag-ingest,rag-query,dashboard}-{ecr,lambda}`.

Each pipeline source folder (`pipeline/alpaca/`, `pipeline/rss/`, `pipeline/reddit/`, `rag_service/`, `dashboard/`) contains a `deploy_*.sh` script that wraps the build-tag-push steps above.

### 2. Apply Terraform

```bash
cd terraform/<module>
terraform init
terraform plan
terraform apply
```

### Recommended Deployment Order

Deploy modules in this order to satisfy dependencies:

1. `state_bucket/` — must exist first (run once, state is local)
2. `secrets_repository/` — populate secret values in AWS console after apply
3. `database/` — RDS must be up before pipelines write to it
4. `rag/` — ChromaDB ECS service and Lambda functions
5. `pipeline/` — Lambdas reference the RAG ingest Lambda ARN
6. `dashboard/` — references the shared ECS cluster

---

## How to Run

### Initialise a module

```bash
cd terraform/<module>
terraform init
```

Run this after cloning the repo, or after any change to provider versions or module sources.

### Plan changes

```bash
terraform plan
```

Always review the plan before applying. Check for unexpected resource replacements (shown as `-/+`).

### Apply changes

```bash
terraform apply
```

Type `yes` when prompted, or use `-auto-approve` in CI pipelines.

### Destroy resources

```bash
terraform destroy
```

> **Warning:** The RDS instance is configured with `skip_final_snapshot = true`. Running `terraform destroy` on `database/` will permanently delete all data with no automated backup.

### Start / stop the dashboard service

The dashboard ECS service is deployed with `desired_count = 0` to save costs. To start it:

```bash
aws ecs update-service \
  --cluster c22-stocksiphon-cluster \
  --service c22-stocksiphon-dashboard-service \
  --desired-count 1 \
  --region eu-west-2
```

Set `--desired-count 0` to stop it again.

### Add a new module

Copy `terraform_template/bucket_state.tf` into your new module folder and update the `key` to a unique path under `stocksiphon/`. Then run `terraform init`.

---

## Notes

- **Pipeline schedule:** All three data pipelines (RSS, Alpaca, Reddit) run every 20 minutes, Monday–Friday, 12:00–22:00 UTC via EventBridge Scheduler (`cron(0/20 12-22 ? * MON-FRI *)`).
- **Cross-Lambda invocation:** The pipeline Lambda IAM role has explicit permission to invoke the RAG ingest Lambda (`c22-stocksiphon-rag-ingest-lambda`), which is the mechanism by which ingested data is embedded and stored in ChromaDB.
- **ChromaDB persistence:** The ChromaDB ECS task mounts an EFS volume at `/chroma`, so vector data survives task restarts. The EFS mount targets use transit encryption.
- **RAG query endpoint:** After applying `rag/`, the public API Gateway URL is printed as the `query_api_url` output. Use `terraform output query_api_url` to retrieve it.
- **RDS is publicly accessible:** The RDS security group allows TCP/5432 from `0.0.0.0/0`. This is intentional for development access but should be restricted to known IPs or a VPC CIDR in production.
- **State bucket has `prevent_destroy`:** The `state_bucket/` module uses a `prevent_destroy` lifecycle rule. You cannot accidentally delete the state bucket with `terraform destroy`.
- **Provider versions:** The `rag/` module pins the AWS provider to `~> 5.0` (currently `5.100.0`); `dashboard/` uses `6.38.0`. Keep these consistent when upgrading.
- **VPC / subnets:** All modules share the same VPC (`vpc-03f0d39570fbaa750`) and subnets (`subnet-046ec8b4e41d59ea8`, `subnet-0cfeaca0e941dea5b`, `subnet-055ac264d45bec709`). These are hardcoded in `terraform.tfvars` — update all modules together if the network changes.
