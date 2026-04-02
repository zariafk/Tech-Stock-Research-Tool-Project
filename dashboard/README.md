# Dashboard

Streamlit web app that consolidates market data, news signals, and Reddit sentiment for tracked tech stocks into a single interactive dashboard. Deployed to AWS ECS Fargate via Docker.

## Overview

The dashboard is the user-facing layer of Stock Siphon. It connects to the project's RDS database, reads the data populated by the Alpaca, RSS, and Reddit pipelines, and renders it across two main tabs:

| Tab | Module | What it shows |
|---|---|---|
| **Market Data** | `trends/` | Return vs volatility scatter, relative volume, close-price charts, and combined sentiment lollipop across all tracked tickers. |
| **Search Company** | `summary/` | Single-stock deep dive — live price, news cards, Reddit discussions, signal convergence, sentiment momentum, and an AI-generated company summary via the RAG service. |

A floating **Ask AI** chatbot (`chatbot.py`) is available on every tab, powered by the RAG API.

### Directory Structure

```
dashboard/
├── dashboard.py              # Entrypoint — tab routing and page config
├── chatbot.py                # RAG-powered chatbot sidebar
├── siphon_logo.png           # App branding asset
├── requirements.txt          # Python dependencies
├── Dockerfile                # Container build for ECS
├── deploy_dashboard.sh       # ECR push script
├── .streamlit/config.toml    # Dark theme config
├── summary/                  # Search Company tab
│   ├── dashboard.py          # DB connection, cached queries, layout
│   ├── charts.py             # Altair chart builders
│   ├── helpers.py            # Render functions and formatting
│   └── queries.py            # SQL statements
└── trends/                   # Market Data tab
    ├── dashboard.py          # DB connection, cached queries, layout
    ├── charts.py             # Altair chart builders
    ├── helpers.py            # Render functions and KPI logic
    ├── queries.py            # SQL statements
    └── returnVvolatility.py  # Standalone return/volatility page (legacy)
```

## What You Need

### Credentials & Environment Variables

The app reads database credentials from environment variables (local dev) or AWS Secrets Manager (production).

| Variable | Purpose | Required |
|---|---|---|
| `DB_HOST` | RDS endpoint | Yes (local) |
| `DB_PORT` | PostgreSQL port | No (default `5432`) |
| `DB_NAME` | Database name | Yes (local) |
| `DB_USER` | Database username | Yes (local) |
| `DB_PASSWORD` | Database password | Yes (local) |
| `SECRETS_REPO_NAME` | AWS Secrets Manager secret name | Yes (prod) |
| `RAG_API_URL` | RAG service endpoint for company summaries and chatbot | Yes |

### Dependencies

```
psycopg2-binary
streamlit
pandas
altair
boto3
python-dotenv
```

Install with:

```bash
pip install -r requirements.txt
```

### Prerequisites

- PostgreSQL RDS with the schema initialised (`rds_schema/schema.sql`)
- At least one pipeline (Alpaca, RSS, or Reddit) has populated the database
- RAG service running and accessible at the configured URL

## How to Deploy

The dashboard runs on **AWS ECS Fargate**. Infrastructure is managed in `terraform/dashboard/`.

### 1. Provision infrastructure (first time only)

```bash
cd terraform/dashboard
terraform init
terraform apply
```

This creates the ECS cluster, ECR repository, task definition, Fargate service, and security groups.

### 2. Build and push the Docker image

```bash
cd dashboard
bash deploy_dashboard.sh
```

The script authenticates with ECR, builds a linux/amd64 image, and pushes it as `latest`.

### 3. Force a new deployment (after image update)

```bash
aws ecs update-service \
  --cluster c22-stocksiphon-cluster \
  --service c22-stocksiphon-dashboard-service \
  --force-new-deployment
```

## How to Run Locally

```bash
cd dashboard
```

Create a `.env` file with the required variables:

```env
DB_HOST=your-rds-endpoint
DB_PORT=5432
DB_NAME=your-db-name
DB_USER=your-username
DB_PASSWORD=your-password
SECRETS_REPO_NAME=your-secret-name
RAG_API_URL=aws-rag-api-url
```

Then start the app:

```bash
streamlit run dashboard.py
```

Opens at `http://localhost:8501`.

To run via Docker locally:

```bash
docker build -t stocksiphon-dashboard .
docker run -p 8501:8501 --env-file .env stocksiphon-dashboard
```

## Notes

- **Theme** — a custom dark theme is defined in `.streamlit/config.toml`. No additional configuration needed.
- **Caching** — all database queries use `@st.cache_data` with a 20-minute TTL to reduce RDS load.
- **Comparison mode** — the Search Company tab supports comparing two tickers side-by-side. Enter a second ticker in the comparison input and all charts update automatically.
- **Secrets Manager fallback** — when `DB_HOST` is not set (e.g. running on ECS), the app falls back to AWS Secrets Manager using `SECRETS_REPO_NAME`.

