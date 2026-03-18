# Houses Backend

UK house price performance API (FastAPI + DynamoDB).

## Setup

Create and use the virtual environment:

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

Requires Python 3.9+. A minimal `setup.py` is included for editable installs on older pip.

## DynamoDB Local (for dev and tests)

Start DynamoDB Local with Docker Compose:

```bash
docker-compose up -d
```

This runs DynamoDB Local on **http://localhost:8000**. Stop with `docker-compose down`. Data is stored in a Docker named volume (`dynamodb_data`) and persists across restarts.

**Using `scripts/serve.sh` (backend + frontend):** The script starts DynamoDB Local (Docker), runs `scripts/init_local.py` to create tables and seed data, then starts the API and frontend. From the repo root run `./scripts/serve.sh`. No separate DynamoDB or table setup needed.

**Create tables and seed only:** With DynamoDB Local running (e.g. on port 8001), run from the repo root:

```bash
HOUSES_DYNAMODB_ENDPOINT_URL=http://localhost:8001 python scripts/init_local.py
```

Or from the backend dir: `uv run python ../scripts/init_local.py`.

## Run API

```bash
uvicorn app.main:app --reload
```

To use DynamoDB Local:

```bash
export HOUSES_DYNAMODB_ENDPOINT_URL=http://localhost:8000
uvicorn app.main:app --reload
```

## Tests

1. Start DynamoDB: `docker-compose up -d`
2. Run tests: `pytest tests/ -v`

Integration tests use **http://localhost:8000** by default (override with `HOUSES_DYNAMODB_ENDPOINT_URL`). If DynamoDB is not reachable, integration tests are skipped.

Unit only: `pytest tests/unit -v` or `pytest -m "not integration" -v`.

## Deployment (AWS)

Deployment scripts live in `backend/deployment/`. They use the default AWS credential chain (e.g. `aws configure` or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`).

**Create DynamoDB tables in AWS:** Run once per AWS account/region to create the tables (`house_price_performance`, `dimension_index`) that the API expects. Idempotent: tables that already exist are left unchanged.

```bash
cd backend
python deployment/create_dynamo_tables.py
# or: uv run python deployment/create_dynamo_tables.py
```

Override region: `AWS_REGION=us-east-1 python deployment/create_dynamo_tables.py` or `--region us-east-1`.

**Lambda (two-step):**

1. **Create infrastructure (once):** Creates ECR repo, IAM role (Lambda + DynamoDB access), Lambda function, and API Gateway HTTP API. Builds and pushes the Docker image, then creates the Lambda and API. Run from `backend` with Docker and AWS CLI available.

   ```bash
   cd backend
   python deployment/create_lambda_infra.py
   # or: uv run python deployment/create_lambda_infra.py
   ```

   The script prints the API invoke URL; set your frontend `VITE_API_URL` to that URL. If the Lambda already exists, it exits and tells you to use the deploy script.

2. **Deploy (update code):** Builds the image, pushes to ECR, and updates the Lambda function code. Use this for code changes after infrastructure exists.

   ```bash
   cd backend
   python deployment/deploy_lambda.py
   # or: uv run python deployment/deploy_lambda.py
   ```

   Override `--function-name`, `--ecr-repo`, `--region` or set `HOUSES_LAMBDA_FUNCTION_NAME`, `HOUSES_ECR_REPO`, `AWS_REGION`.
