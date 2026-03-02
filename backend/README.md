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
