#!/usr/bin/env bash
# Start DynamoDB Local (Docker), create tables + seed, backend (FastAPI), and frontend (Vite). Ctrl+C stops all.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Use DynamoDB Local so the backend does not hit real AWS (avoids "invalid security token").
export HOUSES_DYNAMODB_ENDPOINT_URL="${HOUSES_DYNAMODB_ENDPOINT_URL:-http://localhost:8001}"

cleanup() {
  docker stop houses-dynamo-local 2>/dev/null || true
  kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT

echo "Starting DynamoDB Local (Docker on port 8001) ..."
docker run --rm -d -p 8001:8000 --name houses-dynamo-local amazon/dynamodb-local:2.3.0
for i in 1 2 3 4 5 6 7 8 9 10; do
  if curl -s -o /dev/null --connect-timeout 1 http://localhost:8001/ 2>/dev/null; then
    break
  fi
  if [ "$i" -eq 10 ]; then
    echo "DynamoDB Local did not become reachable on port 8001. Check Docker."
    exit 1
  fi
  sleep 1
done

echo "Creating tables and seeding data..."
(cd backend && uv run python ../scripts/init_local.py)

echo "Starting backend (http://localhost:8000) ..."
(cd backend && uv run uvicorn app.main:app --reload) &

echo "Starting frontend (http://localhost:5173) ..."
(cd frontend && npm run dev) &

wait
