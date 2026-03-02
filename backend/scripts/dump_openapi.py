"""Dump the FastAPI OpenAPI schema to openapi.yaml in the backend root."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

# Allow importing app when run from repo root or backend/
backend = Path(__file__).resolve().parent.parent
if str(backend) not in sys.path:
    sys.path.insert(0, str(backend))

from app.main import app

if __name__ == "__main__":
    out_path = backend / "openapi.yaml"
    with open(out_path, "w") as f:
        yaml.dump(app.openapi(), f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"Wrote {out_path}")
