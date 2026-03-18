"""Lambda entrypoint for API Gateway HTTP API (ASGI via Mangum)."""

from __future__ import annotations

from mangum import Mangum

from app.main import app

handler = Mangum(app)
