#!/bin/sh
cd /app || exit 1
echo "[tantos] starting on :8099"
exec python3 -m uvicorn server:app --host 0.0.0.0 --port 8099 --log-level warning
