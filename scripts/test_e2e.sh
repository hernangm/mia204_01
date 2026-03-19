#!/usr/bin/env bash
# End-to-end smoke test for the hydrocarbon forecast platform.
#
# Prerequisites: docker-compose up -d  (all services healthy)
#
# Usage:
#   bash scripts/test_e2e.sh
set -euo pipefail

API_BASE="http://localhost:8000"
AIRFLOW_BASE="http://localhost:8080"
MLFLOW_BASE="http://localhost:5000"

echo "=== 1. Check API health ==="
curl -sf "${API_BASE}/docs" > /dev/null && echo "OK: FastAPI docs reachable" || echo "FAIL: FastAPI not reachable"

echo ""
echo "=== 2. Check MLflow health ==="
curl -sf "${MLFLOW_BASE}/health" > /dev/null && echo "OK: MLflow reachable" || echo "FAIL: MLflow not reachable"

echo ""
echo "=== 3. Check Airflow API ==="
curl -sf "${AIRFLOW_BASE}/api/v2/version" > /dev/null && echo "OK: Airflow API reachable" || echo "FAIL: Airflow API not reachable"

echo ""
echo "=== 4. Test GET /api/v1/wells ==="
WELLS_RESP=$(curl -sf "${API_BASE}/api/v1/wells?date_query=2024-01-01" || echo "FAIL")
if [ "$WELLS_RESP" != "FAIL" ]; then
  echo "OK: /api/v1/wells returned data"
  echo "$WELLS_RESP" | python -m json.tool | head -20
else
  echo "FAIL: /api/v1/wells not responding (data may not be loaded yet)"
fi

echo ""
echo "=== 5. Test GET /api/v1/forecast ==="
FORECAST_RESP=$(curl -sf "${API_BASE}/api/v1/forecast?id_well=1&date_start=2024-01-01&date_end=2024-12-31" || echo "FAIL")
if [ "$FORECAST_RESP" != "FAIL" ]; then
  echo "OK: /api/v1/forecast returned data"
  echo "$FORECAST_RESP" | python -m json.tool | head -20
else
  echo "FAIL: /api/v1/forecast not responding (model may not be trained yet)"
fi

echo ""
echo "=== Done ==="
