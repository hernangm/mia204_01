#!/usr/bin/env bash
# End-to-end smoke test: hit each service's health endpoint and fail loudly if any are down.
# Assumes the stack is already up via `docker-compose up -d`.
set -euo pipefail

check() {
    local name="$1"
    local url="$2"
    local expected="${3:-200}"
    printf '  %-10s %-50s ' "$name" "$url"
    local code
    code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "$url" || echo "000")
    if [[ "$code" == "$expected" ]]; then
        echo "OK ($code)"
    else
        echo "FAIL (got $code, expected $expected)"
        return 1
    fi
}

echo "== E2E smoke test =="
failed=0
check "API"      "http://localhost:8000/health"          200 || failed=1
check "Airflow"  "http://localhost:8080/api/v2/version"  200 || failed=1
check "MLflow"   "http://localhost:9090/"                200 || failed=1
check "MinIO"    "http://localhost:9001/minio/health/live" 200 || failed=1

if (( failed )); then
    echo "== FAIL =="
    exit 1
fi
echo "== PASS =="
