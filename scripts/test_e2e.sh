#!/usr/bin/env bash
# End-to-end smoke test: chequea cada servicio + valida que el feature store
# este poblado (Req #4 del SPEC missing-features).
# Asume que el stack esta levantado via `docker-compose up -d` y que el DAG
# ml_pipeline corrio al menos una vez para poblar featurestore.features.
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

check_forecast_nonempty() {
    # Hacia el endpoint productivo: prueba E2E que el feature store esta
    # poblado y que Ray Serve responde con predicciones reales.
    local well="${FORECAST_WELL_ID:-96688}"
    local date_start="${FORECAST_DATE_START:-2023-01-01}"
    local date_end="${FORECAST_DATE_END:-2023-12-31}"
    local url="http://localhost:8000/api/v1/forecast?id_well=${well}&date_start=${date_start}&date_end=${date_end}"

    printf '  %-10s %-50s ' "Forecast" "/forecast id_well=${well}"
    local body
    body=$(curl -sS --max-time 30 "$url" || echo '{"data":[]}')
    local data_len
    data_len=$(printf '%s' "$body" | python -c \
        "import json, sys; d=json.load(sys.stdin); print(len(d.get('data', [])))" \
        2>/dev/null || echo "0")
    if [[ "$data_len" =~ ^[0-9]+$ ]] && (( data_len > 0 )); then
        echo "OK (${data_len} puntos)"
    else
        echo "FAIL (data length=${data_len})"
        echo "  Response: $body"
        echo "  Sugerencia: trigger del DAG ml_pipeline antes de re-ejecutar:"
        echo "    docker compose exec airflow-scheduler airflow dags trigger ml_pipeline"
        return 1
    fi
}

echo "== E2E smoke test =="
failed=0
check "API"      "http://localhost:8000/health"          200 || failed=1
check "Airflow"  "http://localhost:8080/api/v2/version"  200 || failed=1
check "MLflow"   "http://localhost:9090/"                200 || failed=1
check "MinIO"    "http://localhost:9001/minio/health/live" 200 || failed=1
check_forecast_nonempty                                           || failed=1

if (( failed )); then
    echo "== FAIL =="
    exit 1
fi
echo "== PASS =="
