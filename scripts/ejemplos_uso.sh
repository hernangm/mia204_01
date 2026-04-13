#!/usr/bin/env bash
# Guía de uso del sistema mia204_01.
# Prerequisito: docker compose up -d && DAG ml_pipeline ejecutado al menos una vez.

set -euo pipefail

BASE_URL="http://localhost:8000"
WORKER="docker compose exec -T airflow-worker"
TRAIN="python /opt/airflow/scripts/train.py"

# ── Helpers ──────────────────────────────────────────────────────────────────

section() { echo; echo "── $1 ──────────────────────────────────────────────"; echo; }

api_get() {
    curl -s "$BASE_URL/$1" | python3 -m json.tool 2>/dev/null || echo "(API no disponible)"
}

stack_up() {
    curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/health" | grep -q "200"
}

# ── 1. ENTRENAMIENTO (train.py) ───────────────────────────────────────────────
# train.py usa features del Feature Store (tabla `features` en Postgres).
# --date define la fecha de corte: solo registros con fecha <= ese valor.
# Misma fecha → mismos datos → métricas idénticas (reproducible).

section "ENTRENAMIENTO"

echo "# Entrenar con datos hasta una fecha específica"
echo "$WORKER $TRAIN --date '2023-10-01'"
echo
echo "# Reproducibilidad: correr dos veces con la misma fecha produce el mismo modelo"
echo "$WORKER $TRAIN --date '2022-06-01'"
echo "$WORKER $TRAIN --date '2022-06-01'"
echo
echo "# Ver resultados en MLflow: http://localhost:9090"
echo

if stack_up; then
    echo ">>> Ejecutando train.py --date 2023-10-01 ..."
    $WORKER bash -c "MLFLOW_TRACKING_URI=http://mlflow:9090 $TRAIN --date '2023-10-01'" \
        2>&1 | grep -E "Experimento completado|promovido|finalizado|ERROR" || true
else
    echo "(stack no disponible, saltando ejecución)"
fi

# ── 2. API REST ───────────────────────────────────────────────────────────────

section "API REST  — $BASE_URL/docs"

echo "# GET /api/v1/wells — pozos disponibles para una fecha"
echo "curl \"$BASE_URL/api/v1/wells?date_query=2026-01-01\""
curl -s "$BASE_URL/api/v1/wells?date_query=2026-01-01" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  {len(d)} pozos — muestra: {[x[\"id_well\"] for x in d[:3]]}')
" 2>/dev/null || echo "  (API no disponible)"
echo

echo "# GET /api/v1/forecast — producción histórica de un pozo"
echo "curl \"$BASE_URL/api/v1/forecast?id_well=96630&date_start=2006-01-01&date_end=2026-02-01\""
curl -s "$BASE_URL/api/v1/forecast?id_well=96630&date_start=2006-01-01&date_end=2026-02-01" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
pts = d['data']
print(f'  {len(pts)} puntos | primero: {pts[0]} | último: {pts[-1]}')
" 2>/dev/null || echo "  (API no disponible)"
echo

echo "# Errores de validación esperados (422)"
echo "curl \"$BASE_URL/api/v1/wells\"                                          # falta date_query"
echo "curl \"$BASE_URL/api/v1/forecast?id_well=X&date_start=2023-12-01&date_end=2022-01-01\"  # start > end"
echo

echo "# Referencias"
echo "  Swagger:  $BASE_URL/docs"
echo "  MLflow:   http://localhost:9090"
echo "  Airflow:  http://localhost:8080  (airflow / airflow)"
