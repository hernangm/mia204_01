#!/usr/bin/env bash
# =============================================================================
# ejemplos_uso.sh — Guía de uso del sistema mia204_01
#
# Cubre:
#   1. Entrenamiento via train.py (CLI standalone)
#   2. API REST: /api/v1/wells y /api/v1/forecast
#
# Prerequisito: stack levantado con `docker-compose up -d`
#               y el DAG ml_pipeline ejecutado al menos una vez
#               (para poblar el Feature Store con datos).
# =============================================================================

set -euo pipefail

BASE_URL="http://localhost:8000"

echo ""
echo "======================================================="
echo " SECCIÓN 1 — ENTRENAMIENTO (train.py)"
echo "======================================================="
echo ""

# -----------------------------------------------------------------------------
# train.py entrena modelos usando los features ya persistidos en el Feature
# Store (tabla 'features' en PostgreSQL). El parámetro --date define la fecha
# de corte: solo se usan registros con fecha <= ese valor. Esto permite
# reproducir exactamente el modelo que se habría entrenado en cualquier
# momento histórico.
#
# El script:
#   - Lee features del Feature Store filtrados por --date
#   - Corre los 3 experimentos definidos en ml_pipeline/config.py
#   - Loguea parámetros y métricas (rmse, r2, mae) en MLflow
#   - Registra cada modelo en el Model Registry como 'hydrocarbon_forecast'
#   - Asigna el alias 'production' al modelo con menor RMSE
#
# Variables de entorno requeridas (ya configuradas dentro del container):
#   MLFLOW_TRACKING_URI       → http://mlflow:9090
#   FEATURESTORE_DATABASE_URL → postgresql+psycopg2://airflow:airflow@postgres:5432/featurestore
# -----------------------------------------------------------------------------

echo "Ejemplo 1.1 — Entrenar con datos hasta el 1 de octubre de 2023"
echo "Comando:"
echo "  docker compose exec airflow-worker python /opt/airflow/scripts/train.py --date '2023-10-01'"
echo ""
echo "Resultado esperado:"
echo "  - 3 runs registrados en MLflow (experimento 'hydrocarbon_forecast')"
echo "  - El run con menor RMSE recibe el alias 'production' en el Model Registry"
echo "  - Ver resultados en: http://localhost:9090"
echo ""

echo "Ejemplo 1.2 — Entrenar con todos los datos disponibles hasta hoy"
echo "Comando:"
echo "  docker compose exec airflow-worker python /opt/airflow/scripts/train.py --date '2026-04-12'"
echo ""

echo "Ejemplo 1.3 — Entrenamiento reproducible (mismo --date = mismos datos = mismo modelo)"
echo "Correr dos veces con la misma fecha produce modelos con métricas idénticas:"
echo "  docker compose exec airflow-worker python /opt/airflow/scripts/train.py --date '2022-06-01'"
echo "  docker compose exec airflow-worker python /opt/airflow/scripts/train.py --date '2022-06-01'"
echo ""

# Ejecutar train.py real si el stack está up
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health | grep -q "200"; then
    echo ">>> Stack detectado. Ejecutando train.py con --date 2023-10-01 ..."
    docker compose exec -T airflow-worker \
        bash -c "MLFLOW_TRACKING_URI=http://mlflow:9090 python /opt/airflow/scripts/train.py --date '2023-10-01'" \
        2>&1 | grep -E "Experimento completado|promovido|finalizado|ERROR" || true
    echo ""
else
    echo "(stack no disponible, saltando ejecución real)"
fi


echo ""
echo "======================================================="
echo " SECCIÓN 2 — API REST"
echo "======================================================="
echo ""
echo "Base URL: $BASE_URL"
echo "Docs interactivas (Swagger): $BASE_URL/docs"
echo ""

# -----------------------------------------------------------------------------
# GET /api/v1/wells
#
# Devuelve todos los pozos disponibles para una fecha de consulta dada.
# Un pozo aparece si fecha_data IS NULL o fecha_data <= date_query.
# Útil para saber qué pozos tienen datos antes de pedir un forecast.
# -----------------------------------------------------------------------------

echo "-------------------------------------------------------"
echo " GET /api/v1/wells"
echo "-------------------------------------------------------"
echo ""

echo "Ejemplo 2.1 — Listar pozos disponibles al 1 de enero de 2026"
echo "Comando:"
echo "  curl \"$BASE_URL/api/v1/wells?date_query=2026-01-01\""
echo ""
echo "Respuesta (primeros 5):"
curl -s "$BASE_URL/api/v1/wells?date_query=2026-01-01" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  Total de pozos: {len(d)}')
print('  Muestra:')
for item in d[:5]:
    print(f'    {json.dumps(item)}')
" 2>/dev/null || echo "  (API no disponible)"
echo ""

echo "Ejemplo 2.2 — Error esperado: falta el parámetro date_query (422)"
echo "Comando:"
echo "  curl \"$BASE_URL/api/v1/wells\""
echo "Respuesta:"
curl -s "$BASE_URL/api/v1/wells" \
    | python3 -c "import sys,json; print(' ', json.dumps(json.load(sys.stdin), indent=2)[:200])" 2>/dev/null || true
echo ""

# -----------------------------------------------------------------------------
# GET /api/v1/forecast
#
# Devuelve la serie histórica de producción (prod_gas por defecto) para un
# pozo en un rango de fechas. Los datos vienen de la tabla 'production' del
# Feature Store. Los pozos con más datos van de 2006-01-01 a 2026-02-01.
#
# Pozos con más registros históricos (242 meses):
#   114889, 96630, 116022
# -----------------------------------------------------------------------------

echo "-------------------------------------------------------"
echo " GET /api/v1/forecast"
echo "-------------------------------------------------------"
echo ""

echo "Ejemplo 2.3 — Forecast del pozo 114889 (2022 completo)"
echo "Comando:"
echo "  curl \"$BASE_URL/api/v1/forecast?id_well=114889&date_start=2022-01-01&date_end=2022-12-01\""
echo ""
echo "Respuesta:"
curl -s "$BASE_URL/api/v1/forecast?id_well=114889&date_start=2022-01-01&date_end=2022-12-01" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  id_well: {d[\"id_well\"]}')
print(f'  Puntos en el rango: {len(d[\"data\"])}')
print('  Primeros 3:')
for p in d['data'][:3]:
    print(f'    {json.dumps(p)}')
" 2>/dev/null || echo "  (API no disponible)"
echo ""

echo "Ejemplo 2.4 — Forecast con rango largo (toda la historia disponible)"
echo "Comando:"
echo "  curl \"$BASE_URL/api/v1/forecast?id_well=96630&date_start=2006-01-01&date_end=2026-02-01\""
echo ""
echo "Respuesta (resumen):"
curl -s "$BASE_URL/api/v1/forecast?id_well=96630&date_start=2006-01-01&date_end=2026-02-01" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
puntos = d['data']
print(f'  id_well: {d[\"id_well\"]}')
print(f'  Total de puntos: {len(puntos)}')
if puntos:
    print(f'  Primer punto:  {json.dumps(puntos[0])}')
    print(f'  Último punto:  {json.dumps(puntos[-1])}')
    prods = [p['prod'] for p in puntos]
    print(f'  Producción promedio: {sum(prods)/len(prods):.1f}')
    print(f'  Producción máxima:   {max(prods):.1f}')
" 2>/dev/null || echo "  (API no disponible)"
echo ""

echo "Ejemplo 2.5 — Error esperado: date_start > date_end (422)"
echo "Comando:"
echo "  curl \"$BASE_URL/api/v1/forecast?id_well=114889&date_start=2023-12-01&date_end=2022-01-01\""
echo "Respuesta:"
curl -s "$BASE_URL/api/v1/forecast?id_well=114889&date_start=2023-12-01&date_end=2022-01-01" \
    | python3 -c "import sys,json; print(' ', json.dumps(json.load(sys.stdin)))" 2>/dev/null || true
echo ""

echo "Ejemplo 2.6 — Pozo sin datos en el rango (respuesta vacía válida)"
echo "Comando:"
echo "  curl \"$BASE_URL/api/v1/forecast?id_well=114889&date_start=2030-01-01&date_end=2030-12-01\""
echo "Respuesta:"
curl -s "$BASE_URL/api/v1/forecast?id_well=114889&date_start=2030-01-01&date_end=2030-12-01" \
    | python3 -c "import sys,json; print(' ', json.dumps(json.load(sys.stdin)))" 2>/dev/null || true
echo ""


echo "======================================================="
echo " REFERENCIAS"
echo "======================================================="
echo ""
echo "  Swagger UI (API docs):   $BASE_URL/docs"
echo "  MLflow experiments:      http://localhost:9090"
echo "  Airflow UI:              http://localhost:8080  (airflow/airflow)"
echo "  Smoke test completo:     bash scripts/test_e2e.sh"
echo ""
