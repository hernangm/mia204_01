"""Tests del módulo ml_pipeline/drift.py.

Cubre:
  - calcular_psi: función pura, sin mocks
  - calcular_ks: función pura, sin mocks
  - detectar_model_decay: con mock de MLflowClient
  - generar_reporte_drift: smoke test de generación de PNG

Corren dentro del contenedor Airflow donde las dependencias están instaladas.
Si no están disponibles (entorno local sin Docker), el archivo se saltea.
"""

from unittest.mock import MagicMock

import pytest

np = pytest.importorskip("numpy", reason="numpy no está instalado")
pytest.importorskip("scipy", reason="scipy no está instalado")
pytest.importorskip("mlflow", reason="mlflow no está instalado")

from ml_pipeline.config import KS_PVALUE_THRESHOLD, MODEL_DECAY_RMSE_THRESHOLD, PSI_THRESHOLD
from ml_pipeline.drift import (
    calcular_ks,
    calcular_psi,
    detectar_model_decay,
    generar_reporte_drift,
)

PSI_BINS = 10


# ---------------------------------------------------------------------------
# Tests de calcular_psi
# ---------------------------------------------------------------------------

def test_psi_identical_distributions_is_near_zero():
    """Distribuciones idénticas → PSI ≈ 0 (sin drift)."""
    rng = np.random.default_rng(seed=42)
    data = rng.normal(loc=100, scale=20, size=1000)
    psi = calcular_psi(data, data.copy(), bins=PSI_BINS)
    assert psi < 0.10, f"PSI debería ser < 0.10, got {psi:.4f}"


def test_psi_very_different_distributions_is_high():
    """Distribuciones completamente distintas → PSI alto."""
    rng = np.random.default_rng(seed=42)
    expected = rng.normal(loc=0, scale=1, size=1000)
    actual = rng.normal(loc=100, scale=1, size=1000)
    psi = calcular_psi(expected, actual, bins=PSI_BINS)
    assert psi >= PSI_THRESHOLD, f"PSI debería ser >= {PSI_THRESHOLD}, got {psi:.4f}"


def test_psi_moderately_different_distributions():
    """Distribuciones con leve corrimiento → PSI >= 0."""
    rng = np.random.default_rng(seed=42)
    expected = rng.normal(loc=0, scale=1, size=2000)
    actual = rng.normal(loc=0.5, scale=1.2, size=500)
    psi = calcular_psi(expected, actual, bins=PSI_BINS)
    assert psi >= 0, "PSI siempre debe ser >= 0"


def test_psi_is_non_negative():
    """PSI siempre es >= 0 por definición matemática."""
    rng = np.random.default_rng(seed=99)
    for _ in range(10):
        a = rng.exponential(scale=50, size=500)
        b = rng.exponential(scale=70, size=500)
        assert calcular_psi(a, b, bins=PSI_BINS) >= 0


def test_psi_returns_float():
    """calcular_psi debe devolver un float."""
    rng = np.random.default_rng(seed=7)
    a = rng.normal(size=200)
    b = rng.normal(size=200)
    result = calcular_psi(a, b, bins=PSI_BINS)
    assert isinstance(result, float)


# ---------------------------------------------------------------------------
# Tests de calcular_ks
# ---------------------------------------------------------------------------

def test_ks_identical_distributions_high_pvalue():
    """Distribuciones idénticas → p-value alto (sin evidencia de drift)."""
    rng = np.random.default_rng(seed=42)
    data = rng.normal(loc=100, scale=20, size=500)
    _, p_value = calcular_ks(data, data.copy())
    assert p_value > KS_PVALUE_THRESHOLD, (
        f"p-value debería ser > {KS_PVALUE_THRESHOLD} para dist. idénticas, got {p_value:.4f}"
    )


def test_ks_very_different_distributions_low_pvalue():
    """Distribuciones muy distintas → p-value bajo (drift significativo)."""
    rng = np.random.default_rng(seed=42)
    dist_a = rng.normal(loc=0, scale=1, size=500)
    dist_b = rng.normal(loc=100, scale=1, size=500)
    _, p_value = calcular_ks(dist_a, dist_b)
    assert p_value < KS_PVALUE_THRESHOLD, (
        f"p-value debería ser < {KS_PVALUE_THRESHOLD} para dist. distintas, got {p_value:.4f}"
    )


def test_ks_returns_stat_and_pvalue():
    """calcular_ks devuelve tupla (stat, p_value) con valores en rango válido."""
    rng = np.random.default_rng(seed=0)
    a = rng.normal(size=200)
    b = rng.normal(size=200)
    stat, p_value = calcular_ks(a, b)
    assert 0 <= stat <= 1, f"KS stat fuera de rango: {stat}"
    assert 0 <= p_value <= 1, f"p-value fuera de rango: {p_value}"


def test_ks_returns_floats():
    """calcular_ks devuelve floats (no numpy scalars)."""
    rng = np.random.default_rng(seed=1)
    a = rng.normal(size=100)
    b = rng.normal(size=100)
    stat, p_value = calcular_ks(a, b)
    assert isinstance(stat, float)
    assert isinstance(p_value, float)


# ---------------------------------------------------------------------------
# Tests de detectar_model_decay
# ---------------------------------------------------------------------------

def _make_mock_client(baseline_rmse_str):
    """Crea un mock de MlflowClient que devuelve el tag dado."""
    from ml_pipeline.config import MLFLOW_BASELINE_RMSE_TAG
    run_mock = MagicMock()
    run_mock.data.tags = {MLFLOW_BASELINE_RMSE_TAG: baseline_rmse_str} if baseline_rmse_str else {}
    client = MagicMock()
    client.get_run.return_value = run_mock
    return client


def test_detectar_model_decay_ok_when_similar_rmse():
    """RMSE similar al baseline → tiene_decay = False."""
    client = _make_mock_client("100.0")
    result = detectar_model_decay("run-abc", rmse_nuevo=105.0, mlflow_client=client)
    assert result["tiene_decay"] is False
    assert result["rmse_baseline"] == 100.0
    assert result["rmse_nuevo"] == 105.0


def test_detectar_model_decay_degraded_when_rmse_much_higher():
    """RMSE mucho mayor que baseline → tiene_decay = True."""
    client = _make_mock_client("10.0")
    # RMSE 20x mayor → degradacion = 1900% >> threshold del 15%
    result = detectar_model_decay("run-abc", rmse_nuevo=200.0, mlflow_client=client)
    assert result["tiene_decay"] is True
    assert result["degradacion_pct"] > MODEL_DECAY_RMSE_THRESHOLD


def test_detectar_model_decay_tag_missing_returns_no_decay():
    """Si el tag baseline no existe → tiene_decay = False (safe default)."""
    client = _make_mock_client(None)
    result = detectar_model_decay("run-xyz", rmse_nuevo=300.0, mlflow_client=client)
    assert result["tiene_decay"] is False
    assert result["rmse_baseline"] is None


def test_detectar_model_decay_returns_expected_keys():
    """El dict de resultado tiene todas las claves esperadas."""
    client = _make_mock_client("50.0")
    result = detectar_model_decay("run-1", rmse_nuevo=60.0, mlflow_client=client)
    assert set(result.keys()) == {"tiene_decay", "rmse_baseline", "rmse_nuevo", "degradacion_pct"}


# ---------------------------------------------------------------------------
# Tests de generar_reporte_drift
# ---------------------------------------------------------------------------

def test_generar_reporte_drift_returns_png_path():
    """generar_reporte_drift devuelve una ruta a un archivo .png existente."""
    resultados = {
        "prod_pet": {"psi": 0.05, "ks_stat": 0.1, "ks_pvalue": 0.3},
        "prod_agua": {"psi": 0.25, "ks_stat": 0.4, "ks_pvalue": 0.01},
        "tef": {"psi": 0.08, "ks_stat": 0.05, "ks_pvalue": 0.8},
    }
    path = generar_reporte_drift(resultados, psi_threshold=PSI_THRESHOLD)
    import os
    assert path.endswith(".png")
    assert os.path.exists(path)


def test_generar_reporte_drift_handles_empty_features():
    """Con dict vacío no lanza excepciones."""
    path = generar_reporte_drift({}, psi_threshold=PSI_THRESHOLD)
    import os
    assert os.path.exists(path)
