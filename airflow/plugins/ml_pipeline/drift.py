"""Modulo de deteccion de data drift y model decay.

Implementa tests estadisticos (PSI, KS) para detectar cambios en las
distribuciones de features y degradacion del rendimiento del modelo.
"""

import logging
import os
import tempfile

import numpy as np

logger = logging.getLogger(__name__)


def calcular_psi(esperado, actual, bins):
    """Calcula el Population Stability Index (PSI) entre dos distribuciones.

    PSI mide cuanto cambio la distribucion de una variable entre el periodo
    de entrenamiento (esperado) y el periodo reciente (actual).

    Args:
        esperado: array de valores del periodo de referencia (entrenamiento).
        actual: array de valores del periodo actual (reciente).
        bins: numero de bins para discretizar (recomendado: 10 o 20).

    Returns:
        float con el valor PSI. < 0.1 estable, 0.1-0.2 moderado, > 0.2 severo.
    """
    eps = 1e-6

    # Definir bins a partir de la distribucion de referencia
    _, bin_edges = np.histogram(esperado, bins=bins)

    # Calcular proporciones en cada bin
    esperado_counts = np.histogram(esperado, bins=bin_edges)[0]
    actual_counts = np.histogram(actual, bins=bin_edges)[0]

    esperado_pct = esperado_counts / len(esperado) + eps
    actual_pct = actual_counts / len(actual) + eps

    # PSI = sum( (actual% - esperado%) * ln(actual% / esperado%) )
    psi = np.sum((actual_pct - esperado_pct) * np.log(actual_pct / esperado_pct))
    return float(psi)


def calcular_ks(esperado, actual):
    """Aplica el test de Kolmogorov-Smirnov de dos muestras.

    Args:
        esperado: array de valores de referencia.
        actual: array de valores recientes.

    Returns:
        Tupla (estadistico_ks, p_value). P-value < 0.05 indica drift significativo.
    """
    from scipy.stats import ks_2samp

    stat, p_value = ks_2samp(esperado, actual)
    return float(stat), float(p_value)


def detectar_model_decay(run_id_produccion, rmse_nuevo, mlflow_client):
    """Compara el RMSE del modelo en produccion con el RMSE sobre datos recientes.

    Lee el tag MLFLOW_BASELINE_RMSE_TAG del run de produccion actual y compara
    con el RMSE obtenido sobre datos recientes (rmse_nuevo).

    Args:
        run_id_produccion: run_id del modelo actualmente en produccion.
        rmse_nuevo: RMSE calculado sobre datos recientes.
        mlflow_client: instancia de mlflow.MlflowClient.

    Returns:
        dict con claves: tiene_decay (bool), rmse_baseline (float),
        rmse_nuevo (float), degradacion_pct (float).
    """
    from ml_pipeline.config import MLFLOW_BASELINE_RMSE_TAG, MODEL_DECAY_RMSE_THRESHOLD

    run = mlflow_client.get_run(run_id_produccion)
    rmse_baseline_str = run.data.tags.get(MLFLOW_BASELINE_RMSE_TAG)

    if rmse_baseline_str is None:
        logger.warning(
            "Tag '%s' no encontrado en run %s. No se puede evaluar decay.",
            MLFLOW_BASELINE_RMSE_TAG, run_id_produccion,
        )
        return {
            "tiene_decay": False,
            "rmse_baseline": None,
            "rmse_nuevo": rmse_nuevo,
            "degradacion_pct": 0.0,
        }

    rmse_baseline = float(rmse_baseline_str)
    degradacion_pct = (rmse_nuevo - rmse_baseline) / rmse_baseline if rmse_baseline > 0 else 0.0
    tiene_decay = degradacion_pct > MODEL_DECAY_RMSE_THRESHOLD

    logger.info(
        "Model decay — baseline=%.4f, nuevo=%.4f, degradacion=%.2f%%, decay=%s",
        rmse_baseline, rmse_nuevo, degradacion_pct * 100, tiene_decay,
    )

    return {
        "tiene_decay": tiene_decay,
        "rmse_baseline": rmse_baseline,
        "rmse_nuevo": rmse_nuevo,
        "degradacion_pct": float(degradacion_pct),
    }


def generar_reporte_drift(resultados_drift, psi_threshold):
    """Genera y guarda un grafico PNG con el resumen del reporte de drift.

    Args:
        resultados_drift: dict con feature como clave y sub-dict con 'psi', 'ks_stat', 'ks_pvalue'.
        psi_threshold: umbral de PSI para la linea de referencia.

    Returns:
        Ruta al archivo PNG temporal generado.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    features = list(resultados_drift.keys())
    psi_values = [resultados_drift[f]["psi"] for f in features]

    fig, ax = plt.subplots(figsize=(10, max(3, len(features) * 0.8)))
    bars = ax.barh(features, psi_values, color=["#e74c3c" if v > psi_threshold else "#2ecc71" for v in psi_values])
    ax.axvline(x=psi_threshold, color="red", linestyle="--", linewidth=1.5, label=f"Umbral PSI = {psi_threshold}")
    ax.set_xlabel("PSI")
    ax.set_title("Reporte de Data Drift — Population Stability Index")
    ax.legend()
    fig.tight_layout()

    tmp_path = os.path.join(tempfile.gettempdir(), "drift_report.png")
    fig.savefig(tmp_path, dpi=100)
    plt.close(fig)

    logger.info("Grafico de drift guardado en: %s", tmp_path)
    return tmp_path
