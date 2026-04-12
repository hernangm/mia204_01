"""DAG de deteccion de data drift y model decay.

Ejecuta semanalmente tests estadisticos (PSI, KS) sobre las features
del modelo de produccion y evalua si hay degradacion de rendimiento.
Si detecta drift o decay, dispara automaticamente el DAG ml_pipeline
para reentrenar el modelo.
"""

from datetime import datetime

from airflow.sdk import dag, task
from ml_pipeline.config import DRIFT_SCHEDULE


@dag(
    dag_id='drift_report',
    description='Deteccion de data drift y model decay para el modelo de produccion',
    start_date=datetime(2025, 1, 1),
    schedule=DRIFT_SCHEDULE,
    catchup=False,
)
def drift_report():

    @task
    def cargar_datos_referencia():
        """Descarga el dataset completo de referencia y lo guarda en /tmp.

        Usa la misma URL que el pipeline de entrenamiento para obtener la
        distribucion historica completa de las features.

        Returns:
            str: ruta al CSV de referencia.
        """
        import logging

        import pandas as pd
        from ml_pipeline.config import DATASET_DOWNLOAD_URL

        logger = logging.getLogger(__name__)
        logger.info("Descargando dataset de referencia desde: %s", DATASET_DOWNLOAD_URL)

        df = pd.read_csv(DATASET_DOWNLOAD_URL)
        path = '/tmp/drift_referencia.csv'
        df.to_csv(path, index=False)
        logger.info("Dataset de referencia guardado: %d filas en %s", len(df), path)
        return path

    @task
    def cargar_datos_recientes():
        """Consulta el feature store para obtener datos de los ultimos N dias.

        Filtra la tabla features por la ventana temporal definida en
        DRIFT_WINDOW_DAYS para usarla como distribucion 'actual'.

        Returns:
            str: ruta al CSV de datos recientes.
        """
        import logging
        from datetime import timedelta

        import pandas as pd
        from sqlalchemy import create_engine, text
        from ml_pipeline.config import DRIFT_WINDOW_DAYS

        logger = logging.getLogger(__name__)
        db_url = "postgresql+psycopg2://airflow:airflow@postgres:5432/featurestore"
        engine = create_engine(db_url)

        fecha_limite = (datetime.utcnow() - timedelta(days=DRIFT_WINDOW_DAYS)).strftime('%Y-%m-%d')
        query = text("SELECT * FROM features WHERE fecha >= :fecha_limite")

        logger.info("Consultando feature store desde %s", fecha_limite)
        df = pd.read_sql(query, engine, params={"fecha_limite": fecha_limite})

        path = '/tmp/drift_recientes.csv'
        df.to_csv(path, index=False)
        logger.info("Datos recientes guardados: %d filas en %s", len(df), path)
        return path

    @task
    def calcular_drift(path_referencia, path_recientes):
        """Aplica PSI y KS test a cada feature monitorizada.

        Loguea los resultados como metricas en un run de MLflow del
        experimento drift_monitoring y genera un grafico PNG como artefacto.

        Args:
            path_referencia: ruta al CSV de datos de referencia.
            path_recientes: ruta al CSV de datos recientes.

        Returns:
            dict con resultados de drift por feature.
        """
        import logging

        import mlflow
        import pandas as pd
        from ml_pipeline.config import (
            DRIFT_EXPERIMENT_NAME,
            DRIFT_FEATURES,
            PSI_BINS,
            PSI_THRESHOLD,
            KS_PVALUE_THRESHOLD,
            TIPOEXTRACCION_MAP,
        )
        from ml_pipeline.drift import calcular_ks, calcular_psi, generar_reporte_drift

        logger = logging.getLogger(__name__)
        logging.getLogger("mlflow").setLevel(logging.WARNING)

        df_ref = pd.read_csv(path_referencia)
        df_rec = pd.read_csv(path_recientes)

        # Codificar tipoextraccion en referencia (recientes ya viene codificado del feature store)
        if 'tipoextraccion' in df_ref.columns:
            df_ref['tipoextraccion'] = df_ref['tipoextraccion'].map(TIPOEXTRACCION_MAP)

        mlflow.set_experiment(DRIFT_EXPERIMENT_NAME)

        resultados = {}
        hay_drift = False

        with mlflow.start_run(run_name="drift_check") as run:
            for feature in DRIFT_FEATURES:
                if feature not in df_ref.columns or feature not in df_rec.columns:
                    logger.warning("Feature '%s' no encontrada, saltando.", feature)
                    continue

                ref_values = df_ref[feature].dropna().values
                rec_values = df_rec[feature].dropna().values

                if len(ref_values) == 0 or len(rec_values) == 0:
                    logger.warning("Feature '%s' sin datos suficientes, saltando.", feature)
                    continue

                psi = calcular_psi(ref_values, rec_values, PSI_BINS)
                ks_stat, ks_pvalue = calcular_ks(ref_values, rec_values)

                resultados[feature] = {
                    "psi": psi,
                    "ks_stat": ks_stat,
                    "ks_pvalue": ks_pvalue,
                    "drift_psi": psi > PSI_THRESHOLD,
                    "drift_ks": ks_pvalue < KS_PVALUE_THRESHOLD,
                }

                mlflow.log_metric(f"psi_{feature}", psi)
                mlflow.log_metric(f"ks_stat_{feature}", ks_stat)
                mlflow.log_metric(f"ks_pvalue_{feature}", ks_pvalue)

                if psi > PSI_THRESHOLD or ks_pvalue < KS_PVALUE_THRESHOLD:
                    hay_drift = True
                    logger.warning(
                        "DRIFT detectado en '%s': PSI=%.4f, KS p-value=%.4f",
                        feature, psi, ks_pvalue,
                    )
                else:
                    logger.info(
                        "Feature '%s' estable: PSI=%.4f, KS p-value=%.4f",
                        feature, psi, ks_pvalue,
                    )

            mlflow.log_metric("hay_drift", int(hay_drift))

            # Generar y loguear grafico de drift
            if resultados:
                png_path = generar_reporte_drift(resultados, PSI_THRESHOLD)
                mlflow.log_artifact(png_path, artifact_path="drift_reports")

        logger.info("Analisis de drift completado. Drift detectado: %s", hay_drift)
        return resultados

    @task
    def evaluar_model_decay(path_recientes):
        """Evalua si el modelo de produccion tiene decay sobre datos recientes.

        Carga el modelo con alias 'production', predice sobre los datos
        recientes del feature store y compara el RMSE contra el baseline.

        Args:
            path_recientes: ruta al CSV de datos recientes.

        Returns:
            dict con resultado del analisis de decay.
        """
        import logging

        import mlflow
        import numpy as np
        import pandas as pd
        from sklearn.metrics import mean_squared_error
        from ml_pipeline.config import (
            ALL_FEATURES_GAS,
            DRIFT_EXPERIMENT_NAME,
            MLFLOW_EXPERIMENT_NAME,
        )
        from ml_pipeline.drift import detectar_model_decay

        logger = logging.getLogger(__name__)
        logging.getLogger("mlflow").setLevel(logging.WARNING)

        df = pd.read_csv(path_recientes)

        # Verificar que hay datos suficientes
        required_cols = ALL_FEATURES_GAS + ['prod_gas']
        missing = set(required_cols) - set(df.columns)
        if missing:
            logger.warning("Columnas faltantes para evaluar decay: %s", missing)
            return {"tiene_decay": False, "motivo": "columnas_faltantes"}

        df = df.dropna(subset=required_cols)
        if len(df) < 10:
            logger.warning("Datos insuficientes para evaluar decay (%d filas)", len(df))
            return {"tiene_decay": False, "motivo": "datos_insuficientes"}

        # Cargar modelo de produccion
        model_uri = f"models:/{MLFLOW_EXPERIMENT_NAME}@production"
        modelo = mlflow.pyfunc.load_model(model_uri)

        # Predecir y calcular RMSE
        X = df[ALL_FEATURES_GAS]
        y = df['prod_gas']
        predicciones = modelo.predict(X)
        rmse_nuevo = float(np.sqrt(mean_squared_error(y, predicciones)))

        # Obtener run_id del modelo en produccion
        client = mlflow.MlflowClient()
        model_version = client.get_model_version_by_alias(MLFLOW_EXPERIMENT_NAME, "production")
        run_id = model_version.run_id

        # Evaluar decay
        resultado = detectar_model_decay(run_id, rmse_nuevo, client)

        # Loguear resultado en MLflow
        mlflow.set_experiment(DRIFT_EXPERIMENT_NAME)
        with mlflow.start_run(run_name="model_decay_check"):
            mlflow.log_metric("rmse_nuevo", rmse_nuevo)
            mlflow.log_metric("rmse_baseline", resultado["rmse_baseline"] or 0.0)
            mlflow.log_metric("degradacion_pct", resultado["degradacion_pct"])
            mlflow.log_metric("tiene_decay", int(resultado["tiene_decay"]))

        logger.info("Evaluacion de decay completada: %s", resultado)
        return resultado

    @task
    def decidir_retraining(drift_results, decay_results):
        """Decide si se debe disparar retraining basado en drift y decay.

        Si detecta drift significativo en alguna feature o model decay,
        dispara el DAG ml_pipeline para reentrenar el modelo.

        Args:
            drift_results: dict con resultados de drift por feature.
            decay_results: dict con resultado del analisis de decay.
        """
        import logging

        logger = logging.getLogger(__name__)

        hay_drift = any(
            r.get("drift_psi") or r.get("drift_ks")
            for r in drift_results.values()
        )
        hay_decay = decay_results.get("tiene_decay", False)

        if hay_drift or hay_decay:
            motivos = []
            if hay_drift:
                features_drift = [f for f, r in drift_results.items() if r.get("drift_psi") or r.get("drift_ks")]
                motivos.append(f"drift en: {', '.join(features_drift)}")
            if hay_decay:
                motivos.append(f"model decay ({decay_results.get('degradacion_pct', 0):.1%})")

            logger.warning(
                "RETRAINING requerido — %s. Disparando DAG ml_pipeline.",
                "; ".join(motivos),
            )

            # Disparar retraining via API REST de Airflow
            import requests
            try:
                resp = requests.post(
                    "http://airflow-apiserver:8080/api/v2/dags/ml_pipeline/dagRuns",
                    json={"logical_date": None, "conf": {"triggered_by": "drift_report"}},
                    auth=("airflow", "airflow"),
                    timeout=30,
                )
                resp.raise_for_status()
                logger.info("DAG ml_pipeline disparado exitosamente: %s", resp.json())
            except Exception as exc:
                logger.error("Error al disparar ml_pipeline: %s", exc)
                raise RuntimeError(f"No se pudo disparar retraining: {exc}") from exc
        else:
            logger.info("No se requiere retraining. Modelo y datos estables.")

    # --- Secuencia de tasks ---
    path_ref = cargar_datos_referencia()
    path_rec = cargar_datos_recientes()
    drift = calcular_drift(path_ref, path_rec)
    decay = evaluar_model_decay(path_rec)
    decidir_retraining(drift, decay)


drift_report()
