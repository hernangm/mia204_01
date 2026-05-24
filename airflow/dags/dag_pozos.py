from datetime import datetime

from airflow.sdk import Param, dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from ml_pipeline.config import PIPELINE_SCHEDULE


@dag(
    dag_id='ml_pipeline',
    description='Pipeline de Machine Learning con Airflow',
    start_date=datetime(2025, 1, 1),
    schedule=PIPELINE_SCHEDULE,
    catchup=False,
    params={
        'date_from': Param(
            default=None,
            type=['null', 'string'],
            description='Fecha inicio del rango (YYYY-MM-DD). Dejar vacío para no filtrar.',
        ),
        'date_to': Param(
            default=None,
            type=['null', 'string'],
            description='Fecha fin del rango (YYYY-MM-DD). Dejar vacío para no filtrar.',
        ),
    },
)
def ml_pipeline():
    start = EmptyOperator(
        task_id='start',
    )

    @task
    def download_dataset(save_path):
        """Descarga el dataset CSV desde la URL definida en ml_pipeline.config y lo guarda en disco.

        Args:
            save_path (str): ruta local del archivo.

        Returns:
            str: la ruta del archivo guardado.
        """
        import logging

        import pandas as pd
        from ml_pipeline.config import DATASET_DOWNLOAD_URL, DEV_ROW_LIMIT

        logger = logging.getLogger(__name__)
        logger.info("Iniciando descarga desde: %s", DATASET_DOWNLOAD_URL)
        df = pd.read_csv(DATASET_DOWNLOAD_URL, nrows=DEV_ROW_LIMIT)
        if DEV_ROW_LIMIT:
            logger.info("DEV_ROW_LIMIT=%d activo", DEV_ROW_LIMIT)
        logger.info("Descarga completa: %d filas, %d columnas", len(df), len(df.columns))
        df.to_csv(save_path, index=False)
        logger.info("Archivo guardado en: %s", save_path)
        return save_path

    @task
    def preprocess(csv_path):
        """Lee el CSV, aplica filtro de fechas, codifica tipoextraccion y selecciona
        columnas del feature store (id_pozo, fecha + columnas de EXPERIMENTS).

        Args:
            csv_path (str): ruta al archivo CSV.

        Returns:
            dict: columnas como claves y listas de valores, listo para el Feature Store.
        """
        import logging

        import pandas as pd
        from airflow.sdk import get_current_context
        from ml_pipeline.config import EXPERIMENTS, TIPOEXTRACCION_MAP

        logger = logging.getLogger(__name__)
        logger.info("Leyendo CSV desde: %s", csv_path)
        df = pd.read_csv(csv_path)
        logger.info("CSV cargado: %d filas, %d columnas", len(df), len(df.columns))

        # Filtramos por rango de fechas si se indicaron params
        context = get_current_context()
        params = context['params']
        date_from = params.get('date_from')
        date_to = params.get('date_to')

        df['fecha_data'] = pd.to_datetime(df['fecha_data'])
        if date_from:
            df = df[df['fecha_data'] >= date_from]
            logger.info("Filtrado desde %s: %d filas", date_from, len(df))
        if date_to:
            df = df[df['fecha_data'] <= date_to]
            logger.info("Filtrado hasta %s: %d filas", date_to, len(df))

        # Recopilamos todas las columnas necesarias de los experimentos
        feature_cols = set()
        for exp in EXPERIMENTS:
            feature_cols.update(exp['features'])
            feature_cols.add(exp['target'])

        faltantes = feature_cols - set(df.columns)
        if faltantes:
            raise RuntimeError(
                f"Columnas requeridas no encontradas en el CSV: {faltantes}. "
                f"Columnas disponibles: {list(df.columns)}"
            )

        columnas = ['idpozo', 'fecha_data'] + list(feature_cols)
        df = df[columnas]

        n_antes = len(df)
        df = df.dropna()
        logger.info("Filas eliminadas por nulos: %d", n_antes - len(df))

        # Codificamos 'tipoextraccion' con el mapa estático de config
        if 'tipoextraccion' in df.columns:
            df['tipoextraccion'] = df['tipoextraccion'].map(TIPOEXTRACCION_MAP)
            df = df.dropna(subset=['tipoextraccion'])
            df['tipoextraccion'] = df['tipoextraccion'].astype(int)

        # Shift temporal: features del mes T predicen prod_gas del mes T+1.
        # Esto evita data leakage: al operar el modelo en produccion, las variables
        # correlacionadas del mes en curso (prod_pet, prod_agua, tef) no se conocen
        # al principio del mes, pero si las del mes anterior.
        df = df.sort_values(['idpozo', 'fecha_data'])
        df['prod_gas'] = df.groupby('idpozo')['prod_gas'].shift(-1)
        n_pre = len(df)
        df = df.dropna(subset=['prod_gas'])
        logger.info("Shift temporal aplicado: %d filas eliminadas (ultimo mes por pozo)", n_pre - len(df))

        df = df.rename(columns={'idpozo': 'id_pozo', 'fecha_data': 'fecha'})
        df['id_pozo'] = df['id_pozo'].astype(str)
        df['fecha'] = pd.to_datetime(df['fecha']).dt.strftime('%Y-%m-%d')

        logger.info("Preprocesado completo: %d filas, columnas: %s", len(df), list(df.columns))
        return df.to_dict(orient='list')

    @task
    def persist_features(data):
        """Persiste las features preprocesadas en el Feature Store.

        Delega en FeatureStore.save_features(), que aplica DELETE-then-INSERT
        idempotente sobre el rango de fechas del DataFrame.

        Args:
            data: dict de listas devuelto por preprocess().

        Returns:
            dict con metadata de la persistencia (rows, date_from, date_to).
        """
        import logging

        import pandas as pd
        from ml_pipeline.feature_store import FeatureStore

        logger = logging.getLogger(__name__)

        df = pd.DataFrame(data)
        if len(df) == 0:
            raise RuntimeError("preprocess() no produjo filas — no hay nada que persistir.")

        fecha_min = df['fecha'].min()
        fecha_max = df['fecha'].max()
        logger.info(
            "Persistiendo %d filas en el Feature Store (rango %s a %s)",
            len(df), fecha_min, fecha_max,
        )

        fs = FeatureStore()
        rows = fs.save_features(df, fecha_min=fecha_min, fecha_max=fecha_max)

        metadata = {"rows": rows, "date_from": fecha_min, "date_to": fecha_max}
        logger.info("Feature Store: persistencia completada — %s", metadata)
        return metadata

    @task
    def read_features_from_store(persist_metadata):
        """Valida que el Feature Store tenga datos para el rango solicitado.

        Falla ruidosamente si no hay features, para que el entrenamiento
        no corra silenciosamente sobre datos incorrectos.
        Devuelve solo metadata (no el DataFrame) para evitar XComs grandes
        que saturan el API server de Airflow 3.

        Args:
            persist_metadata: salida de persist_features() (dependencia de orden).

        Returns:
            dict con rows, date_from, date_to del Feature Store.
        """
        import logging
        from datetime import date

        from airflow.sdk import get_current_context
        from ml_pipeline.feature_store import FeatureStore

        logger = logging.getLogger(__name__)

        context = get_current_context()
        params = context['params']
        date_from_str = params.get('date_from')
        date_to_str = params.get('date_to')

        cutoff = None
        if date_to_str:
            cutoff = date.fromisoformat(date_to_str)

        fs = FeatureStore()
        df = fs.get_training_features(cutoff_date=cutoff)

        if len(df) == 0:
            raise RuntimeError(
                f"Feature Store vacío para el rango solicitado "
                f"(date_from={date_from_str}, date_to={date_to_str}). "
                "El entrenamiento no puede continuar sin features persistidas."
            )

        if date_from_str:
            df = df[df['fecha'] >= date_from_str]
            logger.info("Filtrado desde %s: %d filas", date_from_str, len(df))

        meta = {"rows": len(df), "date_from": date_from_str or "all", "date_to": date_to_str or "all"}
        logger.info("Feature Store validado: %d filas disponibles para entrenamiento", len(df))
        return meta

    @task
    def train_experiments(feature_meta):
        """Entrena todos los experimentos definidos en config y registra en MLflow.

        Lee directamente del Feature Store para evitar XComs gigantes.

        Args:
            feature_meta: metadata del Feature Store (rows, date_from, date_to).

        Returns:
            lista de dicts con run_id y métricas de cada experimento.
        """
        import logging
        from datetime import date

        import pandas as pd
        from ml_pipeline.config import EXPERIMENTS
        from ml_pipeline.feature_store import FeatureStore
        from ml_pipeline.training import train_and_log

        logger = logging.getLogger(__name__)

        # Leer directamente del Feature Store (evita XCom de cientos de MB)
        date_to_str = feature_meta.get("date_to")
        cutoff = date.fromisoformat(date_to_str) if date_to_str and date_to_str != "all" else None
        fs = FeatureStore()
        df = fs.get_training_features(cutoff_date=cutoff)

        date_from_str = feature_meta.get("date_from")
        if date_from_str and date_from_str != "all":
            df = df[df["fecha"] >= date_from_str]

        data = df.to_dict(orient="list")

        results = []
        for i, exp_cfg in enumerate(EXPERIMENTS):
            logger.info(
                "Experimento %d/%d — target=%s, features=%s, params=%s",
                i + 1, len(EXPERIMENTS),
                exp_cfg['target'], exp_cfg['features'], exp_cfg['model_params'],
            )
            result = train_and_log(data, exp_cfg, feature_store_meta=feature_meta)
            results.append(result)
            logger.info(
                "Experimento %d/%d completado — RMSE=%.4f",
                i + 1, len(EXPERIMENTS), result['rmse'],
            )

        logger.info("Todos los experimentos completados (%d runs)", len(results))
        return results

    @task
    def promote_model(results):
        """Promueve el modelo con menor RMSE como 'production' en MLflow.

        Args:
            results (list): lista de dicts con run_id y métricas.
        """
        import logging

        from ml_pipeline.training import promote_best_model

        logger = logging.getLogger(__name__)
        best = promote_best_model(results)
        logger.info(
            "Mejor modelo: run=%s, RMSE=%.4f, MAE=%.4f, R²=%.4f",
            best['run_id'], best['rmse'], best['mae'], best['r2'],
        )

    # --- Secuencia de tasks ---
    csv_path = download_dataset('/tmp/pozos.csv')
    preprocessed = preprocess(csv_path)
    persist_meta = persist_features(preprocessed)
    data = read_features_from_store(persist_meta)
    results = train_experiments(data)
    promote_model(results)

    start >> csv_path


ml_pipeline()
