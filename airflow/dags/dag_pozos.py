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
    """Descarga un dataset CSV desde la URL definida en ml_pipeline.config y lo guarda en disco.
    Args: save_path (str) - ruta local del archivo.
    Retorna: la ruta del archivo guardado.
    """
    import logging

    import pandas as pd
    from ml_pipeline.config import DATASET_DOWNLOAD_URL

    logger = logging.getLogger(__name__)
    logger.info("Iniciando descarga desde: %s", DATASET_DOWNLOAD_URL)
    df = pd.read_csv(DATASET_DOWNLOAD_URL)
    logger.info("Descarga completa: %d filas, %d columnas", len(df), len(df.columns))
    logger.info("Columnas: %s", list(df.columns))
    df.to_csv(save_path, index=False)
    logger.info("Archivo guardado en: %s", save_path)
    return save_path

  @task
  def preprocess(csv_path):
    """Lee el CSV, aplica filtro de fechas, codifica tipoextraccion y selecciona
    columnas del feature store (id_pozo, fecha + columnas de EXPERIMENTS).

    Args: csv_path (str) - ruta al archivo CSV.
    Retorna: dict con nombres de columnas como claves y listas de valores,
             listo para persistirse en la tabla features.
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
    if not date_from and not date_to:
      logger.info("Sin filtro de fechas aplicado")

    # Recopilamos todas las columnas necesarias de los experimentos
    feature_cols = set()
    for exp in EXPERIMENTS:
      feature_cols.update(exp['features'])
      feature_cols.add(exp['target'])

    # Verificamos que todas las columnas existan en el CSV
    faltantes = feature_cols - set(df.columns)
    if faltantes:
      raise RuntimeError(
          f"Columnas requeridas por EXPERIMENTS no encontradas en el CSV: {faltantes}. "
          f"Columnas disponibles: {list(df.columns)}"
      )

    # Conservamos las claves del feature store junto a las features
    columnas = ['idpozo', 'fecha_data'] + list(feature_cols)
    df = df[columnas]

    # Eliminamos filas con valores nulos
    n_antes = len(df)
    df = df.dropna()
    logger.info("Filas eliminadas por nulos: %d", n_antes - len(df))

    # Codificamos 'tipoextraccion' con el mapa estático de config
    if 'tipoextraccion' in df.columns:
      df['tipoextraccion'] = df['tipoextraccion'].map(TIPOEXTRACCION_MAP)
      # map() devuelve NaN para valores no mapeados — descartamos esas filas
      df = df.dropna(subset=['tipoextraccion'])
      df['tipoextraccion'] = df['tipoextraccion'].astype(int)

    # Renombramos a las claves del feature store
    df = df.rename(columns={'idpozo': 'id_pozo', 'fecha_data': 'fecha'})
    df['id_pozo'] = df['id_pozo'].astype(str)
    df['fecha'] = pd.to_datetime(df['fecha']).dt.strftime('%Y-%m-%d')

    logger.info("Preprocesado completo: %d filas, columnas: %s", len(df), list(df.columns))

    return df.to_dict(orient='list')

  @task
  def persist_features(data):
    """Persiste las features preprocesadas en featurestore.features.

    Es idempotente: borra el rango de fechas que esta por insertar antes
    del INSERT, de modo que reejecutar el DAG con los mismos parametros
    deja la tabla en el mismo estado.

    Args:
        data: dict de listas devuelto por preprocess().

    Returns:
        dict con metadata de la persistencia (rows, date_from, date_to).
    """
    import logging
    import os

    import pandas as pd
    from sqlalchemy import create_engine, text

    logger = logging.getLogger(__name__)

    db_url = os.environ.get("FEATURESTORE_DB_URL")
    if not db_url:
      raise RuntimeError(
          "FEATURESTORE_DB_URL no esta definida en el entorno. "
          "Configurarla en .env y propagarla via docker-compose."
      )

    df = pd.DataFrame(data)
    if len(df) == 0:
      raise RuntimeError(
          "preprocess() no produjo filas — no hay nada que persistir."
      )

    fecha_min = df['fecha'].min()
    fecha_max = df['fecha'].max()
    logger.info(
        "Persistiendo %d filas en featurestore.features (rango %s a %s)",
        len(df), fecha_min, fecha_max,
    )

    engine = create_engine(db_url)
    try:
      with engine.begin() as conn:
        deleted = conn.execute(
            text("DELETE FROM features WHERE fecha BETWEEN :f AND :t"),
            {"f": fecha_min, "t": fecha_max},
        ).rowcount
        logger.info("Filas removidas del rango antes del INSERT: %d", deleted)

        # to_sql via la misma conexion para que respete la transaccion
        df.to_sql(
            "features",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
    except Exception as exc:
      raise RuntimeError(f"Error al persistir features: {exc}") from exc

    metadata = {
        "rows": len(df),
        "date_from": fecha_min,
        "date_to": fecha_max,
    }
    logger.info("Features persistidas: %s", metadata)
    return metadata

  @task
  def read_features_from_store(persist_metadata):
    """Lee las features desde featurestore.features filtrando por date_from/date_to.

    Esta es la unica fuente de datos del entrenamiento — si la tabla esta
    vacia para el rango solicitado, falla loud para garantizar que el
    pipeline no se entrene silenciosamente sobre raw CSV.

    Args:
        persist_metadata: salida de persist_features() (dependencia de orden).

    Returns:
        dict de listas con todas las columnas del feature store.
    """
    import logging
    import os

    import pandas as pd
    from airflow.sdk import get_current_context
    from sqlalchemy import create_engine, text

    logger = logging.getLogger(__name__)

    db_url = os.environ.get("FEATURESTORE_DB_URL")
    if not db_url:
      raise RuntimeError(
          "FEATURESTORE_DB_URL no esta definida en el entorno."
      )

    context = get_current_context()
    params = context['params']
    date_from = params.get('date_from')
    date_to = params.get('date_to')

    query = "SELECT id_pozo, fecha, tipoextraccion, prod_gas, prod_agua, tef, prod_pet, profundidad FROM features"
    where = []
    args = {}
    if date_from:
      where.append("fecha >= :date_from")
      args['date_from'] = date_from
    if date_to:
      where.append("fecha <= :date_to")
      args['date_to'] = date_to
    if where:
      query += " WHERE " + " AND ".join(where)

    logger.info("Leyendo feature store: %s args=%s", query, args)
    engine = create_engine(db_url)
    df = pd.read_sql(text(query), engine, params=args)

    if len(df) == 0:
      raise RuntimeError(
          f"Feature store vacio para el rango solicitado "
          f"(date_from={date_from}, date_to={date_to}). "
          "El entrenamiento no puede continuar sin features persistidas."
      )

    # Anotamos el rango leido para que train_experiments pueda tagearlo en MLflow
    payload = df.to_dict(orient='list')
    payload['_feature_store_meta'] = {
        "rows": len(df),
        "date_from": date_from or "all",
        "date_to": date_to or "all",
    }
    logger.info(
        "Lectura del feature store completa: %d filas (rango %s -> %s)",
        len(df), date_from or "all", date_to or "all",
    )
    return payload

  @task
  def train_experiments(data):
    """Entrena todos los experimentos definidos en config y registra en MLflow.

    Args:
        data: payload del feature store (dict de listas + _feature_store_meta).

    Returns:
        lista de dicts con run_id y metricas de cada experimento.
    """
    import logging

    from ml_pipeline.config import EXPERIMENTS
    from ml_pipeline.training import train_and_log

    logger = logging.getLogger(__name__)
    meta = data.pop('_feature_store_meta', None)
    if meta is None:
      raise RuntimeError(
          "data no contiene metadata del feature store — el entrenamiento "
          "solo puede correr a partir de read_features_from_store()."
      )

    results = []
    for i, exp_cfg in enumerate(EXPERIMENTS):
      logger.info(
          "Experimento %d/%d — target=%s, features=%s, params=%s",
          i + 1, len(EXPERIMENTS),
          exp_cfg['target'], exp_cfg['features'], exp_cfg['model_params'],
      )
      result = train_and_log(data, exp_cfg, feature_store_meta=meta)
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
    Args: results (list) - lista de dicts con run_id y métricas.
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

  # 1. Descargamos el dataset
  csv_path = download_dataset('/tmp/pozos.csv')

  # 2. Preprocesamos los datos crudos en filas listas para el feature store
  preprocessed = preprocess(csv_path)

  # 3. Persistimos las features en featurestore.features (DELETE-then-INSERT idempotente)
  persist_meta = persist_features(preprocessed)

  # 4. Leemos las features desde el store (unica fuente del entrenamiento)
  data = read_features_from_store(persist_meta)

  # 5. Entrenamos todos los experimentos consumiendo del store
  results = train_experiments(data)

  # 6. Promovemos el mejor modelo como 'production'
  promote_model(results)

  # 7. Dependencias
  start >> csv_path


ml_pipeline()
