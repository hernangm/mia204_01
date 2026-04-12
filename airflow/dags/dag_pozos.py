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
    """Lee el CSV, selecciona las columnas necesarias según los experimentos
    definidos en config y codifica 'tipoextraccion' como enteros.
    Args: csv_path (str) - ruta al archivo CSV.
    Retorna: dict con nombres de columnas como claves y listas de valores.
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

    if date_from or date_to:
      df['fecha_data'] = pd.to_datetime(df['fecha_data'])
      if date_from:
        df = df[df['fecha_data'] >= date_from]
        logger.info("Filtrado desde %s: %d filas", date_from, len(df))
      if date_to:
        df = df[df['fecha_data'] <= date_to]
        logger.info("Filtrado hasta %s: %d filas", date_to, len(df))
    else:
      logger.info("Sin filtro de fechas aplicado")

    # Recopilamos todas las columnas necesarias de los experimentos
    columnas = set()
    for exp in EXPERIMENTS:
      columnas.update(exp['features'])
      columnas.add(exp['target'])

    # Verificamos que todas las columnas existan en el CSV
    faltantes = columnas - set(df.columns)
    if faltantes:
      raise RuntimeError(
          f"Columnas requeridas por EXPERIMENTS no encontradas en el CSV: {faltantes}. "
          f"Columnas disponibles: {list(df.columns)}"
      )

    df = df[list(columnas)]

    # Eliminamos filas con valores nulos
    n_antes = len(df)
    df = df.dropna()
    logger.info("Filas eliminadas por nulos: %d", n_antes - len(df))

    # Codificamos 'tipoextraccion' con el mapa estático de config
    if 'tipoextraccion' in df.columns:
      df['tipoextraccion'] = df['tipoextraccion'].map(TIPOEXTRACCION_MAP)

    logger.info("Preprocesado completo: %d filas, columnas: %s", len(df), list(df.columns))

    # Convertimos a dict de listas (JSON-serializable para XCom)
    return df.to_dict(orient='list')

  @task
  def train_experiments(data):
    """Entrena todos los experimentos definidos en config y registra en MLflow.
    Args: data (dict) - dataset preprocesado.
    Retorna: lista de dicts con run_id y métricas de cada experimento.
    """
    import logging

    from ml_pipeline.config import EXPERIMENTS
    from ml_pipeline.training import train_and_log

    logger = logging.getLogger(__name__)
    results = []

    for i, exp_cfg in enumerate(EXPERIMENTS):
      logger.info(
          "Experimento %d/%d — target=%s, features=%s, params=%s",
          i + 1, len(EXPERIMENTS),
          exp_cfg['target'], exp_cfg['features'], exp_cfg['model_params'],
      )
      result = train_and_log(data, exp_cfg)
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

  # 2. Preprocesamos los datos
  data = preprocess(csv_path)

  # 3. Entrenamos todos los experimentos y registramos en MLflow
  results = train_experiments(data)

  # 4. Promovemos el mejor modelo como 'production'
  promote = promote_model(results)

  # 5. Dependencias
  start >> csv_path

ml_pipeline()
