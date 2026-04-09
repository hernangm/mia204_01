from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

DATASET_DOWNLOAD_URL = "http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c/resource/b5b58cdc-9e07-41f9-b392-fb9ec68b0725/download/produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv"

@dag(
    dag_id='ml_pipeline',
    description='Pipeline de Machine Learning con Airflow y MLflow',
    start_date=datetime(2025, 1, 1),
    schedule=None,
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
  def download_dataset(url, save_path):
    """Descarga un dataset CSV desde una URL y lo guarda en disco."""
    import pandas as pd
    print(f"Iniciando descarga desde: {url}")
    df = pd.read_csv(url)
    print(f"Descarga completa: {len(df)} filas, {len(df.columns)} columnas")
    print(f"Columnas: {list(df.columns)}")
    df.to_csv(save_path, index=False)
    print(f"Archivo guardado en: {save_path}")
    return save_path

  @task
  def preprocess(csv_path):
    """Lee el CSV, selecciona columnas relevantes y codifica 'tipoextraccion'
    con un mapa estático para reproducibilidad."""
    import pandas as pd
    from airflow.operators.python import get_current_context

    TIPOEXTRACCION_MAP = {
        'Bombeo Hidráulico': 0,
        'Bombeo Mecánico': 1,
        'Cavidad Progresiva': 2,
        'Electrosumergible': 3,
        'Gas Lift': 4,
        'Jet Pump': 5,
        'Otros Tipos de Extracción': 6,
        'Pistoneo (Swabbing)': 7,
        'Plunger Lift': 8,
        'Sin Sistema de Extracción': 9,
        'Surgencia Natural': 10,
    }

    print(f"Leyendo CSV desde: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"CSV cargado: {len(df)} filas, {len(df.columns)} columnas")

    # Filtramos por rango de fechas si se indicaron params
    context = get_current_context()
    params = context['params']
    date_from = params.get('date_from')
    date_to = params.get('date_to')

    if date_from or date_to:
      df['fecha_data'] = pd.to_datetime(df['fecha_data'])
      if date_from:
        df = df[df['fecha_data'] >= date_from]
        print(f"Filtrado desde {date_from}: {len(df)} filas")
      if date_to:
        df = df[df['fecha_data'] <= date_to]
        print(f"Filtrado hasta {date_to}: {len(df)} filas")
    else:
      print("Sin filtro de fechas aplicado")

    # 1. Seleccionamos las columnas relevantes (incluye profundidad para los experimentos)
    columnas = ['tipoextraccion', 'prod_pet', 'prod_gas', 'prod_agua', 'tef', 'profundidad']
    df = df[columnas]

    # 2. Eliminamos filas con valores nulos
    df = df.dropna()

    # 3. Codificamos 'tipoextraccion' con mapa estático (reproducible entre corridas)
    df['tipoextraccion'] = df['tipoextraccion'].map(TIPOEXTRACCION_MAP)
    df = df.dropna(subset=['tipoextraccion'])
    df['tipoextraccion'] = df['tipoextraccion'].astype(int)

    print(f"Preprocesado completo: {len(df)} filas, columnas: {list(df.columns)}")

    return df.to_dict(orient='list')

  @task
  def train_model(data, experiment_config):
    """Entrena un modelo según la configuración del experimento y registra
    hiperparámetros, métricas y modelo en MLflow.

    Args:
        data (dict): dataset preprocesado (dict de listas).
        experiment_config (dict): configuración del experimento con claves:
            - model_type (str): tipo de modelo (ej. 'random_forest')
            - model_params (dict): hiperparámetros del modelo
            - target (str): columna objetivo
            - features (list): columnas de entrada
    """
    import pandas as pd
    import mlflow
    import mlflow.sklearn
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    MLFLOW_EXPERIMENT_NAME = "hydrocarbon_forecast"

    # 1. Extraer configuración del experimento
    model_type = experiment_config['model_type']
    model_params = experiment_config['model_params']
    target = experiment_config['target']
    features = experiment_config['features']

    # 2. Reconstruir DataFrame
    df = pd.DataFrame(data)
    X = df[features]
    y = df[target]

    # 3. Split train/test para poder calcular métricas reales
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=model_params.get('random_state', 42)
    )

    print(f"Experimento: model_type={model_type}, target={target}")
    print(f"Features: {features}")
    print(f"Params: {model_params}")
    print(f"Train: {len(X_train)} filas, Test: {len(X_test)} filas")

    # 4. Configurar MLflow
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    # 5. Habilitar autologging de sklearn (registra params, métricas y modelo)
    mlflow.sklearn.autolog()

    # 6. Construir y entrenar el modelo dentro de un run de MLflow
    if model_type == 'random_forest':
      modelo = RandomForestRegressor(**model_params)
    else:
      raise ValueError(f"model_type no soportado: {model_type}")

    # El autologger captura fit() automáticamente y crea un run
    modelo.fit(X_train, y_train)

    context = get_current_context()
    params = context['params']
    date_from = params.get('date_from')
    date_to = params.get('date_to')

    # 7. Loguear parámetros adicionales que el autologger no captura
    mlflow.log_param('from', date_from)
    mlflow.log_param('to', date_to)
    mlflow.log_param('target', target)
    mlflow.log_param('features', features)
    mlflow.log_param('n_samples_train', len(X_train))
    mlflow.log_param('n_samples_test', len(X_test))

    # 8. Calcular y loguear métricas sobre el set de test
    y_pred = modelo.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    mlflow.log_metric('test_mae', mae)
    mlflow.log_metric('test_mse', mse)
    mlflow.log_metric('test_r2', r2)

    print(f"Métricas de test — MAE: {mae:.4f}, MSE: {mse:.4f}, R2: {r2:.4f}")

    run_id = mlflow.active_run().info.run_id
    print(f"MLflow run_id: {run_id}")

    # 9. Finalizar el run (autolog lo deja abierto)
    mlflow.end_run()

    return {
        'model_type': model_type,
        'target': target,
        'features': features,
        'model_params': model_params,
        'test_mae': mae,
        'test_mse': mse,
        'test_r2': r2,
        'mlflow_run_id': run_id,
        'n_samples': len(df),
    }

  @task
  def show_results(results):
    """Imprime un resumen de los resultados de cada experimento."""
    print("=" * 60)
    print("RESULTADOS DEL EXPERIMENTO")
    print("=" * 60)
    print(f"Modelo:       {results['model_type']}")
    print(f"Params:       {results['model_params']}")
    print(f"Target:       {results['target']}")
    print(f"Features:     {results['features']}")
    print(f"N muestras:   {results['n_samples']}")
    print(f"Test MAE:     {results['test_mae']:.4f}")
    print(f"Test MSE:     {results['test_mse']:.4f}")
    print(f"Test R2:      {results['test_r2']:.4f}")
    print(f"MLflow Run:   {results['mlflow_run_id']}")
    print("=" * 60)

  # --- Secuencia de tasks ---

  # 1. Descargamos el dataset
  csv_path = download_dataset(DATASET_DOWNLOAD_URL, '/tmp/pozos.csv')

  # 2. Preprocesamos los datos
  data = preprocess(csv_path)

  # 3. Entrenamos un modelo por cada experimento (dynamic task mapping)
  #    Los experimentos se definen en config.py pero se replican aquí porque
  #    el DAG-level code se parsea en el dag-processor que no tiene plugins en sys.path.
  ALL_FEATURES_GAS = ['prod_pet', 'prod_agua', 'tef', 'profundidad', 'tipoextraccion']
  REDUCED_FEATURES_GAS = ['prod_pet', 'tef', 'profundidad']
  experiments = [
      {'model_type': 'random_forest', 'model_params': {'n_estimators': 50,  'random_state': 204}, 'target': 'prod_gas', 'features': ALL_FEATURES_GAS},
      {'model_type': 'random_forest', 'model_params': {'n_estimators': 100, 'random_state': 204}, 'target': 'prod_gas', 'features': ALL_FEATURES_GAS},
      {'model_type': 'random_forest', 'model_params': {'n_estimators': 200, 'random_state': 204}, 'target': 'prod_gas', 'features': ALL_FEATURES_GAS},
      {'model_type': 'random_forest', 'model_params': {'n_estimators': 100, 'random_state': 204}, 'target': 'prod_gas', 'features': REDUCED_FEATURES_GAS},
  ]

  experiment_results = train_model.expand(
      data=[data],
      experiment_config=experiments,
  )

  # 4. Mostramos resultados de cada experimento
  show_results.expand(results=experiment_results)

  # 5. El operador start va antes de todo
  start >> csv_path

ml_pipeline()
