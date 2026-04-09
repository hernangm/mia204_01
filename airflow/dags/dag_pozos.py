from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id='ml_pipeline',
    description='Pipeline de Machine Learning con Airflow',
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
  def download_dataset(save_path):
    """Descarga un dataset CSV desde la URL definida en ml_pipeline.config y lo guarda en disco.
    Args: save_path (str) - ruta local del archivo.
    Retorna: la ruta del archivo guardado.
    """
    # Importamos dentro de la funcion porque en Airflow cada task se ejecuta
    # en su propio proceso; los imports de arriba del archivo son solo para Airflow.
    import pandas as pd
    from ml_pipeline.config import DATASET_DOWNLOAD_URL
    print(f"Iniciando descarga desde: {DATASET_DOWNLOAD_URL}")
    df = pd.read_csv(DATASET_DOWNLOAD_URL)
    print(f"Descarga completa: {len(df)} filas, {len(df.columns)} columnas")
    print(f"Columnas: {list(df.columns)}")
    df.to_csv(save_path, index=False)
    print(f"Archivo guardado en: {save_path}")
    return save_path

  @task
  def preprocess(csv_path):
    """Lee el CSV, selecciona las columnas relevantes y codifica
    la columna 'tipoextraccion' como enteros.
    Args: csv_path (str) - ruta al archivo CSV.
    Retorna: dict con nombres de columnas como claves y listas de valores.
    """
    import pandas as pd
    from airflow.operators.python import get_current_context
    from ml_pipeline.config import TIPOEXTRACCION_MAP

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

    # 1. Seleccionamos solo las columnas relevantes para el modelo
    columnas = ['tipoextraccion', 'prod_pet', 'prod_gas', 'prod_agua', 'tef']
    df = df[columnas]

    # 2. Eliminamos filas con valores nulos para evitar errores en el modelo
    df = df.dropna()

    # 3. Codificamos 'tipoextraccion' con el mapa estatico de ml_pipeline.config
    #    (reproducible entre corridas, a diferencia de .astype('category').cat.codes
    #    que depende del orden en que aparecen los valores).
    df['tipoextraccion'] = df['tipoextraccion'].map(TIPOEXTRACCION_MAP)

    print(f"Preprocesado completo: {len(df)} filas, columnas: {list(df.columns)}")

    # 4. Convertimos a dict de listas para que Airflow pueda
    #    serializar el resultado como JSON (XCom)
    return df.to_dict(orient='list')

  @task
  def train_model(data, target, features, model_path):
    """Entrena un RandomForestRegressor para predecir el target indicado.
    Guarda el modelo entrenado en la ruta especificada en formato pickle.
    Args: data (dict) - dataset preprocesado, target (str) - columna a predecir,
          features (list) - columnas de entrada, model_path (str) - ruta donde guardar el modelo.
    Retorna: dict con metadata del modelo (target, features, ruta, etc.).
    """
    import pickle
    import random
    import pandas as pd
    from sklearn.ensemble import RandomForestRegressor

    # 1. Reconstruimos el DataFrame a partir del dict que llego por XCom
    df = pd.DataFrame(data)

    # 2. Separamos en X (features = entradas) e y (target = lo que queremos predecir)
    X = df[features]
    y = df[target]
    print(f"Features (X): {features}")
    print(f"Target (y): {target}")

    # 3. Entrenamos el modelo
    print("Entrenando RandomForestRegressor (n_estimators=100)...")
    modelo = RandomForestRegressor(n_estimators=100, random_state=42)
    modelo.fit(X, y)
    print("Modelo entrenado exitosamente")

    # 4. Guardamos el modelo entrenado en disco con pickle
    with open(model_path, 'wb') as f:
      pickle.dump(modelo, f)
    print(f"Modelo guardado en: {model_path}")

    # 5. Elegimos una muestra aleatoria para usar despues en prediccion
    idx = random.randint(0, len(df) - 1)
    muestra = X.iloc[idx].tolist()
    print(f"Muestra aleatoria seleccionada (indice {idx}): {muestra}")

    # 6. Retornamos metadata (no el modelo, porque no es serializable a JSON)
    return {
      'target': target,
      'features': features,
      'model_path': model_path,
      'n_samples': len(df),
      'random_sample': muestra,
    }

  @task
  def predict(model_info, sample):
    """Carga un modelo entrenado desde disco y predice el valor del target.
    Args: model_info (dict) - metadata del modelo incluyendo 'model_path',
          sample (list) - dato de entrada para predecir.
    Retorna: dict con model_info más 'sample' y 'prediction'.
    """
    import pickle

    # 1. Cargamos el modelo guardado en disco (el inverso de pickle.dump)
    print(f"Cargando modelo desde: {model_info['model_path']}")
    with open(model_info['model_path'], 'rb') as f:
      modelo = pickle.load(f)
    print("Modelo cargado exitosamente")

    # 2. Predecimos: el modelo espera una lista de muestras (2D),
    #    por eso envolvemos sample en otra lista [sample]
    print(f"Realizando prediccion para muestra: {sample}")
    prediccion = modelo.predict([sample])[0]
    print(f"Prediccion de '{model_info['target']}': {prediccion}")

    # 3. Retornamos toda la info + la muestra y su prediccion
    model_info['sample'] = sample
    model_info['prediction'] = prediccion
    return model_info

  @task
  def show_results(results):
    """Imprime un resumen de los resultados de predicción del modelo.
    Args: results (dict) - contiene target, features, n_samples, random_sample, prediction.
    """
    print("=" * 50)
    print("RESULTADOS DEL MODELO")
    print("=" * 50)
    print(f"Target:       {results['target']}")
    print(f"Features:     {results['features']}")
    print(f"N muestras:   {results['n_samples']}")
    print(f"Muestra:      {results['sample']}")
    print(f"Prediccion:   {results['prediction']}")
    print("=" * 50)

  @task
  def extract_sample(model_info):
    """Extrae la muestra aleatoria de la metadata del modelo.
    Necesario porque no se puede acceder a claves de un XCom reference
    en la secuencia de tasks (solo se resuelve en tiempo de ejecucion).
    """
    print(f"Muestra extraida: {model_info['random_sample']}")
    return model_info['random_sample']

  # --- Secuencia de tasks ---

  # 1. Descargamos el dataset (la URL vive en ml_pipeline.config)
  csv_path = download_dataset('/tmp/pozos.csv')

  # 2. Preprocesamos los datos
  data = preprocess(csv_path)

  # 3. Entrenamos el modelo para predecir produccion de petroleo
  features = ['tipoextraccion', 'prod_gas', 'prod_agua', 'tef']
  model_info = train_model(data, 'prod_pet', features, '/tmp/modelo.pkl')

  # 4. Extraemos la muestra y predecimos
  sample = extract_sample(model_info)
  resultados = predict(model_info, sample)

  # 5. Mostramos resultados
  show_results(resultados)

  # 6. El operador start va antes de todo
  start >> csv_path

ml_pipeline()

"""
Preguntas disparadoras

Q: Supongamos que nuestro foco es la iteración. Diariamente entrenamos múltiples modelos. ¿Cómo haríamos para registrar y comparar las métricas que producen cada uno?
A: Hoy los resultados solo se imprimen en los logs y se pierden. Habría que usar una herramienta que registre las métricas de cada corrida y permita compararlas visualmente.

Q: Si cambiáramos el número de estimadores, ¿cómo sabríamos cuál dió mejor resultado? ¿en algún lugar tenemos registro de los hiperparametros usados en cada ejecución?
A: No. El n_estimators=100 está hardcodeado y no queda asociado a ningún resultado. Se necesita un sistema que loguee parámetros y métricas juntos para saber qué configuración dio cada resultado.

Q: El DAG guarda los modelos en archivos de formato pickle, pisando siempre la última versión. ¿Cómo podrías volver a una versión anterior del modelo si la nueva resultó ser peor?
A: Lo ideal es usar un registro de modelos que los versione automáticamente y permita hacer rollback.

Q: Supongamos que tuvimos un buen resultado en un entrenamiento, ¿tenemos toda la información necesaria para reproducirlo otra vez? ¿Cómo procedemos, si por el contrario, recibimos quejas respectos al comportamiento del modelo?
A: Hoy no. El random no tiene seed fijo, no se guardan los params de fecha, el dataset puede cambiar y los hiperparámetros no se registran. Para reproducir un entrenamiento hay que trackear todos esos elementos.

"""