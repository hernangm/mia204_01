"""Feature Store backed by PostgreSQL.

Provee una interfaz unificada para persistir y recuperar features pre-computadas.
Es la única pieza del sistema que "sabe" cómo hablar con la tabla `features` en
Postgres: el resto del pipeline (feature engineering, training, inferencia) opera
exclusivamente a través de esta clase.

Decisión de diseño — ¿por qué PostgreSQL y no una herramienta dedicada?
-----------------------------------------------------------------------
Herramientas como Feast, Hopsworks o Tecton resuelven problemas que aparecen a
escala de miles de features y equipos grandes: gobernanza, feature sharing entre
proyectos, low-latency online store, etc. Para este proyecto, la complejidad
adicional de operar esas herramientas no agrega valor frente a PostgreSQL, que ya
está en el stack y cumple todos los requisitos funcionales:

  ✓ Persistencia de features pre-computadas (separadas de los datos crudos).
  ✓ Point-in-time correctness: get_training_features(cutoff) garantiza que el
    entrenamiento solo ve datos disponibles antes de esa fecha.
  ✓ Dos rutas de lectura diferenciadas: bulk para training, filtrada por pozo y
    fecha para inferencia — el patrón offline/online store simplificado.
  ✓ Idempotencia en escritura: save_features() hace DELETE-then-INSERT por rango.
  ✓ Índice (idx_features_pozo_fecha) que hace eficientes las lecturas de inferencia.

Esta decisión es análoga a cómo Feast usa PostgreSQL como "online store" en sus
deployments locales. La clave no es la herramienta sino la abstracción: ningún
consumidor (entrenamiento, API, drift) toca SQL directamente.

Ejemplo de uso
--------------
    # Escritura (desde dag_pozos.py)
    fs = FeatureStore()
    fs.save_features(features_df)

    # Training (lectura bulk con corte temporal)
    df = fs.get_training_features(cutoff_date=date(2023, 10, 1))

    # Inferencia (lectura por pozo y rango de fechas)
    rows = fs.get_inference_features("96630", date(2022, 1, 1), date(2022, 12, 31))
"""

import logging
from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ml_pipeline.db import get_engine

log = logging.getLogger(__name__)


class FeatureStore:
    """Interfaz del Feature Store sobre PostgreSQL.

    Encapsula todas las operaciones contra la tabla `features` del featurestore.
    Ningún otro módulo del pipeline debe ejecutar SQL sobre esa tabla directamente.

    Args:
        engine: Engine de SQLAlchemy. Si no se pasa, se obtiene de get_engine()
                (usa FEATURESTORE_DB_URL o el valor por defecto).
    """

    def __init__(self, engine: Engine | None = None) -> None:
        self._engine = engine or get_engine()

    # ------------------------------------------------------------------
    # Escritura
    # ------------------------------------------------------------------

    def save_features(self, features_df: pd.DataFrame, fecha_min=None, fecha_max=None) -> int:
        """Persiste el DataFrame de features en el Feature Store.

        La operación es idempotente: elimina el rango de fechas que va a insertar
        antes del INSERT, de modo que reejecutar con los mismos parámetros deja
        la tabla en el mismo estado. Usa bulk insert por chunks para eficiencia
        con datasets grandes.

        Args:
            features_df: DataFrame con columnas [id_pozo, fecha, tipoextraccion,
                         prod_gas, prod_agua, tef, prod_pet, profundidad].
            fecha_min: Límite inferior del rango a eliminar antes de insertar.
                       Si es None se infiere de features_df['fecha'].
            fecha_max: Límite superior del rango. Si es None se infiere.

        Returns:
            Número de filas insertadas.

        Raises:
            RuntimeError: Si falla la inserción.
        """
        if features_df.empty:
            raise ValueError("No se puede guardar un DataFrame vacío en el Feature Store")

        f_min = fecha_min or features_df['fecha'].min()
        f_max = fecha_max or features_df['fecha'].max()

        log.info("Feature Store: eliminando rango %s → %s antes de insertar", f_min, f_max)
        with self._engine.begin() as conn:
            deleted = conn.execute(
                text("DELETE FROM features WHERE fecha BETWEEN :f AND :t"),
                {"f": f_min, "t": f_max},
            ).rowcount
            log.info("Feature Store: %d filas eliminadas del rango", deleted)

        log.info("Feature Store: insertando %d filas", len(features_df))
        try:
            features_df.to_sql(
                "features",
                self._engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )
        except Exception as exc:
            raise RuntimeError(f"Feature Store: error al insertar features: {exc}") from exc

        log.info("Feature Store: %d features guardadas correctamente", len(features_df))
        return len(features_df)

    # ------------------------------------------------------------------
    # Lectura para entrenamiento (offline store)
    # ------------------------------------------------------------------

    def get_training_features(self, cutoff_date: date | None = None) -> pd.DataFrame:
        """Carga features para entrenamiento con point-in-time correctness.

        Cuando se provee cutoff_date, solo se devuelven features con
        fecha <= cutoff_date. Esto garantiza que reentrenar con la misma fecha
        produce exactamente el mismo dataset — condición necesaria para la
        reproducibilidad del entrenamiento.

        Args:
            cutoff_date: Fecha de corte inclusiva. None = sin filtro.

        Returns:
            DataFrame con columnas de features listo para usar en entrenamiento.

        Raises:
            RuntimeError: Si no se puede leer del Feature Store.
        """
        base_query = (
            "SELECT id_pozo, fecha, tipoextraccion, prod_gas, prod_agua, "
            "tef, prod_pet, profundidad FROM features"
        )
        params: dict = {}
        if cutoff_date is not None:
            query = text(base_query + " WHERE fecha <= :cutoff")
            params = {"cutoff": cutoff_date}
        else:
            query = text(base_query)

        try:
            df = pd.read_sql(query, self._engine, params=params)
        except Exception as exc:
            raise RuntimeError(
                f"Feature Store: error al leer features de entrenamiento: {exc}"
            ) from exc

        log.info(
            "Feature Store: %d features de entrenamiento cargadas (cutoff=%s)",
            len(df),
            cutoff_date or "ninguno",
        )
        return df

    # ------------------------------------------------------------------
    # Lectura para inferencia (online store simplificado)
    # ------------------------------------------------------------------

    def get_inference_features(
        self, id_pozo: str, date_start: date, date_end: date
    ) -> list[dict]:
        """Carga features de un pozo para un rango de fechas, para inferencia.

        Equivalente al "online store": sirve features en tiempo real a la API
        para alimentar el modelo de predicción. Los features ya están
        pre-computados y encodificados, listos para pasar al modelo.

        Args:
            id_pozo: Identificador del pozo.
            date_start: Fecha inicial del rango (inclusiva).
            date_end: Fecha final del rango (inclusiva).

        Returns:
            Lista de dicts con keys [fecha, prod_pet, prod_agua, tef,
            profundidad, tipoextraccion], ordenada cronológicamente.

        Raises:
            RuntimeError: Si no se puede leer del Feature Store.
        """
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT fecha, prod_pet, prod_agua, tef, profundidad, tipoextraccion
                        FROM features
                        WHERE id_pozo = :id_pozo
                          AND fecha BETWEEN :date_start AND :date_end
                        ORDER BY fecha
                        """
                    ),
                    {"id_pozo": id_pozo, "date_start": date_start, "date_end": date_end},
                )
                result = [dict(r._mapping) for r in rows]
        except Exception as exc:
            raise RuntimeError(
                f"Feature Store: error al leer features de inferencia para pozo {id_pozo}: {exc}"
            ) from exc

        log.debug(
            "Feature Store: %d features de inferencia para pozo %s (%s → %s)",
            len(result), id_pozo, date_start, date_end,
        )
        return result

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------

    def count(self) -> int:
        """Devuelve la cantidad de features almacenadas."""
        with self._engine.connect() as conn:
            return conn.execute(text("SELECT COUNT(*) FROM features")).scalar() or 0
