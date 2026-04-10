from abc import ABC, abstractmethod
from datetime import date


class WellRepository(ABC):
    @abstractmethod
    def list_wells(self, date_query: date) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_forecast(self, id_well: str, date_start: date, date_end: date) -> list[dict[str, object]]:
        raise NotImplementedError

    @abstractmethod
    def get_features(
        self, id_well: str, date_start: date, date_end: date
    ) -> list[dict[str, object]]:
        """Devuelve las filas de features de un pozo en un rango de fechas.

        Cada fila debe contener al menos: date, prod_pet, prod_agua, tef,
        profundidad, tipoextraccion. El endpoint de forecast usa estas filas
        para alimentar el modelo productivo de MLflow.
        """
        raise NotImplementedError
