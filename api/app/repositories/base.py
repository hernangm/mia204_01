from abc import ABC, abstractmethod
from datetime import date


class WellRepository(ABC):
    @abstractmethod
    def list_wells(self, date_query: date) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_forecast(self, id_well: str, date_start: date, date_end: date) -> list[dict[str, object]]:
        raise NotImplementedError
