from abc import ABC, abstractmethod
from datetime import date


class WellRepository(ABC):
    """Abstract contract for well data access.

    Implementations can back this with PostgreSQL, a feature store,
    an ML model registry, or an in-memory store for testing.
    """

    @abstractmethod
    def get_wells(self, date_query: date) -> list[dict]:
        """Return wells with data for the given date.

        Each dict must contain at least: {"id_well": str}
        """
        ...

    @abstractmethod
    def get_forecast(
        self, id_well: str, date_start: date, date_end: date
    ) -> list[dict]:
        """Return production forecast for a well in the date range.

        Each dict must contain: {"date": date, "prod": float}
        """
        ...
