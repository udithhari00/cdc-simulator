from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Dict, Iterable, Optional


@dataclass
class CDCEvent:
    table: str
    operation: str = "c"  # 'c' (create), 'u' (update), 'd' (delete), 'r' (read/snapshot)
    payload: Dict[str, Any] = field(default_factory=dict)
    key_field: Optional[str] = None
    key_value: Optional[str] = None
    source_timestamp: Optional[int] = None

    def __post_init__(self):
        if self.source_timestamp is None:
            self.source_timestamp = int(datetime.now(UTC).timestamp() * 1000)
        self.table = self.table.upper()
        self.operation = self.operation.lower()


class BaseSourceAdapter(ABC):
    """
    Abstract Base Class for pluggable data source adapters.
    Any source (Mock, CSV, JSON, Postgres, MySQL, Kafka) implements this interface.
    """

    @abstractmethod
    def produce_events(self, **kwargs) -> Iterable[CDCEvent]:
        """
        Yields CDCEvents to be sent to Kafka and processed by downstream Lambdas.
        """
        pass

