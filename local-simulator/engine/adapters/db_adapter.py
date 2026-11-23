import logging
from typing import Any, Dict, Iterable, Optional

from ..base import BaseSourceAdapter, CDCEvent

logger = logging.getLogger("db_adapter")


class GenericDatabaseAdapter(BaseSourceAdapter):
    """
    Adapter for querying external databases (PostgreSQL, MySQL, SQLite)
    and streaming rows as CDCEvents to Kafka.
    """

    def produce_events(
        self,
        table: str,
        query: str,
        connection_url: str,
        operation: str = "c",
        key_field: Optional[str] = None,
        **kwargs,
    ) -> Iterable[CDCEvent]:
        try:
            from sqlalchemy import create_engine, text
        except ImportError:
            raise ImportError("SQLAlchemy is required for GenericDatabaseAdapter. Please install sqlalchemy.")

        logger.info(f"Connecting to database with query: {query}")
        engine = create_engine(connection_url)
        with engine.connect() as conn:
            result = conn.execute(text(query))
            for row in result.mappings():
                payload = dict(row)
                key_val = str(payload.get(key_field)) if key_field and key_field in payload else None
                yield CDCEvent(
                    table=table,
                    operation=operation,
                    payload=payload,
                    key_field=key_field,
                    key_value=key_val,
                )

