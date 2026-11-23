import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from ..base import BaseSourceAdapter, CDCEvent


class FileSourceAdapter(BaseSourceAdapter):
    """
    Adapter to ingest data from CSV, JSON, or JSONL files.
    Useful for replaying recorded production dumps or external client files.
    """

    def produce_events(
        self,
        file_path: str,
        table: str,
        operation: str = "c",
        key_field: Optional[str] = None,
        **kwargs,
    ) -> Iterable[CDCEvent]:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")

        ext = path.suffix.lower()

        if ext == ".csv":
            yield from self._read_csv(path, table, operation, key_field)
        elif ext == ".json":
            yield from self._read_json(path, table, operation, key_field)
        elif ext in (".jsonl", ".ndjson"):
            yield from self._read_jsonl(path, table, operation, key_field)
        else:
            raise ValueError(f"Unsupported file extension: {ext}. Expected .csv, .json, or .jsonl")

    def _read_csv(self, path: Path, table: str, operation: str, key_field: Optional[str]) -> Iterable[CDCEvent]:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cleaned_row = {
                    k.strip().upper(): (v.strip() if isinstance(v, str) and v.strip() else None)
                    for k, v in row.items()
                    if k
                }
                key_val = cleaned_row.get(key_field.upper()) if key_field else None
                yield CDCEvent(
                    table=table,
                    operation=operation,
                    payload=cleaned_row,
                    key_field=key_field,
                    key_value=key_val,
                )

    def _read_json(self, path: Path, table: str, operation: str, key_field: Optional[str]) -> Iterable[CDCEvent]:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        records = data if isinstance(data, list) else [data]
        for row in records:
            if not isinstance(row, dict):
                continue
            key_val = row.get(key_field) if key_field else None
            yield CDCEvent(
                table=table,
                operation=operation,
                payload=row,
                key_field=key_field,
                key_value=str(key_val) if key_val is not None else None,
            )

    def _read_jsonl(self, path: Path, table: str, operation: str, key_field: Optional[str]) -> Iterable[CDCEvent]:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                key_val = row.get(key_field) if key_field else None
                yield CDCEvent(
                    table=table,
                    operation=operation,
                    payload=row,
                    key_field=key_field,
                    key_value=str(key_val) if key_val is not None else None,
                )

