
import csv, os
from typing import Iterable, Mapping
from .base import Store

class FileSystemStore(Store):
    def write_text(self, uri: str, text: str) -> None:
        folder = os.path.dirname(uri) or "."
        os.makedirs(folder, exist_ok=True)
        with open(uri, "w", encoding="utf-8") as f:
            f.write(text)

    def write_rows_to_csv(self, uri: str, rows: Iterable[Mapping], append: bool) -> None:
        rows = list(rows)
        if not rows:
            return
        folder = os.path.dirname(uri) or "."
        os.makedirs(folder, exist_ok=True)
        mode = "a" if append and os.path.exists(uri) else "w"
        with open(uri, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            if mode == "w":
                writer.writeheader()
            writer.writerows(rows)
