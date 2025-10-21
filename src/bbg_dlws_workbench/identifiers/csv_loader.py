
import csv
from typing import Iterator, Dict, List

def load_identifiers_from_csv(path: str, id_col: str, yk_col: str, type_col: str, extra_cols: List[str]) -> Iterator[Dict]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {id_col, yk_col, type_col}
        missing = [c for c in required if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"Missing required columns in {path}: {missing}")
        for row in reader:
            yield {
                "id": row[id_col],
                "yellow_key": row[yk_col],
                "type": row[type_col],
                "extras": {k: row[k] for k in extra_cols if k in row and row[k] != ""}
            }
