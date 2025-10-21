
from typing import List
from pathlib import Path

def load_fields(cfg) -> List[str]:
    # cfg is FieldsConfig
    if cfg.file:
        p = Path(cfg.file)
        with p.open("r", encoding="utf-8") as f:
            # accept comma, semicolon, or newline separated
            raw = f.read()
        parts = []
        for token in raw.replace(",", "\n").replace(";", "\n").splitlines():
            token = token.strip()
            if token:
                parts.append(token)
        return parts
    return list(dict.fromkeys(cfg.inline))  # de-dup preserve order
