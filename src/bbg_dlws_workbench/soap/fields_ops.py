# api.py
from typing import Any, Dict

def get_fields(client, criteria: Any | None = None) -> Any:
    # Pass an actual FieldSearchCriteria instance when present
    if criteria is not None:
        return client.service.getFields(criteria=criteria)
    return client.service.getFields()
