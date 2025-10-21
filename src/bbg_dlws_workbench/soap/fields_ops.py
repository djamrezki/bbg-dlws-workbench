from typing import Any, Dict

def get_fields(client, criteria: Dict | None, timeout: int | None = None) -> Any:
    """
    Calls getFields(criteria=...) and returns the raw zeep response.
    """
    if criteria:
        return client.service.getFields(criteria=criteria, _timeout=timeout)
    return client.service.getFields(_timeout=timeout)
