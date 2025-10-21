
from .registry import OP_HANDLERS

def submit_request(client, endpoint: str, kind: str, payload: dict) -> str:
    op = OP_HANDLERS[kind]
    method = getattr(client.service, op["submit"])
    # Example: resp = method(**payload)
    # return resp.responseId
    raise NotImplementedError("Wire submit_request to actual DLWS operation")

def get_response_by_id(client, kind: str, response_id: str, timeout: int):
    op = OP_HANDLERS[kind]
    method = getattr(client.service, op["retrieve"])
    # Example: return method(responseId=response_id, _timeout=timeout)
    return None

def call_sync(client, endpoint: str, kind: str, payload: dict, timeout: int):
    op = OP_HANDLERS[kind]
    method = getattr(client.service, op["call"])
    # Example: return method(**payload, _timeout=timeout)
    return None
