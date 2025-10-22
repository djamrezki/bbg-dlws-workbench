# src/bbg_dlws_workbench/soap/submitter.py
from typing import Any, Dict
import logging
from zeep.exceptions import Fault, TransportError
from .registry import OP_HANDLERS

logger = logging.getLogger("bbg-dlws-workbench.submitter")


def submit_request(client, endpoint: str, kind: str, payload: Dict) -> str:
    """
    Submit an asynchronous Bloomberg DLWS request (history or data).
    Returns the responseId / jobId to poll later.
    """
    op = OP_HANDLERS[kind]
    method_name = op["submit"]
    method = getattr(client.service, method_name)

    logger.info(f"Submitting {kind} request via {method_name}…")

    try:
        resp = method(**payload)
    except Fault as e:
        logger.error(f"SOAP Fault during {method_name}: {e.message}")
        raise
    except TransportError as e:
        logger.error(f"Transport error contacting Bloomberg endpoint {endpoint}: {e}")
        raise

    # The DLWS 'submit' responses typically include <responseId> inside the returned object.
    response_id = getattr(resp, "responseId", None) or getattr(resp, "responseID", None)
    if not response_id:
        # Some payloads may return nested structure
        response_id = _extract_response_id(resp)

    if not response_id:
        raise RuntimeError(f"No responseId found in {kind} submit response: {resp!r}")

    logger.info(f"{kind} request submitted successfully. responseId={response_id}")
    return str(response_id)


def get_response_by_id(client, kind: str, response_id: str, timeout: int) -> Any:
    """
    Retrieve an asynchronous DLWS response using its responseId.
    Returns the SOAP response object if ready; otherwise None.
    """
    op = OP_HANDLERS[kind]
    method_name = op["retrieve"]
    method = getattr(client.service, method_name)

    logger.debug(f"Polling {kind} responseId={response_id} with timeout={timeout}s…")

    try:
        resp = method(responseId=response_id, _timeout=timeout)
    except Fault as e:
        # Bloomberg returns Fault when job is not ready yet or invalid
        logger.debug(f"SOAP Fault during retrieve (possibly not ready yet): {e}")
        return None
    except TransportError as e:
        logger.warning(f"Transport error while polling {response_id}: {e}")
        return None

    # Depending on WSDL version, may include .status or .processingStatus
    status = getattr(resp, "status", None) or getattr(resp, "processingStatus", None)
    if status and str(status).lower() not in ("completed", "success", "done"):
        logger.debug(f"{kind} responseId={response_id} status={status} (not ready yet)")
        return None

    logger.info(f"{kind} responseId={response_id} ready. Returning payload.")
    return resp


def call_sync(client, endpoint: str, kind: str, payload: Dict, timeout: int) -> Any:
    """
    Execute a synchronous DLWS request (e.g., getFields).
    Returns the SOAP response object directly.
    """
    op = OP_HANDLERS[kind]
    method_name = op["call"]
    method = getattr(client.service, method_name)

    logger.info(f"Calling synchronous operation {method_name} for {kind}…")

    try:
        resp = method(**payload, _timeout=timeout)
    except Fault as e:
        logger.error(f"SOAP Fault during {method_name}: {e.message}")
        raise
    except TransportError as e:
        logger.error(f"Transport error contacting Bloomberg endpoint {endpoint}: {e}")
        raise

    logger.info(f"Synchronous call {method_name} completed successfully.")
    return resp


# ----------------- Helper -----------------

def _extract_response_id(resp: Any) -> str | None:
    """
    Walk generic Zeep/dict responses to find a responseId field.
    """
    if not resp:
        return None
    if isinstance(resp, dict):
        for k, v in resp.items():
            if k.lower() in ("responseid", "jobid", "requestid"):
                return v
            if isinstance(v, (dict, list)):
                sub = _extract_response_id(v)
                if sub:
                    return sub
    elif hasattr(resp, "__dict__"):
        for k, v in vars(resp).items():
            if k.lower() in ("responseid", "jobid", "requestid"):
                return v
            if isinstance(v, (dict, list, object)):
                sub = _extract_response_id(v)
                if sub:
                    return sub
    elif isinstance(resp, list):
        for item in resp:
            sub = _extract_response_id(item)
            if sub:
                return sub
    return None
