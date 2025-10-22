# src/bbg_dlws_workbench/soap/poller.py
import time
import logging
from typing import Any, Optional

import requests

logger = logging.getLogger("bbg-dlws-workbench.poller")

READY_CODES = {0}
CONTINUE_CODES = {100, 300}

def _extract_status_code(resp: Any) -> Optional[int]:
    """
    Try multiple common DLWS shapes to extract an integer status code.
    Returns None if unknown/absent.
    """
    if resp is None:
        return None

    # direct int
    if isinstance(resp, int):
        return resp

    # attr shapes
    for path in (
            ("statusCode", "code"),            # resp.statusCode.code
            ("statusCode",),                   # resp.statusCode (int or str)
            ("responseStatus", "statusCode"),  # resp.responseStatus.statusCode
            ("status", "code"),                # resp.status.code
            ("processingStatus", "code"),      # resp.processingStatus.code
    ):
        obj = resp
        try:
            for p in path:
                obj = getattr(obj, p)
            if isinstance(obj, (int,)) or (isinstance(obj, str) and obj.isdigit()):
                return int(obj)
        except Exception:
            pass

    # dict-like fallbacks
    if isinstance(resp, dict):
        for keys in (("statusCode", "code"), ("statusCode",), ("responseStatus", "statusCode")):
            obj = resp
            try:
                for k in keys:
                    obj = obj[k]
                if isinstance(obj, (int,)) or (isinstance(obj, str) and obj.isdigit()):
                    return int(obj)
            except Exception:
                pass

    return None


class Poller:
    def __init__(self, attempts: int, interval_s: int, per_attempt_timeout_s: int):
        self.attempts = attempts
        self.interval_s = interval_s
        self.per_attempt_timeout_s = per_attempt_timeout_s

    def poll(self, fetch_fn):
        """
        fetch_fn() should perform one retrieve attempt and return:
          - a response object when available (even if still 'processing')
          - or None if not ready yet / transient failure
        We decode a status code from the response:
          0   -> success (return response)
          100/300 -> keep polling
          other -> raise RuntimeError (terminal/unknown)
        """
        last_resp: Any = None
        for i in range(1, self.attempts + 1):
            try:
                resp = fetch_fn()  # your get_response_by_id already applies per-attempt timeout
                last_resp = resp
            except requests.Timeout:
                logger.debug(f"[poll] Attempt {i}/{self.attempts}: request timeout")
                resp = None
            except Exception as e:
                # Treat unexpected transient errors as "not ready", but log them
                logger.debug(f"[poll] Attempt {i}/{self.attempts}: transient error: {e}")
                resp = None

            if resp is None:
                logger.debug(f"[poll] Attempt {i}/{self.attempts}: no response yet")
            else:
                code = _extract_status_code(resp)
                if code is None:
                    # If there is a response but no code, assume success and return (conservative)
                    logger.info(f"[poll] Attempt {i}/{self.attempts}: no status code present; assuming ready")
                    return resp

                if code in READY_CODES:
                    logger.info(f"[poll] Attempt {i}/{self.attempts}: completed (statusCode={code})")
                    return resp
                if code in CONTINUE_CODES:
                    logger.debug(f"[poll] Attempt {i}/{self.attempts}: still processing (statusCode={code})")
                else:
                    # Unknown/terminal code -> stop with context
                    raise RuntimeError(f"Polling stopped: terminal statusCode={code} on attempt {i}/{self.attempts}")

            # sleep before next attempt
            logger.debug(f"[poll] Sleeping {self.interval_s}s before next attempt")
            time.sleep(self.interval_s)

        # Exhausted attempts
        raise TimeoutError(
            f"Polling exceeded {self.attempts} attempts (interval={self.interval_s}s). "
            f"Last statusCode={_extract_status_code(last_resp)}"
        )
