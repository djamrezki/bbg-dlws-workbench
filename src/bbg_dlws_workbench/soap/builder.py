# src/bbg_dlws_workbench/soap/builder.py
from typing import List, Dict, Any, Optional

def _build_instruments(identifiers_batch: List[Dict]) -> Dict[str, Any]:
    """
    Convert our normalized identifiers (id, yellow_key, type) into the WSDL
    <instruments><instrument>...</instrument></instruments> shape.
    """
    instruments = [
        {
            "id": x["id"],
            "yellowkey": x.get("yellow_key", ""),  # note: lowercase per WSDL
            "type": x.get("type", ""),
        }
        for x in identifiers_batch
    ]
    return {"instrument": instruments}

def _build_fields(fields: List[str]) -> Dict[str, Any]:
    """
    Wrap fields as <fields><field>PX_LAST</field>...</fields>.
    Zeep will handle list-of-simple-elements fine with this shape.
    """
    return {"field": list(fields or [])}

def _build_overrides(overrides: List[Dict[str, str]]) -> Optional[Dict[str, Any]]:
    """
    Optional; if provided, map to <overrides><override><field/><value/></override>...</overrides>.
    """
    if not overrides:
        return None
    ov = [{"field": o["name"], "value": o["value"]} for o in overrides]
    return {"override": ov}

def build_payload(
        kind: str,
        fields: List[str],
        identifiers_batch: List[Dict],
        overrides: List[Dict],
        params: Optional[Dict],
) -> Dict[str, Any]:
    """
    Build the kwargs dict you pass to zeep:
        client.service.submitGetHistoryRequest(**payload)
        client.service.submitGetDataRequest(**payload)
        client.service.getFields(**payload)
    """
    params = params or {}

    if kind == "history":
        # submitGetHistoryRequest(headers?, fields, instruments, overrides?)
        payload: Dict[str, Any] = {
            # headers: pass through whatever you set in YAML (dateRange/programFlag/etc.)
            # Example YAML:
            # history_params:
            #   dateRange: { duration: { days: 3 } }
            #   programFlag: adhoc
            "headers": params if params else None,
            "fields": _build_fields(fields),
            "instruments": _build_instruments(identifiers_batch),
        }
        ov = _build_overrides(overrides)
        if ov:
            payload["overrides"] = ov
        # Prune Nones so zeep doesn't send empty elements
        return {k: v for k, v in payload.items() if v is not None}

    if kind == "data":
        # submitGetDataRequest(headers?, fields, instruments, overrides?)
        payload = {
            # Some profiles may accept/require headers (programFlag, etc.)
            "headers": params if params else None,
            "fields": _build_fields(fields),
            "instruments": _build_instruments(identifiers_batch),
        }
        ov = _build_overrides(overrides)
        if ov:
            payload["overrides"] = ov
        return {k: v for k, v in payload.items() if v is not None}

    if kind == "fundamentals_headers":
        # getFields(criteria=FieldSearchCriteria)
        # We default dlCategories=["Fundamentals"] upstream; you can add keyword, sectors, etc.
        criteria = {"dlCategories": ["Fundamentals"]}
        criteria.update(params)
        return {"criteria": criteria}

    raise ValueError(f"Unsupported kind: {kind}")
