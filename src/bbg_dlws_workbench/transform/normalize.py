# src/bbg_dlws_workbench/transform/normalize.py
from typing import Any, Dict, Iterable, Iterator, List, Tuple

def soap_to_rows(kind: str, soap_response: Any, request_fields: List[str]) -> Iterable[Dict]:
    """
    Default behavior: build rows using the fields *as received in the SOAP response*
    (response order), not the requested fields. We still prepend an identifier (and
    date for history) when available.
    """
    if kind == "history":
        yield from parse_history(soap_response)
    elif kind == "data":
        yield from parse_data(soap_response)
    elif kind == "fundamentals_headers":
        yield from parse_fundamentals_headers(soap_response)
    else:
        return []


# -------------------- HISTORY --------------------

def parse_history(resp: Any) -> Iterator[Dict]:
    """
    Yields rows like:
      identifier, date, <fields...in the order they appear in the response>
    """
    if not resp:
        return

    # Try a few common shapes for DLWS history responses.
    # You should tighten these once you inspect real objects with zeep.
    histories = (
            _get_attr(resp, ["histories", "history", "data"])
            or []
    )

    # If zeep gave us something iterable (and not a plain dict/str)
    if _is_iterable(histories):
        for h in histories:
            ident = _extract_identifier_from_security(
                _get_attr(h, ["security", "instrument"])
            ) or ""
            entries = _get_attr(h, ["dates", "values"]) or []
            if _is_iterable(entries):
                for e in entries:
                    date = _get_attr(e, ["date", "pricingDate"])
                    # Extract ordered field pairs from e (or e.fields if present)
                    field_pairs = _extract_ordered_fields(_get_attr(e, ["fields"]) or e)
                    row: Dict[str, Any] = {}
                    row["identifier"] = ident
                    row["date"] = _fmt_date(date)
                    # preserve response order of fields
                    for name, val in field_pairs:
                        if name not in row:  # keep first occurrence in case of dupes
                            row[name] = val
                    yield row
        return

    # Fallback for dict-like
    if isinstance(resp, dict):
        for h in resp.get("histories", resp.get("history", [])):
            ident = _extract_identifier_from_security(h.get("security") or h.get("instrument")) or ""
            for e in h.get("dates", h.get("values", [])):
                date = e.get("date") or e.get("pricingDate")
                field_pairs = _extract_ordered_fields(e.get("fields") or e)
                row = {"identifier": ident, "date": _fmt_date(date)}
                for name, val in field_pairs:
                    if name not in row:
                        row[name] = val
                yield row


# -------------------- DATA --------------------

def parse_data(resp: Any) -> Iterator[Dict]:
    """
    Yields rows like:
      identifier, <fields...in the order they appear in the response>
    """
    if not resp:
        return

    block = _get_attr(resp, ["dataResponse", "responses"]) or resp
    securities = _get_attr(block, ["securityData", "securities"]) or []

    if _is_iterable(securities):
        for s in securities:
            ident = _extract_identifier_from_security(
                _get_attr(s, ["security", "instrument"])
            ) or ""
            field_source = _get_attr(s, ["fieldData"]) or s
            field_pairs = _extract_ordered_fields(field_source)
            row: Dict[str, Any] = {"identifier": ident}
            for name, val in field_pairs:
                if name not in row:
                    row[name] = val
            yield row
        return

    if isinstance(resp, dict):
        for s in resp.get("dataResponse", {}).get("securityData", []):
            ident = _extract_identifier_from_security(s.get("security") or s.get("instrument")) or ""
            field_pairs = _extract_ordered_fields(s.get("fieldData") or s)
            row = {"identifier": ident}
            for name, val in field_pairs:
                if name not in row:
                    row[name] = val
            yield row


# -------------------- FIELDS CATALOG / FUNDAMENTALS HEADERS --------------------

def parse_fundamentals_headers(resp: Any) -> Iterator[Dict]:
    """
    Returns metadata rows as received. Typical columns:
      field, displayName, category, datatype, description
    We keep the order: field, displayName, category, datatype, description
    (missing keys left blank).
    """
    if not resp:
        return

    container = _get_attr(resp, ["fields", "FieldSearchResponse"]) or resp
    items = _get_attr(container, ["field", "fields"]) or []

    if _is_iterable(items):
        for f in items:
            row = {
                "field":       _get_any(f, ["field", "mnemonic", "name", "id"]) or "",
                "displayName": _get_any(f, ["displayName", "label", "description"]) or "",
                "category":    _get_any(f, ["category", "dlCategory"]) or "",
                "datatype":    _get_any(f, ["datatype", "type"]) or "",
                "description": _get_any(f, ["description", "longDescription"]) or "",
            }
            yield row
        return

    if isinstance(resp, dict):
        for f in resp.get("fields", {}).get("field", []):
            row = {
                "field":       f.get("field") or f.get("mnemonic") or f.get("name") or f.get("id") or "",
                "displayName": f.get("displayName") or f.get("label") or f.get("description") or "",
                "category":    f.get("category") or f.get("dlCategory") or "",
                "datatype":    f.get("datatype") or f.get("type") or "",
                "description": f.get("description") or f.get("longDescription") or "",
            }
            yield row


# -------------------- Helpers --------------------

def _is_iterable(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, (str, bytes, dict)):
        return False
    try:
        iter(x)
        return True
    except TypeError:
        return False

def _fmt_date(d: Any) -> str:
    return "" if d is None else str(d)

def _get_attr(obj: Any, names: List[str]) -> Any:
    """
    Try attributes in order, then dict keys, return first hit.
    """
    if obj is None:
        return None
    for n in names:
        v = getattr(obj, n, None)
        if v is not None:
            return v
        if isinstance(obj, dict) and n in obj and obj[n] is not None:
            return obj[n]
    return None

def _get_any(obj: Any, keys: List[str]):
    return _get_attr(obj, keys)

def _extract_identifier_from_security(sec: Any) -> str:
    if not sec:
        return ""
    # Typical keys: id / security / ticker
    for k in ("id", "security", "ticker"):
        v = getattr(sec, k, None)
        if v:
            return str(v)
    if isinstance(sec, dict):
        for k in ("id", "security", "ticker"):
            if k in sec and sec[k]:
                return str(sec[k])
    return ""

def _extract_ordered_fields(obj: Any) -> List[Tuple[str, Any]]:
    """
    Return a list of (fieldName, value) in the order they appear in the response.
    Tries multiple common shapes:
      - dict-like with field mnemonics as keys
      - iterable of items with (field/name/mnemonic, value/text/val)
      - zeep object with attributes (fallback heuristic)
    """
    # 1) dict-like: keep insertion order from the source if possible
    if isinstance(obj, dict):
        pairs: List[Tuple[str, Any]] = []
        for k, v in obj.items():
            if _looks_like_fieldname(k):
                pairs.append((k, v))
        if pairs:
            return pairs

    # 2) iterable of name/value items
    if _is_iterable(obj):
        pairs = []
        for item in obj:
            name = _get_any(item, ["field", "name", "mnemonic", "id"])
            val = _get_any(item, ["value", "text", "val"])
            if name is not None:
                pairs.append((str(name), val))
        if pairs:
            return pairs

    # 3) zeep object with attributes that look like fields (heuristic)
    if hasattr(obj, "__dict__"):
        pairs = []
        for k, v in obj.__dict__.items():
            if _looks_like_fieldname(k):
                pairs.append((k, v))
        if pairs:
            return pairs

    return []

def _looks_like_fieldname(name: str) -> bool:
    # Bloomberg field mnemonics tend to be ALLCAPS with underscores, but we keep this permissive.
    return isinstance(name, str) and name == name.upper() and any(c.isalpha() for c in name)
