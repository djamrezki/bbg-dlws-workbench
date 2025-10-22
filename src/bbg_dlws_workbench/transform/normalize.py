# src/bbg_dlws_workbench/transform/normalize.py
from typing import Any, Dict, Iterable, Iterator, List, Tuple

def soap_to_rows(kind: str, soap_response: Any, request_fields: List[str]) -> Iterable[Dict]:
    """
    Build rows using the fields as received in the SOAP response (response order),
    not the requested fields. We still prepend identifier (and date for history).
    """
    if kind == "history":
        yield from parse_history(soap_response)
    elif kind == "data":
        yield from parse_data(soap_response)
    elif kind == "fundamentals_headers":
        yield from parse_fundamentals_headers(soap_response)
    else:
        return []


# -------------------- HISTORY (DLWS WSDL-compliant) --------------------

def parse_history(resp: Any) -> Iterator[Dict]:
    """
    WSDL shape (RetrieveGetHistoryResponse):
      - responseId
      - headers (GetHistoryHeaders)
      - fields (Fields): fields.field[] (ordered)
      - instrumentDatas (HistInstrumentDatas):
           instrumentData (HistInstrumentData) {
             code (string)
             instrument (Instrument { id, ... })
             macro (optional)
             pricingSource (optional)
             date (xs:date)
             data[] (HistData { @value })
           }

    Row produced:
      identifier, date, <field1>, <field2>, ...
      where field names come from response.fields.field (positional mapping to data[])
    """
    if not resp:
        return

    # 1) Resolve ordered field names for history
    fields_node = _get_attr(resp, ["fields"])
    field_names: List[str] = []
    if fields_node:
        # Prefer simple string list: fields.field[]
        f_list = _get_attr(fields_node, ["field"]) or []
        if _is_iterable(f_list):
            field_names = [str(x) for x in f_list if x is not None]
        # Fallback: fieldWithOverrides[].field if present
        if not field_names:
            f_ovr_list = _get_attr(fields_node, ["fieldWithOverrides"]) or []
            if _is_iterable(f_ovr_list):
                for fo in f_ovr_list:
                    fname = _get_any(fo, ["field", "mnemonic", "name", "id"])
                    if fname:
                        field_names.append(str(fname))

    # 2) Iterate instrumentDatas.instrumentData[]
    container = _get_attr(resp, ["instrumentDatas"]) or resp
    items = _get_attr(container, ["instrumentData"]) or []

    if _is_iterable(items):
        for it in items:
            ident = (
                    _extract_identifier_from_security(_get_attr(it, ["instrument"]))
                    or str(_get_attr(it, ["code"]) or "")
            )
            date = _fmt_date(_get_attr(it, ["date"]))
            hist_values = _get_attr(it, ["data"]) or []

            # values are HistData elements with @value
            values: List[Any] = []
            if _is_iterable(hist_values):
                for hv in hist_values:
                    values.append(_get_any(hv, ["value"]))

            row: Dict[str, Any] = {"identifier": ident, "date": date}

            if field_names and len(values) >= len(field_names):
                # Map by position
                for i, fname in enumerate(field_names):
                    if fname and fname not in row:
                        row[fname] = values[i] if i < len(values) else None
            else:
                # Fallback: position-based generic columns
                for i, val in enumerate(values, start=1):
                    key = f"COL_{i}"
                    if key not in row:
                        row[key] = val
            yield row
        return

    # 3) Dict-like fallback
    if isinstance(resp, dict):
        items = (
                resp.get("instrumentDatas", {}).get("instrumentData", [])
                or resp.get("instrumentData", [])
        )
        for it in items:
            ident = (
                    _extract_identifier_from_security((it.get("instrument") or {}))
                    or str(it.get("code") or "")
            )
            date = _fmt_date(it.get("date"))
            values = []
            for hv in it.get("data", []):
                values.append((hv.get("value") if isinstance(hv, dict) else hv))

            row = {"identifier": ident, "date": date}
            if field_names and len(values) >= len(field_names):
                for i, fname in enumerate(field_names):
                    if fname and fname not in row:
                        row[fname] = values[i] if i < len(values) else None
            else:
                for i, val in enumerate(values, start=1):
                    key = f"COL_{i}"
                    if key not in row:
                        row[key] = val
            yield row


# -------------------- DATA (DLWS WSDL-compliant) --------------------

def parse_data(resp: Any) -> Iterator[Dict]:
    """
    WSDL shape (RetrieveGetDataResponse):
      - fields (Fields)   [may be present, but each data item already has @field]
      - instrumentDatas (InstrumentDatas):
           instrumentData (InstrumentData) {
             code (string)
             instrument (Instrument { id, ... })
             macro (optional)
             data[] (Data {
               @field, @value, @isArray, @rows,
               bulkarray? { @columns, data[] { @value, @type } }
             })
           }

    Row produced per instrument:
      identifier, <FIELD_A>, <FIELD_B>, ...
      If a Data item is an array, we flatten bulkarray into a JSON-like string.
    """
    if not resp:
        return

    container = _get_attr(resp, ["instrumentDatas"]) or resp
    items = _get_attr(container, ["instrumentData"]) or []

    if _is_iterable(items):
        for it in items:
            ident = (
                    _extract_identifier_from_security(_get_attr(it, ["instrument"]))
                    or str(_get_attr(it, ["code"]) or "")
            )

            datas = _get_attr(it, ["data"]) or []
            row: Dict[str, Any] = {"identifier": ident}

            if _is_iterable(datas):
                for d in datas:
                    fname = _get_any(d, ["field"])
                    if not fname:
                        # Skip nameless cells
                        continue
                    val = _get_any(d, ["value"])
                    # Arrays: flatten bulkarray → JSON-like string to keep single CSV cell
                    bulk = _get_attr(d, ["bulkarray"])
                    if bulk is not None:
                        val = _format_bulkarray(bulk)
                    if fname not in row:
                        row[str(fname)] = val
            yield row
        return

    # Dict-like fallback
    if isinstance(resp, dict):
        items = (
                resp.get("instrumentDatas", {}).get("instrumentData", [])
                or resp.get("instrumentData", [])
        )
        for it in items:
            ident = (
                    _extract_identifier_from_security(it.get("instrument") or {})
                    or str(it.get("code") or "")
            )
            row = {"identifier": ident}
            for d in it.get("data", []):
                if not isinstance(d, dict):
                    continue
                fname = d.get("field")
                if not fname:
                    continue
                val = d.get("value")
                bulk = d.get("bulkarray")
                if bulk is not None:
                    val = _format_bulkarray(bulk)
                if fname not in row:
                    row[fname] = val
            yield row


# -------------------- FIELDS CATALOG / FUNDAMENTALS HEADERS --------------------

def parse_fundamentals_headers(resp: Any) -> Iterator[Dict]:
    """
    Returns metadata rows as received. Typical columns:
      field, displayName, category, datatype, description
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
    # Instrument has an 'id' per WSDL
    for k in ("id", "security", "ticker", "code"):
        v = getattr(sec, k, None)
        if v:
            return str(v)
    if isinstance(sec, dict):
        for k in ("id", "security", "ticker", "code"):
            if k in sec and sec[k]:
                return str(sec[k])
    return ""

def _extract_ordered_fields(obj: Any) -> List[Tuple[str, Any]]:
    """
    (Kept for completeness; not used by the new WSDL-compliant paths.)
    """
    if isinstance(obj, dict):
        pairs: List[Tuple[str, Any]] = []
        for k, v in obj.items():
            if _looks_like_fieldname(k):
                pairs.append((k, v))
        if pairs:
            return pairs

    if _is_iterable(obj):
        pairs = []
        for item in obj:
            name = _get_any(item, ["field", "name", "mnemonic", "id"])
            val = _get_any(item, ["value", "text", "val"])
            if name is not None:
                pairs.append((str(name), val))
        if pairs:
            return pairs

    if hasattr(obj, "__dict__"):
        pairs = []
        for k, v in obj.__dict__.items():
            if _looks_like_fieldname(k):
                pairs.append((k, v))
        if pairs:
            return pairs

    return []

def _looks_like_fieldname(name: str) -> bool:
    return isinstance(name, str) and name == name.upper() and any(c.isalpha() for c in name)

def _format_bulkarray(bulk: Any) -> str:
    """
    Convert BulkArray to a compact JSON-like string:
      [[r1c1, r1c2, ...], [r2c1, r2c2, ...], ...]
    """
    # Extract entries and optional columns
    cols = _get_any(bulk, ["columns"])
    try:
        cols = int(cols) if cols is not None else None
    except Exception:
        cols = None

    entries = _get_attr(bulk, ["data"]) or []
    flat_vals: List[Any] = []
    if _is_iterable(entries):
        for e in entries:
            flat_vals.append(_get_any(e, ["value"]))
    elif isinstance(entries, list):
        for e in entries:
            if isinstance(e, dict):
                flat_vals.append(e.get("value"))
            else:
                flat_vals.append(e)

    if cols and cols > 0:
        rows: List[List[Any]] = []
        for i in range(0, len(flat_vals), cols):
            rows.append(flat_vals[i : i + cols])
        return "[" + ",".join("[" + ",".join(_safe_scalar(x) for x in r) + "]" for r in rows) + "]"

    # No column hint: return flat list
    return "[" + ",".join(_safe_scalar(x) for x in flat_vals) + "]"

def _safe_scalar(x: Any) -> str:
    if x is None:
        return "null"
    s = str(x)
    # Minimal escaping of quotes/backslashes for CSV-safety inside a single cell
    s = s.replace("\\", "\\\\").replace("\"", "\\\"")
    return f"\"{s}\""
