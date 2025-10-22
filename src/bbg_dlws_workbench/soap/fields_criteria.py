# fields_criteria.py
from typing import List, Any

# Exact enums per your WSDL
VALID_SECTORS = {
    "Govt", "Corp", "Mtge", "M-Mkt", "Muni", "Pfd", "Equity", "Comdty", "Index", "Curncy"
}

# handy alias map for sectors users tend to type
SECTOR_ALIASES = {
    "equities": "Equity",
    "equity": "Equity",
    "fx": "Curncy",
    "currencies": "Curncy",
    "currency": "Curncy",
    "commodities": "Comdty",
    "commodity": "Comdty",
    "mmkt": "M-Mkt",
    "money-market": "M-Mkt",
    "mortgage": "Mtge",
    "muni": "Muni",
    "pfd": "Pfd",
    "govt": "Govt",
    "corp": "Corp",
    "index": "Index",
}

# A short, safe subset of DLCategory values you actually want to use.
VALID_CATEGORIES = {
    "Fundamentals",
    "Security Master",
    "End of Day Pricing",
    "Historical Time Series",
    "Corporate Actions",
    "Derived Data",
    "Estimates",
    "Quote Composite",
    # add more from the WSDL if you need them
}

CATEGORY_ALIASES = {
    "fundamental": "Fundamentals",
    "fundamentals": "Fundamentals",
    "security master": "Security Master",
    "eod": "End of Day Pricing",
    "end of day": "End of Day Pricing",
    "historical": "Historical Time Series",
    "hist": "Historical Time Series",
    "corp actions": "Corporate Actions",
    "corporate actions": "Corporate Actions",
    "est": "Estimates",
    "quote": "Quote Composite",
    # Note: "Market Data" is NOT in the enum → will be dropped
}

def _normalize_sectors(sectors: List[str]) -> List[str]:
    out: List[str] = []
    for s in sectors:
        if not s:
            continue
        key = s.strip()
        norm = SECTOR_ALIASES.get(key.lower(), key)
        if norm in VALID_SECTORS:
            out.append(norm)
    # respect maxOccurs=10
    return out[:10]

def _normalize_categories(categories: List[str]) -> List[str]:
    out: List[str] = []
    for c in categories:
        if not c:
            continue
        key = c.strip()
        norm = CATEGORY_ALIASES.get(key.lower(), key)
        if norm in VALID_CATEGORIES:
            out.append(norm)
    # respect maxOccurs=5
    return out[:5]

def build_fields_criteria_zeep(client, categories: List[str], sectors: List[str], keywords: List[str]) -> Any:
    FieldSearchCriteria = client.get_type("ns0:FieldSearchCriteria")

    kwargs = {}
    cats = _normalize_categories(categories)
    secs = _normalize_sectors(sectors)

    if keywords:
        kwargs["keyword"] = " ".join(k for k in keywords if k).strip() or None
    if cats:
        kwargs["dlCategories"] = cats
    if secs:
        kwargs["marketsectors"] = secs

    # Create an instance of the Zeep type – Zeep will serialize exactly per the WSDL
    return FieldSearchCriteria(**{k: v for k, v in kwargs.items() if v is not None})
