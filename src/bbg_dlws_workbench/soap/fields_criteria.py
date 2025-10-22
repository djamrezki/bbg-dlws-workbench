# builders.py
from typing import List, Any

def build_fields_criteria_zeep(client, categories: List[str], sectors: List[str], keywords: List[str]) -> Any:
    """
    Return a proper FieldSearchCriteria instance based on the actual WSDL,
    choosing the correct singular/plural element names at runtime.
    """
    FieldSearchCriteria = client.get_type("ns0:FieldSearchCriteria")
    # Peek element names defined in WSDL for this type
    elem_names = {el.name for el in FieldSearchCriteria._xsd_type.elements}

    key_cat = "dlCategory" if "dlCategory" in elem_names else ("dlCategories" if "dlCategories" in elem_names else None)
    key_sec = "marketsector" if "marketsector" in elem_names else ("marketsectors" if "marketsectors" in elem_names else None)

    kwargs = {}

    # keyword is xs:string (single)
    if keywords:
        kwargs["keyword"] = " ".join(k for k in keywords if k)

    # categories (maxOccurs list). Use exact element name the schema expects.
    if categories and key_cat:
        # Trim to the schema’s documented maxOccurs (usually 5).
        kwargs[key_cat] = categories[:5]

    # sectors (maxOccurs list). Use exact element name the schema expects.
    if sectors and key_sec:
        # Trim to the schema’s documented maxOccurs (usually 10).
        kwargs[key_sec] = sectors[:10]

    # IMPORTANT: omit empty strings/None — some BAS validators reject empty tags
    for k in list(kwargs.keys()):
        v = kwargs[k]
        if v is None or (isinstance(v, str) and not v.strip()) or (isinstance(v, list) and not any(s.strip() for s in v if isinstance(s, str))):
            kwargs.pop(k, None)

    return FieldSearchCriteria(**kwargs)
