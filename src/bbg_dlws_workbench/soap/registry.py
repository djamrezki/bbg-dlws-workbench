OP_HANDLERS = {
    "history": {
        "submit": "submitGetHistoryRequest",
        "retrieve": "retrieveGetHistoryResponse",
        "async": True,
    },
    "data": {
        "submit": "submitGetDataRequest",
        "retrieve": "retrieveGetDataResponse",
        "async": True,
    },
    "fundamentals_headers": {
        "call": "getFields",
        "async": False,
        "criteria": {"dlCategories": ["Fundamentals"]}  # default filter
    },
}