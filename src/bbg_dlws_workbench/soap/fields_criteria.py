from typing import Dict, List

def build_fields_criteria(
        categories: List[str],
        sectors: List[str],
        keywords: List[str],
) -> Dict:
    crit: Dict = {}
    if categories:
        crit["dlCategories"] = categories
    if sectors:
        crit["marketSectors"] = sectors
    if keywords:
        crit["keywords"] = keywords
    return crit
