# build_fields_criteria.py
from typing import Dict, List

def build_fields_criteria(
        categories: List[str],
        sectors: List[str],
        keywords: List[str],
) -> Dict:
    crit: Dict = {}
    if categories:
        # WSDL: maxOccurs=5 (Zeep handles list-of-elements fine)
        crit["dlCategories"] = categories
    if sectors:
        # WSDL: maxOccurs=10 (Zeep handles list-of-elements fine)
        crit["marketsectors"] = sectors
    if keywords:
        # WSDL: keyword is xs:string (single), NOT a list
        # Option 1: join tokens into a single search phrase
        crit["keyword"] = " ".join(keywords)
        # Option 2 (stricter): use only the first term
        # crit["keyword"] = keywords[0]
    return crit
