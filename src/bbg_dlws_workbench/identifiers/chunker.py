
from typing import Iterator, List, Dict

def chunk(items: Iterator[Dict], size: int) -> Iterator[List[Dict]]:
    batch = []
    for it in items:
        batch.append(it)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
