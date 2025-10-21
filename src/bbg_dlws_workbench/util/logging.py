
import logging, json, sys

def setup(level: str = "INFO", json_mode: bool = False):
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=lvl, stream=sys.stdout, format="%(asctime)s %(levelname)s %(message)s")
    if json_mode:
        # Simple JSON formatter wrapper (optional enhancement)
        pass
    return logging.getLogger("bbg-dlws-workbench")
