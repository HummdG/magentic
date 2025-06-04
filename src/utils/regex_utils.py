"""Helpers around the compiled pattern (import-friendly for tests)."""
from .config import RX_MATNUM
from typing import Optional

def extract_matnum(text: str) -> Optional[str]:
    """Return the first 4–10-digit stand-alone run, or None."""
    m = RX_MATNUM.search(text or "") # test of empty order name - perhaps more robuts input test checking?
    return m.group(0) if m else None
