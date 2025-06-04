"""Tiny helpers around the compiled pattern (import-friendly for tests)."""
from .config import RX_MATNUM
from typing import Optional

def extract_matnum(text: str) -> Optional[str]:
    """Return the first 4–10-digit stand-alone run, or None."""
    m = RX_MATNUM.search(text or "")
    return m.group(0) if m else None
