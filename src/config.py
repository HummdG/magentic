"""
Centralised config so tests & main script share constants.
Modify here
"""
from pathlib import Path
from datetime import date
import re

# --- Paths -------------------------------------------------------------
BASE_DIR         = Path("/data")                 # docker mount-point
PRICE_CSV        = BASE_DIR / "price_list.csv"   # master list
DELIVERY_DIR     = BASE_DIR / "deliver_items"    # folder with daily feeds

# --- Batch identity ----------------------------------------------------
BATCH_DATE       = date.today()                  # may be overridden by CLI

# --- Regex pattern-------------------------------------------
RX_MATNUM        = re.compile(r"\b\d{4,10}\b")   # 4–10 stand-alone digits

# --- Ingest tuning -----------------------------------------------------
CHUNK_SIZE       = None   # None = load whole file   🡆 adjust per RAM:  1_000_000 to stream the ingestion if whole file can't be uploaded
