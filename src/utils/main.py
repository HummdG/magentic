"""
Exact match (CSV-only).
Example:
    python -m src.utils.main \
        --input /data/deliver_items/delivery_items_2025-06-04.csv
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date
from pathlib import Path

import pandas as pd
from pydantic import TypeAdapter

from .config import BASE_DIR, CHUNK_SIZE, RX_MATNUM
from .models import PriceRow, DeliveryRow
from .vector_stage import shortlist_for_llm
from .llm_bridge import classify_batches

from .config import (
    RX_MATNUM,
)

# ----------------------------------------------------------------------
def build_price_index(price_csv: Path):
    """Validate price list and return (DataFrame, dict for O(1) look-ups)."""
    price_df = pd.read_csv(
        price_csv,
        usecols=["material_number", "price"],
        dtype={"material_number": "string", "price": "float32"},
    )
    # bulk Pydantic validation (v2.6-compatible)
    TypeAdapter(list[PriceRow]).validate_python(price_df.to_dict("records"))
    index = dict(zip(price_df["material_number"].str.strip(), price_df.index))
    return price_df, index


# -----------------------------------------------------------------------------
# Core batch job
# -----------------------------------------------------------------------------
def process_file(
    delivery_csv: Path,
    out_dir: Path,
    chunk_size: int | None = None,
    price_csv: Path | None = None,
    validate_chunks: bool = False,
) -> None:
    """Run full pipeline on a single delivery CSV."""
    price_csv = price_csv or (BASE_DIR / "price_list.csv")
    price_df, price_index = build_price_index(price_csv)

    out_dir.mkdir(parents=True, exist_ok=True)
    today = date.today()
    matched_csv  = out_dir / f"matched_{today}.csv"
    residual_csv = out_dir / f"residual_{today}.csv"

    first_chunk   = True
    residual_buf: list[pd.DataFrame] = []

    # pandas reader: single DF if chunk_size is None/0, else iterator
    reader = pd.read_csv(
        delivery_csv,
        usecols=["order_id", "order_name", "qty", "ordered_price"],
        dtype={"order_id": "int64", "order_name": "string",
               "qty": "int32", "ordered_price": "float32"},
        chunksize=chunk_size if chunk_size else None,
        low_memory=False,
    )

    for chunk in reader:
        # optional row-level validation
        if validate_chunks:
            TypeAdapter(list[DeliveryRow]).validate_python(chunk.to_dict("records"))

        # Tier-A: regex material-number pull + exact dict lookup
        chunk["matnum_found"] = chunk["order_name"].str.extract(RX_MATNUM, expand=False)
        chunk["pl_idx"]       = chunk["matnum_found"].map(price_index, na_action="ignore")

        matched   = chunk[chunk["pl_idx"].notna()].copy()
        residual  = chunk[chunk["pl_idx"].isna()].copy()
        residual_buf.append(residual)

        if not matched.empty:
            matched["price_list_price"] = price_df.loc[matched["pl_idx"], "price"].to_numpy()

        # append to CSV sinks
        matched.to_csv(matched_csv, mode="w" if first_chunk else "a",
                       header=first_chunk, index=False)
        residual.to_csv(residual_csv, mode="w" if first_chunk else "a",
                        header=first_chunk, index=False)
        first_chunk = False

    # ------------------------------------------------------------------
    # Vector shortlist + LLM gate for all residual rows
    # ------------------------------------------------------------------
    residual_full = pd.concat(residual_buf, ignore_index=True)
    batches: list[tuple[str, str]] = []           # (delivery_prompt, prices_prompt)

    for order_row, cand_df in shortlist_for_llm(residual_full):
        d_prompt = f'DELIVERY: "{order_row.order_name}"  price={order_row.ordered_price}'
        p_prompt = "\n".join(
            f'{r.material_number}: {r.name}  price={r.price}' for r in cand_df.itertuples()
        )
        batches.append((d_prompt, p_prompt))

    if batches:
        groups = [batches[i:i + 20] for i in range(0, len(batches), 20)]
        results = asyncio.run(classify_batches(groups))
        # TODO: parse `results` JSON and append successful matches to matched_csv
        print(f"✅  LLM batches sent: {len(groups)}  (rows gated by price delta: {len(batches)})")

def cli():

    """Entry point for command-line usage: parses input arguments and initiates the CSV processing pipeline.

    This function enables the script to be run from the command line with user-specified options,
    making it reusable and flexible for batch processing without modifying the code.
    It separates configuration (inputs, outputs, chunk size) from logic, following good CLI design practices.
    """
    
    p = argparse.ArgumentParser(description="Tier-A CSV matcher (regex + dict).") # Create an argument parser with a description of the tool
    
    p.add_argument("--input", required=True, help="Delivery items CSV") # Required argument: path to the input delivery CSV file
    
    p.add_argument("--out-dir", default="/data/batch", help="Output folder (will be created)") # Optional argument: output directory for result files (default: /data/batch)
    
    p.add_argument("--chunk-size", type=int, default=0,
                   help="Rows per chunk (0 = read entire file)")

    p.add_argument("--validate-chunks", action="store_true",
               help="Run Pydantic validation on each delivery chunk") # Optional argument: number of rows per chunk; 0 means read the entire file at once
    
    args = p.parse_args() # Parse the arguments from the command line
    
    chunk_size = None if args.chunk_size == 0 else args.chunk_size # full file 

    process_file(
        Path(args.input),
        Path(args.out_dir),
        chunk_size,
        price_csv=None,
        validate_chunks=args.validate_chunks   # ← pass it
    )


if __name__ == "__main__":
    cli()
