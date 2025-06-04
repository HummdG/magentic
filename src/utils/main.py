"""
Exact match (CSV-only).
Example:
    python -m src.utils.main \
        --input /data/deliver_items/delivery_items_2025-06-04.csv
"""
import argparse
from pathlib import Path
from datetime import date
import pandas as pd

from .config import (
    PRICE_CSV, CHUNK_SIZE, RX_MATNUM,
)

# ----------------------------------------------------------------------
def build_price_index(price_csv: Path):
    """Read price list once & build O(1) lookup."""
    price_df = pd.read_csv(
        price_csv,
        usecols=["material_number", "price"],
        dtype={"material_number": "string", "price": "float32"}, # explictly declaring the types increases RAM efficiency 
    )
    index = dict(zip(price_df["material_number"].str.strip(), price_df.index))
    return price_df, index


def process_file(delivery_csv: Path, out_dir: Path, chunk_size=None):
    """Stream (or fully load) delivery CSV, run regex + dict lookup, write two CSVs."""
    price_df, price_index = build_price_index(PRICE_CSV)
    out_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    matched_csv  = out_dir / f"matched_{today}.csv" # matched items in one csv
    residual_csv = out_dir / f"residual_{today}.csv" # remaining items in other csv

    # ensure headers written once
    first_chunk = True

    for chunk in pd.read_csv(
        delivery_csv,
        usecols=["order_id", "order_name", "qty", "ordered_price"],
        dtype={"order_id": "int64", "order_name": "string",
               "qty": "int32", "ordered_price": "float32"},
        encoding="utf-8",
        chunksize=chunk_size,
        low_memory=False,
    ):
        # --- Extract material numbers and match them exactly using regex + dictionary index lookup -----------------
        chunk["matnum_found"] = chunk["order_name"].str.extract(RX_MATNUM, expand=False) # vectorised operations - good for fast matching .str.extract() and .map() - applies fucntion once to whole column - more efficient than for loop
        chunk["pl_idx"] = chunk["matnum_found"].map(price_index, na_action="ignore")

        matched  = chunk[chunk["pl_idx"].notna()].copy() # Select rows with a valid price list index (i.e. matched entries) notna - not empty therefore match
        residual = chunk[chunk["pl_idx"].isna()].copy() # Select rows without a price list index (i.e. unmatched or residual entries) isna - empty therefore no match

        # copy to create new DF making safe edits less memeory efficient as you are making a new Df every time but ensure no unexpected behaviour happens


        if not matched.empty:
            matched["price_list_price"] = price_df.loc[matched["pl_idx"], "price"].to_numpy() # If there are matched rows, fetch corresponding prices from price_df using pl_idx and assign to new column

        # --- Write to output files -------------------------------------------
        matched.to_csv(matched_csv, mode="w" if first_chunk else "a",
                       header=first_chunk, index=False)
        residual.to_csv(residual_csv, mode="w" if first_chunk else "a",
                        header=first_chunk, index=False)

        first_chunk = False # to not repeat headers


# ----------------------------------------------------------------------
def cli():

    """Entry point for command-line usage: parses input arguments and initiates the CSV processing pipeline.

    This function enables the script to be run from the command line with user-specified options,
    making it reusable and flexible for batch processing without modifying the code.
    It separates configuration (inputs, outputs, chunk size) from logic, following good CLI design practices.
    """
    
    p = argparse.ArgumentParser(description="Tier-A CSV matcher (regex + dict).") # Create an argument parser with a description of the tool
    
    p.add_argument("--input", required=True, help="Delivery items CSV") # Required argument: path to the input delivery CSV file
    
    p.add_argument("--out-dir", default="/data/batch", help="Output folder (will be created)") # Optional argument: output directory for result files (default: /data/batch)
    
    p.add_argument("--chunk-size", type=int, default=CHUNK_SIZE or 0,
                   help="Rows per chunk (0 = full file)") # Optional argument: number of rows per chunk; 0 means read the entire file at once
    
    args = p.parse_args() # Parse the arguments from the command line
    
    chunk_size = None if args.chunk_size == 0 else args.chunk_size # full file 

    process_file(Path(args.input), Path(args.out_dir), chunk_size)


if __name__ == "__main__":
    cli()
