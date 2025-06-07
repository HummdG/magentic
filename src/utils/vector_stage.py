"""
Fast helper that:
• encodes residual delivery names,
• finds their top-k price-list candidates,
• returns only cases worth spending an LLM call on.
"""

from __future__ import annotations
import numpy as np, pandas as pd, faiss
from pathlib import Path
from sentence_transformers import SentenceTransformer

from .config import BASE_DIR

# ─── load once in each process ────────────────────────────────────────
MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
IDX   = faiss.read_index(str(Path(__file__).parents[2] / "models/price_ivfpq.faiss"))
PRICE = pd.read_csv(BASE_DIR / "price_list.csv",
                    usecols=["material_number", "name", "price"])

# ─── cost gate (GPT-4 price June-2025) ────────────────────────────────
TOKENS_PER_CALL = 120
USD_PER_1K       = 0.03 + 0.06      # prompt + completion
LLM_COST_CALL    = TOKENS_PER_CALL * USD_PER_1K / 1_000  # ≈ $0.0108
SAFETY           = 1.5              # raise/lower in config map


def _worth_llm(order_price: float, list_price: float) -> bool:
    delta = abs(order_price - list_price)
    return delta > LLM_COST_CALL * SAFETY


# ─── public API ───────────────────────────────────────────────────────
def shortlist_for_llm(
    residual_df: pd.DataFrame,
    k: int = 5,
    sim_cut: float = 0.80,
):
    """
    Yields (delivery_row, price_list_subset<DataFrame>) tuples for which
    the price delta justifies an LLM call.
    """
    if residual_df.empty:
        return

    vec = MODEL.encode(residual_df.order_name.tolist(), batch_size=1024)
    D, I = IDX.search(vec, k)

    for row_i, (dists, ids) in enumerate(zip(D, I)):
        cand_rows = []
        best_price = None

        for dist, pid in zip(dists, ids):
            if pid < 0:                       # FAISS padding
                continue
            sim = 1.0 - dist / 2.0            # L2-to-cosine approx (unit vecs)
            if sim < sim_cut:
                continue
            pl_row = PRICE.iloc[pid]
            best_price = best_price or pl_row.price
            cand_rows.append(pl_row)

        if not cand_rows:
            continue

        order_price = residual_df.ordered_price.iat[row_i]
        if _worth_llm(order_price, best_price):
            yield residual_df.iloc[row_i], pd.DataFrame(cand_rows)
