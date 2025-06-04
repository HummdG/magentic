import pandas as pd
from pathlib import Path
from datetime import date
from src.utils import main as m

def test_process_file(tmp_path: Path):
    # ---- fake inputs -------------------------------------
    price_csv   = tmp_path / "price_list.csv"
    orders_csv  = tmp_path / "orders.csv"
    out_dir     = tmp_path / "out"

    pd.DataFrame({
        "material_number": ["1234", "5678"],
        "price": [1.0, 2.0],
    }).to_csv(price_csv, index=False)

    pd.DataFrame({
        "order_id": [1, 2, 3],
        "order_name": ["Bolt 1234", "Widget", "Nut 5678"],
        "qty": [10, 5, 15],
        "ordered_price": [1.0, 9.9, 2.0],
    }).to_csv(orders_csv, index=False)

    # monkey-patch config path
    from src.utils import config
    config.PRICE_CSV = price_csv

    # ---- run main.py ---------------------------------------------------
    m.process_file(orders_csv, out_dir, chunk_size=None, price_csv=price_csv)

    matched_csv  = out_dir / f"matched_{date.today()}.csv"
    residual_csv = out_dir / f"residual_{date.today()}.csv"

    assert matched_csv.exists() and residual_csv.exists()

    matched = pd.read_csv(matched_csv)
    residual = pd.read_csv(residual_csv)

    # two rows should match, one should fall through
    assert len(matched) == 2
    assert len(residual) == 1
