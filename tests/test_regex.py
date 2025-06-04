import pytest
from src.utils.regex_utils import extract_matnum

@pytest.mark.parametrize("text,expected", [
    ("Bolt 00091234 M10 x 80", "00091234"),
    ("Screw 12345678 stainless", "12345678"),
    ("barcode 123456789012", None),        # 12 digits → ignore
    ("Qty 20 of item 9876", "9876"),       # 4 digits
    ("No digits here", None),
])
def test_extract(text, expected):
    assert extract_matnum(text) == expected