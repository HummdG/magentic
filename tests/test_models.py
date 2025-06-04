from pydantic import ValidationError
from src.utils.models import PriceRow, DeliveryRow
import pytest

def test_price_row_ok():
    PriceRow(material_number="1234", price=0.99)

def test_price_row_bad_number():
    with pytest.raises(ValidationError):
        PriceRow(material_number="ABC", price=0.5)

def test_delivery_row_qty_negative():
    with pytest.raises(ValidationError):
        DeliveryRow(order_id=1, order_name="Bolt", qty=-1, ordered_price=0.5)
