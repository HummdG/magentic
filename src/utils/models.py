from pydantic import BaseModel, Field, PositiveInt, PositiveFloat, field_validator

class PriceRow(BaseModel):
    material_number: str = Field(pattern=r"^\d{4,10}$")
    price: PositiveFloat

class DeliveryRow(BaseModel):
    order_id: PositiveInt
    order_name: str
    qty: PositiveInt
    ordered_price: PositiveFloat

    # optional sanity-check: ordered price must be >0.01
    @field_validator("ordered_price")
    def price_not_too_small(cls, v):
        if v < 0.01:
            raise ValueError("ordered_price looks suspiciously low")
        return v
