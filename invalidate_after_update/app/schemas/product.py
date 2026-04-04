"""
Pydantic-схемы для Product.
"""

from __future__ import annotations

import datetime

from pydantic import BaseModel, Field


class ProductRead(BaseModel):
    id: int
    name: str
    description: str
    price: float
    category: str
    stock: int
    updated_at: datetime.datetime | None = None

    model_config = {"from_attributes": True}


class ProductUpdate(BaseModel):
    """Все поля необязательны — partial update."""
    name: str | None = None
    description: str | None = None
    price: float | None = Field(None, gt=0)
    category: str | None = None
    stock: int | None = Field(None, ge=0)


class DiscountRequest(BaseModel):
    """Скидка в процентах."""
    percent: float = Field(..., gt=0, le=100)


class ProductStats(BaseModel):
    total_products: int = 0
    in_stock: int = 0
    out_of_stock: int = 0
    avg_price: float = 0.0
    categories: dict[str, int] = {}
