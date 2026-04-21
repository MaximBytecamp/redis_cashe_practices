from __future__ import annotations

import datetime

from pydantic import BaseModel


class ProductRead(BaseModel):
    id: int
    name: str
    description: str
    price: float
    category: str
    stock: int
    is_active: bool
    updated_at: datetime.datetime | None = None

    model_config = {"from_attributes": True}
