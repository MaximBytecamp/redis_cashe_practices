from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ProductRead(BaseModel):
    id: int
    name: str
    description: str = ""
    price: float
    category: str
    stock: int = 0
    is_active: bool = True
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}
