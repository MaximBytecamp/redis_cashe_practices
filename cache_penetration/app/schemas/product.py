from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ProductCreate(BaseModel):
    name: str
    description: str = ""
    price: float = Field(gt=0)
    category: str
    stock: int = Field(ge=0, default=0)
    is_active: bool = True


class ProductUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    category: Optional[str] = None
    stock: Optional[int] = None
    is_active: Optional[bool] = None


class ProductRead(BaseModel):
    id: int
    name: str
    description: str
    price: float
    category: str
    stock: int
    is_active: bool
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}