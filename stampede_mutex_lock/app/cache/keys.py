
from __future__ import annotations


def product_key(product_id: int) -> str:
    """Ключ кэша товара."""
    return f"product:{product_id}"


def product_lock_key(product_id: int) -> str:
    """Ключ mutex lock для stampede-защиты."""
    return f"lock:product:{product_id}"
