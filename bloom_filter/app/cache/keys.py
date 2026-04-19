from __future__ import annotations

def product_key(product_id: int) -> str:
    """Ключ карточки товара: product:{id}."""
    return f"product:{product_id}"


def negative_key(product_id: int) -> str:
    """Ключ negative cache: neg:product:{id}."""
    return f"neg:product:{product_id}"