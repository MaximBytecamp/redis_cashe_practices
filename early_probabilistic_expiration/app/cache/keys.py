"""Централизованные ключи Redis.

Кэш товара:   product:{id}
Метаданные:   meta:product:{id}   ← хранит delta (время вычисления) и created_at
"""

from __future__ import annotations


def product_key(product_id: int) -> str:
    """Ключ кэша товара."""
    return f"product:{product_id}"


def product_meta_key(product_id: int) -> str:
    """Ключ метаданных XFetch (delta, created_at)."""
    return f"meta:product:{product_id}"
