"""Централизованное управление ключами Redis."""

from __future__ import annotations


def product_key(product_id: int) -> str:
    """Ключ карточки товара: product:{id}."""
    return f"product:{product_id}"


def products_all_key() -> str:
    """Ключ списка всех товаров."""
    return "products:all"


def products_category_key(category: str) -> str:
    """Ключ списка товаров по категории."""
    return f"products:category:{category}"


def products_stats_key() -> str:
    """Ключ агрегированной статистики."""
    return "stats:products"


# Паттерны для массового удаления
PATTERN_CATEGORY_ALL = "products:category:*"
PATTERN_ALL_PRODUCTS = "products:*"