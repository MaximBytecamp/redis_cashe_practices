from __future__ import annotations


def product_key(product_id: int) -> str:
    """Ключ карточки товара."""
    return f"product:{product_id}"


def products_all_key() -> str:
    """Ключ списка всех товаров."""
    return "products:all"


def products_by_category_key(category: str) -> str:
    """Ключ списка товаров по категории."""
    return f"products:category:{category}"


def products_stats_key() -> str:
    """Ключ статистики."""
    return "stats:products"


# ── Шаблоны для SCAN (batch invalidate) ───────────────────

def product_pattern() -> str:
    """Шаблон для всех карточек товаров."""
    return "product:*"


def products_category_pattern() -> str:
    """Шаблон для всех category-списков."""
    return "products:category:*"
