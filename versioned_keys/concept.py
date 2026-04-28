from __future__ import annotations

import time
from typing import Any



class VersionedCache:
    """
    Кеш с версионированными ключами.

    Вместо того чтобы удалять ключи → инкрементируем версию namespace.
    Старые ключи игнорируются (их версия устарела), очищаются по TTL.
    """

    def __init__(self, ttl: float = 60.0):
        self.ttl = ttl
        self._store: dict[str, tuple[Any, float]] = {}
        self._versions: dict[str, int] = {}  # namespace → версия

    def _get_version(self, namespace: str) -> int:
        return self._versions.get(namespace, 1)

    def _make_key(self, namespace: str, key: str) -> str:
        """Строим ключ с версией: namespace:v{ver}:{key}"""
        ver = self._get_version(namespace)
        return f"{namespace}:v{ver}:{key}"

    def get(self, namespace: str, key: str) -> tuple[Any, bool]:
        full_key = self._make_key(namespace, key)
        entry = self._store.get(full_key)
        if entry is None:
            return None, False
        value, expire_at = entry
        if time.monotonic() > expire_at:
            del self._store[full_key]
            return None, False
        return value, True

    def set(self, namespace: str, key: str, value: Any) -> str:
        full_key = self._make_key(namespace, key)
        self._store[full_key] = (value, time.monotonic() + self.ttl)
        return full_key  # вернём для наглядности

    def invalidate_namespace(self, namespace: str) -> int:
        """
        Инвалидировать весь namespace одним INCR.
        O(1) операция независимо от числа ключей!
        """
        old_ver = self._get_version(namespace)
        self._versions[namespace] = old_ver + 1
        return old_ver + 1  # новая версия

    def invalidate_key(self, namespace: str, key: str) -> None:
        """Инвалидировать один ключ."""
        full_key = self._make_key(namespace, key)
        self._store.pop(full_key, None)


# ── Tag-based инвалидация ─────────────────────────────────────

class TaggedCache:
    """
    Каждый кеш-ключ помечен тегами.
    Инвалидация по тегу удаляет все ключи с этим тегом.

    Пример:
      cache.set("product:1", data, tags=["products", "category:electronics"])
      cache.set("product:2", data, tags=["products", "category:books"])
      cache.invalidate_tag("products")  → удалены ОБА ключа
      cache.invalidate_tag("category:books")  → удалён только product:2
    """

    def __init__(self):
        self._store: dict[str, Any] = {}
        self._tags: dict[str, set[str]] = {}   # tag → set of keys

    def set(self, key: str, value: Any, tags: list[str] = None) -> None:
        self._store[key] = value
        for tag in (tags or []):
            if tag not in self._tags:
                self._tags[tag] = set()
            self._tags[tag].add(key)

    def get(self, key: str) -> Any:
        return self._store.get(key)

    def invalidate_tag(self, tag: str) -> int:
        """Удалить все ключи с данным тегом. Вернуть количество удалённых."""
        keys = self._tags.pop(tag, set())
        for key in keys:
            self._store.pop(key, None)
        return len(keys)



# ── Демонстрация 

def demo():
    print("=== Versioned Keys Demo ===\n")

    cache = VersionedCache(ttl=60.0)

    # Наполняем кеш
    k1 = cache.set("products", "1", {"id": 1, "name": "Apple", "price": 1.5})
    k2 = cache.set("products", "2", {"id": 2, "name": "Banana", "price": 0.5})
    k3 = cache.set("users", "100", {"id": 100, "name": "Alice"})

    print(f"Ключи в кеше:")
    print(f"  {k1}")
    print(f"  {k2}")
    print(f"  {k3}")

    val, found = cache.get("products", "1")
    print(f"\nGET products:1 → found={found}, value={val['name']}")

    # Инвалидируем весь namespace "products"
    new_ver = cache.invalidate_namespace("products")
    print(f"\nИнвалидируем namespace 'products' → новая версия: v{new_ver}")

    val, found = cache.get("products", "1")
    print(f"GET products:1 → found={found}  ← уже не находим!")

    val, found = cache.get("users", "100")
    print(f"GET users:100  → found={found}, value={val['name']}  ← не тронут!")

    print("\n--- Tag-based инвалидация ---\n")
    tagged = TaggedCache()
    tagged.set("product:1", {"name": "Apple"}, tags=["products", "category:fruits"])
    tagged.set("product:2", {"name": "Banana"}, tags=["products", "category:fruits"])
    tagged.set("product:3", {"name": "Laptop"}, tags=["products", "category:electronics"])
    tagged.set("user:1", {"name": "Alice"}, tags=["users"])

    print("Кеш: product:1, product:2, product:3, user:1")
    deleted = tagged.invalidate_tag("category:fruits")
    print(f"\nИнвалидируем тег 'category:fruits' → удалено {deleted} ключей")
    print(f"  product:1 → {tagged.get('product:1')}")   # None
    print(f"  product:2 → {tagged.get('product:2')}")   # None
    print(f"  product:3 → {tagged.get('product:3')}")   # ещё живой
    print(f"  user:1    → {tagged.get('user:1')}")      # ещё живой




if __name__ == "__main__":
    demo()
