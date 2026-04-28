from __future__ import annotations

import math
import mmh3


# Чистая Python-реализация (без Redis) 

class CountingBloomFilter:
    """Counting Bloom Filter: поддерживает add + remove + check."""

    def __init__(self, expected_items: int = 10_000, fp_rate: float = 0.01):
        # Те же формулы что и у классического Bloom Filter
        self.m = int(-expected_items * math.log(fp_rate) / (math.log(2) ** 2))
        self.k = max(1, int((self.m / max(expected_items, 1)) * math.log(2)))

        # КЛЮЧЕВОЕ ОТЛИЧИЕ: массив счётчиков вместо битов
        self.counters = [0] * self.m

        self.n_items = 0  # текущее число элементов

    
    def _positions(self, item: str) -> list[int]:
        return [mmh3.hash(item, seed, signed=False) % self.m for seed in range(self.k)]

    
    def add(self, item: str) -> None:
        """Добавить элемент: инкрементировать k счётчиков."""
        for pos in self._positions(item):
            self.counters[pos] += 1
        self.n_items += 1

    
    def remove(self, item: str) -> bool:
        """
        Удалить элемент: декрементировать k счётчиков.

        ВАЖНО: нельзя удалять элемент которого не добавляли!
        Это приводит к "under-counting" → ложные отрицания.
        """
        if not self.check(item):
            return False  # точно нет — не трогаем счётчики

        for pos in self._positions(item):
            if self.counters[pos] > 0:
                self.counters[pos] -= 1
        self.n_items -= 1
        return True

    
    def check(self, item: str) -> bool:
        """
        Проверить:
          all > 0 → "maybe exists" (True)
          any = 0 → "definitely not" (False)
        """
        return all(self.counters[pos] > 0 for pos in self._positions(item))

    @property
    def memory_bytes(self) -> int:
        # uint8 на счётчик (если счётчик ≤ 255, иначе uint16)
        return self.m  # 1 байт × m счётчиков


# Демонстрация алгоритма

def demo():
    cbf = CountingBloomFilter(expected_items=1000, fp_rate=0.01)

    # ADD: добавляем товары
    for product_id in range(1, 21):
        cbf.add(str(product_id))

    print(f"m={cbf.m} счётчиков, k={cbf.k} хешей")

    # CHECK: проверка
    print(cbf.check("5"))       # True  — добавляли
    print(cbf.check("999999"))  # False — точно нет

    # REMOVE: удаляем — ГЛАВНОЕ отличие от классического Bloom Filter!
    cbf.remove("5")
    print(cbf.check("5"))       # False — удалили, теперь точно нет!



# │ Память              │ m бит (~12 KB)      │ m × 4 байт (~48 KB)  │



if __name__ == "__main__":
    demo()
