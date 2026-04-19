# Cache Penetration Guard (Null Caching)

## О проекте

Демонстрация защиты от **Cache Penetration** через **Null Caching**:
- **Проблема**: запросы к несуществующим ID «пробивают» кэш насквозь
- **Решение**: кэшировать факт ОТСУТСТВИЯ данных (маркер `__NULL__`)
- **Результат**: 99% снижение нагрузки при атаке

---

## Проблема: Cache Penetration

```
Атакующий:
GET /products/999999  → MISS → DB: SELECT → NULL → 404
GET /products/888888  → MISS → DB: SELECT → NULL → 404
GET /products/777777  → MISS → DB: SELECT → NULL → 404
...× 10,000 RPS

→ Кэш НИКОГДА не заполняется (данных нет!)
→ КАЖДЫЙ запрос идёт в БД
→ БД под нагрузкой
```

## Решение: Null Caching

```
GET /products/999999  → MISS → DB: SELECT → NULL → SET "__NULL__" → 404
GET /products/999999  → NULL_HIT → 404 (БД не трогаем!)
GET /products/999999  → NULL_HIT → 404 (БД не трогаем!)
...

→ Один поход в БД
→ Остальные запросы отвечает Redis
```

---


## Быстрый старт

```bash
cd cache_penetration_guard
pip install -r requirements.txt
docker compose up -d
python run_demo.py
```


## Ключевые паттерны

### CacheResult dataclass
```python
@dataclass
class CacheResult:
    state: str   # "hit" | "null_hit" | "miss"
    data: Any

async def cache_get(key: str) -> CacheResult:
    raw = await r.get(key)
    if raw is None:
        return CacheResult("miss", None)
    if raw == "__NULL__":
        return CacheResult("null_hit", None)
    return CacheResult("hit", json.loads(raw))
```

### Null caching с jitter
```python
async def cache_set_null(key: str, ttl: int = 30):
    jitter = random.randint(0, ttl // 5)  # ±20%
    await r.set(key, "__NULL__", ex=ttl + jitter)
```

### Бизнес-логика
```python
cr = await CacheService.get_product(id)

if cr.state == "hit":
    return cr.data  # 200

if cr.state == "null_hit":
    return None  # 404 (БД НЕ трогаем!)

# miss → идём в БД
data = await db.get(id)
if data:
    await CacheService.set_product(id, data)
else:
    await CacheService.set_null(id)  # ← Защита!
```

---


## Отчёт

### 1. Почему короткий TTL для null?
- Если товар создан → появится через 30 сек (а не 5 минут)
- Меньше памяти на мусорные ключи
- Атакующий всё равно заблокирован (1 запрос / 30 сек)

### 2. Что делать при создании товара?
- `set_product()` ПЕРЕЗАПИСЫВАЕТ null marker
- Не нужно сначала удалять, потом записывать

### 3. Стратегии удаления
- `delete_only`: просто DELETE ключа
- `write_null`: DELETE + SET `__NULL__` (рекомендуется)

---

## Вывод

> **Null Caching — простая и мощная защита.**
>
> Кэшируем факт отсутствия данных как `__NULL__`.
> Короткий TTL (30 сек) — товар может появиться.
> Jitter — предотвращает одновременное истечение.
> Стоимость: ~10 строк кода. Защита: 99% снижение при атаке.
