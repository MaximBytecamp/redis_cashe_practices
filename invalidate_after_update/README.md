# Invalidate After Update — Demo

Практика паттерна **Cache-Aside + Invalidate After Update** на стеке
FastAPI · Redis 7 · SQLite (aiosqlite) · SQLAlchemy 2 async · Pydantic v2.

---

## Содержание

- [Архитектура](#архитектура)
- [Требования](#требования)
- [Установка и запуск](#установка-и-запуск)
- [Запуск тестовых сценариев](#запуск-тестовых-сценариев)
- [API Endpoints](#api-endpoints)
- [Структура проекта](#структура-проекта)

---

## Архитектура

```
Client  ──►  FastAPI (порт 8010)
                │
                ├── GET  → Redis cache (cache-aside)
                │            miss? → SQLite → cache SET
                │
                └── PUT/PATCH → SQLite update
                                  → invalidate связанных ключей Redis
                                  → следующий GET получит свежие данные
```

**Принцип работы:**
1. **Чтение** — сначала проверяем Redis; при miss идём в БД и кладём результат в кэш с TTL + jitter.
2. **Запись** — обновляем БД, затем **инвалидируем** все связанные ключи (товар, списки, категории, статистика).
3. **TTL + Jitter** — страховка на случай пропущенного invalidate: ключи всё равно истекут, но в разное время (без stampede).

---

## Требования

- **Python** 3.11+
- **Docker** и **Docker Compose** (для Redis)
- pip (менеджер пакетов Python)

---

## Установка и запуск

### 1. Клонирование репозитория

```bash
git clone https://github.com/MaximBytecamp/redis_cashe_practices.git
cd redis_cashe_practices/invalidate_after_update
```

### 2. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 3. Запуск Redis через Docker

```bash
docker compose up -d
```

Проверка, что Redis работает:

```bash
docker ps
# redis_invalidate_demo   0.0.0.0:6379->6379/tcp   Up ... (healthy)

redis-cli -p 6379 ping
# PONG
```

### 4. Запуск FastAPI сервера

```bash
uvicorn app.main:app --port 8010
```

Или, если запускаете из корня репозитория:

```bash
uvicorn app.main:app --port 8010 --app-dir invalidate_after_update
```

Сервер будет доступен на `http://127.0.0.1:8010`.

### 5. Проверка работоспособности

```bash
curl http://127.0.0.1:8010/health
# {"status":"ok","redis":true}
```

Swagger-документация: [http://127.0.0.1:8010/docs](http://127.0.0.1:8010/docs)

---

## Запуск тестовых сценариев

> **Важно:** сервер должен быть запущен перед запуском сценариев.

### Все 7 сценариев

```bash
python run_scenarios.py
```

### Конкретный сценарий

```bash
python run_scenarios.py --scenario 3
```

---

## API Endpoints

### Чтение (GET)

| Метод | Путь                            | Описание
|-------|
| `GET` | `/health`                       | Проверка сервера и Redis 
| `GET` | `/products`                     | Все товары 
| `GET` | `/products/{id}`                | Карточка одного товара 
| `GET` | `/products/category/{category}` | Товары по категории 
| `GET` | `/products/stats`               | Статистика по товарам 

### Запись (PUT / PATCH)

| Метод   |Путь                                 | Описание 
|-------  |
| `PUT`   | `/products/{id}`                    | Обновить товар + invalidate 
| `PATCH` | `/products/category/{cat}/discount` | Скидка на категорию + групповая инвалидация 



---

## Структура проекта

```
invalidate_after_update/
├── .env                          # переменные окружения
├── docker-compose.yml            # Redis контейнер
├── requirements.txt              # Python зависимости
├── run_scenarios.py              # 7 тестовых сценариев
└── app/
    ├── main.py                   # FastAPI, lifespan, health
    ├── config.py                 # pydantic-settings из .env
    ├── db.py                     # SQLAlchemy async engine + session
    ├── seed.py                   # начальные данные (20 товаров)
    ├── cache/
    │   ├── redis_client.py       # подключение к Redis
    │   ├── keys.py               # генерация ключей кэша
    │   └── helpers.py            # cache_get / cache_set / invalidate
    ├── models/
    │   └── product.py            # SQLAlchemy модель Product
    ├── schemas/
    │   └── product.py            # Pydantic схемы (ProductRead, ProductUpdate, ...)
    ├── repositories/
    │   └── product_repository.py # CRUD операции с БД
    ├── routers/
    │   └── products.py           # HTTP endpoints
    └── services/
        ├── product_service.py            # бизнес-логика + кэширование
        └── cache_invalidation_service.py # точечная инвалидация ключей
```

---

## Остановка

```bash
# Остановить сервер: Ctrl+C в терминале uvicorn

# Остановить Redis
docker compose down
```
