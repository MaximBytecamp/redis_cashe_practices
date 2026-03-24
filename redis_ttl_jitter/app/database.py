from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field

from app.config import settings


PRODUCTS: dict[int, dict] = {
    i: {
        "id": i,
        "name": f"Product #{i}",
        "price": round(random.uniform(9.99, 999.99), 2),
        "category": random.choice(["hot", "normal", "rare"]),
        "in_stock": random.choice([True, True, True, False]),
    }
    for i in range(1, 201)
}


@dataclass
class DBstats:
    total_queries: int = 0
    latencies: list[float] = field(default_factory=list)

    def reset(self) -> None:
        self.total_queries = 0
        self.latencies.clear()


db_stats = DBstats()


async def fetch_product_from_db(product_id: int) -> dict | None:
    delay = random.uniform(settings.db_delay_min, settings.db_delay_max)
    await asyncio.sleep(delay)

    db_stats.total_queries += 1
    db_stats.latencies.append(delay)

    return PRODUCTS.get(product_id)
