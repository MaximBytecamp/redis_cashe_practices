"""
Seed данных — наполнение БД тестовыми товарами.
"""

from __future__ import annotations

import datetime
import random

from sqlalchemy import select

from app.models.product import Base, Product
from app.db import engine, async_session

CATEGORIES = ["laptops", "phones", "tablets", "accessories", "monitors"]

SAMPLE_PRODUCTS = [
    ("MacBook Pro 16", "Apple M3 Max, 36GB RAM", 3499.00, "laptops", 15),
    ("ThinkPad X1 Carbon", "Intel i7, 32GB RAM", 1899.00, "laptops", 22),
    ("Dell XPS 15", "Intel i9, 64GB RAM", 2499.00, "laptops", 8),
    ("iPhone 15 Pro", "A17 Pro chip, 256GB", 1199.00, "phones", 120),
    ("Samsung Galaxy S24", "Snapdragon 8 Gen 3", 999.00, "phones", 85),
    ("Pixel 8 Pro", "Google Tensor G3", 899.00, "phones", 45),
    ("iPad Pro 12.9", "M2 chip, 256GB", 1099.00, "tablets", 33),
    ("Samsung Galaxy Tab S9", "Snapdragon 8 Gen 2", 849.00, "tablets", 27),
    ("Surface Pro 9", "Intel i7, 16GB RAM", 1599.00, "tablets", 12),
    ("AirPods Pro 2", "USB-C, ANC", 249.00, "accessories", 200),
    ("MX Master 3S", "Logitech wireless mouse", 99.00, "accessories", 150),
    ("Magic Keyboard", "Apple, Touch ID", 199.00, "accessories", 80),
    ("Dell U2723QE", "27\" 4K IPS USB-C hub", 619.00, "monitors", 35),
    ("LG 27GP950-B", "27\" 4K 160Hz Nano IPS", 799.00, "monitors", 18),
    ("Samsung Odyssey G7", "32\" 1440p 240Hz VA", 649.00, "monitors", 24),
    ("ASUS ROG Swift", "27\" 1440p 270Hz IPS", 729.00, "monitors", 10),
    ("Razer BlackWidow V4", "Mechanical keyboard RGB", 169.00, "accessories", 60),
    ("Sony WH-1000XM5", "Wireless ANC headphones", 349.00, "accessories", 95),
    ("Lenovo Tab P12 Pro", "Snapdragon 870, OLED", 599.00, "tablets", 20),
    ("OnePlus 12", "Snapdragon 8 Gen 3, 16GB", 799.00, "phones", 55),
]


async def seed_database() -> int:
    """Создать таблицы и заполнить тестовыми данными. Возвращает кол-во добавленных."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        existing = await session.execute(select(Product.id).limit(1))
        if existing.scalar() is not None:
            count = (await session.execute(select(Product))).scalars().all()
            return len(count)

        now = datetime.datetime.utcnow()
        products = []
        for name, desc, price, cat, stock in SAMPLE_PRODUCTS:
            products.append(Product(
                name=name,
                description=desc,
                price=price,
                category=cat,
                stock=stock,
                updated_at=now - datetime.timedelta(hours=random.randint(1, 72)),
            ))

        session.add_all(products)
        await session.commit()
        return len(products)
