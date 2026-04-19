"""Seed данных — 20 товаров, 5 категорий."""

from __future__ import annotations

import datetime

from sqlalchemy import select

from app.db import async_session, engine
from app.models.product import Base, Product

SAMPLE_PRODUCTS = [
    ("MacBook Pro 16", "Apple M3 Max, 36GB RAM", 3499.00, "laptops", 15, True),
    ("ThinkPad X1 Carbon", "Intel i7, 32GB RAM", 1899.00, "laptops", 22, True),
    ("Dell XPS 15", "Intel i9, 64GB RAM", 2499.00, "laptops", 8, True),
    ("iPhone 15 Pro", "A17 Pro chip, 256GB", 1199.00, "phones", 120, True),
    ("Samsung Galaxy S24", "Snapdragon 8 Gen 3", 999.00, "phones", 85, True),
    ("Pixel 8 Pro", "Google Tensor G3", 899.00, "phones", 45, True),
    ("iPad Pro 12.9", "M2 chip, 256GB", 1099.00, "tablets", 33, True),
    ("Samsung Galaxy Tab S9", "Snapdragon 8 Gen 2", 849.00, "tablets", 27, True),
    ("Surface Pro 9", "Intel i7, 16GB RAM", 1599.00, "tablets", 12, True),
    ("AirPods Pro 2", "USB-C, ANC", 249.00, "accessories", 200, True),
    ("MX Master 3S", "Logitech wireless mouse", 99.00, "accessories", 150, True),
    ("Magic Keyboard", "Apple, Touch ID", 199.00, "accessories", 80, True),
    ("Dell U2723QE", '27" 4K IPS USB-C hub', 619.00, "monitors", 35, True),
    ("LG 27GP950-B", '27" 4K 160Hz Nano IPS', 799.00, "monitors", 18, True),
    ("Samsung Odyssey G7", '32" 1440p 240Hz VA', 649.00, "monitors", 24, True),
    ("ASUS ROG Swift", '27" 1440p 270Hz IPS', 729.00, "monitors", 10, True),
    ("Razer BlackWidow V4", "Mechanical keyboard RGB", 169.00, "accessories", 60, True),
    ("Sony WH-1000XM5", "Wireless ANC headphones", 349.00, "accessories", 95, True),
    ("Lenovo Tab P12 Pro", "Snapdragon 870, OLED", 599.00, "tablets", 0, False),
    ("OnePlus 12", "Snapdragon 8 Gen 3, 16GB", 799.00, "phones", 55, True),
]


async def seed_database() -> int:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        existing = await session.execute(select(Product.id).limit(1))
        if existing.scalar() is not None:
            count = (await session.execute(select(Product))).scalars().all()
            return len(count)

        now = datetime.datetime.utcnow()
        products = []
        for name, desc, price, cat, stock, active in SAMPLE_PRODUCTS:
            products.append(Product(
                name=name, description=desc, price=price,
                category=cat, stock=stock, is_active=active,
                created_at=now,
            ))

        session.add_all(products)
        await session.commit()
        return len(products)
