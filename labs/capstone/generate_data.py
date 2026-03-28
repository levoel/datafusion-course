"""
Генерация тестовых данных для capstone-лабораторной.
Создаёт Parquet-файлы в /app/data/ для упражнений.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import os
import random
from datetime import datetime, timedelta

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

# --- sales_events.parquet ---
# Данные о продажах: 10 000 записей
random.seed(42)

categories = ["electronics", "clothing", "food", "books", "sports"]
regions = ["moscow", "spb", "novosibirsk", "ekaterinburg", "kazan"]
channels = ["web", "mobile", "store"]

n = 10_000
base_date = datetime(2024, 1, 1)

event_ids = list(range(1, n + 1))
timestamps = [
    (base_date + timedelta(hours=random.randint(0, 8760))).isoformat()
    for _ in range(n)
]
cats = [random.choice(categories) for _ in range(n)]
regs = [random.choice(regions) for _ in range(n)]
chans = [random.choice(channels) for _ in range(n)]
amounts = [round(random.uniform(100, 50000), 2) for _ in range(n)]
quantities = [random.randint(1, 20) for _ in range(n)]
user_ids = [random.randint(1000, 9999) for _ in range(n)]

sales_table = pa.table({
    "event_id": pa.array(event_ids, type=pa.int64()),
    "timestamp": pa.array(timestamps, type=pa.string()),
    "category": pa.array(cats, type=pa.string()),
    "region": pa.array(regs, type=pa.string()),
    "channel": pa.array(chans, type=pa.string()),
    "amount": pa.array(amounts, type=pa.float64()),
    "quantity": pa.array(quantities, type=pa.int32()),
    "user_id": pa.array(user_ids, type=pa.int64()),
})

pq.write_table(sales_table, os.path.join(DATA_DIR, "sales_events.parquet"))

# --- products.parquet ---
# Каталог продуктов: 200 записей
product_names = {
    "electronics": ["Laptop", "Phone", "Tablet", "Headphones", "Camera"],
    "clothing": ["Jacket", "Shirt", "Pants", "Shoes", "Hat"],
    "food": ["Coffee", "Tea", "Chocolate", "Cheese", "Bread"],
    "books": ["Novel", "Textbook", "Guide", "Comics", "Dictionary"],
    "sports": ["Ball", "Racket", "Gloves", "Mat", "Weights"],
}

products = []
pid = 1
for cat, names in product_names.items():
    for name in names:
        for i in range(8):
            products.append({
                "product_id": pid,
                "name": f"{name} {chr(65 + i)}",
                "category": cat,
                "base_price": round(random.uniform(500, 30000), 2),
                "weight_kg": round(random.uniform(0.1, 15.0), 2),
            })
            pid += 1

products_table = pa.table({
    "product_id": pa.array([p["product_id"] for p in products], type=pa.int64()),
    "name": pa.array([p["name"] for p in products], type=pa.string()),
    "category": pa.array([p["category"] for p in products], type=pa.string()),
    "base_price": pa.array([p["base_price"] for p in products], type=pa.float64()),
    "weight_kg": pa.array([p["weight_kg"] for p in products], type=pa.float64()),
})

pq.write_table(products_table, os.path.join(DATA_DIR, "products.parquet"))

print(f"Generated {n} sales events and {len(products)} products in {DATA_DIR}/")
