"""
Генерация мультитенантных данных для лабораторной по паттернам.
Создаёт Parquet-файлы для двух тенантов в /app/data/.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import os
import random
from datetime import datetime, timedelta

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

random.seed(42)

products_a = ["Widget Pro", "Gadget X", "Sensor V2", "Module Alpha", "Controller Z"]
products_b = ["Service Plan", "Premium License", "Support Tier", "API Access", "Data Pack"]


def generate_tenant_orders(tenant_id: str, products: list[str], n: int) -> pa.Table:
    """Генерирует таблицу заказов для одного тенанта."""
    base_date = datetime(2024, 1, 1)

    order_ids = list(range(1, n + 1))
    tenant_ids = [tenant_id] * n
    prods = [random.choice(products) for _ in range(n)]
    amounts = [round(random.uniform(50, 15000), 2) for _ in range(n)]
    created_at = [
        (base_date + timedelta(hours=random.randint(0, 8760))).isoformat()
        for _ in range(n)
    ]

    return pa.table({
        "order_id": pa.array(order_ids, type=pa.int64()),
        "tenant_id": pa.array(tenant_ids, type=pa.string()),
        "product": pa.array(prods, type=pa.string()),
        "amount": pa.array(amounts, type=pa.float64()),
        "created_at": pa.array(created_at, type=pa.string()),
    })


# Тенант A: 3 000 заказов
tenant_a = generate_tenant_orders("tenant_a", products_a, 3_000)
pq.write_table(tenant_a, os.path.join(DATA_DIR, "tenant_a_orders.parquet"))

# Тенант B: 3 000 заказов
tenant_b = generate_tenant_orders("tenant_b", products_b, 3_000)
pq.write_table(tenant_b, os.path.join(DATA_DIR, "tenant_b_orders.parquet"))

print(f"Generated 3000 orders for tenant_a and 3000 for tenant_b in {DATA_DIR}/")
