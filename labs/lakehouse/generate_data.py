"""
Генерация тестовых данных для лабораторной по Lakehouse-интеграции.
Создаёт Parquet и Delta Lake таблицы в /app/data/ для упражнений.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import os
import random
from datetime import datetime, timedelta

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

random.seed(42)

# --- events.parquet + events_delta/ ---
# Таблица событий: 5 000 записей

categories = ["purchase", "view", "click", "refund", "signup"]
regions = ["moscow", "spb", "novosibirsk", "ekaterinburg", "kazan"]

n = 5_000
base_date = datetime(2024, 1, 1)

event_ids = list(range(1, n + 1))
timestamps = [
    (base_date + timedelta(hours=random.randint(0, 8760))).isoformat()
    for _ in range(n)
]
cats = [random.choice(categories) for _ in range(n)]
amounts = [round(random.uniform(10, 25000), 2) for _ in range(n)]
regs = [random.choice(regions) for _ in range(n)]

events_table = pa.table({
    "event_id": pa.array(event_ids, type=pa.int64()),
    "timestamp": pa.array(timestamps, type=pa.string()),
    "category": pa.array(cats, type=pa.string()),
    "amount": pa.array(amounts, type=pa.float64()),
    "region": pa.array(regs, type=pa.string()),
})

# Сохраняем в Parquet
pq.write_table(events_table, os.path.join(DATA_DIR, "events.parquet"))

# Сохраняем в Delta Lake формат
from deltalake import write_deltalake

delta_path = os.path.join(DATA_DIR, "events_delta")
write_deltalake(delta_path, events_table, mode="overwrite")

# Добавляем вторую версию (append) для демонстрации time travel
extra_ids = list(range(n + 1, n + 101))
extra_timestamps = [
    (base_date + timedelta(days=366, hours=random.randint(0, 720))).isoformat()
    for _ in range(100)
]
extra_cats = [random.choice(categories) for _ in range(100)]
extra_amounts = [round(random.uniform(10, 25000), 2) for _ in range(100)]
extra_regs = [random.choice(regions) for _ in range(100)]

extra_table = pa.table({
    "event_id": pa.array(extra_ids, type=pa.int64()),
    "timestamp": pa.array(extra_timestamps, type=pa.string()),
    "category": pa.array(extra_cats, type=pa.string()),
    "amount": pa.array(extra_amounts, type=pa.float64()),
    "region": pa.array(extra_regs, type=pa.string()),
})

write_deltalake(delta_path, extra_table, mode="append")

print(f"Generated {n} events (Parquet + Delta v0) and 100 appended events (Delta v1) in {DATA_DIR}/")
