"""
Генерация тестовых данных для лабораторной по оптимизации производительности.
Создаёт Parquet-файлы в /app/data/ для упражнений.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import os
import random
from datetime import datetime, timedelta

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)

random.seed(42)

# --- query_logs.parquet ---
# Логи выполнения запросов: 10 000 записей

sql_templates = [
    "SELECT * FROM events WHERE region = '{region}'",
    "SELECT category, COUNT(*) FROM events GROUP BY category",
    "SELECT * FROM events ORDER BY timestamp DESC LIMIT {limit}",
    "SELECT region, SUM(amount) FROM events GROUP BY region",
    "SELECT * FROM events WHERE amount > {threshold}",
    "SELECT e.*, p.name FROM events e JOIN products p ON e.category = p.category",
    "SELECT DISTINCT region FROM events WHERE duration_ms > {threshold}",
    "SELECT category, AVG(amount) FROM events GROUP BY category HAVING AVG(amount) > {threshold}",
]
regions = ["moscow", "spb", "novosibirsk", "ekaterinburg", "kazan"]

n = 10_000
base_date = datetime(2024, 1, 1)

query_ids = list(range(1, n + 1))
timestamps = [
    (base_date + timedelta(hours=random.randint(0, 8760))).isoformat()
    for _ in range(n)
]
sql_texts = [
    random.choice(sql_templates).format(
        region=random.choice(regions),
        limit=random.choice([10, 50, 100, 500]),
        threshold=random.randint(100, 10000),
    )
    for _ in range(n)
]
durations = [round(random.uniform(0.5, 5000.0), 2) for _ in range(n)]
rows_scanned = [random.randint(10, 500_000) for _ in range(n)]
memory_used = [round(random.uniform(0.1, 512.0), 2) for _ in range(n)]
partitions_used = [random.randint(1, 16) for _ in range(n)]

query_logs_table = pa.table({
    "query_id": pa.array(query_ids, type=pa.int64()),
    "timestamp": pa.array(timestamps, type=pa.string()),
    "sql_text": pa.array(sql_texts, type=pa.string()),
    "duration_ms": pa.array(durations, type=pa.float64()),
    "rows_scanned": pa.array(rows_scanned, type=pa.int64()),
    "memory_used_mb": pa.array(memory_used, type=pa.float64()),
    "partitions_used": pa.array(partitions_used, type=pa.int32()),
})

pq.write_table(query_logs_table, os.path.join(DATA_DIR, "query_logs.parquet"))

# --- config_experiments.parquet ---
# Результаты экспериментов с конфигурацией: 1 000 записей

config_names = [
    "default", "high_parallelism", "low_memory", "large_batch",
    "small_batch", "balanced", "io_optimized", "cpu_optimized",
]
batch_sizes = [1024, 2048, 4096, 8192, 16384, 32768, 65536]
target_partitions_opts = [1, 2, 4, 8, 12, 16, 24, 32]

m = 1_000
config_names_col = [random.choice(config_names) for _ in range(m)]
batch_sizes_col = [random.choice(batch_sizes) for _ in range(m)]
target_parts_col = [random.choice(target_partitions_opts) for _ in range(m)]
exec_times_col = [round(random.uniform(10.0, 8000.0), 2) for _ in range(m)]

config_table = pa.table({
    "config_name": pa.array(config_names_col, type=pa.string()),
    "batch_size": pa.array(batch_sizes_col, type=pa.int32()),
    "target_partitions": pa.array(target_parts_col, type=pa.int32()),
    "execution_time_ms": pa.array(exec_times_col, type=pa.float64()),
})

pq.write_table(config_table, os.path.join(DATA_DIR, "config_experiments.parquet"))

print(f"Generated {n} query logs and {m} config experiments in {DATA_DIR}/")
