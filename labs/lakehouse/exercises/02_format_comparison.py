"""
Упражнение 2: Сравнение форматов данных

Цель: сравнить производительность и возможности
Parquet и Delta Lake при работе через DataFusion.

Инструкции:
  Заполните блоки TODO реальным кодом. Запустите скрипт:
    python exercises/02_format_comparison.py
"""

import time
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()

# =============================================================
# Шаг 1: Зарегистрируйте оба источника данных
# =============================================================

# TODO: зарегистрируйте Parquet-файл как "events_parquet"
# ctx.register_parquet("events_parquet", "data/events.parquet")

# TODO: зарегистрируйте Delta-таблицу как "events_delta"
# dt = DeltaTable("data/events_delta")
# ctx.register_dataset("events_delta", dt.to_pyarrow_dataset())


# =============================================================
# Шаг 2: Сравните количество записей
# =============================================================

# TODO: выведите количество записей в каждой таблице
# parquet_count = ctx.sql("SELECT COUNT(*) as cnt FROM events_parquet")
# delta_count = ctx.sql("SELECT COUNT(*) as cnt FROM events_delta")
# print("Parquet:")
# parquet_count.show()
# print("Delta:")
# delta_count.show()


# =============================================================
# Шаг 3: Сравните время выполнения агрегирующего запроса
# =============================================================

query = """
    SELECT region, category,
           COUNT(*) as cnt,
           AVG(amount) as avg_amount,
           SUM(amount) as total_amount
    FROM {table}
    GROUP BY region, category
    ORDER BY total_amount DESC
"""

# TODO: выполните запрос для Parquet и замерьте время
# start = time.time()
# result_parquet = ctx.sql(query.format(table="events_parquet"))
# result_parquet.collect()
# parquet_time = time.time() - start
# print(f"Parquet: {parquet_time:.4f} сек")

# TODO: выполните тот же запрос для Delta и замерьте время
# start = time.time()
# result_delta = ctx.sql(query.format(table="events_delta"))
# result_delta.collect()
# delta_time = time.time() - start
# print(f"Delta:   {delta_time:.4f} сек")


# =============================================================
# Шаг 4: Сравните планы выполнения
# =============================================================

# TODO: выведите EXPLAIN для Parquet-запроса
# explain_parquet = ctx.sql(f"EXPLAIN {query.format(table='events_parquet')}")
# print("=== План Parquet ===")
# explain_parquet.show()

# TODO: выведите EXPLAIN для Delta-запроса
# explain_delta = ctx.sql(f"EXPLAIN {query.format(table='events_delta')}")
# print("=== План Delta ===")
# explain_delta.show()


# =============================================================
# Шаг 5: Преимущества Delta — schema enforcement
# =============================================================

# TODO: попробуйте записать данные с несовпадающей схемой в Delta
# import pyarrow as pa
# from deltalake import write_deltalake
#
# bad_table = pa.table({
#     "event_id": pa.array([99999], type=pa.int64()),
#     "timestamp": pa.array(["2025-01-01T00:00:00"], type=pa.string()),
#     "category": pa.array(["test"], type=pa.string()),
#     "amount": pa.array(["not_a_number"], type=pa.string()),  # Неправильный тип!
#     "region": pa.array(["test"], type=pa.string()),
# })
#
# try:
#     write_deltalake("data/events_delta", bad_table, mode="append")
#     print("Запись прошла (неожиданно!)")
# except Exception as e:
#     print(f"Delta Lake отклонил запись: {e}")


print("Упражнение 2 завершено.")
