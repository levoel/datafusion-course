"""
Упражнение 1: Работа с Delta Lake таблицами

Цель: зарегистрировать Delta-таблицу в DataFusion,
выполнять SQL-запросы и работать с time travel.

Инструкции:
  Заполните блоки TODO реальным кодом. Запустите скрипт:
    python exercises/01_delta_lake.py
"""

from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()

# =============================================================
# Шаг 1: Загрузите Delta-таблицу и зарегистрируйте в DataFusion
# =============================================================

# TODO: откройте Delta-таблицу
# dt = DeltaTable("data/events_delta")

# TODO: зарегистрируйте Delta-таблицу как источник данных DataFusion
# Подсказка: преобразуйте в PyArrow Dataset через dt.to_pyarrow_dataset()
# dataset = dt.to_pyarrow_dataset()
# ctx.register_dataset("events", dataset)

# TODO: выведите первые 10 записей
# df = ctx.sql("SELECT * FROM events LIMIT 10")
# df.show()


# =============================================================
# Шаг 2: Проанализируйте данные через SQL
# =============================================================

# TODO: посчитайте общее количество записей
# count_df = ctx.sql("SELECT COUNT(*) as total FROM events")
# count_df.show()

# TODO: агрегация по категориям
# cat_stats = ctx.sql("""
#     SELECT category, COUNT(*) as cnt, AVG(amount) as avg_amount
#     FROM events
#     GROUP BY category
#     ORDER BY cnt DESC
# """)
# cat_stats.show()

# TODO: агрегация по регионам
# region_stats = ctx.sql("""
#     SELECT region, SUM(amount) as total_amount
#     FROM events
#     GROUP BY region
#     ORDER BY total_amount DESC
# """)
# region_stats.show()


# =============================================================
# Шаг 3: Time Travel — загрузите предыдущую версию
# =============================================================

# TODO: откройте версию 0 Delta-таблицы
# dt_v0 = DeltaTable("data/events_delta", version=0)

# TODO: зарегистрируйте версию 0 как "events_v0"
# dataset_v0 = dt_v0.to_pyarrow_dataset()
# ctx.register_dataset("events_v0", dataset_v0)

# TODO: сравните количество записей текущей и предыдущей версии
# comparison = ctx.sql("""
#     SELECT
#         (SELECT COUNT(*) FROM events) as current_count,
#         (SELECT COUNT(*) FROM events_v0) as v0_count
# """)
# comparison.show()


# =============================================================
# Шаг 4: Метаданные Delta-таблицы
# =============================================================

# TODO: выведите историю коммитов
# print("История версий:")
# for entry in dt.history():
#     print(f"  v{entry['version']}: {entry['timestamp']} — {entry.get('operation', 'N/A')}")

# TODO: выведите схему таблицы
# print(f"\nСхема: {dt.schema()}")
# print(f"Файлы: {dt.files()}")
# print(f"Версия: {dt.version()}")


print("Упражнение 1 завершено.")
