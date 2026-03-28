"""
Упражнение 1: Исследование данных с DataFusion Python

Цель: научиться читать Parquet-файлы, выполнять SQL-запросы
и использовать DataFrame API для анализа данных.

Инструкции:
  Заполните блоки TODO реальным кодом. Запустите скрипт:
    python exercises/01_explore_data.py
"""

from datafusion import SessionContext

ctx = SessionContext()

# =============================================================
# Шаг 1: Зарегистрируйте Parquet-файлы как таблицы
# =============================================================

# TODO: зарегистрируйте data/sales_events.parquet как таблицу "sales"
# ctx.register_parquet("sales", ...)

# TODO: зарегистрируйте data/products.parquet как таблицу "products"
# ctx.register_parquet("products", ...)


# =============================================================
# Шаг 2: Базовые SQL-запросы
# =============================================================

# TODO: выведите первые 10 записей из таблицы sales
# df = ctx.sql("SELECT * FROM sales LIMIT 10")
# df.show()

# TODO: посчитайте общее количество записей в sales
# count_df = ctx.sql(...)
# count_df.show()

# TODO: найдите топ-5 категорий по общей сумме продаж
# Подсказка: GROUP BY category, ORDER BY SUM(amount) DESC
# top_categories = ctx.sql(...)
# top_categories.show()


# =============================================================
# Шаг 3: DataFrame API
# =============================================================

from datafusion import col, lit
from datafusion import functions as f

# TODO: прочитайте sales_events.parquet через DataFrame API
# sales_df = ctx.read_parquet("data/sales_events.parquet")

# TODO: отфильтруйте продажи с amount > 10000
# big_sales = sales_df.filter(col("amount") > lit(10000))
# big_sales.show()

# TODO: сгруппируйте по region, посчитайте среднюю сумму и количество
# Подсказка: .aggregate([col("region")], [f.avg(col("amount")), f.count(col("event_id"))])
# regional_stats = ...
# regional_stats.show()


# =============================================================
# Шаг 4: Сохранение результатов в Parquet
# =============================================================

# TODO: сохраните regional_stats в файл data/regional_report.parquet
# regional_stats.write_parquet("data/regional_report.parquet")

# TODO: прочитайте сохранённый файл обратно и выведите
# verify = ctx.read_parquet("data/regional_report.parquet")
# verify.show()


print("Упражнение 1 завершено.")
