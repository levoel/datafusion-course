"""
Упражнение 1: Мультитенантная изоляция

Цель: создать изолированные SessionContext для каждого тенанта
с раздельным доступом к данным.

Инструкции:
  Заполните блоки TODO реальным кодом. Запустите скрипт:
    python exercises/01_multi_tenant.py
"""

from datafusion import SessionContext

# =============================================================
# Шаг 1: Создайте изолированные контексты для каждого тенанта
# =============================================================

# TODO: создайте SessionContext для тенанта A
# ctx_a = SessionContext()

# TODO: зарегистрируйте только данные тенанта A
# ctx_a.register_parquet("orders", "data/tenant_a_orders.parquet")

# TODO: создайте SessionContext для тенанта B
# ctx_b = SessionContext()

# TODO: зарегистрируйте только данные тенанта B
# ctx_b.register_parquet("orders", "data/tenant_b_orders.parquet")


# =============================================================
# Шаг 2: Проверьте изоляцию данных
# =============================================================

# TODO: выведите количество записей и tenant_id для каждого контекста
# print("=== Тенант A ===")
# ctx_a.sql("SELECT tenant_id, COUNT(*) as cnt FROM orders GROUP BY tenant_id").show()
#
# print("=== Тенант B ===")
# ctx_b.sql("SELECT tenant_id, COUNT(*) as cnt FROM orders GROUP BY tenant_id").show()

# TODO: убедитесь, что тенант A не видит данные тенанта B
# Подсказка: в ctx_a не должно быть записей с tenant_id = 'tenant_b'
# check_a = ctx_a.sql("SELECT COUNT(*) as cnt FROM orders WHERE tenant_id = 'tenant_b'")
# check_a.show()  # Должно быть 0


# =============================================================
# Шаг 3: Выполните аналитические запросы по тенантам
# =============================================================

analytics_query = """
    SELECT product,
           COUNT(*) as order_count,
           AVG(amount) as avg_amount,
           SUM(amount) as total_revenue
    FROM orders
    GROUP BY product
    ORDER BY total_revenue DESC
"""

# TODO: выполните аналитику для тенанта A
# print("=== Аналитика тенанта A ===")
# ctx_a.sql(analytics_query).show()

# TODO: выполните ту же аналитику для тенанта B
# print("=== Аналитика тенанта B ===")
# ctx_b.sql(analytics_query).show()


# =============================================================
# Шаг 4: Фабрика контекстов
# =============================================================

# TODO: создайте функцию-фабрику для создания тенантных контекстов
# def create_tenant_context(tenant_id: str) -> SessionContext:
#     """Создаёт изолированный SessionContext для указанного тенанта."""
#     ctx = SessionContext()
#     data_path = f"data/{tenant_id}_orders.parquet"
#     ctx.register_parquet("orders", data_path)
#     return ctx

# TODO: используйте фабрику для создания контекстов
# for tid in ["tenant_a", "tenant_b"]:
#     t_ctx = create_tenant_context(tid)
#     print(f"=== {tid} ===")
#     t_ctx.sql("SELECT COUNT(*) as total FROM orders").show()


# =============================================================
# Шаг 5: Общий контекст администратора (все тенанты)
# =============================================================

# TODO: создайте контекст администратора с доступом ко всем данным
# ctx_admin = SessionContext()
# ctx_admin.register_parquet("orders_a", "data/tenant_a_orders.parquet")
# ctx_admin.register_parquet("orders_b", "data/tenant_b_orders.parquet")

# TODO: объедините данные через UNION ALL
# admin_report = ctx_admin.sql("""
#     SELECT tenant_id, COUNT(*) as orders, SUM(amount) as revenue
#     FROM (
#         SELECT * FROM orders_a
#         UNION ALL
#         SELECT * FROM orders_b
#     )
#     GROUP BY tenant_id
#     ORDER BY revenue DESC
# """)
# admin_report.show()


print("Упражнение 1 завершено.")
