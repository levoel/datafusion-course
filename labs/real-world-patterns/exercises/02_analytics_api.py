"""
Упражнение 2: Простой аналитический API

Цель: построить простой аналитический API поверх DataFusion
для выполнения параметризованных запросов.

Инструкции:
  Заполните блоки TODO реальным кодом. Запустите скрипт:
    python exercises/02_analytics_api.py
"""

from datafusion import SessionContext

# =============================================================
# Шаг 1: Создайте базовый аналитический движок
# =============================================================

class AnalyticsEngine:
    """Простой аналитический движок поверх DataFusion."""

    def __init__(self):
        self.ctx = SessionContext()
        self._tables: list[str] = []

    def register_data(self, table_name: str, parquet_path: str):
        """Регистрирует Parquet-файл как таблицу."""
        # TODO: зарегистрируйте Parquet-файл
        # self.ctx.register_parquet(table_name, parquet_path)
        # self._tables.append(table_name)
        pass

    def list_tables(self) -> list[str]:
        """Возвращает список зарегистрированных таблиц."""
        return self._tables

    def query(self, sql: str):
        """Выполняет SQL-запрос и возвращает DataFrame."""
        # TODO: выполните запрос через SessionContext
        # return self.ctx.sql(sql)
        pass


# =============================================================
# Шаг 2: Добавьте предопределённые аналитические отчёты
# =============================================================

class ReportEngine(AnalyticsEngine):
    """Расширенный движок с готовыми отчётами."""

    def top_products(self, table: str, limit: int = 5):
        """Топ продуктов по выручке."""
        # TODO: реализуйте запрос
        # return self.query(f"""
        #     SELECT product,
        #            COUNT(*) as orders,
        #            SUM(amount) as revenue,
        #            AVG(amount) as avg_order
        #     FROM {table}
        #     GROUP BY product
        #     ORDER BY revenue DESC
        #     LIMIT {limit}
        # """)
        pass

    def revenue_summary(self, table: str):
        """Сводка по выручке."""
        # TODO: реализуйте запрос
        # return self.query(f"""
        #     SELECT
        #         COUNT(*) as total_orders,
        #         SUM(amount) as total_revenue,
        #         AVG(amount) as avg_order_value,
        #         MIN(amount) as min_order,
        #         MAX(amount) as max_order
        #     FROM {table}
        # """)
        pass

    def orders_by_period(self, table: str, date_column: str = "created_at"):
        """Заказы по месяцам."""
        # TODO: реализуйте запрос с группировкой по месяцу
        # Подсказка: используйте SUBSTR для извлечения года-месяца
        # return self.query(f"""
        #     SELECT SUBSTR({date_column}, 1, 7) as month,
        #            COUNT(*) as orders,
        #            SUM(amount) as revenue
        #     FROM {table}
        #     GROUP BY month
        #     ORDER BY month
        # """)
        pass


# =============================================================
# Шаг 3: Используйте движок
# =============================================================

# TODO: создайте экземпляр ReportEngine
# engine = ReportEngine()

# TODO: зарегистрируйте данные обоих тенантов
# engine.register_data("orders_a", "data/tenant_a_orders.parquet")
# engine.register_data("orders_b", "data/tenant_b_orders.parquet")

# TODO: выведите отчёты
# print("=== Тенант A: Топ продуктов ===")
# engine.top_products("orders_a").show()

# print("=== Тенант B: Сводка по выручке ===")
# engine.revenue_summary("orders_b").show()

# print("=== Тенант A: Заказы по месяцам ===")
# engine.orders_by_period("orders_a").show()


# =============================================================
# Шаг 4: Добавьте валидацию входных данных
# =============================================================

# TODO: добавьте проверку существования таблицы
# def safe_query(engine: ReportEngine, table: str, report: str):
#     """Выполняет отчёт с проверкой таблицы."""
#     if table not in engine.list_tables():
#         print(f"Ошибка: таблица '{table}' не зарегистрирована")
#         print(f"Доступные таблицы: {engine.list_tables()}")
#         return None
#
#     if report == "top_products":
#         return engine.top_products(table)
#     elif report == "revenue":
#         return engine.revenue_summary(table)
#     elif report == "by_period":
#         return engine.orders_by_period(table)
#     else:
#         print(f"Неизвестный отчёт: {report}")
#         return None

# TODO: протестируйте safe_query
# print("=== Валидный запрос ===")
# result = safe_query(engine, "orders_a", "top_products")
# if result:
#     result.show()

# print("=== Невалидная таблица ===")
# safe_query(engine, "nonexistent", "revenue")

# print("=== Невалидный отчёт ===")
# safe_query(engine, "orders_a", "unknown_report")


print("Упражнение 2 завершено.")
