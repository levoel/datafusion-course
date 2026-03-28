"""
Упражнение 3: Мини аналитический движок

Цель: построить аналитический движок поверх DataFusion, который:
  - Загружает данные из Parquet-файлов
  - Регистрирует пользовательские функции
  - Предоставляет SQL-интерфейс для аналитических запросов
  - Генерирует отчёты в Parquet-формате

Инструкции:
  Заполните блоки TODO реальным кодом. Запустите скрипт:
    python exercises/03_analytics_engine.py
"""

from datafusion import SessionContext, udf, col
import pyarrow as pa
import pyarrow.parquet as pq
import os


class AnalyticsEngine:
    """
    Мини аналитический движок на основе DataFusion.
    Инкапсулирует SessionContext, регистрацию данных и UDF.
    """

    def __init__(self, data_dir: str = "data"):
        self.ctx = SessionContext()
        self.data_dir = data_dir
        self.reports_dir = os.path.join(data_dir, "reports")
        os.makedirs(self.reports_dir, exist_ok=True)

    # ---------------------------------------------------------
    # Шаг 1: Загрузка данных
    # ---------------------------------------------------------

    def load_sources(self) -> None:
        """Регистрирует все Parquet-файлы из data_dir как таблицы."""
        # TODO: реализуйте автоматическую регистрацию
        # Для каждого .parquet файла в self.data_dir:
        #   имя таблицы = имя файла без расширения
        #   self.ctx.register_parquet(name, path)
        #
        # for filename in os.listdir(self.data_dir):
        #     if filename.endswith(".parquet"):
        #         name = filename.replace(".parquet", "")
        #         path = os.path.join(self.data_dir, filename)
        #         self.ctx.register_parquet(name, path)
        #         print(f"  Зарегистрирована таблица: {name}")
        pass

    # ---------------------------------------------------------
    # Шаг 2: Регистрация UDF
    # ---------------------------------------------------------

    def register_functions(self) -> None:
        """Регистрирует бизнес-UDF в контексте."""

        # UDF: revenue_tier — классификация выручки
        # TODO: создайте UDF, который классифицирует amount:
        #   < 500    -> "micro"
        #   < 5000   -> "standard"
        #   < 20000  -> "premium"
        #   >= 20000 -> "enterprise"
        #
        # def revenue_tier(amounts: pa.Array) -> pa.Array:
        #     ...
        # tier_udf = udf(revenue_tier, [pa.float64()], pa.string(), "immutable")
        # self.ctx.register_udf(tier_udf)
        pass

    # ---------------------------------------------------------
    # Шаг 3: Аналитические запросы
    # ---------------------------------------------------------

    def run_sales_summary(self) -> None:
        """Сводка продаж по категориям и регионам."""
        # TODO: напишите SQL-запрос, который:
        # 1. Группирует sales по category и region
        # 2. Считает: total_revenue (SUM amount), avg_order (AVG amount),
        #    order_count (COUNT *), total_items (SUM quantity)
        # 3. Сортирует по total_revenue DESC
        #
        # query = """
        #     SELECT
        #         category,
        #         region,
        #         SUM(amount) AS total_revenue,
        #         AVG(amount) AS avg_order,
        #         COUNT(*) AS order_count,
        #         SUM(quantity) AS total_items
        #     FROM sales_events
        #     GROUP BY category, region
        #     ORDER BY total_revenue DESC
        # """
        # df = self.ctx.sql(query)
        # df.show(20)
        # self.save_report(df, "sales_summary")
        pass

    def run_channel_analysis(self) -> None:
        """Анализ каналов продаж."""
        # TODO: напишите SQL-запрос, который:
        # 1. Группирует по channel
        # 2. Считает total_revenue, order_count, avg_amount
        # 3. Добавляет revenue_tier для средней суммы (если UDF зарегистрирован)
        #
        # query = """
        #     SELECT
        #         channel,
        #         SUM(amount) AS total_revenue,
        #         COUNT(*) AS order_count,
        #         AVG(amount) AS avg_amount
        #     FROM sales_events
        #     GROUP BY channel
        #     ORDER BY total_revenue DESC
        # """
        # df = self.ctx.sql(query)
        # df.show()
        # self.save_report(df, "channel_analysis")
        pass

    def run_top_users(self, limit: int = 20) -> None:
        """Топ пользователей по выручке."""
        # TODO: напишите SQL-запрос, который:
        # 1. Группирует по user_id
        # 2. Считает total_spent, order_count, avg_order
        # 3. Ограничивает LIMIT
        #
        # query = f"""
        #     SELECT
        #         user_id,
        #         SUM(amount) AS total_spent,
        #         COUNT(*) AS order_count,
        #         AVG(amount) AS avg_order
        #     FROM sales_events
        #     GROUP BY user_id
        #     ORDER BY total_spent DESC
        #     LIMIT {limit}
        # """
        # df = self.ctx.sql(query)
        # df.show()
        # self.save_report(df, "top_users")
        pass

    # ---------------------------------------------------------
    # Шаг 4: Сохранение отчётов
    # ---------------------------------------------------------

    def save_report(self, df, name: str) -> None:
        """Сохраняет DataFrame как Parquet-отчёт."""
        # TODO: реализуйте сохранение
        # path = os.path.join(self.reports_dir, f"{name}.parquet")
        # df.write_parquet(path)
        # print(f"  Отчёт сохранён: {path}")
        pass

    # ---------------------------------------------------------
    # Шаг 5: Запуск движка
    # ---------------------------------------------------------

    def run(self) -> None:
        """Запускает полный аналитический конвейер."""
        print("=== DataFusion Analytics Engine ===")
        print()

        print("[1/4] Загрузка данных...")
        self.load_sources()
        print()

        print("[2/4] Регистрация функций...")
        self.register_functions()
        print()

        print("[3/4] Выполнение аналитики...")
        print("--- Сводка продаж ---")
        self.run_sales_summary()
        print()
        print("--- Анализ каналов ---")
        self.run_channel_analysis()
        print()
        print("--- Топ пользователей ---")
        self.run_top_users()
        print()

        print("[4/4] Готово. Отчёты в:", self.reports_dir)


if __name__ == "__main__":
    engine = AnalyticsEngine()
    engine.run()
