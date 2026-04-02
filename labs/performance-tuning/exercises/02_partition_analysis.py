"""
Упражнение 2: Анализ стратегий партиционирования

Цель: анализировать стратегии партиционирования с помощью
EXPLAIN ANALYZE и сравнивать планы выполнения.

Инструкции:
  Заполните блоки TODO реальным кодом. Запустите скрипт:
    python exercises/02_partition_analysis.py
"""

from datafusion import SessionContext, SessionConfig

# =============================================================
# Шаг 1: Просмотрите план выполнения простого запроса
# =============================================================

ctx = SessionContext()

# TODO: зарегистрируйте data/query_logs.parquet как таблицу "query_logs"
# ctx.register_parquet("query_logs", "data/query_logs.parquet")

# TODO: выведите логический план запроса с помощью EXPLAIN
# explain_df = ctx.sql("""
#     EXPLAIN
#     SELECT partitions_used, AVG(duration_ms) as avg_duration
#     FROM query_logs
#     GROUP BY partitions_used
# """)
# explain_df.show()


# =============================================================
# Шаг 2: Используйте EXPLAIN ANALYZE для реального профилирования
# =============================================================

# TODO: выведите физический план с метриками через EXPLAIN ANALYZE
# analyze_df = ctx.sql("""
#     EXPLAIN ANALYZE
#     SELECT partitions_used, AVG(duration_ms) as avg_duration
#     FROM query_logs
#     GROUP BY partitions_used
# """)
# analyze_df.show()


# =============================================================
# Шаг 3: Сравните планы с разным числом партиций
# =============================================================

# TODO: создайте контекст с 2 партициями и выполните EXPLAIN ANALYZE
# config_2 = SessionConfig().with_target_partitions(2)
# ctx_2 = SessionContext(config_2)
# ctx_2.register_parquet("query_logs", "data/query_logs.parquet")
# plan_2 = ctx_2.sql("""
#     EXPLAIN ANALYZE
#     SELECT partitions_used, AVG(duration_ms) as avg_duration
#     FROM query_logs
#     GROUP BY partitions_used
# """)
# print("=== 2 партиции ===")
# plan_2.show()

# TODO: создайте контекст с 16 партициями и выполните EXPLAIN ANALYZE
# config_16 = SessionConfig().with_target_partitions(16)
# ctx_16 = SessionContext(config_16)
# ctx_16.register_parquet("query_logs", "data/query_logs.parquet")
# plan_16 = ctx_16.sql("""
#     EXPLAIN ANALYZE
#     SELECT partitions_used, AVG(duration_ms) as avg_duration
#     FROM query_logs
#     GROUP BY partitions_used
# """)
# print("=== 16 партиций ===")
# plan_16.show()


# =============================================================
# Шаг 4: Анализ плана JOIN-запроса
# =============================================================

# TODO: зарегистрируйте data/config_experiments.parquet как "experiments"
# ctx.register_parquet("experiments", "data/config_experiments.parquet")

# TODO: выведите EXPLAIN ANALYZE для JOIN между query_logs и experiments
# Подсказка: JOIN по partitions_used = target_partitions
# join_plan = ctx.sql("""
#     EXPLAIN ANALYZE
#     SELECT q.partitions_used, q.duration_ms, e.config_name, e.execution_time_ms
#     FROM query_logs q
#     JOIN experiments e ON q.partitions_used = e.target_partitions
#     WHERE q.duration_ms > 1000
# """)
# join_plan.show()


# =============================================================
# Шаг 5: Запись партиционированных данных
# =============================================================

# TODO: запишите query_logs в партиционированный формат по partitions_used
# import pyarrow.parquet as pq
# df = ctx.sql("SELECT * FROM query_logs")
# batches = df.collect()
# table = batches[0]  # или pa.concat_tables(...)
# pq.write_to_dataset(
#     table,
#     root_path="data/query_logs_partitioned",
#     partition_cols=["partitions_used"],
# )
# print("Партиционированные данные записаны в data/query_logs_partitioned/")

# TODO: зарегистрируйте партиционированную директорию и сравните EXPLAIN ANALYZE
# ctx.register_parquet("query_logs_part", "data/query_logs_partitioned/")
# partitioned_plan = ctx.sql("""
#     EXPLAIN ANALYZE
#     SELECT partitions_used, AVG(duration_ms)
#     FROM query_logs_part
#     WHERE partitions_used = 4
#     GROUP BY partitions_used
# """)
# partitioned_plan.show()


print("Упражнение 2 завершено.")
