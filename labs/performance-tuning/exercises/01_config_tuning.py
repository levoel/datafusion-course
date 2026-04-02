"""
Упражнение 1: Настройка параметров SessionConfig

Цель: экспериментировать с параметрами SessionConfig
и измерять влияние на производительность запросов.

Инструкции:
  Заполните блоки TODO реальным кодом. Запустите скрипт:
    python exercises/01_config_tuning.py
"""

import time
from datafusion import SessionContext, SessionConfig, RuntimeConfig

# =============================================================
# Шаг 1: Создайте контекст с настройками по умолчанию
# =============================================================

# TODO: создайте SessionContext с настройками по умолчанию
# ctx_default = SessionContext()

# TODO: зарегистрируйте data/query_logs.parquet как таблицу "query_logs"
# ctx_default.register_parquet("query_logs", "data/query_logs.parquet")

# TODO: выполните агрегирующий запрос и замерьте время выполнения
# query = """
#     SELECT partitions_used, COUNT(*) as cnt,
#            AVG(duration_ms) as avg_duration,
#            MAX(memory_used_mb) as max_memory
#     FROM query_logs
#     GROUP BY partitions_used
#     ORDER BY partitions_used
# """
# start = time.time()
# result = ctx_default.sql(query)
# result.show()
# default_time = time.time() - start
# print(f"Время по умолчанию: {default_time:.4f} сек")


# =============================================================
# Шаг 2: Создайте контекст с увеличенным параллелизмом
# =============================================================

# TODO: создайте SessionConfig с target_partitions=8
# config = SessionConfig().with_target_partitions(8)

# TODO: создайте RuntimeConfig с увеличенным пулом памяти
# runtime = RuntimeConfig()

# TODO: создайте SessionContext(config, runtime)
# ctx_parallel = SessionContext(config, runtime)

# TODO: зарегистрируйте те же данные и выполните тот же запрос
# ctx_parallel.register_parquet("query_logs", "data/query_logs.parquet")
# start = time.time()
# result = ctx_parallel.sql(query)
# result.show()
# parallel_time = time.time() - start
# print(f"Время (8 партиций): {parallel_time:.4f} сек")


# =============================================================
# Шаг 3: Сравните конфигурации batch_size
# =============================================================

# TODO: создайте контекст с batch_size=2048
# config_small = SessionConfig().with_batch_size(2048)
# ctx_small = SessionContext(config_small)
# ctx_small.register_parquet("query_logs", "data/query_logs.parquet")

# TODO: выполните тот же запрос и замерьте время
# start = time.time()
# result = ctx_small.sql(query)
# result.show()
# small_batch_time = time.time() - start
# print(f"Время (batch 2048): {small_batch_time:.4f} сек")

# TODO: создайте контекст с batch_size=65536
# config_large = SessionConfig().with_batch_size(65536)
# ctx_large = SessionContext(config_large)
# ctx_large.register_parquet("query_logs", "data/query_logs.parquet")

# start = time.time()
# result = ctx_large.sql(query)
# result.show()
# large_batch_time = time.time() - start
# print(f"Время (batch 65536): {large_batch_time:.4f} сек")


# =============================================================
# Шаг 4: Загрузите config_experiments и проанализируйте результаты
# =============================================================

# TODO: зарегистрируйте data/config_experiments.parquet
# ctx_default.register_parquet("experiments", "data/config_experiments.parquet")

# TODO: найдите лучшую комбинацию config_name + batch_size по среднему execution_time_ms
# best_config = ctx_default.sql("""
#     SELECT config_name, batch_size,
#            AVG(execution_time_ms) as avg_time,
#            COUNT(*) as samples
#     FROM experiments
#     GROUP BY config_name, batch_size
#     HAVING COUNT(*) >= 3
#     ORDER BY avg_time ASC
#     LIMIT 10
# """)
# best_config.show()


print("Упражнение 1 завершено.")
