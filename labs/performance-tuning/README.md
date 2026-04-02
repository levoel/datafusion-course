# Performance Tuning Lab: Оптимизация производительности DataFusion

Лабораторная работа к модулю 11 курса DataFusion.

## Требования

- **Docker Desktop** 4.x или выше
- **RAM:** минимум 4 GB
- **Платформы:**
  - macOS (Apple Silicon M1/M2/M3/M4 или Intel)
  - Linux (x86_64, ARM64)
  - Windows с WSL2

## Быстрый старт

### 1. Соберите и запустите контейнер

```bash
cd labs/performance-tuning
docker compose up -d --build
```

### 2. Подключитесь к контейнеру

```bash
docker compose exec lab bash
```

### 3. Проверьте установку

```python
python -c "import datafusion; print(datafusion.__version__)"
```

## Структура

```
performance-tuning/
  Dockerfile            # Python 3.12 + DataFusion + PyArrow + Pandas
  docker-compose.yml    # Конфигурация контейнера
  generate_data.py      # Генерация тестовых Parquet-файлов
  data/                 # Тестовые данные (генерируются при сборке)
    query_logs.parquet          # 10 000 записей логов запросов
    config_experiments.parquet  # 1 000 записей экспериментов
  exercises/            # Упражнения
    01_config_tuning.py   # Настройка параметров SessionConfig
    02_partition_analysis.py # Анализ стратегий партиционирования
```

## Упражнения

### Упражнение 1: Настройка конфигурации

Цель: экспериментировать с параметрами SessionConfig и измерять влияние на производительность запросов.

```bash
python exercises/01_config_tuning.py
```

### Упражнение 2: Анализ партиционирования

Цель: анализировать стратегии партиционирования с помощью EXPLAIN ANALYZE и сравнивать планы выполнения.

```bash
python exercises/02_partition_analysis.py
```

## Тестовые данные

- **query_logs.parquet** --- 10 000 записей логов выполнения запросов. Поля: query_id, timestamp, sql_text, duration_ms, rows_scanned, memory_used_mb, partitions_used.
- **config_experiments.parquet** --- 1 000 записей экспериментов с конфигурацией. Поля: config_name, batch_size, target_partitions, execution_time_ms.

## Остановка

```bash
docker compose down
```

Для удаления собранного образа:

```bash
docker compose down --rmi local
```
