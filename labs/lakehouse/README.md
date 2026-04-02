# Lakehouse Lab: Интеграция DataFusion с Lakehouse-форматами

Лабораторная работа к модулю 12 курса DataFusion.

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
cd labs/lakehouse
docker compose up -d --build
```

### 2. Подключитесь к контейнеру

```bash
docker compose exec lab bash
```

### 3. Проверьте установку

```python
python -c "import datafusion; import deltalake; print(f'DataFusion {datafusion.__version__}, Delta Lake {deltalake.__version__}')"
```

## Структура

```
lakehouse/
  Dockerfile            # Python 3.12 + DataFusion + Delta Lake + PyIceberg
  docker-compose.yml    # Конфигурация контейнера
  generate_data.py      # Генерация Parquet и Delta-таблиц
  data/                 # Тестовые данные (генерируются при сборке)
    events.parquet        # 5 000 записей событий (Parquet)
    events_delta/         # Те же данные в формате Delta Lake
  exercises/            # Упражнения
    01_delta_lake.py      # Работа с Delta Lake таблицами
    02_format_comparison.py # Сравнение форматов данных
```

## Упражнения

### Упражнение 1: Работа с Delta Lake

Цель: зарегистрировать Delta-таблицу в DataFusion, выполнять SQL-запросы и работать с версионированием (time travel).

```bash
python exercises/01_delta_lake.py
```

### Упражнение 2: Сравнение форматов

Цель: сравнить производительность и возможности Parquet и Delta Lake при работе через DataFusion.

```bash
python exercises/02_format_comparison.py
```

## Тестовые данные

- **events.parquet** --- 5 000 записей событий. Поля: event_id, timestamp, category, amount, region.
- **events_delta/** --- те же данные в формате Delta Lake с поддержкой версионирования.

## Остановка

```bash
docker compose down
```

Для удаления собранного образа:

```bash
docker compose down --rmi local
```
