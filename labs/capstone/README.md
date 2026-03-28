# Capstone Lab: DataFusion Python Analytics Engine

Лабораторная работа к модулю 10 курса DataFusion.

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
cd labs/capstone
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
capstone/
  Dockerfile            # Python 3.12 + DataFusion + PyArrow + Pandas
  docker-compose.yml    # Конфигурация контейнера
  generate_data.py      # Генерация тестовых Parquet-файлов
  data/                 # Тестовые данные (генерируются при сборке)
    sales_events.parquet  # 10 000 записей о продажах
    products.parquet      # 200 продуктов
  exercises/            # Упражнения
    01_explore_data.py    # Исследование данных
    02_custom_udf.py      # Пользовательские функции
    03_analytics_engine.py # Мини аналитический движок
```

## Упражнения

### Упражнение 1: Исследование данных

Цель: освоить чтение Parquet-файлов, SQL-запросы и DataFrame API.

```bash
python exercises/01_explore_data.py
```

### Упражнение 2: Пользовательские функции (UDF)

Цель: создать скалярные и агрегатные UDF для бизнес-логики.

```bash
python exercises/02_custom_udf.py
```

### Упражнение 3: Мини аналитический движок

Цель: построить аналитический движок поверх DataFusion с кастомными источниками данных, UDF и конвейерами обработки.

```bash
python exercises/03_analytics_engine.py
```

## Тестовые данные

- **sales_events.parquet** --- 10 000 записей о продажах за 2024 год. Поля: event_id, timestamp, category, region, channel, amount, quantity, user_id.
- **products.parquet** --- 200 продуктов в 5 категориях. Поля: product_id, name, category, base_price, weight_kg.

## Остановка

```bash
docker compose down
```

Для удаления собранного образа:

```bash
docker compose down --rmi local
```
