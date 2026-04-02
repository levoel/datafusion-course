# Real-World Patterns Lab: Практические паттерны DataFusion

Лабораторная работа к модулю 13 курса DataFusion.

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
cd labs/real-world-patterns
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
real-world-patterns/
  Dockerfile            # Python 3.12 + DataFusion + PyArrow + Pandas
  docker-compose.yml    # Конфигурация контейнера
  generate_data.py      # Генерация мультитенантных данных
  data/                 # Тестовые данные (генерируются при сборке)
    tenant_a_orders.parquet  # 3 000 заказов тенанта A
    tenant_b_orders.parquet  # 3 000 заказов тенанта B
  exercises/            # Упражнения
    01_multi_tenant.py    # Изоляция контекстов по тенантам
    02_analytics_api.py   # Простой аналитический API
```

## Упражнения

### Упражнение 1: Мультитенантная изоляция

Цель: создать изолированные SessionContext для каждого тенанта с раздельным доступом к данным.

```bash
python exercises/01_multi_tenant.py
```

### Упражнение 2: Аналитический API

Цель: построить простой аналитический API поверх DataFusion для выполнения параметризованных запросов.

```bash
python exercises/02_analytics_api.py
```

## Тестовые данные

- **tenant_a_orders.parquet** --- 3 000 заказов тенанта A. Поля: order_id, tenant_id, product, amount, created_at.
- **tenant_b_orders.parquet** --- 3 000 заказов тенанта B. Поля: order_id, tenant_id, product, amount, created_at.

## Остановка

```bash
docker compose down
```

Для удаления собранного образа:

```bash
docker compose down --rmi local
```
