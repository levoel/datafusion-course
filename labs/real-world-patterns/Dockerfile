FROM python:3.12-slim

LABEL maintainer="DataFusion Course"
LABEL description="DataFusion Python real-world patterns lab environment"

WORKDIR /app

# Системные зависимости
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Python-зависимости
RUN pip install --no-cache-dir \
    datafusion>=52.0.0 \
    pyarrow>=18.0.0 \
    pandas>=2.2.0

# Копируем упражнения
COPY exercises/ /app/exercises/
COPY data/ /app/data/

# Генерируем тестовые данные при сборке
COPY generate_data.py /app/generate_data.py
RUN python generate_data.py

CMD ["bash"]
