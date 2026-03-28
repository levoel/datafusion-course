"""
Упражнение 2: Пользовательские функции (UDF и UDAF)

Цель: создать скалярные UDF и агрегатные UDAF для бизнес-логики,
зарегистрировать их в SessionContext и использовать в SQL-запросах.

Инструкции:
  Заполните блоки TODO реальным кодом. Запустите скрипт:
    python exercises/02_custom_udf.py
"""

from datafusion import SessionContext, udf, udaf, Accumulator, col
import pyarrow as pa

ctx = SessionContext()
ctx.register_parquet("sales", "data/sales_events.parquet")


# =============================================================
# Шаг 1: Скалярный UDF — категория суммы
# =============================================================

def classify_amount(amounts: pa.Array) -> pa.Array:
    """
    Классифицирует сумму продажи:
      amount < 1000   -> "small"
      1000 <= amount < 10000 -> "medium"
      amount >= 10000 -> "large"
    """
    # TODO: реализуйте классификацию
    # result = []
    # for val in amounts:
    #     v = val.as_py()
    #     if v is None:
    #         result.append(None)
    #     elif v < 1000:
    #         result.append("small")
    #     elif v < 10000:
    #         result.append("medium")
    #     else:
    #         result.append("large")
    # return pa.array(result, type=pa.string())
    pass

# TODO: зарегистрируйте UDF
# classify_udf = udf(classify_amount, [pa.float64()], pa.string(), "immutable")
# ctx.register_udf(classify_udf)

# TODO: используйте UDF в SQL
# df = ctx.sql("""
#     SELECT category, classify_amount(amount) AS size_class, COUNT(*) AS cnt
#     FROM sales
#     GROUP BY category, classify_amount(amount)
#     ORDER BY category, cnt DESC
# """)
# df.show()


# =============================================================
# Шаг 2: Агрегатный UDAF — средневзвешенное
# =============================================================

class WeightedAvg(Accumulator):
    """
    Считает средневзвешенное значение:
    weighted_avg = SUM(value * weight) / SUM(weight)
    """
    def __init__(self):
        self._sum_vw = 0.0
        self._sum_w = 0.0

    def state(self) -> list[pa.Scalar]:
        return [pa.scalar(self._sum_vw), pa.scalar(self._sum_w)]

    def update(self, values: pa.Array, weights: pa.Array) -> None:
        # TODO: реализуйте накопление
        # for v, w in zip(values, weights):
        #     if v.is_valid and w.is_valid:
        #         self._sum_vw += v.as_py() * w.as_py()
        #         self._sum_w += w.as_py()
        pass

    def merge(self, states: list[pa.Array]) -> None:
        # TODO: реализуйте слияние состояний
        # self._sum_vw += states[0].as_py()
        # self._sum_w += states[1].as_py()
        pass

    def evaluate(self) -> pa.Scalar:
        # TODO: верните результат
        # if self._sum_w == 0:
        #     return pa.scalar(None)
        # return pa.scalar(self._sum_vw / self._sum_w)
        pass

# TODO: зарегистрируйте UDAF
# weighted_avg = udaf(
#     WeightedAvg,
#     [pa.float64(), pa.int32()],
#     pa.float64(),
#     [pa.float64(), pa.float64()],
#     "immutable",
# )
# ctx.register_udaf(weighted_avg)

# TODO: используйте UDAF — средневзвешенная цена по количеству в каждом регионе
# df = ctx.sql("""
#     SELECT region, weighted_avg(amount, quantity) AS wavg_amount
#     FROM sales
#     GROUP BY region
#     ORDER BY wavg_amount DESC
# """)
# df.show()


# =============================================================
# Шаг 3: Комбинация UDF и UDAF в аналитическом запросе
# =============================================================

# TODO: напишите запрос, который:
# 1. Классифицирует каждую продажу по classify_amount
# 2. Группирует по region и size_class
# 3. Считает weighted_avg(amount, quantity) в каждой группе
# 4. Сортирует по region, затем по wavg DESC

# combined = ctx.sql("""
#     SELECT
#         region,
#         classify_amount(amount) AS size_class,
#         weighted_avg(amount, quantity) AS wavg,
#         COUNT(*) AS cnt
#     FROM sales
#     GROUP BY region, classify_amount(amount)
#     ORDER BY region, wavg DESC
# """)
# combined.show()


print("Упражнение 2 завершено.")
