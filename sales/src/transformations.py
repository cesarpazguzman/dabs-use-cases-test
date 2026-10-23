"""Pure transformations of the sales use case, unit-tested in tests/ without Databricks."""
from pyspark.sql import DataFrame, functions as F


def daily_sales(orders: DataFrame) -> DataFrame:
    """Aggregate TPC-H orders per order date."""
    return orders.groupBy(F.col("o_orderdate").alias("order_date")).agg(
        F.count("*").alias("orders"),
        F.sum("o_totalprice").alias("revenue"),
        F.avg("o_totalprice").alias("avg_order_value"),
        F.min("o_totalprice").alias("min_order"),
        F.max("o_totalprice").alias("max_order"),
    )
