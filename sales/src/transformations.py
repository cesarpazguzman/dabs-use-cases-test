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


def monthly_sales(daily: DataFrame) -> DataFrame:
    """Roll the daily aggregate up to months."""
    return (
        daily.groupBy(F.trunc(F.to_date("order_date"), "month").alias("month"))
        .agg(F.sum("orders").alias("orders"), F.sum("revenue").alias("revenue"))
        .withColumn("avg_order_value", F.col("revenue") / F.col("orders"))
    )


def top_customers(orders: DataFrame, customers: DataFrame, n: int = 10) -> DataFrame:
    """The n customers with the highest revenue, with their name."""
    per_customer = orders.groupBy("o_custkey").agg(
        F.count("*").alias("orders"), F.sum("o_totalprice").alias("revenue"),
    )
    return (
        per_customer.join(customers, per_customer.o_custkey == customers.c_custkey)
        .select(
            F.col("c_custkey").alias("customer_key"),
            F.col("c_name").alias("customer_name"),
            "orders",
            "revenue",
        )
        .orderBy(F.desc("revenue"), "customer_key")
        .limit(n)
    )
