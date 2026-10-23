from transformations import daily_sales

ORDERS = [
    ("1995-01-01", 10.0),
    ("1995-01-01", 30.0),
    ("1995-01-02", 5.0),
]


def test_daily_sales_has_one_row_per_day(spark):
    orders = spark.createDataFrame(ORDERS, ["o_orderdate", "o_totalprice"])

    assert daily_sales(orders).count() == 2


def test_daily_sales_aggregates(spark):
    orders = spark.createDataFrame(ORDERS, ["o_orderdate", "o_totalprice"])

    row = daily_sales(orders).filter("order_date = '1995-01-01'").first()

    assert row.orders == 2
    assert row.revenue == 40.0
    assert row.avg_order_value == 20.0
    assert row.min_order == 10.0
    assert row.max_order == 30.0


def test_daily_sales_columns(spark):
    orders = spark.createDataFrame(ORDERS, ["o_orderdate", "o_totalprice"])

    assert daily_sales(orders).columns == [
        "order_date", "orders", "revenue", "avg_order_value", "min_order", "max_order",
    ]
