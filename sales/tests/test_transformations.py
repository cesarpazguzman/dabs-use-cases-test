from transformations import daily_sales, monthly_sales, top_customers

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


def test_monthly_sales_rolls_days_up(spark):
    daily = daily_sales(spark.createDataFrame(ORDERS + [("1995-02-03", 60.0)], ["o_orderdate", "o_totalprice"]))

    rows = {str(r.month): r for r in monthly_sales(daily).collect()}

    assert rows["1995-01-01"].orders == 3
    assert rows["1995-01-01"].revenue == 45.0
    assert rows["1995-01-01"].avg_order_value == 15.0
    assert rows["1995-02-01"].orders == 1


def test_top_customers_ranks_by_revenue(spark):
    orders = spark.createDataFrame([(1, 10.0), (1, 30.0), (2, 50.0), (3, 5.0)], ["o_custkey", "o_totalprice"])
    customers = spark.createDataFrame([(1, "Ann"), (2, "Bob"), (3, "Cy")], ["c_custkey", "c_name"])

    rows = top_customers(orders, customers, n=2).collect()

    assert [(r.customer_name, r.orders, r.revenue) for r in rows] == [("Bob", 1, 50.0), ("Ann", 2, 40.0)]
