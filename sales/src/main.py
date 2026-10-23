# Databricks notebook source
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
target = f'{dbutils.widgets.get("catalog")}.{dbutils.widgets.get("schema")}.daily_sales'

spark.sql("""
    SELECT o_orderdate AS order_date, COUNT(*) AS orders, SUM(o_totalprice) AS revenue,
           AVG(o_totalprice) AS avg_order_value
    FROM samples.tpch.orders
    GROUP BY o_orderdate
""").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
