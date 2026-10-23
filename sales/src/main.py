# Databricks notebook source
# Daily sales aggregation (change A)
from transformations import daily_sales

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
target = f'{dbutils.widgets.get("catalog")}.{dbutils.widgets.get("schema")}.daily_sales'

daily_sales(spark.table("samples.tpch.orders")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
