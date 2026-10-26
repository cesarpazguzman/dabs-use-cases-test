# Databricks notebook source
# Monthly sales, rolled up from daily_sales
from transformations import monthly_sales

for p in ["catalog", "schema"]:
    dbutils.widgets.text(p, "")
catalog, schema = (dbutils.widgets.get(p) for p in ["catalog", "schema"])

monthly_sales(spark.table(f"{catalog}.{schema}.daily_sales")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.monthly_sales")
