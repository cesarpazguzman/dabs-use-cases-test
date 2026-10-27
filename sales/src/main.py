# Databricks notebook source
# Daily sales aggregation, from the orders ingested by the ingestion use case
from transformations import daily_sales

for p in ["catalog", "schema", "source_schema"]:
    dbutils.widgets.text(p, "")
catalog, schema, source = (dbutils.widgets.get(p) for p in ["catalog", "schema", "source_schema"])

daily_sales(spark.table(f"{catalog}.{source}.raw_orders")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.daily_sales")
