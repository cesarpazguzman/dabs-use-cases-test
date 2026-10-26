# Databricks notebook source
# Top customers by revenue, from the orders and customers ingested by the ingestion use case
from transformations import top_customers

for p in ["catalog", "schema", "source_schema"]:
    dbutils.widgets.text(p, "")
catalog, schema, source = (dbutils.widgets.get(p) for p in ["catalog", "schema", "source_schema"])

top_customers(spark.table(f"{catalog}.{source}.raw_orders"), spark.table(f"{catalog}.{source}.raw_customer")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.top_customers")
