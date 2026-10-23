# Databricks notebook source
params = ["dataset", "source_path", "catalog", "schema"]
for p in params:
    dbutils.widgets.text(p, "")
v = {p: dbutils.widgets.get(p) for p in params}

target = f'{v["catalog"]}.{v["schema"]}.raw_{v["dataset"]}'
df = spark.read.option("header", True).option("inferSchema", True).csv(v["source_path"])
df.write.mode("overwrite").saveAsTable(target)
print(f"{df.count()} rows loaded into {target}")
