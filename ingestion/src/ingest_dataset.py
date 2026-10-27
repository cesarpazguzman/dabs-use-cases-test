# Databricks notebook source
import json

params = ["dataset", "source_path", "format", "options", "mode", "catalog", "schema"]
for p in params:
    dbutils.widgets.text(p, "")
v = {p: dbutils.widgets.get(p) for p in params}

target = f'{v["catalog"]}.{v["schema"]}.raw_{v["dataset"]}'
df = spark.read.format(v["format"]).options(**json.loads(v["options"] or "{}")).load(v["source_path"])
writer = df.write.mode(v["mode"])
if v["mode"] == "overwrite":
    writer = writer.option("overwriteSchema", "true")
writer.saveAsTable(target)
print(f"{df.count()} rows loaded into {target}")
