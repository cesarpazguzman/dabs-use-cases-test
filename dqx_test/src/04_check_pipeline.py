# Databricks notebook source
# MAGIC %md
# MAGIC # Case 4: check the Lakeflow pipeline
# MAGIC Runs after the `dqx_test-lab-pipeline` pipeline (source: `04_lakeflow_pipeline.py`).
# MAGIC In the pipeline graph, the `orders_dq` view feeds the silver and quarantine tables.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# Variant A: rules in YAML, split with get_valid / get_invalid
expect("pl_silver_orders", order_ids(spark.table(f"{BASE}.pl_silver_orders")), [1002, 1006, 1008, 1010])
expect("pl_quarantine_orders", spark.table(f"{BASE}.pl_quarantine_orders").count(), 8)
expect("pl_silver_orders has no quality columns",
       {"_errors", "_warnings"} & set(spark.table(f"{BASE}.pl_silver_orders").columns), set())

# Variant B: rules in code, annotate only
expect("pl_silver_orders_annotated", spark.table(f"{BASE}.pl_silver_orders_annotated").count(), 10)

# Case 10, part 4: metrics computed in a materialized view (10 rows, 2 with errors, 8 valid)
metrics = {r.metric_name: r.metric_value for r in spark.table(f"{BASE}.pl_quality_metrics").collect()}
display(spark.table(f"{BASE}.pl_quality_metrics"))
expect("pipeline input_row_count", int(metrics["input_row_count"]), 10)
expect("pipeline error_row_count", int(metrics["error_row_count"]), 2)
expect("pipeline valid_row_count", int(metrics["valid_row_count"]), 8)
