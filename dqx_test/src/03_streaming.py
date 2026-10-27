# Databricks notebook source
# MAGIC %md
# MAGIC # Case 3: in-transit, streaming
# MAGIC Process only the **new** orders, validate them and split them between silver and quarantine in one call.
# MAGIC The `availableNow` trigger reads what is pending, processes it and stops: an incremental batch that a job
# MAGIC can schedule.
# MAGIC
# MAGIC The lab appends rows to `bronze_orders`; here they go to a copy (`stream_source_orders`), so the other
# MAGIC cases of the job keep seeing the 10 original rows.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# Start from scratch: output tables, checkpoints and source copy
CKPT = f"{VOL}/checkpoints/case03"
for t in ["silver_orders_stream", "quarantine_orders_stream"]:
    spark.sql(f"DROP TABLE IF EXISTS {BASE}.{t}")
dbutils.fs.rm(CKPT, True)
spark.table(f"{BASE}.bronze_orders").write.mode("overwrite").saveAsTable(f"{BASE}.stream_source_orders")

# COMMAND ----------

from databricks.labs.dqx.config import InputConfig, OutputConfig, WorkspaceFileChecksStorageConfig

# The rules of case 2 without the dataset rules: is_unique uses a window function, which Structured Streaming
# rejects (NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING). Uniqueness is checked at rest (case 5) or with a MERGE.
ORDERS_CHECKS = f"{CHECKS}/orders_checks.yml"
row_checks = [c for c in dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=ORDERS_CHECKS))
              if c["check"]["function"] != "is_unique"]


def run_stream():
    """End to end: read new rows, validate, write. Each output needs its own checkpoint."""
    dq_engine.apply_checks_by_metadata_and_save_in_table(
        checks=row_checks,
        checks_location=ORDERS_CHECKS,                         # only for reporting
        input_config=InputConfig(location=f"{BASE}.stream_source_orders", is_streaming=True),
        output_config=OutputConfig(
            location=f"{BASE}.silver_orders_stream",
            mode="append",
            trigger={"availableNow": True},
            options={"checkpointLocation": f"{CKPT}/silver"},
        ),
        quarantine_config=OutputConfig(
            location=f"{BASE}.quarantine_orders_stream",
            mode="append",
            trigger={"availableNow": True},
            options={"checkpointLocation": f"{CKPT}/quarantine"},
        ),
    )


# Without order_id_unique, both 1001 rows reach silver: 6 rows in silver and 6 in quarantine (case 2: 4 and 8)
run_stream()
expect("silver_orders_stream (run 1)", order_ids(spark.table(f"{BASE}.silver_orders_stream")),
       [1001, 1001, 1002, 1006, 1008, 1010])
expect("quarantine_orders_stream (run 1)", order_ids(spark.table(f"{BASE}.quarantine_orders_stream")),
       [1003, 1004, 1005, 1006, 1007, 1008])

# COMMAND ----------

# MAGIC %md
# MAGIC Two new orders arrive, a good one and a bad one. The second run only processes those two.

# COMMAND ----------

import datetime as dt

new_orders = spark.createDataFrame([
    (1011, "C001", dt.date(2026, 10, 7), 75.0, "NEW", "ana@mail.com",  "ES"),
    (1012, "C002", dt.date(2026, 10, 7), -5.0, "NEW", "luis@mail.com", "PT"),
], spark.table(f"{BASE}.stream_source_orders").schema)
new_orders.write.mode("append").saveAsTable(f"{BASE}.stream_source_orders")

run_stream()
expect("silver_orders_stream (run 2)", order_ids(spark.table(f"{BASE}.silver_orders_stream")),
       [1001, 1001, 1002, 1006, 1008, 1010, 1011])
expect("quarantine_orders_stream (run 2)", order_ids(spark.table(f"{BASE}.quarantine_orders_stream")),
       [1003, 1004, 1005, 1006, 1007, 1008, 1012])

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataset rules in streaming:** the lab says `order_id_unique` would only find duplicates inside each
# MAGIC micro-batch. With DQX 0.17.0 on serverless it does not run at all: `is_unique` uses a window function and the
# MAGIC stream fails with `NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING`. That is why this case leaves it out and the
# MAGIC duplicated 1001 reaches silver. For uniqueness use a `MERGE` into silver or an at-rest check (case 5).
