# Databricks notebook source
# MAGIC %md
# MAGIC # Case 1: in-transit, rules in code
# MAGIC Validate the bronze orders **before** writing silver; rows with errors go to a quarantine table.
# MAGIC The rules are Python classes.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# The rules are Python classes, defined in orders_rules.py (case 5 reuses them)
from orders_rules import ORDER_CHECKS as checks

for c in checks:
    print(c.name, c.criticality, c.check_func.__name__)

# COMMAND ----------

from databricks.labs.dqx.config import OutputConfig

bronze = spark.table(f"{BASE}.bronze_orders")
valid_df, quarantine_df = dq_engine.apply_checks_and_split(bronze, checks)

dq_engine.save_results_in_table(
    output_df=valid_df,
    quarantine_df=quarantine_df,
    output_config=OutputConfig(location=f"{BASE}.silver_orders", mode="overwrite"),
    quarantine_config=OutputConfig(location=f"{BASE}.quarantine_orders", mode="overwrite"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC Warnings do not block: 1006 and 1008 are in **both** tables.

# COMMAND ----------

expect("silver_orders", order_ids(spark.table(f"{BASE}.silver_orders")), [1002, 1006, 1008, 1010])
expect("quarantine_orders", order_ids(spark.table(f"{BASE}.quarantine_orders")),
       [1001, 1001, 1003, 1004, 1005, 1006, 1007, 1008])

display(spark.table(f"{BASE}.quarantine_orders").select("order_id", "_errors.name", "_warnings.name"))
