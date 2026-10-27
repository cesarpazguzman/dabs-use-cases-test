# Databricks notebook source
# MAGIC %md
# MAGIC # Case 5: at-rest, rules in code
# MAGIC Audit a table that is already written, without touching the pipeline that loads it or moving its data.
# MAGIC Only the problem rows (quarantine-only mode) and the metrics of the run are written: the usual pattern for
# MAGIC a nightly audit job.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQDatasetRule
from orders_rules import ORDER_CHECKS

# The rules of case 1, plus a dataset rule that looks at groups of rows
audit_checks = ORDER_CHECKS + [
    DQDatasetRule(name="min_2_orders_per_country", criticality="warn",
                  check_func=check_funcs.is_aggr_not_less_than, column="order_id",
                  check_func_kwargs={"aggr_type": "count", "group_by": ["country"], "limit": 2}),
]

# COMMAND ----------

from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.metrics_observer import DQMetricsObserver

dq_auditor = DQEngine(WorkspaceClient(), observer=DQMetricsObserver(name="audit_bronze_orders"))

# No output_config: DQX does not rewrite the good rows, the source table stays as it is
dq_auditor.apply_checks_and_save_in_table(
    checks=audit_checks,
    input_config=InputConfig(location=f"{BASE}.bronze_orders"),
    quarantine_config=OutputConfig(location=f"{BASE}.audit_orders", mode="overwrite"),
    metrics_config=OutputConfig(location=f"{BASE}.dq_metrics", mode="append"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC The new rule warns PT, FR and IT (one order each), so 1002 is in the audit table too.

# COMMAND ----------

expect("audit_orders", order_ids(spark.table(f"{BASE}.audit_orders")),
       [1001, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008])

last_run = spark.sql(f"""
    SELECT metric_name, metric_value FROM {BASE}.dq_metrics
    WHERE run_name = 'audit_bronze_orders'
      AND run_time = (SELECT max(run_time) FROM {BASE}.dq_metrics WHERE run_name = 'audit_bronze_orders')
""")
display(last_run)
metrics = {r.metric_name: r.metric_value for r in last_run.collect()}
expect("input_row_count", int(metrics["input_row_count"]), 10)
expect("error_row_count", int(metrics["error_row_count"]), 6)
expect("warning_row_count", int(metrics["warning_row_count"]), 5)
expect("valid_row_count", int(metrics["valid_row_count"]), 1)

# COMMAND ----------

# MAGIC %md
# MAGIC Interactive variant, writing nothing. With an engine without observer, `apply_checks` returns only the
# MAGIC DataFrame; with an observer it returns a tuple `(df, observation)`.

# COMMAND ----------

annotated = dq_engine.apply_checks(spark.table(f"{BASE}.bronze_orders"), audit_checks)
display(dq_engine.get_invalid(annotated))
