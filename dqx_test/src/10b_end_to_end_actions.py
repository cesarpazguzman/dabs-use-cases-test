# Databricks notebook source
# MAGIC %md
# MAGIC # Case 10, part 2: save the metrics and fire the actions by themselves
# MAGIC With the end-to-end methods, metrics are saved and actions evaluated at the end with no extra code. In batch,
# MAGIC when `FailPipeline` fires, the exception is raised after the alerts are sent.
# MAGIC
# MAGIC **Known issue (DQX 0.17.0, serverless):** run as a task of this bundle job, the end-to-end method gets no
# MAGIC observed metrics (`available: []`): it saves no metrics and fires no action, with no error. The same notebook
# MAGIC run on its own (`databricks jobs submit`) saves the metrics and raises `PipelineFailedError`. This notebook
# MAGIC checks the expected behaviour and, when it meets the issue, reports it instead of failing.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from databricks.labs.dqx.config import InputConfig, OutputConfig, WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.actions import DQAction, DQAlert, DQLogAlertDestination, FailPipeline
from databricks.labs.dqx.errors import PipelineFailedError

observer = DQMetricsObserver(name="lab_orders_e2e", custom_metrics=["sum(array_size(_errors)) as total_errors"])
dq_alerts = DQEngine(
    WorkspaceClient(),
    observer=observer,
    actions=[
        DQAction(condition="error_row_count > 0",
                 action=DQAlert(destinations=[DQLogAlertDestination(name="log", level="warning")])),
        DQAction(condition="error_row_count * 1.0 / input_row_count > 0.5",
                 action=FailPipeline(message="More than 50% of the rows have errors")),
    ],
)
checks = dq_alerts.load_checks(config=WorkspaceFileChecksStorageConfig(location=f"{CHECKS}/orders_checks.yml"))

# COMMAND ----------

try:
    dq_alerts.apply_checks_by_metadata_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=f"{BASE}.bronze_orders"),
        output_config=OutputConfig(location=f"{BASE}.silver_orders_alerts", mode="overwrite"),
        quarantine_config=OutputConfig(location=f"{BASE}.quarantine_orders_alerts", mode="overwrite"),
        metrics_config=OutputConfig(location=f"{BASE}.dq_metrics", mode="append"),
    )
    stopped = False
except PipelineFailedError as e:      # in a real job, do not catch it: let the job fail
    print("Pipeline stopped:", e)
    stopped = True

# COMMAND ----------

this_run = spark.sql(f"""
    SELECT metric_name, metric_value FROM {BASE}.dq_metrics
    WHERE run_name = 'lab_orders_e2e' AND run_id = '{observer.id}'""") \
    if spark.catalog.tableExists(f"{BASE}.dq_metrics") else None
saved = {r.metric_name: r.metric_value for r in this_run.collect()} if this_run is not None else {}

if not stopped and not saved:
    # Known issue: no observed metrics, so nothing was saved and no action was evaluated (see the top cell)
    print("KNOWN ISSUE (DQX 0.17.0 in a bundle job on serverless): the end-to-end run got no observed metrics, "
          "so it saved no metrics and fired no action. Expected: metrics saved and PipelineFailedError raised.")
else:
    display(this_run)
    expect("FailPipeline in the end-to-end run", stopped, True)
    expect("saved input_row_count", int(saved["input_row_count"]), 10)
    expect("saved error_row_count", int(saved["error_row_count"]), 6)
    expect("saved total_errors", int(saved["total_errors"]), 6)
