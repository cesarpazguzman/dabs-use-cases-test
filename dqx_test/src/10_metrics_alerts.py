# Databricks notebook source
# MAGIC %md
# MAGIC # Case 10: summary metrics and alerts
# MAGIC Measure every run (total rows, rows with errors, with warnings, valid, and a breakdown per rule), raise an
# MAGIC alert when there are errors and **stop the job** if the share of errors is above a threshold. The alert goes
# MAGIC to the driver log, so no Slack or Teams is needed.
# MAGIC
# MAGIC Part 2 (end-to-end run) is in `10b_end_to_end_actions.py` and part 4 (metrics inside Lakeflow) in the
# MAGIC pipeline of case 4 (`pl_quality_metrics`).

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Observer, actions and run

# COMMAND ----------

from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.actions import DQAction, DQAlert, DQLogAlertDestination, FailPipeline
from databricks.labs.dqx.errors import PipelineFailedError


def alerting_engine(fail_threshold, actions=None):
    observer = DQMetricsObserver(name="lab_orders", custom_metrics=["sum(array_size(_errors)) as total_errors"])
    actions = actions or [
        DQAction(condition="error_row_count > 0",
                 action=DQAlert(destinations=[DQLogAlertDestination(name="log", level="warning")])),
        DQAction(condition=f"error_row_count * 1.0 / input_row_count > {fail_threshold}",
                 action=FailPipeline(message=f"More than {fail_threshold:.0%} of the rows have errors")),
    ]
    return DQEngine(WorkspaceClient(), observer=observer, actions=actions)


dq_alerts = alerting_engine(0.5)
checks = dq_alerts.load_checks(config=WorkspaceFileChecksStorageConfig(location=f"{CHECKS}/orders_checks.yml"))

annotated, observation = dq_alerts.apply_checks_by_metadata(spark.table(f"{BASE}.bronze_orders"), checks)
annotated.count()            # metrics only exist after a Spark action; reading them before blocks the cell
metrics = observation.get
print(metrics)

# COMMAND ----------

expect("input_row_count", int(metrics["input_row_count"]), 10)
expect("error_row_count", int(metrics["error_row_count"]), 6)
expect("warning_row_count", int(metrics["warning_row_count"]), 2)
expect("valid_row_count", int(metrics["valid_row_count"]), 2)        # only 1002 and 1010 are clean
expect("total_errors", int(metrics["total_errors"]), 6)

# COMMAND ----------

# MAGIC %md
# MAGIC 6 of 10 rows is 60%, so `FailPipeline` raises `PipelineFailedError` after the alert (status unhealthy in the
# MAGIC log). In a real job, do not catch it: let the job fail. Here it is caught to check it happens.

# COMMAND ----------

def fails(engine, observed):
    try:
        engine.evaluate_actions(observed, input_location=f"{BASE}.bronze_orders")
        return False
    except PipelineFailedError as e:
        print("Pipeline stopped:", e)
        return True


expect("FailPipeline above 50%", fails(dq_alerts, metrics), True)
expect("FailPipeline above 70%", fails(alerting_engine(0.7), metrics), False)    # only the alert fires

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The same actions in YAML
# MAGIC `checks/actions.yml`, deployed with the bundle. For Slack, use `type: slack` with
# MAGIC `webhook_url: {secret: <scope>/<key>}` and keep the URL in a secret scope.

# COMMAND ----------

from databricks.labs.dqx.actions import DQActionManager

yaml_actions = DQActionManager.load_actions_from_local_file(f"{CHECKS}/actions.yml")
print(yaml_actions)
expect("FailPipeline with the YAML actions", fails(alerting_engine(None, actions=yaml_actions), metrics), True)
