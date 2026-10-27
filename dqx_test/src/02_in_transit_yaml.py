# Databricks notebook source
# MAGIC %md
# MAGIC # Case 2: in-transit, rules in YAML
# MAGIC The rules of case 1 in `checks/orders_checks.yml`. The file lives in Git and the bundle deploys it to the
# MAGIC workspace, so business or a data steward can change a threshold with a pull request, without touching
# MAGIC the pipeline code. Declarative rules are applied with the `*_by_metadata*` methods.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig, VolumeFileChecksStorageConfig, OutputConfig

checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=f"{CHECKS}/orders_checks.yml"))

status = DQEngine.validate_checks(checks)
assert not status.has_errors, status

# A copy in the volume, for the DQX workflows of case 7 (they read rules from a volume or a table)
dq_engine.save_checks(checks, config=VolumeFileChecksStorageConfig(location=f"{VOL}/checks/orders_checks.yml"))

# COMMAND ----------

bronze = spark.table(f"{BASE}.bronze_orders")
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(bronze, checks)

dq_engine.save_results_in_table(
    output_df=valid_df,
    quarantine_df=quarantine_df,
    output_config=OutputConfig(location=f"{BASE}.silver_orders_yaml", mode="overwrite"),
    quarantine_config=OutputConfig(location=f"{BASE}.quarantine_orders_yaml", mode="overwrite"),
)

# COMMAND ----------

expect("silver_orders_yaml", order_ids(spark.table(f"{BASE}.silver_orders_yaml")), [1002, 1006, 1008, 1010])
expect("quarantine_orders_yaml", order_ids(spark.table(f"{BASE}.quarantine_orders_yaml")),
       [1001, 1001, 1003, 1004, 1005, 1006, 1007, 1008])

# The warning of 1006 carries the user_metadata of the rule
warning_1006 = (spark.table(f"{BASE}.quarantine_orders_yaml").where("order_id = 1006")
                .select(F.explode("_warnings").alias("w")).where("w.name = 'email_valid'").first().w)
expect("user_metadata of email_valid", dict(warning_1006.user_metadata), {"owner": "crm-team"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercises
# MAGIC 1. A typo in a function name is caught by `validate_checks`, before touching any data.

# COMMAND ----------

import copy

broken = copy.deepcopy(checks)
broken[0]["check"]["function"] = "is_not_nul"
status = DQEngine.validate_checks(broken)
print(status)
expect("typo detected", status.has_errors, True)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. A duplicated rule is caught by the semantic validation. In `fail` mode it raises a `ValueError`
# MAGIC    (in the default `warn` mode it only logs a warning).

# COMMAND ----------

duplicated = checks + [copy.deepcopy(checks[0])]
try:
    DQEngine.validate_checks(duplicated, semantic_validation_mode="fail")
    detected = False
except ValueError as e:
    print(e)
    detected = True
expect("duplicated rule detected", detected, True)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. With `filter: country = 'ES'`, `email_valid` is only evaluated on Spanish orders.

# COMMAND ----------

filtered = copy.deepcopy(checks)
for c in filtered:
    if c.get("name") == "email_valid":
        c["filter"] = "country = 'ES'"
annotated = dq_engine.apply_checks_by_metadata(bronze, filtered)
display(annotated.select("order_id", "country", "email", "_warnings.name"))
