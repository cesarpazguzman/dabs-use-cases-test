# Databricks notebook source
# MAGIC %md
# MAGIC # Case 6: at-rest, rules in a Delta table, several tables at once
# MAGIC Keep the declarative rules in a central Delta table (the option recommended for production), one row per
# MAGIC rule and versioned automatically, and validate **two tables in one call**.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Save the rules of each table
# MAGIC `run_config_name` says which table a rule belongs to. The convention is the full name of the input table.

# COMMAND ----------

from databricks.labs.dqx.config import TableChecksStorageConfig, WorkspaceFileChecksStorageConfig

RULES = f"{BASE}.dq_rules"
# Start from scratch: saving rules whose fingerprint is already in the table writes nothing, so the version
# saved in step 3 of a previous run would stay the latest one
spark.sql(f"DROP TABLE IF EXISTS {RULES}")

order_rules = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=f"{CHECKS}/orders_checks.yml"))
dq_engine.save_checks(order_rules, config=TableChecksStorageConfig(
    location=RULES, run_config_name=f"{BASE}.bronze_orders", mode="overwrite"))

customer_rules = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=f"{CHECKS}/customers_checks.yml"))
dq_engine.save_checks(customer_rules, config=TableChecksStorageConfig(
    location=RULES, run_config_name=f"{BASE}.ref_customers", mode="overwrite"))

display(spark.table(RULES).select("run_config_name", "name", "criticality", "check", "created_at", "rule_set_fingerprint"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate both tables

# COMMAND ----------

from databricks.labs.dqx.config import RunConfig, InputConfig, OutputConfig


def run_config(table):
    return RunConfig(
        name=f"{BASE}.{table}",                       # must match the run_config_name of the rules
        input_config=InputConfig(location=f"{BASE}.{table}"),
        output_config=OutputConfig(location=f"{BASE}.{table}_ok", mode="overwrite"),
        quarantine_config=OutputConfig(location=f"{BASE}.{table}_quarantine", mode="overwrite"),
        checks_location=RULES,
    )


dq_engine.apply_checks_and_save_in_tables(run_configs=[run_config("bronze_orders"), run_config("ref_customers")])

expect("bronze_orders_ok", spark.table(f"{BASE}.bronze_orders_ok").count(), 4)
expect("bronze_orders_quarantine", spark.table(f"{BASE}.bronze_orders_quarantine").count(), 8)
expect("ref_customers_ok", spark.table(f"{BASE}.ref_customers_ok").count(), 8)
expect("ref_customers_quarantine", spark.table(f"{BASE}.ref_customers_quarantine").count()
       if spark.catalog.tableExists(f"{BASE}.ref_customers_quarantine") else 0, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Version a rule change
# MAGIC Raise the threshold of `amount_reasonable` to 20,000 and save in `append` mode.

# COMMAND ----------

for r in order_rules:
    if r.get("name") == "amount_reasonable":
        r["check"]["arguments"]["limit"] = 20000

dq_engine.save_checks(order_rules, config=TableChecksStorageConfig(
    location=RULES, run_config_name=f"{BASE}.bronze_orders", mode="append"))

versions = (spark.table(RULES).where(f"run_config_name = '{BASE}.bronze_orders'")
            .select("rule_set_fingerprint").distinct().count())
expect("rule set versions of bronze_orders", versions, 2)

# load_checks returns the latest version; pass rule_set_fingerprint= to pick a specific one
latest = dq_engine.load_checks(config=TableChecksStorageConfig(location=RULES, run_config_name=f"{BASE}.bronze_orders"))
expect("latest amount_reasonable limit",
       next(r for r in latest if r.get("name") == "amount_reasonable")["check"]["arguments"]["limit"], 20000)

# Saving exactly the same rules again writes nothing: the fingerprint already exists
before = spark.table(RULES).count()
dq_engine.save_checks(order_rules, config=TableChecksStorageConfig(
    location=RULES, run_config_name=f"{BASE}.bronze_orders", mode="append"))
expect("rows after saving the same rules again", spark.table(RULES).count(), before)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variant: by patterns
# MAGIC Validate every table of the schema that has rules in `dq_rules`. The outputs get the suffixes `_dq_output`
# MAGIC and `_dq_quarantine`, and those tables are excluded automatically on the next run.

# COMMAND ----------

for t in spark.catalog.listTables(f"{BASE}"):
    if t.name.endswith(("_dq_output", "_dq_quarantine")):
        spark.sql(f"DROP TABLE {BASE}.{t.name}")              # start from scratch on every run

dq_engine.apply_checks_and_save_in_tables_for_patterns(
    patterns=[f"{BASE}.bronze_*", f"{BASE}.ref_*"],
    exclude_patterns=["*_ok", "*_quarantine"],                # do not validate the outputs of step 2
    checks_location=RULES,
)

for t in ["bronze_orders", "ref_customers"]:
    for suffix in ["_dq_output", "_dq_quarantine"]:
        name = f"{BASE}.{t}{suffix}"
        print(name, spark.table(name).count() if spark.catalog.tableExists(name) else "(not created)")
