# Databricks notebook source
# MAGIC %md
# MAGIC # Case 8: profiler, generate rules, review them and apply them
# MAGIC Let DQX propose rules from the data, review them and apply them. The main lesson: the profiler learns from
# MAGIC the data you give it, **bad rows included**.
# MAGIC
# MAGIC Good practice: profile in development with representative, **clean** data, review the rules and promote them
# MAGIC (here, to `checks/` in Git). Do not run the profiler in production on a schedule.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import yaml
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator

ws = WorkspaceClient()
bronze = spark.table(f"{BASE}.bronze_orders")

# No sampling: the table is small (by default it uses 30% and at most 1,000 rows)
summary_stats, profiles = DQProfiler(ws).profile(bronze, options={"sample_fraction": None, "limit": None})

generated = DQGenerator(ws).generate_dq_rules(profiles)     # criticality "error"
print(yaml.safe_dump(generated, allow_unicode=True, sort_keys=False))
print(f"{len(generated)} rules generated (the lab expects 10)")

# COMMAND ----------

# MAGIC %md
# MAGIC | Proposed rule | What it says | Is it good? |
# MAGIC | --- | --- | --- |
# MAGIC | `is_not_null` on order_id, order_date, amount | Never null | Yes |
# MAGIC | `is_not_empty` on customer_id | Not empty (but accepts nulls) | Incomplete: there is a null, it should be `is_not_null` |
# MAGIC | `is_in_range` on amount from -10 to 15,000 | Observed range | **No**: it includes the negative and the excessive amount |
# MAGIC | `is_in_range` on order_date up to 2030-01-01 | Observed range | **No**: it accepts the future date |
# MAGIC | `is_in_range` on order_id from 1001 to 1010 | Observed range | It makes no business sense |
# MAGIC
# MAGIC The profiler does not know the business: it does not propose uniqueness (except with AI key detection) and
# MAGIC does not know that LOST is not a valid status.

# COMMAND ----------

from databricks.labs.dqx.config import VolumeFileChecksStorageConfig

GENERATED = f"{VOL}/checks/orders_checks_generated.yml"
dq_engine.save_checks(generated, config=VolumeFileChecksStorageConfig(location=GENERATED))
print("Generated rules saved in", GENERATED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC In the lab you download the file from Catalog → Volumes, edit it and upload it. Here the four corrections are
# MAGIC made in code, and the result is saved next to it as `orders_checks_reviewed.yml`:
# MAGIC 1. Range of `amount`: `min_limit: 0` and `max_limit: 10000`.
# MAGIC 2. `max_limit` of `order_date`: `current_date()`.
# MAGIC 3. Remove the range rule of `order_id` and add `is_unique`.
# MAGIC 4. Replace `is_not_empty` on `customer_id` with `is_not_null`.

# COMMAND ----------

def target(rule):
    check = rule["check"]
    return check["function"], check.get("arguments", {}).get("column")


reviewed = []
for rule in dq_engine.load_checks(config=VolumeFileChecksStorageConfig(location=GENERATED)):
    function, column = target(rule)
    args = rule["check"].setdefault("arguments", {})
    if (function, column) == ("is_in_range", "amount"):
        args.update(min_limit=0, max_limit=10000)
    elif (function, column) == ("is_in_range", "order_date"):
        args["max_limit"] = "current_date()"
    elif (function, column) == ("is_in_range", "order_id"):
        continue
    elif (function, column) == ("is_not_empty", "customer_id"):
        rule["check"] = {"function": "is_not_null", "arguments": {"column": "customer_id"}}  # no trim_strings
        rule["name"] = "customer_id_is_null"
    reviewed.append(rule)
reviewed.append({"name": "order_id_unique", "criticality": "error",
                 "check": {"function": "is_unique", "arguments": {"columns": ["order_id"]}}})

REVIEWED = f"{VOL}/checks/orders_checks_reviewed.yml"
dq_engine.save_checks(reviewed, config=VolumeFileChecksStorageConfig(location=REVIEWED))
print(yaml.safe_dump(reviewed, allow_unicode=True, sort_keys=False))

# COMMAND ----------

reviewed = dq_engine.load_checks(config=VolumeFileChecksStorageConfig(location=REVIEWED))
status = DQEngine.validate_checks(reviewed)
assert not status.has_errors, status        # e.g. an argument left over from the generated rule
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(bronze, reviewed)
display(quarantine_df.select("order_id", "_errors.name", "_warnings.name"))

# The review catches what the generated rules let through: duplicates, null customer, negative and too high
# amounts, future date. LOST (1005) is still accepted unless the generated status rule rejects it.
caught = set(order_ids(quarantine_df))
expect("defects caught after the review", {1001, 1003, 1004, 1007, 1008} <= caught, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra: Lakeflow expectations
# MAGIC If the team uses native expectations, the same profile generates them.

# COMMAND ----------

from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator

for e in DQDltGenerator(ws).generate_dlt_rules(profiles, language="SQL"):
    print(e)          # CONSTRAINT ... EXPECT (...)
