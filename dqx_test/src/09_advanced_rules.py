# Databricks notebook source
# MAGIC %md
# MAGIC # Case 9: advanced rules, reference data, SQL and a custom function
# MAGIC Four kinds of rule cover what the basic functions do not: referential integrity against another table, a SQL
# MAGIC condition per row, a SQL query over groups and a Python function of your own. First in YAML
# MAGIC (`checks/advanced_checks.yml`), then the same idea in code.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The custom function
# MAGIC It returns a condition that is **true when there is a problem**, built with `make_condition(condition, message, alias)`.

# COMMAND ----------

from pyspark.sql import Column
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.rule import register_rule


@register_rule("row")
def email_not_temporary(column: str, domain: str) -> Column:
    c = F.col(column)
    return make_condition(
        F.lower(c).endswith(domain),
        F.concat_ws("", F.lit("The email uses a temporary domain: "), c),
        f"{column}_temporary_domain",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. YAML version
# MAGIC - `sql_expression` describes what **must hold**; the row fails when the expression is false.
# MAGIC - `sql_query` returns a boolean column where true means **failure**; `{{ orders }}` is the input DataFrame and
# MAGIC   `merge_columns` joins the result back to each row.

# COMMAND ----------

from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig

advanced = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=f"{CHECKS}/advanced_checks.yml"))
custom = {"email_not_temporary": email_not_temporary}
status = DQEngine.validate_checks(advanced, custom_check_functions=custom)
print(status)
assert not status.has_errors, status

valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(
    spark.table(f"{BASE}.bronze_orders"),
    advanced,
    custom_check_functions=custom,
    ref_dfs={"customers": spark.table(f"{BASE}.ref_customers")},
)
display(quarantine_df.select("order_id", "customer_id", "_errors.name", "_warnings.name"))

# COMMAND ----------

# MAGIC %md
# MAGIC | order_id | Errors | Warnings |
# MAGIC | --- | --- | --- |
# MAGIC | 1001 (customer C008) | | `max_1_order_per_country_day` (two ES orders on 6 October) |
# MAGIC | 1002, 1004, 1007 | | `min_orders_per_country` (PT, FR and IT have one order) |
# MAGIC | 1008 | `shipped_amount_too_high` | |
# MAGIC | 1010 | `customer_exists` | `max_1_order_per_country_day`, `email_not_temporary` |
# MAGIC
# MAGIC Look at 1003: its customer_id is null and `foreign_key` does **not** flag it. Nulls are caught with
# MAGIC `is_not_null`, so combine both rules.

# COMMAND ----------

expect("quarantine (YAML)", order_ids(quarantine_df), [1001, 1002, 1004, 1007, 1008, 1010])
expect("valid rows (YAML)", valid_df.count(), 8)

rules_1010 = quarantine_df.where("order_id = 1010").first()
expect("errors of 1010", sorted(e.name for e in rules_1010._errors), ["customer_exists"])
expect("warnings of 1010", sorted(w.name for w in rules_1010._warnings),
       ["email_not_temporary", "max_1_order_per_country_day"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Code version
# MAGIC In code the custom function goes straight into `check_func`; no `custom_check_functions` mapping is needed.

# COMMAND ----------

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule

code_rules = [
    DQDatasetRule(name="customer_exists", criticality="error",
                  check_func=check_funcs.foreign_key, columns=["customer_id"],
                  check_func_kwargs={"ref_columns": ["customer_id"], "ref_df_name": "customers"}),
    DQRowRule(name="shipped_amount_too_high", criticality="error",
              check_func=check_funcs.sql_expression,
              check_func_kwargs={"expression": "NOT (status = 'SHIPPED' AND amount > 10000)",
                                 "msg": "Shipped order with an amount above 10000"}),
    DQRowRule(name="email_not_temporary", criticality="warn",
              check_func=email_not_temporary, column="email",
              check_func_kwargs={"domain": "@tempmail.com"}),
]

valid_df, quarantine_df = dq_engine.apply_checks_and_split(
    spark.table(f"{BASE}.bronze_orders"), code_rules,
    ref_dfs={"customers": spark.table(f"{BASE}.ref_customers")})

expect("quarantine (code)", order_ids(quarantine_df), [1008, 1010])
