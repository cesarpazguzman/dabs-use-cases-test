# Databricks notebook source
# MAGIC %md
# MAGIC # Case 0: sample data
# MAGIC A bronze table with 10 orders, 8 of them with defects planted on purpose, and a customer reference table.
# MAGIC The schema and the `files` volume are created by the platform bundle.
# MAGIC
# MAGIC | order_id | Defect | Rule that catches it | Criticality |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1001 (x2) | Duplicated order_id | `order_id_unique` | error |
# MAGIC | 1002 | None | | |
# MAGIC | 1003 | Null customer_id | `customer_id_not_null` | error |
# MAGIC | 1004 | Negative amount | `amount_not_negative` | error |
# MAGIC | 1005 | Status LOST not allowed | `status_valid` | error |
# MAGIC | 1006 | Malformed email | `email_valid` | warn |
# MAGIC | 1007 | Date in the future (2030) | `order_date_not_in_future` | error |
# MAGIC | 1008 | Very high amount (15,000) | `amount_reasonable` | warn |
# MAGIC | 1010 | Customer C999 does not exist, temporary email | `customer_exists`, `email_not_temporary` (case 9) | error / warn |

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import datetime as dt

d = dt.date
orders = spark.createDataFrame([
    (1001, "C001", d(2026, 10, 1),   120.50, "NEW",     "ana@mail.com",         "ES"),
    (1002, "C002", d(2026, 10, 2),    80.00, "SHIPPED", "luis@mail.com",        "PT"),
    (1003, None,   d(2026, 10, 2),    45.00, "NEW",     "no.customer@mail.com", "ES"),
    (1004, "C003", d(2026, 10, 3),   -10.00, "NEW",     "marta@mail.com",       "FR"),
    (1005, "C004", d(2026, 10, 3),    60.00, "LOST",    "pedro@mail.com",       "ES"),
    (1006, "C005", d(2026, 10, 4),    30.00, "NEW",     "bad-email",            "ES"),
    (1007, "C006", d(2030, 1, 1),     99.00, "NEW",     "eva@mail.com",         "IT"),
    (1008, "C007", d(2026, 10, 5), 15000.00, "SHIPPED", "raul@mail.com",        "ES"),
    (1001, "C008", d(2026, 10, 6),    20.00, "NEW",     "sara@mail.com",        "ES"),
    (1010, "C999", d(2026, 10, 6),    55.00, "NEW",     "ghost@tempmail.com",   "ES"),
], "order_id INT, customer_id STRING, order_date DATE, amount DOUBLE, status STRING, email STRING, country STRING")

customers = spark.createDataFrame([(f"C00{i}", f"Customer {i}") for i in range(1, 9)],
                                  "customer_id STRING, name STRING")

orders.write.mode("overwrite").saveAsTable(f"{BASE}.bronze_orders")
customers.write.mode("overwrite").saveAsTable(f"{BASE}.ref_customers")
display(spark.table(f"{BASE}.bronze_orders"))

# COMMAND ----------

expect("bronze_orders rows", spark.table(f"{BASE}.bronze_orders").count(), 10)
expect("ref_customers rows", spark.table(f"{BASE}.ref_customers").count(), 8)
