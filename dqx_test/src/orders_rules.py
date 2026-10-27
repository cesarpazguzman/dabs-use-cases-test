"""Quality rules for bronze_orders written as Python classes (case 1 of the DQX lab, reused in case 5)."""
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule, DQForEachColRule

ORDER_CHECKS = [
    DQRowRule(name="customer_id_not_null", criticality="error",
              check_func=check_funcs.is_not_null, column="customer_id"),
    DQRowRule(name="amount_not_negative", criticality="error",
              check_func=check_funcs.is_not_less_than, column="amount",
              check_func_kwargs={"limit": 0}),
    DQRowRule(name="amount_reasonable", criticality="warn",     # try "error": 1008 leaves silver
              check_func=check_funcs.is_not_greater_than, column="amount",
              check_func_kwargs={"limit": 10000}),
    # Quoted values: since DQX 0.16 a bare string in is_in_list is a column name
    DQRowRule(name="status_valid", criticality="error",
              check_func=check_funcs.is_in_list, column="status",
              check_func_kwargs={"allowed": ["'NEW'", "'SHIPPED'", "'DELIVERED'"]}),
    DQRowRule(name="email_valid", criticality="warn",
              check_func=check_funcs.is_valid_email, column="email"),
    # current_date() is a SQL expression, evaluated on every run
    DQRowRule(name="order_date_not_in_future", criticality="error",
              check_func=check_funcs.is_not_greater_than, column="order_date",
              check_func_kwargs={"limit": "current_date()"}),
    *DQForEachColRule(columns=["order_id", "order_date"], criticality="error",
                      check_func=check_funcs.is_not_null).get_rules(),
    DQDatasetRule(name="order_id_unique", criticality="error",
                  check_func=check_funcs.is_unique, columns=["order_id"]),
]
