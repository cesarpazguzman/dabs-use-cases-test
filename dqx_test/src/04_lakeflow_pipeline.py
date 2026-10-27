# Case 4: DQX inside a Lakeflow pipeline (source file of the dqx_test-lab-pipeline pipeline).
# DQX does not use Lakeflow expectations: it applies its own checks in a view and splits the rows
# with get_valid and get_invalid. The pipeline writes into the catalog and schema of the bundle target.
#
# The tables are materialized views (batch reads), not streaming tables: dataset rules such as is_unique use
# window functions, which Structured Streaming does not support (NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING).
# With only row rules, spark.readStream + dlt.read_stream works as well.
import dlt
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.checks_serializer import serialize_checks
from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DQRowRule

BRONZE = spark.conf.get("source_table")            # set in the pipeline configuration of the bundle

dq_engine = DQEngine(WorkspaceClient())

# Variant A: rules in YAML (the file of case 2, deployed with the bundle), with quarantine
yaml_checks = dq_engine.load_checks(
    config=WorkspaceFileChecksStorageConfig(location=spark.conf.get("checks_path")))


@dlt.view
def orders_dq():
    return dq_engine.apply_checks_by_metadata(spark.read.table(BRONZE), yaml_checks)


@dlt.table(name="pl_silver_orders")
def pl_silver_orders():
    # Rows without errors, without the _errors / _warnings columns
    return dq_engine.get_valid(dlt.read("orders_dq"))


@dlt.table(name="pl_quarantine_orders")
def pl_quarantine_orders():
    # Rows with errors or warnings, with the _errors / _warnings columns
    return dq_engine.get_invalid(dlt.read("orders_dq"))


# Variant B: rules in code, annotate only (consumers filter on the quality columns)
code_checks = [
    DQRowRule(name="customer_id_not_null", criticality="error",
              check_func=check_funcs.is_not_null, column="customer_id"),
    DQRowRule(name="amount_not_negative", criticality="error",
              check_func=check_funcs.is_not_less_than, column="amount",
              check_func_kwargs={"limit": 0}),
]


@dlt.table(name="pl_silver_orders_annotated")
def pl_silver_orders_annotated():
    return dq_engine.apply_checks(spark.read.table(BRONZE), code_checks)


# Case 10, part 4: the observer collects no metrics in a pipeline, because the pipeline owns the writes.
# A materialized view aggregates them instead. compute_summary_metrics expects metadata rules.
dq_engine_metrics = DQEngine(WorkspaceClient(), observer=DQMetricsObserver(name="pipeline_orders"))


@dlt.table(name="pl_quality_metrics")
def pl_quality_metrics():
    return dq_engine_metrics.compute_summary_metrics(
        dlt.read("pl_silver_orders_annotated"), checks=serialize_checks(code_checks))
