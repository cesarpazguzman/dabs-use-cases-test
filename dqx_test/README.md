# dqx_test

A use case that runs the DQX lab ([Databricks Labs DQX](https://databrickslabs.github.io/dqx/) 0.17.0) as a bundle.
Ten cases on one orders dataset with planted defects: in-transit and at-rest validation, rules in YAML and in Python.
Every notebook compares its results with the ones the lab expects, so a green job run means DQX behaves as described.

Sample data only (10 synthetic orders and 8 customers). No real data.

## Layout

```
dqx_test/
├── databricks.yml                     targets dev / qa / prd, like the other use cases
├── checks/                            rules in Git, deployed to the workspace with the bundle
│   ├── orders_checks.yml              cases 2, 3, 4, 6 and 10
│   ├── customers_checks.yml           case 6
│   ├── advanced_checks.yml            case 9
│   └── actions.yml                    case 10
├── resources/
│   ├── dqx_test.job.yml               dqx_test-lab-<target>: every case, in two waves
│   └── dqx_test.pipeline.yml          dqx_test-lab-pipeline-<target>: case 4
└── src/
    ├── _common.py                     widgets, paths, DQEngine and expect(), run with %run
    ├── orders_rules.py                rules as Python classes (cases 1 and 5)
    ├── 00_setup.py … 10_metrics_alerts.py
    ├── 10b_end_to_end_actions.py      case 10, part 2, in a task of its own
    ├── 04_lakeflow_pipeline.py        pipeline source
    └── 04_check_pipeline.py           checks the pipeline tables
```

The schema `dqx_test_<env>` and its volume `files` are created by the platform bundle (platform rule 1).
The volume holds a copy of the rules for case 7, the profiler output of case 8 and the checkpoints of case 3.

## Cases

| Case | Mode | Rules in | Runs as | What it shows |
| --- | --- | --- | --- | --- |
| 0 | | | Notebook | Sample tables `bronze_orders` and `ref_customers` |
| 1 | In-transit | Code | Notebook | `DQRowRule`, `DQDatasetRule`, split into silver + quarantine |
| 2 | In-transit | YAML | Notebook | Rules in a file, `load_checks`, validation |
| 3 | In-transit | YAML | Streaming (`availableNow`) | End-to-end method with checkpoints |
| 4 | In-transit | Code + YAML | Lakeflow pipeline | `get_valid` / `get_invalid` in pipeline tables |
| 5 | At-rest | Code | Notebook | Audit a written table without moving it |
| 6 | At-rest | YAML in a Delta table | Notebook | Versioned rules in a table, several tables in one call |
| 7 | At-rest | YAML | DQX workflows (no code) | `config.yml` and `databricks labs dqx` commands, see below |
| 8 | Both | Generated YAML | Notebook | Profiler: propose rules and review them |
| 9 | Both | YAML + code | Notebook | `foreign_key`, `sql_expression`, `sql_query`, custom function |
| 10 | Both | YAML + code | Notebook | Summary metrics, alerts and `FailPipeline` |

Differences with the lab document:

- The rules of cases 2, 6, 9 and 10 are files in `checks/`, not strings written from a notebook: a rule change is
  a pull request that goes through qa before prd.
- Case 3 appends its two new orders to a copy (`stream_source_orders`), so the other cases keep the 10 original rows.
- Case 8 makes the four review corrections in code and saves the result as `orders_checks_reviewed.yml`.
- Case 10 catches `PipelineFailedError` to check that it is raised. In a real job, let it fail. Its part 2
  (end-to-end run with metrics and actions) is a task of its own, `10b_end_to_end_actions.py`.
- Names are in English: `quarantine_orders` instead of `cuarentena_pedidos`, `customer_id_not_null` instead of
  `customer_id_no_nulo`, and so on.

What the lab document gets wrong or does not say, found by running it (DQX 0.17.0, serverless):

- Case 2: `validate_checks(..., semantic_validation_mode="fail")` raises a `ValueError` for a duplicated rule;
  it does not return a status with `has_errors`.
- Cases 3 and 4: dataset rules such as `is_unique` use a window function, and streaming DataFrames reject it
  (`NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING`). The stream of case 3 uses only the row rules, so the duplicated
  1001 reaches silver (6 rows in silver and 6 in quarantine, not 4 and 8). The pipeline of case 4 uses
  materialized views (batch reads) to keep every rule.
- Case 6: saving a rule set whose fingerprint is already in the table writes nothing, even in `overwrite` mode, so
  an older version can stay the latest one. The notebook drops `dq_rules` at the start of each run.
- Case 8: the generated `is_not_empty` rule has a `trim_strings` argument; turned into `is_not_null` it must be
  removed, or validation fails.
- Case 10, part 2 (**known issue**): run as a task of the bundle job, the end-to-end method gets no observed
  metrics (`available: []`), so it saves no metrics and fires no action, with no error. The same notebook run on
  its own (`databricks jobs submit`) works. The task reports the issue instead of failing; the cause is not known.

## Run it in dev

```bash
cd dqx_test
databricks bundle deploy                 # target dev, a personal copy
databricks bundle run dqx_test_lab       # every case; about 10 minutes on serverless
```

Each notebook can also be opened and run by hand from the bundle folder in the workspace
(`.bundle/dqx_test/dev/files/src`). The widgets default to `demo_dabs.dqx_test_dev`.

qa and prd are deployed by CI, like the other use cases. The job has no trigger: run it by hand.

## Case 7: DQX workflows, no code

DQX can also be installed as a tool that creates its own jobs (profiler, quality-checker, e2e). It is installed
per user from a laptop, not with the bundle, so it is not part of the job. It needs Databricks CLI v0.241+ and
Python 3.10–3.12.

```bash
databricks labs install dqx@v0.17.0      # default folder, serverless: yes, any input/output (changed below)
databricks labs dqx open-remote-config
```

`run_configs` section of the config (case 2 has already copied the rules to the volume):

```yaml
serverless_clusters: true
run_configs:
  - name: orders
    input_config:
      location: demo_dabs.dqx_test_dev.bronze_orders
      format: delta
    output_config:
      location: demo_dabs.dqx_test_dev.wf_silver_orders
      format: delta
      mode: overwrite
    quarantine_config:
      location: demo_dabs.dqx_test_dev.wf_quarantine_orders
      format: delta
      mode: overwrite
    metrics_config:
      location: demo_dabs.dqx_test_dev.dq_metrics
      format: delta
      mode: append
    checks_location: /Volumes/demo_dabs/dqx_test_dev/files/checks/orders_checks.yml

  - name: orders_profile
    input_config:
      location: demo_dabs.dqx_test_dev.bronze_orders
      format: delta
    checks_location: /Volumes/demo_dabs/dqx_test_dev/files/checks/orders_checks_profiled.yml   # not your reviewed rules
    profiler_config:
      sample_fraction: 1.0
      limit: 1000
      criticality: warn
```

```bash
databricks labs dqx validate-checks --run-config "orders"
databricks labs dqx apply-checks --run-config "orders"       # wf_silver_orders: 4 rows, wf_quarantine_orders: 8
databricks labs dqx logs --workflow quality-checker
databricks labs dqx profile --run-config "orders_profile"
databricks labs dqx workflows
databricks labs uninstall dqx                                 # when done
```

## Looking at the results

```sql
-- Rows failed by each rule
SELECT r.name AS rule, count(*) AS rows
FROM (
  SELECT explode(_errors) AS r FROM demo_dabs.dqx_test_dev.quarantine_orders WHERE _errors IS NOT NULL
  UNION ALL
  SELECT explode(_warnings) AS r FROM demo_dabs.dqx_test_dev.quarantine_orders WHERE _warnings IS NOT NULL
)
GROUP BY r.name
ORDER BY rows DESC;

-- Row by row, with the reason
SELECT order_id, e.name AS rule, e.message AS reason
FROM demo_dabs.dqx_test_dev.quarantine_orders
LATERAL VIEW explode(_errors) t AS e
ORDER BY order_id;

-- Metrics over time
SELECT run_time, run_name,
       max(CASE WHEN metric_name = 'input_row_count'   THEN CAST(metric_value AS INT) END) AS rows,
       max(CASE WHEN metric_name = 'error_row_count'   THEN CAST(metric_value AS INT) END) AS with_error,
       max(CASE WHEN metric_name = 'warning_row_count' THEN CAST(metric_value AS INT) END) AS with_warning
FROM demo_dabs.dqx_test_dev.dq_metrics
GROUP BY run_time, run_name
ORDER BY run_time DESC;

-- Breakdown per rule
SELECT run_id, c.check_name, c.error_count, c.warning_count, c.checked_count
FROM demo_dabs.dqx_test_dev.dq_metrics
LATERAL VIEW explode(from_json(metric_value,
  'array<struct<check_name:string,rule_fingerprint:string,error_count:bigint,warning_count:bigint,checked_count:bigint>>')) t AS c
WHERE metric_name = 'check_metrics';
```
