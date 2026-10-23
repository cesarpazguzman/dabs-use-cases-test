from databricks.sdk import WorkspaceClient
from databricks.bundles.core import (
    Bundle, Resources, Variable, load_resources_from_current_package_module, variables,
)
from databricks.bundles.jobs import Job

METADATA_TABLE = "demo_dabs.metadata.ingestion_datasets"
ALERT_EMAIL = "data-team@example.com"


@variables
class Variables:
    warehouse_id: Variable[str]   # defined in databricks.yml


def read_metadata(warehouse_id: str) -> list[dict]:
    """Read the metadata table: one row per active dataset."""
    w = WorkspaceClient()   # same authentication as the CLI
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"SELECT dataset, source_path FROM {METADATA_TABLE} "
                  "WHERE active ORDER BY dataset",
        wait_timeout="30s",
    )
    columns = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in (resp.result.data_array or [])]


def create_job(ds: dict) -> Job:
    """Job template for one dataset. Complies with the platform rules."""
    return Job.from_dict({
        "name": "ingestion-" + ds["dataset"] + "-${bundle.target}",
        "email_notifications": {"on_failure": [ALERT_EMAIL]},
        "tasks": [{
            "task_key": "ingest",
            "notebook_task": {
                "notebook_path": "src/ingest_dataset.py",
                "base_parameters": {
                    "dataset": ds["dataset"],
                    "source_path": ds["source_path"],
                    "catalog": "${var.catalog}",
                    "schema": "${var.schema}",
                },
            },
        }],
    })


def load_resources(bundle: Bundle) -> Resources:
    """Called by the CLI on validate, plan and deploy."""
    resources = load_resources_from_current_package_module()
    warehouse_id = bundle.resolve_variable(Variables.warehouse_id)
    for ds in read_metadata(warehouse_id):
        resources.add_resource("ingest_" + ds["dataset"], create_job(ds))
    return resources
