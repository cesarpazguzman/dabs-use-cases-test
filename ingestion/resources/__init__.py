import json
import re
from pathlib import Path

import yaml
from databricks.bundles.core import Bundle, Resources, load_resources_from_current_package_module
from databricks.bundles.jobs import Job

DATASETS_FILE = Path(__file__).parent.parent / "datasets.yml"
LANDING = "/Volumes/${var.catalog}/landing/files"   # landing volume, created by the platform

FILE_FIELDS = {"owner", "alerts", "datasets"}
DATASET_FIELDS = {"name", "source", "format", "options", "mode", "schedule", "enabled"}
FORMATS = {"csv", "json", "parquet"}
MODES = {"overwrite", "append"}
DEFAULT_OPTIONS = {"csv": {"header": "true", "inferSchema": "true"}}


class UniqueKeyLoader(yaml.SafeLoader):
    """YAML loader that rejects repeated keys; by default PyYAML silently keeps the last one."""

    def construct_mapping(self, node, deep=False):
        keys = [self.construct_object(k, deep=deep) for k, _ in node.value]
        repeated = sorted({k for k in keys if keys.count(k) > 1})
        if repeated:
            raise ValueError(f"{DATASETS_FILE.name}: repeated key(s) {repeated} (line {node.start_mark.line + 1})")
        return super().construct_mapping(node, deep=deep)


def read_datasets(path: Path = DATASETS_FILE) -> dict:
    """Read and check the datasets file. Any mistake fails validate, so the PR cannot be merged."""
    spec = yaml.load(path.read_text(), Loader=UniqueKeyLoader) or {}
    errors = [f"unknown field '{k}'" for k in sorted(set(spec) - FILE_FIELDS)]
    if not (spec.get("alerts") or {}).get("on_failure"):
        errors.append("alerts.on_failure is required")

    names = set()
    for i, ds in enumerate(spec.get("datasets") or []):
        where = f"datasets[{i}] ({ds.get('name', '?')})"
        errors += [f"{where}: unknown field '{k}'" for k in sorted(set(ds) - DATASET_FIELDS)]
        if not re.fullmatch(r"[a-z][a-z0-9_]*", str(ds.get("name", ""))):
            errors.append(f"{where}: name must be lowercase letters, digits and _")
        elif ds["name"] in names:
            errors.append(f"{where}: duplicated name")
        names.add(ds.get("name"))
        if not ds.get("source"):
            errors.append(f"{where}: source is required")
        if ds.get("format", "csv") not in FORMATS:
            errors.append(f"{where}: format must be one of {sorted(FORMATS)}")
        if ds.get("mode", "overwrite") not in MODES:
            errors.append(f"{where}: mode must be one of {sorted(MODES)}")

    if errors:
        raise ValueError(f"{path.name} is not valid:\n  " + "\n  ".join(errors))
    return spec


def create_job(ds: dict, spec: dict, target: str) -> Job:
    """Job template for one dataset. Complies with the platform rules."""
    fmt = ds.get("format", "csv")
    job = {
        "name": "ingestion-" + ds["name"] + "-${bundle.target}",
        "email_notifications": {"on_failure": spec["alerts"]["on_failure"]},
        "tasks": [{
            "task_key": "ingest",
            "notebook_task": {
                "notebook_path": "src/ingest_dataset.py",
                "base_parameters": {
                    "dataset": ds["name"],
                    "source_path": f"{LANDING}/{ds['source']}",
                    "format": fmt,
                    "options": json.dumps({**DEFAULT_OPTIONS.get(fmt, {}), **ds.get("options", {})}),
                    "mode": ds.get("mode", "overwrite"),
                    "catalog": "${var.catalog}",
                    "schema": "${var.schema}",
                },
            },
        }],
    }
    if spec.get("owner"):
        job["tags"] = {"owner": spec["owner"]}
    if ds.get("schedule"):
        job["schedule"] = {
            "quartz_cron_expression": ds["schedule"],
            "timezone_id": "Europe/Madrid",
            "pause_status": "UNPAUSED" if target == "prd" else "PAUSED",   # only prd runs on a schedule
        }
    return Job.from_dict(job)


def load_resources(bundle: Bundle) -> Resources:
    """Called by the CLI on validate, plan and deploy."""
    resources = load_resources_from_current_package_module()
    spec = read_datasets()
    for ds in spec.get("datasets") or []:
        if ds.get("enabled", True):
            resources.add_resource("ingest_" + ds["name"], create_job(ds, spec, bundle.target))
    return resources
