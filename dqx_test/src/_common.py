# Databricks notebook source
# Shared setup for the DQX lab notebooks. Each notebook installs DQX and then runs this one with %run.
import os

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

dbutils.widgets.text("catalog", "demo_dabs")
dbutils.widgets.text("schema", "dqx_test_dev")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BASE = f"{CATALOG}.{SCHEMA}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/files"        # created by the platform
CHECKS = os.path.abspath("../checks")             # rule files deployed with the bundle (workspace files)

dq_engine = DQEngine(WorkspaceClient())


def expect(what, actual, expected):
    """Compare a result with the one the lab expects, so the job fails if DQX behaves differently."""
    assert actual == expected, f"{what}: expected {expected}, got {actual}"
    print(f"OK  {what}: {actual}")


def order_ids(df):
    return sorted(r.order_id for r in df.select("order_id").collect())
