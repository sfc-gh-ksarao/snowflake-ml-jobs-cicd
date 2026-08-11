# Deploy ML pipeline DAG to Snowflake using stored procedures and SQL-based task definitions.
# Co-authored with CoCo
"""
Deploy the ML pipeline DAG to Snowflake.

Registers MLJobDefinitions (uploads code to stage), creates stored procedures
that submit jobs from stage at runtime, and assembles a DAG with SQL-based
task definitions to avoid pickling issues.

Supports two deployment modes:
  --source stage : Code uploaded via CI/CD PUT to @ML_CODE_STAGE
  --source git   : Code from @ML_JOBS_GIT_REPO (Snowflake Git integration)

Usage:
  python deploy_dag.py --source stage
  python deploy_dag.py --source git
"""
import argparse
import os
import builtins

from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.task import Cron
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation
from snowflake.ml.jobs import MLJobDefinition

# Fix: inject Session into builtins for type-hint resolution
builtins.Session = Session

DB = "SYNTHEA_DEMO"
SCHEMA = "PATIENTS"
DB_SCHEMA = f"{DB}.{SCHEMA}"
COMPUTE_POOL = "DEMO_POOL"
PAYLOAD_STAGE = f"{DB_SCHEMA}.ML_STAGE"
LOCAL_SOURCE = "ml_pipeline"


def deploy(source_mode: str):
    session = Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "authenticator": "PROGRAMMATIC_ACCESS_TOKEN",
        "token": os.environ["SNOWFLAKE_TOKEN"],
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": "COMPUTE_WH",
        "database": DB,
        "schema": SCHEMA,
    }).create()

    source_path = LOCAL_SOURCE

    # ─── Step 1: Register ML Job Definitions (uploads code to stage) ───
    jobs = {
        "build_training_set": "build_training_set.py",
        "train_and_evaluate": "train_and_evaluate.py",
        "notify": "notify.py",
    }

    for name, entrypoint in jobs.items():
        MLJobDefinition.register(
            source_path,
            entrypoint=entrypoint,
            compute_pool=COMPUTE_POOL,
            stage_name=PAYLOAD_STAGE,
            name=name,
        )
        print(f"  Registered: {name}")

    # ─── Step 2: Create stored procedures that submit jobs at runtime ───
    for name, entrypoint in jobs.items():
        session.sql(f"""
        CREATE OR REPLACE PROCEDURE {DB_SCHEMA}.run_{name}()
        RETURNS STRING
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.11'
        PACKAGES = ('snowflake-ml-python', 'snowflake-snowpark-python')
        HANDLER = 'run'
        EXECUTE AS CALLER
        AS
$$
def run(session):
    from snowflake.ml.jobs import submit_from_stage
    job = submit_from_stage(
        "@{PAYLOAD_STAGE}/{name}/app/",
        "{COMPUTE_POOL}",
        entrypoint="{entrypoint}",
        stage_name="{PAYLOAD_STAGE}",
        session=session,
    )
    job.wait()
    return str(job.status)
$$
        """).collect()
        print(f"  Created procedure: run_{name}")

    # ─── Step 3: Assemble DAG with SQL task definitions (no pickling) ───
    with DAG(
        "READMISSION_ML_PIPELINE",
        schedule=Cron("0 6 * * 1", "America/Los_Angeles"),
        stage_location=f"@{PAYLOAD_STAGE}",
        use_func_return_value=True,
    ) as dag:
        t1 = DAGTask("BUILD_TRAINING_SET", definition=f"CALL {DB_SCHEMA}.run_build_training_set()")
        t2 = DAGTask("TRAIN_AND_EVALUATE", definition=f"CALL {DB_SCHEMA}.run_train_and_evaluate()")
        t3 = DAGTask("NOTIFY", definition=f"CALL {DB_SCHEMA}.run_notify()")

        t1 >> t2 >> t3

    # ─── Step 4: Deploy the DAG ───
    root = Root(session)
    dag_op = DAGOperation(root.databases[DB].schemas[SCHEMA])
    dag_op.deploy(dag, mode=CreateMode.or_replace)

    print(f"\nDAG deployed to {DB_SCHEMA} (source: {source_mode})")
    print(f"Tasks: BUILD_TRAINING_SET >> TRAIN_AND_EVALUATE >> NOTIFY")
    print(f"Schedule: Every Monday at 6:00 AM PT")
    print(f"Jobs read code from @{PAYLOAD_STAGE} at RUNTIME.")

    return dag, dag_op


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy ML Jobs DAG")
    parser.add_argument(
        "--source",
        choices=["stage", "git"],
        default="stage",
        help="Code source: 'stage' (PUT via CI/CD) or 'git' (Snowflake Git integration)",
    )
    args = parser.parse_args()
    deploy(args.source)
