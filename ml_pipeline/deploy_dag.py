"""
Deploy the ML pipeline DAG to Snowflake.

Uses MLJobDefinition.register() so that DAG tasks reference code on a stage
(or Git repo) by path — not by serialized copy. This means CI/CD pushes to
the stage are automatically picked up on the next DAG run without redeployment.

Supports two deployment modes:
  --source stage  : DAG tasks read code from @ML_CODE_STAGE (uploaded via CI/CD PUT)
  --source git    : DAG tasks read code from @ML_JOBS_GIT_REPO (Snowflake Git integration)

Usage:
  python deploy_dag.py --source stage
  python deploy_dag.py --source git
"""
import argparse

from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.task import Cron
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation
from snowflake.ml.jobs import MLJobDefinition


DB = "SYNTHEA_DEMO"
SCHEMA = "PATIENTS"
DB_SCHEMA = f"{DB}.{SCHEMA}"
COMPUTE_POOL = "DEMO_POOL"
PAYLOAD_STAGE = f"{DB_SCHEMA}.PAYLOAD_STAGE"

# Stage paths for each deployment mode
STAGE_SOURCE = f"@{DB_SCHEMA}.ML_CODE_STAGE/ml_pipeline"
GIT_SOURCE = f"@{DB_SCHEMA}.ML_JOBS_GIT_REPO/branches/main/ml_pipeline"


def deploy(source_mode: str):
    session = Session.builder.getOrCreate()

    # Select source path based on deployment mode
    source_path = GIT_SOURCE if source_mode == "git" else STAGE_SOURCE

    # Register ML Job definitions that reference code on stage/git by path.
    # At DAG runtime, Snowflake reads the CURRENT version of these files.
    build_training_set_job = MLJobDefinition.register(
        source_path,
        entrypoint="build_training_set.py",
        compute_pool=COMPUTE_POOL,
        stage_name=PAYLOAD_STAGE,
        name="build_training_set",
    )

    train_and_evaluate_job = MLJobDefinition.register(
        source_path,
        entrypoint="train_and_evaluate.py",
        compute_pool=COMPUTE_POOL,
        stage_name=PAYLOAD_STAGE,
        name="train_and_evaluate",
    )

    notify_job = MLJobDefinition.register(
        source_path,
        entrypoint="notify.py",
        compute_pool=COMPUTE_POOL,
        stage_name=PAYLOAD_STAGE,
        name="notify",
    )

    # Assemble the DAG
    with DAG(
        "READMISSION_ML_PIPELINE",
        schedule=Cron("0 6 * * 1", "America/Los_Angeles"),
        stage_location=f"@{PAYLOAD_STAGE}",
        use_func_return_value=True,
    ) as dag:
        t1 = DAGTask("BUILD_TRAINING_SET", definition=build_training_set_job)
        t2 = DAGTask("TRAIN_AND_EVALUATE", definition=train_and_evaluate_job)
        t3 = DAGTask("NOTIFY", definition=notify_job)
        t1 >> t2 >> t3

    # Deploy
    root = Root(session)
    dag_op = DAGOperation(root.databases[DB].schemas[SCHEMA])
    dag_op.deploy(dag, mode=CreateMode.or_replace)
    print(f"DAG deployed to {DB_SCHEMA} (source: {source_mode})")
    print(f"Code path: {source_path}")
    print("Tasks will read code from stage at RUNTIME (not deploy time).")

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
