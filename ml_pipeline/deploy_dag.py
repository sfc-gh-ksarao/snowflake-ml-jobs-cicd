"""
Deploy the ML pipeline DAG to Snowflake.

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
from snowflake.ml.jobs import remote


DB = "SYNTHEA_DEMO"
SCHEMA = "PATIENTS"
DB_SCHEMA = f"{DB}.{SCHEMA}"
COMPUTE_POOL = "DEMO_POOL"
PAYLOAD_STAGE = f"{DB_SCHEMA}.PAYLOAD_STAGE"

# Stage paths for each deployment mode
STAGE_PREFIX = f"@{DB_SCHEMA}.ML_CODE_STAGE/ml_pipeline"
GIT_PREFIX = f"@{DB_SCHEMA}.ML_JOBS_GIT_REPO/branches/main/ml_pipeline"


def get_source_path(source_mode: str, filename: str) -> str:
    """Return the full stage path for a given file based on deployment mode."""
    if source_mode == "git":
        return f"{GIT_PREFIX}/{filename}"
    return f"{STAGE_PREFIX}/{filename}"


def deploy(source_mode: str):
    session = Session.builder.getOrCreate()

    # Define DAG task functions using @remote
    # Each task will execute the corresponding .py file from the stage/git source
    @remote(
        session=session,
        compute_pool=COMPUTE_POOL,
        stage_name=PAYLOAD_STAGE,
    )
    def build_training_set():
        from snowflake.snowpark import Session
        from snowflake.ml.feature_store import FeatureStore
        from snowflake.core.task.context import TaskContext

        sess = Session.builder.getOrCreate()
        sess.sql("DROP TABLE IF EXISTS SYNTHEA_DEMO.PATIENTS.READMISSION_DAG_TRAINING").collect()

        fs = FeatureStore(
            session=sess, database="SYNTHEA_DEMO", name="PATIENTS",
            default_warehouse="COMPUTE_WH",
        )
        fv = fs.get_feature_view("PATIENT_READMISSION_FEATURES", "v1")

        spine_df = sess.sql("""
            SELECT DISTINCT PATIENT_ID, DISCHARGE_DATE, READMITTED_30_DAY
            FROM SYNTHEA_DEMO.PATIENTS.READMISSIONS_BASE
            SAMPLE (10000 ROWS)
        """)

        fs.generate_training_set(
            spine_df=spine_df, features=[fv],
            save_as="SYNTHEA_DEMO.PATIENTS.READMISSION_DAG_TRAINING",
            spine_label_cols=["READMITTED_30_DAY"],
        )

        cnt = sess.sql("SELECT COUNT(*) AS CNT FROM SYNTHEA_DEMO.PATIENTS.READMISSION_DAG_TRAINING").collect()[0]["CNT"]
        print(f"Training set: {cnt} rows")
        ctx = TaskContext(sess)
        ctx.set_return_value(f"{cnt}")

    @remote(
        session=session,
        compute_pool=COMPUTE_POOL,
        stage_name=PAYLOAD_STAGE,
    )
    def train_and_evaluate():
        from snowflake.snowpark import Session
        from snowflake.ml.registry import Registry
        from snowflake.core.task.context import TaskContext
        from xgboost import XGBClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score, roc_auc_score
        from datetime import datetime
        import pandas as pd, time, json

        sess = Session.builder.getOrCreate()
        pdf = sess.table("SYNTHEA_DEMO.PATIENTS.READMISSION_DAG_TRAINING").to_pandas()

        feature_cols = [
            "ENCOUNTERS_LAST_12_MONTHS", "ENCOUNTERS_LAST_24_MONTHS",
            "PROCEDURES_LAST_12_MONTHS", "PROCEDURES_LAST_24_MONTHS",
            "AGE_AT_DISCHARGE",
        ]
        X = pdf[feature_cols].fillna(0)
        y = pdf["READMITTED_30_DAY"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        t0 = time.time()
        model = XGBClassifier(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42)
        model.fit(X_train, y_train)
        train_time = round(time.time() - t0, 2)

        proba = model.predict_proba(X_test)[:, 1]
        acc = round(accuracy_score(y_test, model.predict(X_test)), 4)
        auc = round(roc_auc_score(y_test, proba), 4)
        print(f"AUC: {auc} | Accuracy: {acc} | Time: {train_time}s")

        reg = Registry(session=sess, database_name="SYNTHEA_DEMO", schema_name="PATIENTS")
        best_auc = 0.0
        try:
            existing_model = reg.get_model("READMISSION_XGBOOST_ML_JOB")
            for v in existing_model.versions():
                metrics = v.show_metrics()
                v_auc = float(metrics.get("auc", 0))
                if v_auc > best_auc:
                    best_auc = v_auc
        except Exception:
            pass

        registered = False
        version = None
        if auc > best_auc:
            version = "V_" + datetime.now().strftime("%Y%m%d_%H%M%S")
            reg.log_model(
                model=model, model_name="READMISSION_XGBOOST_ML_JOB",
                version_name=version, sample_input_data=pd.DataFrame(X_train.head(10)),
                metrics={"accuracy": acc, "auc": auc},
                comment=f"DAG | acc={acc} auc={auc}",
            )
            registered_model = reg.get_model("READMISSION_XGBOOST_ML_JOB")
            registered_model.default = version
            registered = True
            print(f"REGISTERED & SET DEFAULT: READMISSION_XGBOOST_ML_JOB/{version}")
        else:
            print(f"AUC {auc} <= best {best_auc}. Skipped.")

        result = json.dumps({"registered": registered, "version": version, "auc": auc, "best_auc": best_auc, "accuracy": acc, "train_time": train_time})
        ctx = TaskContext(sess)
        ctx.set_return_value(result)

    @remote(
        session=session,
        compute_pool=COMPUTE_POOL,
        stage_name=PAYLOAD_STAGE,
    )
    def notify():
        from snowflake.snowpark import Session
        from snowflake.core.task.context import TaskContext
        import json

        sess = Session.builder.getOrCreate()
        ctx = TaskContext(sess)
        r = json.loads(ctx.get_predecessor_return_value("TRAIN_AND_EVALUATE"))

        if r.get("registered"):
            subject = f"ML Pipeline: New model registered (AUC={r['auc']})"
            body = f"Version {r['version']}. AUC: {r['auc']} (prev: {r['best_auc']}). Accuracy: {r['accuracy']}"
        else:
            subject = f"ML Pipeline: No improvement (AUC={r['auc']})"
            body = f"AUC {r['auc']} did not beat {r['best_auc']}. Skipped."

        try:
            sess.sql(f"CALL SYSTEM$SEND_EMAIL('DEMO_NOTIFICATION_INTEGRATION', 'karan.sarao@snowflake.com', '{subject}', '{body}')").collect()
            print(f"Email sent: {subject}")
        except Exception as e:
            print(f"Notification skipped: {e}")

    # Assemble the DAG
    with DAG(
        "READMISSION_ML_PIPELINE",
        schedule=Cron("0 6 * * 1", "America/Los_Angeles"),
        stage_location=f"@{PAYLOAD_STAGE}",
        use_func_return_value=True,
    ) as dag:
        t1 = DAGTask("BUILD_TRAINING_SET", definition=build_training_set)
        t2 = DAGTask("TRAIN_AND_EVALUATE", definition=train_and_evaluate)
        t3 = DAGTask("NOTIFY", definition=notify)
        t1 >> t2 >> t3

    # Deploy
    root = Root(session)
    dag_op = DAGOperation(root.databases[DB].schemas[SCHEMA])
    dag_op.deploy(dag, mode=CreateMode.or_replace)
    print(f"DAG deployed to {DB_SCHEMA} (source: {source_mode})")

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
