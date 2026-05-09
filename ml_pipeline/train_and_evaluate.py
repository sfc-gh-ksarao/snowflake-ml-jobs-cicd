"""
Task 2: TRAIN_AND_EVALUATE
Trains an XGBoost model on the training data, evaluates it,
and registers it in the Snowflake ML Registry if it beats the current best.
Runs on a Snowflake Compute Pool via @remote.
"""
from snowflake.snowpark import Session
from snowflake.ml.registry import Registry
from snowflake.core.task.context import TaskContext
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
from datetime import datetime
import pandas as pd
import time
import json


def main():
    sess = Session.builder.getOrCreate()

    # Load training data
    pdf = sess.table("SYNTHEA_DEMO.PATIENTS.READMISSION_DAG_TRAINING").to_pandas()

    feature_cols = [
        "ENCOUNTERS_LAST_12_MONTHS",
        "ENCOUNTERS_LAST_24_MONTHS",
        "PROCEDURES_LAST_12_MONTHS",
        "PROCEDURES_LAST_24_MONTHS",
        "AGE_AT_DISCHARGE",
    ]
    X = pdf[feature_cols].fillna(0)
    y = pdf["READMITTED_30_DAY"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.15, random_state=42
    )

    # Train XGBoost
    t0 = time.time()
    model = XGBClassifier(
        n_estimators=300, max_depth=5, learning_rate=0.1, random_state=42
    )
    model.fit(X_train, y_train)
    train_time = round(time.time() - t0, 2)

    # Evaluate
    proba = model.predict_proba(X_test)[:, 1]
    acc = round(accuracy_score(y_test, model.predict(X_test)), 4)
    auc = round(roc_auc_score(y_test, proba), 4)
    print(f"AUC: {auc} | Accuracy: {acc} | Time: {train_time}s")

    # Check against existing best model
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

    # Register if improvement
    registered = False
    version = None
    if auc > best_auc:
        version = "V_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        reg.log_model(
            model=model,
            model_name="READMISSION_XGBOOST_ML_JOB",
            version_name=version,
            sample_input_data=pd.DataFrame(X_train.head(10)),
            metrics={"accuracy": acc, "auc": auc},
            comment=f"DAG | acc={acc} auc={auc}",
        )
        registered_model = reg.get_model("READMISSION_XGBOOST_ML_JOB")
        registered_model.default = version
        registered = True
        print(f"REGISTERED & SET DEFAULT: READMISSION_XGBOOST_ML_JOB/{version}")
    else:
        print(f"AUC {auc} <= best {best_auc}. Skipped.")

    # Pass results to downstream tasks
    result = json.dumps({
        "registered": registered,
        "version": version,
        "auc": auc,
        "best_auc": best_auc,
        "accuracy": acc,
        "train_time": train_time,
    })
    ctx = TaskContext(sess)
    ctx.set_return_value(result)


if __name__ == "__main__":
    main()
