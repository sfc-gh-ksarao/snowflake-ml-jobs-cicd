"""
Task 1: BUILD_TRAINING_SET
Generates a training dataset from the Feature Store and saves it as a table.
Runs on a Snowflake Compute Pool via @remote.
"""
from snowflake.snowpark import Session
from snowflake.ml.feature_store import FeatureStore
from snowflake.core.task.context import TaskContext


def main():
    sess = Session.builder.getOrCreate()

    # Drop previous training table if exists
    sess.sql("DROP TABLE IF EXISTS SYNTHEA_DEMO.PATIENTS.READMISSION_DAG_TRAINING").collect()

    # Connect to Feature Store
    fs = FeatureStore(
        session=sess,
        database="SYNTHEA_DEMO",
        name="PATIENTS",
        default_warehouse="COMPUTE_WH",
    )
    fv = fs.get_feature_view("PATIENT_READMISSION_FEATURES", "v1")

    # Sample spine dataframe
    spine_df = sess.sql("""
        SELECT DISTINCT PATIENT_ID, DISCHARGE_DATE, READMITTED_30_DAY
        FROM SYNTHEA_DEMO.PATIENTS.READMISSIONS_BASE
        SAMPLE (10000 ROWS)
    """)

    # Generate training set
    fs.generate_training_set(
        spine_df=spine_df,
        features=[fv],
        save_as="SYNTHEA_DEMO.PATIENTS.READMISSION_DAG_TRAINING",
        spine_label_cols=["READMITTED_30_DAY"],
    )

    cnt = sess.sql(
        "SELECT COUNT(*) AS CNT FROM SYNTHEA_DEMO.PATIENTS.READMISSION_DAG_TRAINING"
    ).collect()[0]["CNT"]
    print(f"Training set: {cnt} rows")

    # Pass row count to downstream tasks via TaskContext
    ctx = TaskContext(sess)
    ctx.set_return_value(f"{cnt}")


if __name__ == "__main__":
    main()
