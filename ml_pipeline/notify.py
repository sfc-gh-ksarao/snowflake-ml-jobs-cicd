"""
Task 3: NOTIFY
Reads the result from TRAIN_AND_EVALUATE and sends an email notification.
Runs on a Snowflake Compute Pool via @remote.
"""
from snowflake.snowpark import Session
from snowflake.core.task.context import TaskContext
import json


def main():
    sess = Session.builder.getOrCreate()
    ctx = TaskContext(sess)

    # Get predecessor result
    r = json.loads(ctx.get_predecessor_return_value("TRAIN_AND_EVALUATE"))

    if r.get("registered"):
        subject = f"ML Pipeline: New model registered (AUC={r['auc']})"
        body = (
            f"Version {r['version']}. "
            f"AUC: {r['auc']} (prev: {r['best_auc']}). "
            f"Accuracy: {r['accuracy']}"
        )
    else:
        subject = f"ML Pipeline: No improvement (AUC={r['auc']})"
        body = f"AUC {r['auc']} did not beat {r['best_auc']}. Skipped."

    try:
        sess.sql(
            f"CALL SYSTEM$SEND_EMAIL("
            f"'DEMO_NOTIFICATION_INTEGRATION', "
            f"'karan.sarao@snowflake.com', "
            f"'{subject}', "
            f"'{body}')"
        ).collect()
        print(f"Email sent: {subject}")
    except Exception as e:
        print(f"Notification skipped: {e}")


if __name__ == "__main__":
    main()
