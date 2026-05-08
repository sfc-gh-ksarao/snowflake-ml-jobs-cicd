# Snowflake ML Jobs — CI/CD Demo

End-to-end CI/CD pipeline for Snowflake ML Jobs. Demonstrates two deployment approaches:
1. **Stage PUT** — GitHub Actions uploads code to an internal Snowflake stage via SnowSQL
2. **Git Integration** — Snowflake pulls code directly from this repo via a Git Repository object

## Pipeline

```
BUILD_TRAINING_SET → TRAIN_AND_EVALUATE → NOTIFY
    (Compute Pool)      (Compute Pool)     (Compute Pool)
```

A DAG runs weekly (Mon 6AM PT) on a Snowflake Compute Pool:
1. **Build Training Set** — pulls features from the Feature Store, creates training table
2. **Train & Evaluate** — trains XGBoost, registers model in ML Registry if AUC improves
3. **Notify** — sends email with results

## Quick Start

### 1. Snowflake Setup

Run the SQL in `setup/snowflake_setup.sql` to create:
- Compute pool (`DEMO_POOL`)
- Internal stages (`PAYLOAD_STAGE`, `ML_CODE_STAGE`)
- Git integration (`ML_JOBS_GIT_REPO`)
- Notification integration

### 2. GitHub Secrets

Add these secrets to the repo (Settings → Secrets → Actions):

| Secret | Value |
|--------|-------|
| `SNOWFLAKE_ACCOUNT` | Your account identifier |
| `SNOWFLAKE_USER` | Service account username |
| `SNOWFLAKE_PASSWORD` | Password |
| `SNOWFLAKE_ROLE` | Role with permissions |
| `SNOWFLAKE_WAREHOUSE` | Warehouse for SnowSQL commands |
| `SNOWFLAKE_DATABASE` | `SYNTHEA_DEMO` |
| `SNOWFLAKE_SCHEMA` | `PATIENTS` |

### 3. Deploy

Push to `main` triggers both workflows:
- **deploy-to-stage.yml** — PUTs code to `@ML_CODE_STAGE`
- **deploy-via-git-repo.yml** — refreshes the Git repo object in Snowflake

### 4. Deploy DAG (one-time)

```python
python ml_pipeline/deploy_dag.py --source stage  # or --source git
```

## Project Structure

```
├── ml_pipeline/
│   ├── build_training_set.py    # DAG Task 1
│   ├── train_and_evaluate.py    # DAG Task 2
│   ├── notify.py                # DAG Task 3
│   └── deploy_dag.py           # DAG assembly & deployment
├── setup/
│   └── snowflake_setup.sql     # Snowflake prerequisites
├── .github/workflows/
│   ├── deploy-to-stage.yml     # Approach 1: PUT to stage
│   └── deploy-via-git-repo.yml # Approach 2: Git integration refresh
├── docs/
│   └── architecture.md         # Architecture diagram
├── requirements.txt
└── README.md
```

## Requirements

```
snowflake-ml-python>=1.9.2
snowflake-snowpark-python
xgboost
scikit-learn
pandas
```

## Key Concepts

- **`@remote`** — decorator that runs a function on a Snowflake Compute Pool
- **`TaskContext`** — passes data (JSON) between DAG tasks without intermediate tables
- **`Session.builder.getOrCreate()`** — how code running inside ML Jobs authenticates (no credentials needed)
- **ML Registry** — versioned model store; only registers if AUC improves over existing best

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full CI/CD flow diagram.
