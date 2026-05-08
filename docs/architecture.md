# Architecture: CI/CD for Snowflake ML Jobs

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          GITHUB REPOSITORY                               │
│  sfc-gh-ksarao/snowflake-ml-jobs-cicd                                   │
│                                                                          │
│  ml_pipeline/                                                            │
│    ├── build_training_set.py                                             │
│    ├── train_and_evaluate.py                                             │
│    ├── notify.py                                                         │
│    └── deploy_dag.py                                                     │
└─────────────────────┬───────────────────────────┬───────────────────────┘
                      │ push to main              │ push to main
                      ▼                           ▼
        ┌─────────────────────────┐  ┌─────────────────────────────────┐
        │  GitHub Action          │  │  GitHub Action                   │
        │  deploy-to-stage.yml    │  │  deploy-via-git-repo.yml        │
        │                         │  │                                  │
        │  SnowSQL PUT → Stage    │  │  ALTER GIT REPO ... FETCH       │
        └────────────┬────────────┘  └──────────────┬──────────────────┘
                     │                              │
                     ▼                              ▼
        ┌─────────────────────────┐  ┌─────────────────────────────────┐
        │  @ML_CODE_STAGE         │  │  @ML_JOBS_GIT_REPO              │
        │  /ml_pipeline/          │  │  /branches/main/ml_pipeline/    │
        │  (Internal Stage)       │  │  (Git Integration)              │
        └────────────┬────────────┘  └──────────────┬──────────────────┘
                     │                              │
                     └──────────────┬───────────────┘
                                    ▼
              ┌─────────────────────────────────────────────┐
              │       SNOWFLAKE DAG: READMISSION_ML_PIPELINE │
              │       Schedule: Mon 6AM PT                    │
              │                                              │
              │  ┌───────────────────┐                       │
              │  │ BUILD_TRAINING_SET│ (Compute Pool)        │
              │  └────────┬──────────┘                       │
              │           │ TaskContext → row count           │
              │           ▼                                   │
              │  ┌───────────────────┐                       │
              │  │ TRAIN_AND_EVALUATE│ (Compute Pool)        │
              │  └────────┬──────────┘                       │
              │           │ TaskContext → metrics JSON        │
              │           ▼                                   │
              │  ┌───────────────────┐                       │
              │  │ NOTIFY            │ (Compute Pool)        │
              │  └───────────────────┘                       │
              │       Email: model registered / skipped       │
              └─────────────────────────────────────────────┘
                                    │
                                    ▼
              ┌─────────────────────────────────────────────┐
              │  SNOWFLAKE ML REGISTRY                       │
              │  Model: READMISSION_XGBOOST_ML_JOB          │
              │  (registered only if AUC improves)           │
              └─────────────────────────────────────────────┘
```

## Two Deployment Approaches

### Approach 1: Stage PUT (Traditional CI/CD)
- GitHub Actions uses SnowSQL to `PUT` files onto an internal stage
- DAG tasks reference code from `@ML_CODE_STAGE/ml_pipeline/`
- Pros: Full control over what's deployed, works with any Git provider
- Cons: Requires SnowSQL in CI, explicit PUT for each file

### Approach 2: Git Integration (Native Snowflake)
- Snowflake has a GIT REPOSITORY object pointing at the GitHub repo
- GitHub Actions triggers `ALTER GIT REPOSITORY ... FETCH` after push
- DAG tasks reference code from `@ML_JOBS_GIT_REPO/branches/main/ml_pipeline/`
- Pros: No file copying, Snowflake always has latest code, cleaner
- Cons: Requires API Integration setup, repo must be accessible from Snowflake

## Security Model

```
GitHub Secrets (encrypted) ──→ GitHub Actions env vars ──→ SnowSQL auth
                                                             │
                                                             ▼
                                                        Snowflake
```

Secrets stored in GitHub:
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`

No credentials in code. ML Jobs on compute pool use `Session.builder.getOrCreate()`.
