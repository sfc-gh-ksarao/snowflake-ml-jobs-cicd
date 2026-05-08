-- =============================================================================
-- Snowflake Setup for ML Jobs CI/CD Demo
-- Run these statements in your Snowflake account (with ACCOUNTADMIN or equivalent)
-- =============================================================================

-- 1. Compute Pool for ML Jobs
CREATE COMPUTE POOL IF NOT EXISTS DEMO_POOL
  MIN_NODES = 1
  MAX_NODES = 2
  INSTANCE_FAMILY = CPU_X64_S;

-- 2. Stages
CREATE STAGE IF NOT EXISTS SYNTHEA_DEMO.PATIENTS.PAYLOAD_STAGE;
CREATE STAGE IF NOT EXISTS SYNTHEA_DEMO.PATIENTS.ML_CODE_STAGE
  COMMENT = 'Holds ML pipeline code uploaded via GitHub Actions CI/CD';

-- 3. Notification Integration (for email alerts)
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS DEMO_NOTIFICATION_INTEGRATION
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('karan.sarao@snowflake.com');

-- =============================================================================
-- Git Integration (Approach 2: Snowflake reads code directly from GitHub)
-- =============================================================================

-- 4. API Integration for GitHub
CREATE OR REPLACE API INTEGRATION ML_JOBS_GIT_API_INTEGRATION
  API_PROVIDER = GIT_HTTPS_API
  API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-ksarao/')
  ENABLED = TRUE;

-- 5. Secret for GitHub access (if repo is private, use a PAT; for public repos this is optional)
-- CREATE OR REPLACE SECRET ML_JOBS_GIT_SECRET
--   TYPE = PASSWORD
--   USERNAME = 'sfc-gh-ksarao'
--   PASSWORD = '<GITHUB_PAT>';

-- 6. Git Repository object in Snowflake
CREATE OR REPLACE GIT REPOSITORY SYNTHEA_DEMO.PATIENTS.ML_JOBS_GIT_REPO
  API_INTEGRATION = ML_JOBS_GIT_API_INTEGRATION
  -- GIT_CREDENTIALS = ML_JOBS_GIT_SECRET  -- uncomment if private repo
  ORIGIN = 'https://github.com/sfc-gh-ksarao/snowflake-ml-jobs-cicd.git';

-- 7. Fetch latest from Git
ALTER GIT REPOSITORY SYNTHEA_DEMO.PATIENTS.ML_JOBS_GIT_REPO FETCH;

-- 8. Verify files are accessible
LIST @SYNTHEA_DEMO.PATIENTS.ML_JOBS_GIT_REPO/branches/main/ml_pipeline/;
