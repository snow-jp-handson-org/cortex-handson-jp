// Step1: テーブル作成 //

-- ロールの指定
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;


// Step2: 各種オブジェクトの作成 //

-- データベースの作成
CREATE OR REPLACE DATABASE SNOWRETAIL_DB;
-- スキーマの作成
CREATE OR REPLACE SCHEMA SNOWRETAIL_DB.SNOWRETAIL_SCHEMA;
-- スキーマの指定
USE SCHEMA SNOWRETAIL_DB.SNOWRETAIL_SCHEMA;

-- ステージの作成
CREATE OR REPLACE STAGE SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.FILE DIRECTORY = (ENABLE = TRUE);
CREATE OR REPLACE STAGE SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.SEMANTIC_MODEL_STAGE DIRECTORY = (ENABLE = TRUE);


// Step3: 公開されているGitからデータとスクリプトを取得 //

-- Git連携のため、API統合を作成する
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/snow-jp-handson-org/')
  ENABLED = TRUE;

-- GIT統合の作成
CREATE OR REPLACE GIT REPOSITORY GIT_INTEGRATION_FOR_HANDSON
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/snow-jp-handson-org/cortex-handson-jp.git';

-- チェックする
ls @GIT_INTEGRATION_FOR_HANDSON/branches/main;

-- Githubからファイルを持ってくる
COPY FILES INTO @SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.FILE FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/data/;
COPY FILES INTO @SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.SEMANTIC_MODEL_STAGE FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/handson2/sales_analysis_model.yaml;

// Step4: NotebookとStreamlitを作成 //

-- Notebookの作成
CREATE OR REPLACE NOTEBOOK cortex_handson_part1
    FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/handson1
    MAIN_FILE = 'cortex_handson_seminar_part1.ipynb'
    QUERY_WAREHOUSE = COMPUTE_WH
    WAREHOUSE = COMPUTE_WH;

-- Streamlit in Snowflakeの作成
CREATE OR REPLACE STREAMLIT sis_snowretail_analysis_dev
    FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/handson2/mvp
    MAIN_FILE = 'mainpage.py'
    QUERY_WAREHOUSE = COMPUTE_WH;
