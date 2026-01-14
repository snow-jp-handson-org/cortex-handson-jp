/*
================================================================================
Snowflake EC Analytics - 環境セットアップスクリプト
================================================================================

【概要】
このスクリプトは、ECサイト分析用のデータベース環境を構築します。

【処理内容】
1. データベースとスキーマの作成
2. ステージの作成（データ格納用）
3. GitHub連携の設定（API統合とGitリポジトリ）
4. GitHubからデータファイルの自動取得

【データソース】
GitHub Repository: https://github.com/snow-jp-handson-org/cortex-handson-jp

【実行方法】
このスクリプト全体を選択してSnowflakeで実行してください。

【所要時間】
約1分

【次のステップ】
セットアップ完了後、part1_data_ingest.ipynb でデータのインポートを実行してください。

================================================================================
*/

-- ============================================================================
-- Step 1: 環境設定
-- ============================================================================
-- 管理者ロールとコンピュートウェアハウスを使用
USE ROLE ACCOUNTADMIN;

-- クロスリージョンコールのパラメータを有効化
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ウェアハウスの用意
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
USE WAREHOUSE COMPUTE_WH;

SELECT '【Step 1】環境設定が完了しました' AS status;


-- ============================================================================
-- Step 2: データベースとスキーマの作成
-- ============================================================================
-- ECアナリティクス用のデータベースとスキーマを作成
CREATE OR REPLACE DATABASE GLACIERSTYLE_DB;
CREATE OR REPLACE SCHEMA GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA;
USE SCHEMA GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA;

SELECT '【Step 2】データベースとスキーマの作成が完了しました' AS status;


-- ============================================================================
-- Step 3: データステージの作成
-- ============================================================================
-- CSVファイルを格納するためのステージを作成（暗号化有効）
CREATE OR REPLACE STAGE GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.DATA_STAGE 
  encryption = (type = 'snowflake_sse') 
  DIRECTORY = (ENABLE = TRUE);

SELECT '【Step 3】データステージの作成が完了しました' AS status;


-- ============================================================================
-- Step 4: GitHub連携の設定
-- ============================================================================
-- GitHubからデータを取得するためのAPI統合を作成
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/snow-jp-handson-org/')
  ENABLED = TRUE;

-- Gitリポジトリとの統合を作成
CREATE OR REPLACE GIT REPOSITORY GIT_INTEGRATION_FOR_HANDSON
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/snow-jp-handson-org/cortex-handson-jp.git';

SELECT '【Step 4】GitHub連携の設定が完了しました' AS status;


-- ============================================================================
-- Step 5: GitHubからデータファイルの取得
-- ============================================================================
-- リポジトリの内容を確認
ls @GIT_INTEGRATION_FOR_HANDSON/branches/main;

-- GitHubのdataディレクトリからすべてのファイルをステージにコピー
COPY FILES 
  INTO @GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.DATA_STAGE 
  FROM @GIT_INTEGRATION_FOR_HANDSON/branches/tmp_new_version_2026/data/;
--   FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/data/;

-- ステージ内のファイルを確認
ls @GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.DATA_STAGE;

SELECT '【Step 5】GitHubからのデータ取得が完了しました' AS status;


-- ============================================================================
-- 完了メッセージ
-- ============================================================================
SELECT '
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ 環境セットアップが完了しました！
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ データベース: GLACIERSTYLE_DB
✅ スキーマ: EC_ANALYTICS_SCHEMA
✅ ステージ: DATA_STAGE（データファイル格納済み）
✅ GitHub連携: GIT_INTEGRATION_FOR_HANDSON

【次のステップ】
part1_data_ingest.ipynb を開いてデータのインポートを実行してください。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
' AS "✅ セットアップ完了";