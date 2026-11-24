/*
================================================================================
Snowflake EC Analytics（12/16 ハンズオン）- 環境セットアップスクリプト
================================================================================

【概要】
このスクリプトを実行すると、ハンズオンに必要な環境がすべてそろいます。

【処理内容】
1. 環境設定（クロスリージョン推論の有効化、ウェアハウスの作成）
2. データベースとスキーマの作成
3. ステージの作成（データファイル用・抽出画像用）
4. GitHub 連携の設定（API 統合と Git リポジトリ）
5. GitHub からデータファイルを取得
6. Snowflake Intelligence オブジェクトの作成（CoWork で Agent を使うため）

【データソース】
GitHub Repository: https://github.com/snow-jp-handson-org/cortex-handson-jp（ブランチ pri_20261216）

【実行方法】
このスクリプト全体を選択して実行してください。何度実行しても、作成済みのテーブルやデータは消えません。

【所要時間】
約1分

【次のステップ】
セットアップ完了後、part1_20261216.ipynb でデータを取り込みます。
================================================================================
*/

-- ============================================================================
-- Step 1: 環境設定
-- ============================================================================
-- 管理者ロールを使用
USE ROLE ACCOUNTADMIN;

-- 実行リージョンにないモデルも使えるよう、クロスリージョン推論を有効化
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ウェアハウスの用意
CREATE WAREHOUSE IF NOT EXISTS GLACIERSTYLE_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE GLACIERSTYLE_WH;

SELECT '【Step 1】環境設定が完了しました' AS status;


-- ============================================================================
-- Step 2: データベースとスキーマの作成
-- ============================================================================
CREATE DATABASE IF NOT EXISTS GLACIERSTYLE_DB;
CREATE SCHEMA IF NOT EXISTS GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA;
USE SCHEMA GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA;

SELECT '【Step 2】データベースとスキーマの作成が完了しました' AS status;


-- ============================================================================
-- Step 3: ステージの作成
-- ============================================================================
-- データファイル（CSV・JSON・PDF・音声・画像）を置くステージ
CREATE STAGE IF NOT EXISTS DATA_STAGE DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
-- Part 2 補足パートで、PDF から取り出した画像を保存するステージ
CREATE STAGE IF NOT EXISTS EXTRACTED_IMAGES_STAGE DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

SELECT '【Step 3】ステージの作成が完了しました' AS status;


-- ============================================================================
-- Step 4: GitHub 連携の設定
-- ============================================================================
-- GitHub からデータを取得するための API 統合
CREATE API INTEGRATION IF NOT EXISTS GLACIERSTYLE_HANDSON_GIT
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/snow-jp-handson-org/')
  ENABLED = TRUE;

-- Git リポジトリとの統合
CREATE GIT REPOSITORY IF NOT EXISTS GIT_INTEGRATION_FOR_HANDSON
  API_INTEGRATION = GLACIERSTYLE_HANDSON_GIT
  ORIGIN = 'https://github.com/snow-jp-handson-org/cortex-handson-jp.git';
-- 最新の内容を取得
ALTER GIT REPOSITORY GIT_INTEGRATION_FOR_HANDSON FETCH;

SELECT '【Step 4】GitHub 連携の設定が完了しました' AS status;


-- ============================================================================
-- Step 5: GitHub からデータファイルを取得
-- ============================================================================
-- ハンズオンで使うファイルだけをステージにコピー
--   本編: customers / products / orders / call_transcripts の CSV、PDF、通話音声（mp3）
--   Part 2 補足: ad_creatives.csv、sns_logs.json、広告画像（png）
COPY FILES INTO @DATA_STAGE FROM @GIT_INTEGRATION_FOR_HANDSON/branches/pri_20261216/data/
  PATTERN = '(.*/)?((customers|products|orders|call_transcripts|ad_creatives)[.]csv|sns_logs[.]json|[^/]+[.]pdf|voice_logs/[^/]+[.]mp3|ad_images/[^/]+[.]png)$';
ALTER STAGE DATA_STAGE REFRESH;

-- ステージ内のファイル数を確認（CSV 5、JSON 1、PDF 11、音声 10、画像 15）
SELECT COUNT(*) AS file_count FROM DIRECTORY(@DATA_STAGE);

SELECT '【Step 5】GitHub からのデータ取得が完了しました' AS status;


-- ============================================================================
-- Step 6: Snowflake Intelligence オブジェクトの作成
-- ============================================================================
-- CoWork で Cortex Agent を使うためのオブジェクト（アカウントに1つ）
CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

SELECT '【Step 6】Snowflake Intelligence オブジェクトの作成が完了しました' AS status;


-- ============================================================================
-- 完了メッセージ
-- ============================================================================
SELECT '
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
環境セットアップが完了しました
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

データベース: GLACIERSTYLE_DB
スキーマ: EC_ANALYTICS_SCHEMA
ウェアハウス: GLACIERSTYLE_WH
ステージ: DATA_STAGE（データファイル格納済み）、EXTRACTED_IMAGES_STAGE
GitHub 連携: GIT_INTEGRATION_FOR_HANDSON
Snowflake Intelligence: SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT

【次のステップ】
part1_20261216.ipynb を開いてデータを取り込んでください。
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
' AS "セットアップ完了";
