
/*
================================================================================
Snowflake EC Analytics - クリーンアップ（後片付け）スクリプト
================================================================================

【概要】
setup.sql で作成した GlacierStyle EC分析環境を削除するためのスクリプトです。
※本番/共有環境では誤実行に注意してください（データが完全に削除されます）。

【setup.sql で作成される主なオブジェクト（参考）】
- Warehouse: GLACIERSTYLE_WH
- Database/Schema: GLACIERSTYLE_DB / EC_ANALYTICS_SCHEMA
- Stage: GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.DATA_STAGE
- API Integration: git_api_integration（アカウントレベル）
- Git Repository: GIT_INTEGRATION_FOR_HANDSON（アカウントレベル）
- Streamlit: GLACIERSTYLE_ANALYTICS_APP（アカウントレベル）
- Account Parameter: CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'

【注意点】
- DROP DATABASE は配下のスキーマ/テーブル/ステージ等もまとめて削除します。
- API INTEGRATION / GIT REPOSITORY / STREAMLIT はアカウントレベルのため、
  このファイルではデフォルトで削除していません（必要なら任意実行の箇所を有効化してください）。

【実行順の目安】
1) （任意）Streamlit / Git連携（アカウントレベル）を削除
2) Warehouse を削除
3) Database を削除（中のデータ一式が消えます）
4) （任意）Cortex cross-region parameter を無効化

================================================================================
*/

-- ============================================================================
-- Step 1: 環境設定（権限）
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- Step 2: （任意）setup.sql で作成したアカウントレベルのオブジェクトを削除
-- ============================================================================
-- Streamlit in Snowflake アプリ（setup.sql Step 6）
-- DROP STREAMLIT IF EXISTS GLACIERSTYLE_ANALYTICS_APP;
--
-- Gitリポジトリ統合（setup.sql Step 4）
-- DROP GIT REPOSITORY IF EXISTS GIT_INTEGRATION_FOR_HANDSON;
--
-- API統合（setup.sql Step 4）
-- DROP API INTEGRATION IF EXISTS git_api_integration;

-- ============================================================================
-- Step 3: （任意）クロスリージョンコールのパラメータを無効化
-- ============================================================================
-- クロスリージョンコールのパラメータを無効化
-- ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'DISABLED';

-- ============================================================================
-- Step 4: Warehouse / Database の削除
-- ============================================================================
-- ウェアハウスの削除
DROP WAREHOUSE IF EXISTS GLACIERSTYLE_WH;

-- データベースの削除
-- ※GLACIERSTYLE_DB 配下（EC_ANALYTICS_SCHEMA、DATA_STAGE、テーブル等）が全て削除されます
DROP DATABASE IF EXISTS GLACIERSTYLE_DB;

