/*
================================================================================
Snowflake Managed MCP Server ハンズオン - 環境セットアップスクリプト
================================================================================

【概要】
このスクリプトは、Snowflake Managed MCP Serverハンズオン用の環境を構築します。

【処理内容】
1. ウェアハウスなどの環境設定
2. データベースとスキーマの作成
3. GitHub連携の設定
4. GitHubからバックアップデータの取得
5. テーブルの作成とデータのリストア

【データソース】
GitHub Repository: https://github.com/snow-jp-handson-org/cortex-handson-jp

【実行方法】
このスクリプト全体を選択してSnowflakeで実行してください。

【所要時間】
約2分

【次のステップ】
セットアップ完了後、part1_cortex_search.ipynb でCortex Search Serviceを作成してください。

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
CREATE WAREHOUSE IF NOT EXISTS MCP_HANDSON_WH;
USE WAREHOUSE MCP_HANDSON_WH;

SELECT '【Step 1】環境設定が完了しました' AS status;


-- ============================================================================
-- Step 2: データベースとスキーマの作成
-- ============================================================================
-- MCPハンズオン用のデータベースとスキーマを作成
CREATE OR REPLACE DATABASE MCP_HANDSON_DB;
CREATE OR REPLACE SCHEMA MCP_HANDSON_DB.ANALYTICS_SCHEMA;
USE SCHEMA MCP_HANDSON_DB.ANALYTICS_SCHEMA;

SELECT '【Step 2】データベースとスキーマの作成が完了しました' AS status;


-- ============================================================================
-- Step 3: GitHub連携の設定
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

SELECT '【Step 3】GitHub連携の設定が完了しました' AS status;


-- ============================================================================
-- Step 4: バックアップデータの取得
-- ============================================================================
-- バックアップ用内部ステージの作成
CREATE OR REPLACE STAGE BACKUP_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 0
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
        COMPRESSION = GZIP
    )
    COMMENT = 'テーブルバックアップ用内部ステージ';

-- GitHubからバックアップデータをステージにコピー
COPY FILES 
  INTO @MCP_HANDSON_DB.ANALYTICS_SCHEMA.BACKUP_STAGE
  FROM @GIT_INTEGRATION_FOR_HANDSON/branches/pri_20260203/backup/;

-- ステージ内のファイルを確認
LIST @BACKUP_STAGE;

SELECT '【Step 4】バックアップデータの取得が完了しました' AS status;


-- ============================================================================
-- Step 5: テーブルの作成とデータのリストア
-- ============================================================================

-- タイムスタンプ入力形式を設定
ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'YYYY/MM/DD HH24:MI:SS';

-- ----------------------------------------------------------------------------
-- Dimension層
-- ----------------------------------------------------------------------------

-- 顧客マスタ
CREATE OR REPLACE TABLE dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    email VARCHAR,
    phone VARCHAR,
    last_name VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    birth_date DATE,
    postal_code VARCHAR,
    prefecture VARCHAR,
    city VARCHAR,
    address VARCHAR,
    registration_date DATE,
    membership_tier VARCHAR,
    total_orders INTEGER,
    total_spent DECIMAL(12,2),
    last_order_date DATE,
    email_opt_in BOOLEAN,
    app_installed BOOLEAN
);

COPY INTO MCP_HANDSON_DB.ANALYTICS_SCHEMA.dim_customers
FROM @BACKUP_STAGE/dim_customers/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- 商品マスタ
CREATE OR REPLACE TABLE dim_products (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR,
    product_name_en VARCHAR,
    category_l1 VARCHAR,
    category_l2 VARCHAR,
    category_l3 VARCHAR,
    brand VARCHAR,
    supplier_id VARCHAR,
    cost_price DECIMAL(10,2),
    list_price DECIMAL(10,2),
    current_price DECIMAL(10,2),
    stock_quantity INTEGER,
    product_status VARCHAR,
    launch_date DATE,
    description TEXT,
    weight_g INTEGER,
    dimensions VARCHAR
);

COPY INTO MCP_HANDSON_DB.ANALYTICS_SCHEMA.dim_products
FROM @BACKUP_STAGE/dim_products/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- ----------------------------------------------------------------------------
-- Fact層
-- ----------------------------------------------------------------------------

-- 注文トランザクション
CREATE OR REPLACE TABLE fact_orders (
    order_id VARCHAR PRIMARY KEY,
    order_datetime TIMESTAMP,
    customer_id VARCHAR,
    product_id VARCHAR,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_method VARCHAR,
    shipping_address_id VARCHAR,
    order_channel VARCHAR,
    campaign_id VARCHAR,
    order_status VARCHAR
);

COPY INTO MCP_HANDSON_DB.ANALYTICS_SCHEMA.fact_orders
FROM @BACKUP_STAGE/fact_orders/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- 決済トランザクション
CREATE OR REPLACE TABLE fact_payments (
    payment_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    payment_datetime TIMESTAMP,
    card_brand VARCHAR,
    card_last4 VARCHAR,
    payment_amount DECIMAL(10,2),
    authorization_code VARCHAR,
    payment_status VARCHAR,
    fraud_score DECIMAL(5,2),
    device_fingerprint VARCHAR,
    ip_address VARCHAR,
    billing_country VARCHAR
);

COPY INTO MCP_HANDSON_DB.ANALYTICS_SCHEMA.fact_payments
FROM @BACKUP_STAGE/fact_payments/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Webアクセスログ
CREATE OR REPLACE TABLE fact_web_logs (
    log_id VARCHAR PRIMARY KEY,
    session_id VARCHAR,
    customer_id VARCHAR,
    event_timestamp TIMESTAMP,
    event_type VARCHAR,
    page_url VARCHAR,
    page_category VARCHAR,
    referrer_url VARCHAR,
    utm_source VARCHAR,
    utm_medium VARCHAR,
    utm_campaign VARCHAR,
    device_type VARCHAR,
    browser VARCHAR,
    os VARCHAR,
    time_on_page INTEGER,
    product_id VARCHAR
);

COPY INTO MCP_HANDSON_DB.ANALYTICS_SCHEMA.fact_web_logs
FROM @BACKUP_STAGE/fact_web_logs/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- ----------------------------------------------------------------------------
-- Gold層（非構造化データ）
-- ----------------------------------------------------------------------------

-- SNS投稿分析済みデータ（POSTED_ATをTIMESTAMP型に変更）
CREATE OR REPLACE TABLE gold_sns_mentions_analyzed (
    POST_ID VARCHAR,
    PLATFORM VARCHAR,
    POST_TYPE VARCHAR,
    USERNAME VARCHAR,
    DISPLAY_NAME VARCHAR,
    CONTENT VARCHAR,
    POSTED_AT TIMESTAMP_NTZ,  -- VARCHAR → TIMESTAMP_NTZ に変更
    LIKES NUMBER,
    RETWEETS NUMBER,
    REPLIES NUMBER,
    HASHTAGS ARRAY,
    MENTIONED_PRODUCTS ARRAY,
    MEDIA_URLS ARRAY,
    EXTRACTED_PRODUCT_NAME VARCHAR,
    EXTRACTED_CATEGORY VARCHAR,
    INQUIRY_TYPE VARCHAR,
    OVERALL_SENTIMENT VARCHAR,
    SENTIMENT VARCHAR,
    POST_CATEGORY VARCHAR,
    PROCESSED_AT TIMESTAMP_NTZ
);

-- 一時テーブルにVARCHARとしてインポートしてからTIMESTAMPに変換
CREATE OR REPLACE TEMPORARY TABLE temp_sns_mentions (
    POST_ID VARCHAR,
    PLATFORM VARCHAR,
    POST_TYPE VARCHAR,
    USERNAME VARCHAR,
    DISPLAY_NAME VARCHAR,
    CONTENT VARCHAR,
    POSTED_AT VARCHAR,
    LIKES NUMBER,
    RETWEETS NUMBER,
    REPLIES NUMBER,
    HASHTAGS ARRAY,
    MENTIONED_PRODUCTS ARRAY,
    MEDIA_URLS ARRAY,
    EXTRACTED_PRODUCT_NAME VARCHAR,
    EXTRACTED_CATEGORY VARCHAR,
    INQUIRY_TYPE VARCHAR,
    OVERALL_SENTIMENT VARCHAR,
    SENTIMENT VARCHAR,
    POST_CATEGORY VARCHAR,
    PROCESSED_AT VARCHAR
);

COPY INTO temp_sns_mentions
FROM @BACKUP_STAGE/gold_sns_mentions_analyzed/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

INSERT INTO gold_sns_mentions_analyzed
SELECT 
    POST_ID, PLATFORM, POST_TYPE, USERNAME, DISPLAY_NAME, CONTENT,
    TRY_TO_TIMESTAMP_NTZ(POSTED_AT) AS POSTED_AT,
    LIKES, RETWEETS, REPLIES, HASHTAGS, MENTIONED_PRODUCTS, MEDIA_URLS,
    EXTRACTED_PRODUCT_NAME, EXTRACTED_CATEGORY, INQUIRY_TYPE,
    OVERALL_SENTIMENT, SENTIMENT, POST_CATEGORY,
    TRY_TO_TIMESTAMP_NTZ(REGEXP_REPLACE(PROCESSED_AT, '[-+]\\d{2}$', '')) AS PROCESSED_AT
FROM temp_sns_mentions;

DROP TABLE temp_sns_mentions;

-- 音声ログ
CREATE OR REPLACE TABLE gold_voice_logs (
    CALL_ID VARCHAR,
    SCENARIO_ID VARCHAR,
    AUDIO_FILE VARCHAR,
    CALL_DURATION_SEC NUMBER(10,2),
    CALL_START_TIME TIMESTAMP_NTZ,
    CALL_END_TIME TIMESTAMP_NTZ,
    CATEGORY VARCHAR,
    AGENT_ID VARCHAR,
    CUSTOMER_PHONE VARCHAR,
    CUSTOMER_ID VARCHAR,
    CALL_TYPE VARCHAR,
    TRANSCRIBED_TEXT_MASKED VARCHAR,
    SENTIMENT_RESULT OBJECT,
    OVERALL_SENTIMENT VARCHAR,
    SENTIMENT VARCHAR,
    CLASSIFICATION_RESULT OBJECT,
    INQUIRY_CATEGORY VARCHAR,
    TRANSCRIBED_TEXT_SUMMARY VARCHAR,
    PROCESSED_AT TIMESTAMP_NTZ
);

-- 一時テーブルにVARCHARとしてインポートしてからTIMESTAMPに変換
CREATE OR REPLACE TEMPORARY TABLE temp_voice_logs (
    CALL_ID VARCHAR,
    SCENARIO_ID VARCHAR,
    AUDIO_FILE VARCHAR,
    CALL_DURATION_SEC NUMBER(10,2),
    CALL_START_TIME VARCHAR,
    CALL_END_TIME VARCHAR,
    CATEGORY VARCHAR,
    AGENT_ID VARCHAR,
    CUSTOMER_PHONE VARCHAR,
    CUSTOMER_ID VARCHAR,
    CALL_TYPE VARCHAR,
    TRANSCRIBED_TEXT_MASKED VARCHAR,
    SENTIMENT_RESULT OBJECT,
    OVERALL_SENTIMENT VARCHAR,
    SENTIMENT VARCHAR,
    CLASSIFICATION_RESULT OBJECT,
    INQUIRY_CATEGORY VARCHAR,
    TRANSCRIBED_TEXT_SUMMARY VARCHAR,
    PROCESSED_AT VARCHAR
);

COPY INTO temp_voice_logs
FROM @BACKUP_STAGE/gold_voice_logs/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

INSERT INTO gold_voice_logs
SELECT 
    CALL_ID, SCENARIO_ID, AUDIO_FILE, CALL_DURATION_SEC,
    TRY_TO_TIMESTAMP_NTZ(CALL_START_TIME) AS CALL_START_TIME,
    TRY_TO_TIMESTAMP_NTZ(CALL_END_TIME) AS CALL_END_TIME,
    CATEGORY, AGENT_ID, CUSTOMER_PHONE, CUSTOMER_ID, CALL_TYPE,
    TRANSCRIBED_TEXT_MASKED, SENTIMENT_RESULT, OVERALL_SENTIMENT, SENTIMENT,
    CLASSIFICATION_RESULT, INQUIRY_CATEGORY, TRANSCRIBED_TEXT_SUMMARY,
    TRY_TO_TIMESTAMP_NTZ(PROCESSED_AT) AS PROCESSED_AT
FROM temp_voice_logs;

DROP TABLE temp_voice_logs;

-- FAQドキュメント
CREATE OR REPLACE TABLE gold_faq_documents (
    RELATIVE_PATH VARCHAR,
    FILE_URL VARCHAR,
    SIZE NUMBER,
    LAST_MODIFIED VARCHAR,
    RAW_VALUE VARIANT,
    CONTENT_CHUNK VARCHAR,
    SUMMARY_CATEGORY VARCHAR
);

COPY INTO MCP_HANDSON_DB.ANALYTICS_SCHEMA.gold_faq_documents
FROM @BACKUP_STAGE/gold_faq_documents/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- 運営マニュアル
CREATE OR REPLACE TABLE gold_operation_manuals (
    RELATIVE_PATH VARCHAR,
    FILE_URL VARCHAR,
    SIZE NUMBER,
    LAST_MODIFIED VARCHAR,
    CONTENT_CHUNK VARCHAR
);

COPY INTO MCP_HANDSON_DB.ANALYTICS_SCHEMA.gold_operation_manuals
FROM @BACKUP_STAGE/gold_operation_manuals/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';


SELECT '【Step 5】テーブルの作成とデータのリストアが完了しました' AS status;


-- ============================================================================
-- Step 6: データ確認
-- ============================================================================

SELECT 
    'dim_customers' AS table_name, COUNT(*) AS record_count FROM dim_customers
UNION ALL SELECT 
    'dim_products', COUNT(*) FROM dim_products
UNION ALL SELECT 
    'fact_orders', COUNT(*) FROM fact_orders
UNION ALL SELECT 
    'fact_payments', COUNT(*) FROM fact_payments
UNION ALL SELECT 
    'fact_web_logs', COUNT(*) FROM fact_web_logs
UNION ALL SELECT 
    'gold_sns_mentions_analyzed', COUNT(*) FROM gold_sns_mentions_analyzed
UNION ALL SELECT 
    'gold_voice_logs', COUNT(*) FROM gold_voice_logs
UNION ALL SELECT 
    'gold_faq_documents', COUNT(*) FROM gold_faq_documents
UNION ALL SELECT 
    'gold_operation_manuals', COUNT(*) FROM gold_operation_manuals
ORDER BY table_name;


-- ============================================================================
-- 完了メッセージ
-- ============================================================================
SELECT '
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ 環境セットアップが完了しました！
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ データベース: MCP_HANDSON_DB
✅ スキーマ: ANALYTICS_SCHEMA
✅ ウェアハウス: MCP_HANDSON_WH

【作成されたテーブル】
📊 Dimension層: dim_customers, dim_products
📊 Fact層: fact_orders, fact_payments, fact_web_logs
📊 Gold層: gold_sns_mentions_analyzed, gold_voice_logs, 
          gold_faq_documents, gold_operation_manuals

【次のステップ】
part1_cortex_search.ipynb を開いてCortex Search Serviceを作成してください。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
' AS "✅ セットアップ完了";
