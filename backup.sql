-- ============================================================================
-- GlacierStyle ECデータベース バックアップ/リストア SQL
-- ============================================================================
-- 対象テーブル:
--   Dimension層: dim_customers, dim_products
--   Fact層: fact_orders, fact_payments, fact_web_logs
--   Gold層: gold_sns_mentions_analyzed, gold_voice_logs, gold_ad_creative_analysis,
--           gold_faq_documents, gold_operation_manuals, gold_sns_mentions_with_product_master

-- 使用するスキーマを設定
USE SCHEMA GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA;

ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'YYYY/MM/DD HH24:MI:SS';

-- ============================================================================
-- 1. バックアップ用内部ステージの作成
-- ============================================================================
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

-- ============================================================================
-- 2. データをステージにインポート
-- ============================================================================
COPY FILES 
  INTO @GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.BACKUP_STAGE
  FROM @GIT_INTEGRATION_FOR_HANDSON/branches/tmp_new_version_2026/backup/;

-- ============================================================================
-- 3. エクスポート結果の確認
-- ============================================================================
LIST @BACKUP_STAGE;

-- ============================================================================
-- 4. 内部ステージからテーブルにリストア（COPY INTO テーブル）
-- ============================================================================

-- Dimension層
-- TRUNCATE TABLE dim_customers;
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

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.dim_customers
FROM @BACKUP_STAGE/dim_customers/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TRUNCATE TABLE dim_products;
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

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.dim_products
FROM @BACKUP_STAGE/dim_products/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Fact層
-- TRUNCATE TABLE fact_orders;
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

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.fact_orders
FROM @BACKUP_STAGE/fact_orders/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TRUNCATE TABLE fact_payments;
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

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.fact_payments
FROM @BACKUP_STAGE/fact_payments/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- TRUNCATE TABLE fact_web_logs;
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

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.fact_web_logs
FROM @BACKUP_STAGE/fact_web_logs/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Gold層（DDL + COPY INTO）
-- gold_sns_mentions_analyzed
CREATE OR REPLACE TABLE gold_sns_mentions_analyzed (
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

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.gold_sns_mentions_analyzed
FROM @BACKUP_STAGE/gold_sns_mentions_analyzed/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- gold_voice_logs
CREATE OR REPLACE TABLE gold_voice_logs (
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

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.gold_voice_logs
FROM @BACKUP_STAGE/gold_voice_logs/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- gold_ad_creative_analysis
CREATE OR REPLACE TABLE gold_ad_creative_analysis (
    CREATIVE_ID VARCHAR,
    CREATIVE_NAME VARCHAR,
    CREATIVE_TYPE VARCHAR,
    CAMPAIGN_ID VARCHAR,
    PLATFORM VARCHAR,
    TARGET_SEGMENT VARCHAR,
    COPY_TEXT VARCHAR,
    HEADLINE VARCHAR,
    CTA_TEXT VARCHAR,
    APPEAL_TYPE VARCHAR,
    CTA_TYPE VARCHAR,
    KEYWORDS VARCHAR,
    TARGET_EMOTION VARCHAR,
    USP VARCHAR,
    COPY_STYLE VARCHAR,
    SENTIMENT VARCHAR,
    IMPRESSIONS NUMBER,
    CLICKS NUMBER,
    CONVERSIONS NUMBER,
    SPEND NUMBER(10,2),
    CTR FLOAT,
    CVR FLOAT,
    CPA NUMBER(16,8),
    VISUAL_ANALYSIS_RAW_JSON VARIANT,
    IMAGE_STYLE VARCHAR,
    PROCESSED_AT VARCHAR
);

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.gold_ad_creative_analysis
FROM @BACKUP_STAGE/gold_ad_creative_analysis/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- gold_faq_documents
CREATE OR REPLACE TABLE gold_faq_documents (
    RELATIVE_PATH VARCHAR,
    FILE_URL VARCHAR,
    SIZE NUMBER,
    LAST_MODIFIED VARCHAR,
    RAW_VALUE VARIANT,
    CONTENT_CHUNK VARCHAR,
    SUMMARY_CATEGORY VARCHAR
);

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.gold_faq_documents
FROM @BACKUP_STAGE/gold_faq_documents/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- gold_operation_manuals
CREATE OR REPLACE TABLE gold_operation_manuals (
    RELATIVE_PATH VARCHAR,
    FILE_URL VARCHAR,
    SIZE NUMBER,
    LAST_MODIFIED VARCHAR,
    CONTENT_CHUNK VARCHAR
);

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.gold_operation_manuals
FROM @BACKUP_STAGE/gold_operation_manuals/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- gold_sns_mentions_with_product_master
CREATE OR REPLACE TABLE gold_sns_mentions_with_product_master (
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
    PROCESSED_AT VARCHAR,
    PRODUCT_ID VARCHAR,
    PRODUCT_NAME VARCHAR,
    PRODUCT_NAME_EN VARCHAR,
    CATEGORY_L1 VARCHAR,
    CATEGORY_L2 VARCHAR,
    CATEGORY_L3 VARCHAR,
    BRAND VARCHAR,
    SUPPLIER_ID VARCHAR,
    COST_PRICE NUMBER(10,2),
    LIST_PRICE NUMBER(10,2),
    CURRENT_PRICE NUMBER(10,2),
    STOCK_QUANTITY NUMBER,
    PRODUCT_STATUS VARCHAR,
    LAUNCH_DATE DATE,
    DESCRIPTION VARCHAR,
    WEIGHT_G NUMBER,
    DIMENSIONS VARCHAR,
    SIMILARITY FLOAT
);

COPY INTO GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.gold_sns_mentions_with_product_master
FROM @BACKUP_STAGE/gold_sns_mentions_with_product_master/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
