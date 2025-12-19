/*
================================================================================
Snowflake EC Analytics - 完全自動セットアップスクリプト
================================================================================

【概要】
このスクリプトは、ECサイト分析用のデータベース環境を完全自動で構築します。

【処理内容】
1. データベースとスキーマの作成
2. ステージの作成（データ格納用）
3. GitHub連携の設定（API統合とGitリポジトリ）
4. GitHubからCSVデータの自動取得
5. 5つのテーブルの作成
6. CSVデータの一括インポート

【生成されるテーブル】
┌────────────────────────────────────────────────────────────┐
│ ディメンションテーブル（マスタデータ）                      │
├────────────────────────────────────────────────────────────┤
│ 1. dim_customers   - 顧客マスタ（100件）                   │
│ 2. dim_products    - 商品マスタ（576件）                   │
│                                                             │
│ ファクトテーブル（トランザクションデータ）                  │
├────────────────────────────────────────────────────────────┤
│ 3. fact_orders     - EC取引データ（500件）                 │
│ 4. fact_payments   - クレジット決済情報（360件）           │
│ 5. fact_web_logs   - Webアクセスログ（14,532件）           │
└────────────────────────────────────────────────────────────┘

【データソース】
GitHub Repository: https://github.com/snow-jp-handson-org/cortex-handson-jp

【実行方法】
このスクリプト全体を選択してSnowflakeで実行してください。
すべての処理が自動的に完了します。

【所要時間】
約2-3分（データのダウンロードとインポートを含む）

================================================================================
*/

-- ============================================================================
-- Step 1: 環境設定
-- ============================================================================
-- 管理者ロールとコンピュートウェアハウスを使用
USE ROLE ACCOUNTADMIN;
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

-- GitHubのdataディレクトリからすべてのCSVファイルをステージにコピー
COPY FILES 
  INTO @GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.DATA_STAGE 
  FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/data/ 
  PATTERN = '.*\\.csv$';

-- ステージ内のファイルを確認
ls @GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.DATA_STAGE;

SELECT '【Step 5】GitHubからのデータ取得が完了しました' AS status;


-- ============================================================================
-- Step 6: テーブル定義の作成
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 6-1. 顧客マスタテーブル（dim_customers）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_customers (
    customer_id VARCHAR PRIMARY KEY,              -- 顧客ID（主キー）
    email VARCHAR,                                 -- メールアドレス
    phone VARCHAR,                                 -- 電話番号
    last_name VARCHAR,                             -- 姓
    first_name VARCHAR,                            -- 名
    gender VARCHAR,                                -- 性別
    birth_date DATE,                               -- 生年月日
    postal_code VARCHAR,                           -- 郵便番号
    prefecture VARCHAR,                            -- 都道府県
    city VARCHAR,                                  -- 市区町村
    address VARCHAR,                               -- 住所
    registration_date DATE,                        -- 会員登録日
    membership_tier VARCHAR,                       -- 会員ランク
    total_orders INTEGER,                          -- 累計注文数
    total_spent DECIMAL(12,2),                     -- 累計購入金額
    last_order_date DATE,                          -- 最終注文日
    email_opt_in BOOLEAN,                          -- メール配信許諾
    app_installed BOOLEAN                          -- アプリインストール状況
);

-- ----------------------------------------------------------------------------
-- 6-2. 商品マスタテーブル（dim_products）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_products (
    product_id VARCHAR PRIMARY KEY,                -- 商品ID（主キー）
    product_name VARCHAR,                          -- 商品名（日本語）
    product_name_en VARCHAR,                       -- 商品名（英語）
    category_l1 VARCHAR,                           -- 大カテゴリ
    category_l2 VARCHAR,                           -- 中カテゴリ
    category_l3 VARCHAR,                           -- 小カテゴリ
    brand VARCHAR,                                 -- ブランド
    supplier_id VARCHAR,                           -- 仕入先ID
    cost_price DECIMAL(10,2),                      -- 仕入原価
    list_price DECIMAL(10,2),                      -- 定価
    current_price DECIMAL(10,2),                   -- 現在販売価格
    stock_quantity INTEGER,                        -- 在庫数量
    product_status VARCHAR,                        -- 商品ステータス
    launch_date DATE,                              -- 発売日
    description TEXT,                              -- 商品説明
    weight_g INTEGER,                              -- 重量（グラム）
    dimensions VARCHAR                             -- サイズ
);

-- ----------------------------------------------------------------------------
-- 6-3. EC取引データテーブル（fact_orders）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE fact_orders (
    order_id VARCHAR PRIMARY KEY,                  -- 注文ID（主キー）
    order_datetime TIMESTAMP,                      -- 注文日時
    customer_id VARCHAR,                           -- 顧客ID（外部キー）
    product_id VARCHAR,                            -- 商品ID（外部キー）
    quantity INTEGER,                              -- 購入数量
    unit_price DECIMAL(10,2),                      -- 単価（税抜）
    discount_amount DECIMAL(10,2),                 -- 割引額
    tax_amount DECIMAL(10,2),                      -- 消費税額
    total_amount DECIMAL(10,2),                    -- 合計金額（税込）
    payment_method VARCHAR,                        -- 支払方法
    shipping_address_id VARCHAR,                   -- 配送先ID
    order_channel VARCHAR,                         -- 注文チャネル（web/app/store）
    campaign_id VARCHAR,                           -- キャンペーンID
    order_status VARCHAR                           -- 注文ステータス
);

-- ----------------------------------------------------------------------------
-- 6-4. クレジット決済情報テーブル（fact_payments）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE fact_payments (
    payment_id VARCHAR PRIMARY KEY,                -- 決済ID（主キー）
    order_id VARCHAR,                              -- 注文ID（外部キー）
    payment_datetime TIMESTAMP,                    -- 決済日時
    card_brand VARCHAR,                            -- カードブランド
    card_last4 VARCHAR,                            -- カード番号下4桁
    payment_amount DECIMAL(10,2),                  -- 決済金額
    authorization_code VARCHAR,                    -- オーソリコード
    payment_status VARCHAR,                        -- 決済ステータス
    fraud_score DECIMAL(5,2),                      -- 不正スコア（0-100）
    device_fingerprint VARCHAR,                    -- デバイスフィンガープリント
    ip_address VARCHAR,                            -- IPアドレス
    billing_country VARCHAR                        -- 請求国
);

-- ----------------------------------------------------------------------------
-- 6-5. Webアクセスログテーブル（fact_web_logs）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE fact_web_logs (
    log_id VARCHAR PRIMARY KEY,                    -- ログID（主キー）
    session_id VARCHAR,                            -- セッションID
    customer_id VARCHAR,                           -- 顧客ID（外部キー、ログイン時のみ）
    event_timestamp TIMESTAMP,                     -- イベント発生日時
    event_type VARCHAR,                            -- イベント種別
    page_url VARCHAR,                              -- ページURL
    page_category VARCHAR,                         -- ページカテゴリ
    referrer_url VARCHAR,                          -- 参照元URL
    utm_source VARCHAR,                            -- 流入元
    utm_medium VARCHAR,                            -- 流入媒体
    utm_campaign VARCHAR,                          -- キャンペーン名
    device_type VARCHAR,                           -- デバイス種別
    browser VARCHAR,                               -- ブラウザ
    os VARCHAR,                                    -- OS
    time_on_page INTEGER,                          -- ページ滞在時間（秒）
    product_id VARCHAR                             -- 商品ID（商品ページの場合）
);

SELECT '【Step 6】テーブル定義の作成が完了しました' AS status;


-- ============================================================================
-- Step 7: CSVデータの一括インポート
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 7-1. 顧客マスタのインポート（100件）
-- ----------------------------------------------------------------------------
COPY INTO dim_customers 
  FROM @DATA_STAGE/customers.csv 
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- ----------------------------------------------------------------------------
-- 7-2. 商品マスタのインポート（576件）
-- ----------------------------------------------------------------------------
COPY INTO dim_products 
  FROM @DATA_STAGE/products.csv 
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- ----------------------------------------------------------------------------
-- 7-3. EC取引データのインポート（500件）
-- ----------------------------------------------------------------------------
COPY INTO fact_orders 
  FROM @DATA_STAGE/orders.csv 
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- ----------------------------------------------------------------------------
-- 7-4. クレジット決済情報のインポート（360件）
-- ----------------------------------------------------------------------------
COPY INTO fact_payments 
  FROM @DATA_STAGE/payments.csv 
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- ----------------------------------------------------------------------------
-- 7-5. Webアクセスログのインポート（14,532件）
-- ----------------------------------------------------------------------------
COPY INTO fact_web_logs 
  FROM @DATA_STAGE/web_logs.csv 
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

SELECT '【Step 7】CSVデータのインポートが完了しました' AS status;


-- ============================================================================
-- Step 8: データ確認
-- ============================================================================

-- 各テーブルのレコード数を確認
SELECT 'dim_customers' AS table_name, COUNT(*) AS record_count FROM dim_customers
UNION ALL
SELECT 'dim_products' AS table_name, COUNT(*) AS record_count FROM dim_products
UNION ALL
SELECT 'fact_orders' AS table_name, COUNT(*) AS record_count FROM fact_orders
UNION ALL
SELECT 'fact_payments' AS table_name, COUNT(*) AS record_count FROM fact_payments
UNION ALL
SELECT 'fact_web_logs' AS table_name, COUNT(*) AS record_count FROM fact_web_logs;


-- ============================================================================
-- 完了メッセージ
-- ============================================================================
SELECT '
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎉 セットアップが完了しました！
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ データベース: GLACIERSTYLE_DB
✅ スキーマ: EC_ANALYTICS_SCHEMA
✅ テーブル数: 5個
✅ 総レコード数: 15,568件

次のステップ:
1. データの確認: SELECT * FROM dim_customers LIMIT 10;
2. 分析の開始: Snowflake Cortex Analystなどで分析可能
3. ダッシュボード作成: BIツールと接続

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
' AS "🎉 セットアップ完了";