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
4. GitHubからデータファイルの自動取得
5. 10個のテーブルの作成
6. データの一括インポートとAI解析

【生成されるテーブル】
┌──────────────────────────────────────────────────────────────┐
│ ディメンションテーブル（マスタデータ）                        │
├──────────────────────────────────────────────────────────────┤
│ 1. dim_customers             - 顧客マスタ（100件）           │
│ 2. dim_products              - 商品マスタ（576件）           │
│                                                               │
│ ファクトテーブル（トランザクションデータ）                    │
├──────────────────────────────────────────────────────────────┤
│ 3. fact_orders               - EC取引データ（500件）         │
│ 4. fact_payments             - 決済情報（360件）             │
│ 5. fact_web_logs             - Webログ（14,532件）           │
│                                                               │
│ 非構造化データテーブル（AI解析済み）                          │
├──────────────────────────────────────────────────────────────┤
│ 6. raw_sns_mentions          - SNS投稿（300件）              │
│ 7. raw_voice_logs            - 音声ログメタ（10件）          │
│ 8. raw_voice_messages        - 音声文字起こし（10件）        │
│ 9. raw_ad_creatives          - 広告データ（15件）            │
│10. raw_faq_documents_parsed  - FAQ（PDF解析済み）            │
│11. raw_operation_manuals_parsed - マニュアル（PDF解析済み）  │
└──────────────────────────────────────────────────────────────┘

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
-- ls @GIT_INTEGRATION_FOR_HANDSON/branches/main;

ls @GIT_INTEGRATION_FOR_HANDSON/branches/tmp_new_version_2026;

-- GitHubのdataディレクトリからすべてのファイルをステージにコピー
COPY FILES 
  INTO @GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.DATA_STAGE 
  FROM @GIT_INTEGRATION_FOR_HANDSON/branches/tmp_new_version_2026/data/;
  -- FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/data/;

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

-- ----------------------------------------------------------------------------
-- 6-6. SNS生ログテーブル（raw_sns_mentions）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw_sns_mentions (
    post_id VARCHAR PRIMARY KEY,                   -- 投稿ID（主キー）
    platform VARCHAR,                              -- SNSプラットフォーム（twitter / instagram / facebook）
    post_type VARCHAR,                             -- 投稿の種類（post / mention / reply）
    username VARCHAR,                              -- 投稿者ID
    display_name VARCHAR,                          -- 投稿者ハンドル
    content VARCHAR,                               -- 投稿本文
    posted_at TIMESTAMP,                           -- 投稿日時
    likes INTEGER,                                 -- いいね数
    retweets INTEGER,                              -- RT/シェア数
    replies INTEGER,                               -- コメント数
    hashtags ARRAY,                                -- ハッシュタグ
    mentioned_products ARRAY,                      -- 対象商品
    media_urls ARRAY                               -- 添付メディアURL
);

-- ----------------------------------------------------------------------------
-- 6-7. カスタマー音声ログテーブル（raw_voice_logs）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw_voice_logs (
    call_id VARCHAR PRIMARY KEY,                   -- 通話ID（主キー）
    scenario_id VARCHAR,                           -- シナリオID
    audio_file VARCHAR,                            -- 音声ファイルパス
    call_duration_sec NUMBER(10,2),                -- 通話時間（秒）
    call_start_time TIMESTAMP,                     -- 通話開始日時
    call_end_time TIMESTAMP,                       -- 通話終了日時
    category VARCHAR,                              -- 問い合わせ種別
    agent_id VARCHAR,                              -- オペレーターID
    customer_phone VARCHAR,                        -- 顧客電話番号
    customer_id VARCHAR,                           -- 顧客ID（紐付け済みの場合）
    call_type VARCHAR,                             -- 通話種別（inbound / outbound）
    transcribed_text TEXT                          -- 文字起こしテキスト
);

-- ----------------------------------------------------------------------------
-- 6-8. 広告クリエイティブテーブル（raw_ad_creatives）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw_ad_creatives (
    creative_id VARCHAR PRIMARY KEY,               -- クリエイティブID（主キー）
    campaign_id VARCHAR,                           -- キャンペーンID
    creative_name VARCHAR,                         -- クリエイティブ名
    creative_type VARCHAR,                         -- 種別（image/video/carousel）
    image_file_path VARCHAR,                       -- 画像ファイルパス
    copy_text TEXT,                                -- 広告コピー
    headline VARCHAR,                              -- 見出し
    cta_text VARCHAR,                              -- CTAテキスト
    target_segment VARCHAR,                        -- ターゲットセグメント
    platform VARCHAR,                              -- 配信プラットフォーム
    start_date DATE,                               -- 配信開始日
    end_date DATE,                                 -- 配信終了日
    impressions INTEGER,                           -- インプレッション数
    clicks INTEGER,                                -- クリック数
    conversions INTEGER,                           -- コンバージョン数
    spend DECIMAL(10,2)                            -- 広告費
);

-- ----------------------------------------------------------------------------
-- 6-9. FAQドキュメントテーブル（raw_faq_documents）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw_faq_documents (
    faq_id VARCHAR PRIMARY KEY,                    -- FAQID（主キー）
    document_path VARCHAR,                         -- ドキュメントパス
    category VARCHAR,                              -- カテゴリ
    question TEXT,                                 -- 質問
    answer TEXT,                                   -- 回答
    last_updated DATE,                             -- 最終更新日
    view_count INTEGER,                            -- 閲覧数
    helpful_count INTEGER,                         -- 役に立った数
    version VARCHAR                                -- バージョン
);

-- ----------------------------------------------------------------------------
-- 6-10. 運営マニュアルテーブル（raw_operation_manuals）
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw_operation_manuals (
    manual_id VARCHAR PRIMARY KEY,                 -- マニュアルID（主キー）
    document_path VARCHAR,                         -- ドキュメントパス
    title VARCHAR,                                 -- タイトル
    department VARCHAR,                            -- 対象部門
    chapter VARCHAR,                               -- 章
    section VARCHAR,                               -- 節
    content TEXT,                                  -- 本文
    last_updated DATE,                             -- 最終更新日
    version VARCHAR,                               -- バージョン
    confidentiality VARCHAR                        -- 機密レベル
);

SELECT '【Step 6】テーブル定義の作成が完了しました（全10テーブル + AI解析用1テーブル）' AS status;


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
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

INSERT INTO fact_web_logs (
    log_id, 
    session_id, 
    customer_id, 
    event_timestamp, 
    event_type, 
    page_url,
    page_category,
    referrer_url,
    utm_source,
    utm_medium,
    utm_campaign,
    device_type,
    browser,
    os,
    time_on_page,
    product_id
)
SELECT 
    $1:log_id::VARCHAR,
    $1:session_id::VARCHAR,
    $1:customer_id::VARCHAR,
    $1:event_timestamp::TIMESTAMP,
    $1:event_type::VARCHAR,
    $1:page_url::VARCHAR,
    $1:page_category::VARCHAR,
    $1:referrer_url::VARCHAR,
    $1:utm_source::VARCHAR,
    $1:utm_medium::VARCHAR,
    $1:utm_campaign::VARCHAR,
    $1:device_type::VARCHAR,
    $1:browser::VARCHAR,
    $1:os::VARCHAR,
    $1:time_on_page::VARCHAR,
    $1:product_id::VARCHAR
FROM @DATA_STAGE/web_logs.json
(FILE_FORMAT => JSON_FORMAT);

-- ----------------------------------------------------------------------------
-- 7-6. SNS生ログのインポート（300件）
-- ----------------------------------------------------------------------------
INSERT INTO raw_sns_mentions (
    post_id,
    platform,
    post_type,
    username,
    display_name,
    content,
    posted_at,
    likes,
    retweets,
    replies,
    hashtags,
    mentioned_products,
    media_urls
)
SELECT 
    $1:post_id::VARCHAR,
    $1:platform::VARCHAR,
    $1:post_type::VARCHAR,
    $1:username::VARCHAR,
    $1:display_name::VARCHAR,
    $1:content::VARCHAR,
    $1:posted_at::TIMESTAMP,
    $1:likes::INTEGER,
    $1:retweets::INTEGER,
    $1:replies::INTEGER,
    $1:hashtags::ARRAY,
    $1:mentioned_products::ARRAY,
    $1:media_urls::ARRAY
FROM @DATA_STAGE/sns_logs.json
(FILE_FORMAT => JSON_FORMAT);

-- ----------------------------------------------------------------------------
-- 7-7. カスタマー音声ログのインポート（10件）
-- ----------------------------------------------------------------------------
INSERT INTO raw_voice_logs (
    call_id, 
    scenario_id,
    audio_file,
    call_duration_sec,
    call_start_time, 
    call_end_time,
    category,
    agent_id,
    customer_phone,
    customer_id,
    call_type
)
SELECT
    $1:call_id::VARCHAR, 
    $1:scenario_id::VARCHAR,
    $1:audio_file::VARCHAR,
    $1:call_duration_sec::NUMBER(10,2),
    $1:call_start_time::TIMESTAMP, 
    $1:call_end_time::TIMESTAMP,
    $1:category::VARCHAR,
    $1:agent_id::VARCHAR,
    $1:customer_phone::VARCHAR,
    $1:customer_id::VARCHAR,
    $1:call_type::VARCHAR
FROM @DATA_STAGE/voice_logs/voice_logs_metadata.json
(FILE_FORMAT => JSON_FORMAT);

-- 音声ファイルの文字起こし（AI_TRANSCRIBE使用）
MERGE INTO raw_voice_logs AS target
USING (
    SELECT 
        SPLIT_PART(relative_path, '/', -1) AS file_name,
        AI_TRANSCRIBE(
            TO_FILE('@DATA_STAGE', relative_path)
        ):text::TEXT AS transcribed_text
    FROM DIRECTORY(@DATA_STAGE)
    WHERE REGEXP_LIKE(relative_path, 'voice_logs.*\\.mp3', 'i')
) AS source
ON target.audio_file = source.file_name
WHEN MATCHED THEN
    UPDATE SET target.transcribed_text = source.transcribed_text;

-- ----------------------------------------------------------------------------
-- 7-8. 広告クリエイティブのインポート（15件）
-- ----------------------------------------------------------------------------
COPY INTO raw_ad_creatives 
  FROM @DATA_STAGE/ad_creatives.csv 
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- ----------------------------------------------------------------------------
-- 7-9. FAQドキュメントの解析とインポート
-- ----------------------------------------------------------------------------
-- ステージをリフレッシュして最新のファイルを認識
ALTER STAGE DATA_STAGE REFRESH;

-- FAQドキュメント（PDF）をAI_PARSE_DOCUMENTで解析し、マークダウンヘッダーで分割
CREATE OR REPLACE TABLE raw_faq_documents_parsed AS
WITH parsed_doc AS (
    SELECT 
        *, 
        AI_PARSE_DOCUMENT(
            TO_FILE('@DATA_STAGE', relative_path),
            {'mode': 'LAYOUT', 'page_split': false}
        ) AS contents
    FROM DIRECTORY(@DATA_STAGE)
    WHERE LOWER(relative_path) = 'faq_document.pdf'
) 
SELECT 
    t.relative_path,
    t.file_url,
    t.size,
    t.last_modified,
    t2.value AS raw_value,
    t2.value:headers:header_1::VARCHAR AS category,
    t2.value:headers:header_2::VARCHAR AS subcategory,
    t2.value:headers:header_3::VARCHAR AS question,
    t2.value:headers:header_4::VARCHAR AS detail,
    t2.value:chunk::TEXT AS content_chunk
FROM parsed_doc t,
LATERAL FLATTEN(INPUT => 
    SNOWFLAKE.CORTEX.SPLIT_TEXT_MARKDOWN_HEADER(
        t.contents:content, 
        OBJECT_CONSTRUCT('#', 'header_1', '##', 'header_2', '###', 'header_3', '####', 'header_4'),
        10000
    )
) t2;

-- ----------------------------------------------------------------------------
-- 7-10. 運営マニュアルの解析とインポート
-- ----------------------------------------------------------------------------
-- 運営マニュアル（PDF）をAI_PARSE_DOCUMENTで解析し、マークダウンヘッダーで分割
CREATE OR REPLACE TABLE raw_operation_manuals_parsed AS
WITH parsed_doc AS (
    SELECT 
        *, 
        AI_PARSE_DOCUMENT(
            TO_FILE('@DATA_STAGE', relative_path),
            {'mode': 'LAYOUT', 'page_split': false}
        ) AS contents
    FROM DIRECTORY(@DATA_STAGE)
    WHERE LOWER(relative_path) = 'operation_manual.pdf'
) 
SELECT 
    t.relative_path,
    t.file_url,
    t.size,
    t.last_modified,
    t2.value AS raw_value,
    t2.value:headers:header_1::VARCHAR AS department,
    t2.value:headers:header_2::VARCHAR AS chapter,
    t2.value:headers:header_3::VARCHAR AS section,
    t2.value:headers:header_4::VARCHAR AS subsection,
    t2.value:chunk::TEXT AS content_chunk
FROM parsed_doc t,
LATERAL FLATTEN(INPUT => 
    SNOWFLAKE.CORTEX.SPLIT_TEXT_MARKDOWN_HEADER(
        t.contents:content, 
        OBJECT_CONSTRUCT('#', 'header_1', '##', 'header_2', '###', 'header_3', '####', 'header_4'),
        10000
    )
) t2;

SELECT '【Step 7】全データのインポートが完了しました' AS status;


-- ============================================================================
-- Step 8: データ確認
-- ============================================================================

-- 各テーブルのレコード数を確認
SELECT 
    'dim_customers' AS table_name, 
    COUNT(*) AS record_count,
    '顧客マスタ' AS description
FROM dim_customers
UNION ALL
SELECT 
    'dim_products' AS table_name, 
    COUNT(*) AS record_count,
    '商品マスタ' AS description
FROM dim_products
UNION ALL
SELECT 
    'fact_orders' AS table_name, 
    COUNT(*) AS record_count,
    'EC取引データ' AS description
FROM fact_orders
UNION ALL
SELECT 
    'fact_payments' AS table_name, 
    COUNT(*) AS record_count,
    'クレジット決済情報' AS description
FROM fact_payments
UNION ALL
SELECT 
    'fact_web_logs' AS table_name, 
    COUNT(*) AS record_count,
    'Webアクセスログ' AS description
FROM fact_web_logs
UNION ALL
SELECT 
    'raw_sns_mentions' AS table_name, 
    COUNT(*) AS record_count,
    'SNS生ログ' AS description
FROM raw_sns_mentions
UNION ALL
SELECT 
    'raw_voice_logs' AS table_name, 
    COUNT(*) AS record_count,
    'カスタマー音声ログ' AS description
FROM raw_voice_logs
UNION ALL
SELECT 
    'raw_ad_creatives' AS table_name, 
    COUNT(*) AS record_count,
    '広告クリエイティブ' AS description
FROM raw_ad_creatives
UNION ALL
SELECT 
    'raw_faq_documents_parsed' AS table_name, 
    COUNT(*) AS record_count,
    'FAQドキュメント（解析済み）' AS description
FROM raw_faq_documents_parsed
UNION ALL
SELECT 
    'raw_operation_manuals_parsed' AS table_name, 
    COUNT(*) AS record_count,
    '運営マニュアル（解析済み）' AS description
FROM raw_operation_manuals_parsed
ORDER BY table_name;


-- ============================================================================
-- 完了メッセージ
-- ============================================================================
SELECT '
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎉 セットアップが完了しました！
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ データベース: GLACIERSTYLE_DB
✅ スキーマ: EC_ANALYTICS_SCHEMA
✅ テーブル数: 11個（構造化データ5 + 非構造化データ6）

【構造化データ】
  • dim_customers (100件) - 顧客マスタ
  • dim_products (576件) - 商品マスタ
  • fact_orders (500件) - EC取引データ
  • fact_payments (360件) - クレジット決済情報
  • fact_web_logs (14,532件) - Webアクセスログ

【非構造化データ（AI解析済み）】
  • raw_sns_mentions (300件) - SNS投稿データ
  • raw_voice_logs (10件) - 音声ログメタデータ
  • raw_voice_messages (10件) - 音声文字起こしデータ
  • raw_ad_creatives (15件) - 広告クリエイティブ
  • raw_faq_documents_parsed - FAQドキュメント（PDF解析済み）
  • raw_operation_manuals_parsed - 運営マニュアル（PDF解析済み）

次のステップ:
1. データの確認: SELECT * FROM dim_customers LIMIT 10;
2. AI機能の活用: Snowflake Cortex AIで分析を開始
3. Cortex Analystでの対話的分析
4. ダッシュボード作成: BIツールと接続

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
' AS "🎉 セットアップ完了";