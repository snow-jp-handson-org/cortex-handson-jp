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
6. 構造化データ（CSV）のロード
7. ディメンションテーブル・ファクトテーブルへのメタデータ（コメント）付与
8. Snowflake Intelligence オブジェクトの作成（CoWork で Agent を使うため）

【データソース】
GitHub Repository: https://github.com/snow-jp-handson-org/cortex-handson-jp（ブランチ pri_20261216）

【実行方法】
このスクリプト全体を選択して実行してください。何度実行しても、作成済みのテーブルやデータは消えません
（ロード済みの CSV は COPY INTO が自動でスキップします）。

【所要時間】
約1分

【次のステップ】
セットアップ完了後、part1_20261216.ipynb で CoCo にデータについて質問し、非構造化データを取り込みます。
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
-- Step 6: 構造化データ（CSV）のロード
-- ============================================================================
-- CSV のヘッダー行を列名として使うファイル形式
CREATE FILE FORMAT IF NOT EXISTS HANDSON_CSV
    TYPE = CSV
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- 商品マスタ
CREATE TABLE IF NOT EXISTS DIM_PRODUCTS (
    product_id VARCHAR, product_name VARCHAR,
    category_l1 VARCHAR, category_l2 VARCHAR, category_l3 VARCHAR,
    brand VARCHAR, list_price NUMBER, cost_price NUMBER, launch_date DATE
);

-- 顧客マスタ
CREATE TABLE IF NOT EXISTS DIM_CUSTOMERS (
    customer_id VARCHAR, last_name VARCHAR, first_name VARCHAR, gender VARCHAR,
    birth_date DATE, email VARCHAR, phone VARCHAR,
    prefecture VARCHAR, city VARCHAR, registration_date DATE, membership_tier VARCHAR
);

-- 注文データ（lot_no は商品の入荷ロット）
CREATE TABLE IF NOT EXISTS FACT_ORDERS (
    order_id VARCHAR, order_datetime TIMESTAMP_NTZ, customer_id VARCHAR, product_id VARCHAR,
    quantity NUMBER, unit_price NUMBER, discount_amount NUMBER, total_amount NUMBER,
    order_channel VARCHAR, order_status VARCHAR, lot_no VARCHAR
);

-- 文字起こし済みの通話
CREATE TABLE IF NOT EXISTS RAW_CALL_TRANSCRIPTS (
    call_id VARCHAR, call_datetime TIMESTAMP_NTZ, customer_id VARCHAR, order_id VARCHAR,
    product_id VARCHAR, agent_id VARCHAR, call_duration_sec NUMBER, transcript_text VARCHAR
);

-- CSV の列名とテーブルの列名を対応させてロード（ロード済みのファイルはスキップ）
COPY INTO DIM_PRODUCTS FROM @DATA_STAGE FILES = ('products.csv')
    FILE_FORMAT = (FORMAT_NAME = HANDSON_CSV) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
COPY INTO DIM_CUSTOMERS FROM @DATA_STAGE FILES = ('customers.csv')
    FILE_FORMAT = (FORMAT_NAME = HANDSON_CSV) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
COPY INTO FACT_ORDERS FROM @DATA_STAGE FILES = ('orders.csv')
    FILE_FORMAT = (FORMAT_NAME = HANDSON_CSV) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
COPY INTO RAW_CALL_TRANSCRIPTS FROM @DATA_STAGE FILES = ('call_transcripts.csv')
    FILE_FORMAT = (FORMAT_NAME = HANDSON_CSV) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- 件数を確認（商品 55、顧客 300、注文 2,913、通話 88）
SELECT 'DIM_PRODUCTS' AS table_name, COUNT(*) AS record_count FROM DIM_PRODUCTS
UNION ALL SELECT 'DIM_CUSTOMERS', COUNT(*) FROM DIM_CUSTOMERS
UNION ALL SELECT 'FACT_ORDERS', COUNT(*) FROM FACT_ORDERS
UNION ALL SELECT 'RAW_CALL_TRANSCRIPTS', COUNT(*) FROM RAW_CALL_TRANSCRIPTS;

SELECT '【Step 6】構造化データのロードが完了しました' AS status;


-- ============================================================================
-- Step 7: ディメンションテーブル・ファクトテーブルへのメタデータ付与
-- ============================================================================
-- テーブルと列に日本語の説明（コメント）を付けておくと、CoCo や Cortex Agent が
-- 「どの列が何を表すか」を正しく理解して SQL を書けるようになります

-- 商品マスタ
ALTER TABLE DIM_PRODUCTS SET COMMENT = 'GlacierStyle ECサイトの商品マスタ。1行が1商品（55件）。カテゴリは3階層で、ブランドはすべて GlacierStyle Original';
ALTER TABLE DIM_PRODUCTS ALTER
    COLUMN product_id COMMENT '商品ID（例: PRD-LGT-0001）。FACT_ORDERS.product_id と結合するキー',
    COLUMN product_name COMMENT '商品名',
    COLUMN category_l1 COMMENT '大カテゴリ（インテリア / キッチン / ライフスタイル雑貨 / アパレル）',
    COLUMN category_l2 COMMENT '中カテゴリ（例: 照明、家具、調理器具、収納、食器）',
    COLUMN category_l3 COMMENT '小カテゴリ（例: デスクライト）',
    COLUMN brand COMMENT 'ブランド名',
    COLUMN list_price COMMENT '定価（円・税抜）',
    COLUMN cost_price COMMENT '原価（円）',
    COLUMN launch_date COMMENT '発売日';

-- 顧客マスタ
ALTER TABLE DIM_CUSTOMERS SET COMMENT = 'GlacierStyle ECサイトの顧客マスタ。1行が1顧客（300件）。氏名・連絡先・居住地・会員ランクを持つ';
ALTER TABLE DIM_CUSTOMERS ALTER
    COLUMN customer_id COMMENT '顧客ID（例: CUS-000001）。FACT_ORDERS.customer_id と結合するキー',
    COLUMN last_name COMMENT '姓',
    COLUMN first_name COMMENT '名',
    COLUMN gender COMMENT '性別（female / male）',
    COLUMN birth_date COMMENT '生年月日',
    COLUMN email COMMENT 'メールアドレス',
    COLUMN phone COMMENT '電話番号',
    COLUMN prefecture COMMENT '居住都道府県',
    COLUMN city COMMENT '居住市区町村',
    COLUMN registration_date COMMENT '会員登録日',
    COLUMN membership_tier COMMENT '会員ランク（standard < silver < gold < platinum）';

-- 注文データ
ALTER TABLE FACT_ORDERS SET COMMENT = 'GlacierStyle ECサイトの注文明細。1行が1注文（2,913件、2026年6月〜11月）。顧客・商品・販売チャネル・注文ステータス・入荷ロットを持つ';
ALTER TABLE FACT_ORDERS ALTER
    COLUMN order_id COMMENT '注文ID（例: ORD-2026-000401）',
    COLUMN order_datetime COMMENT '注文日時',
    COLUMN customer_id COMMENT '注文した顧客のID。DIM_CUSTOMERS.customer_id と結合',
    COLUMN product_id COMMENT '注文された商品のID。DIM_PRODUCTS.product_id と結合',
    COLUMN quantity COMMENT '数量',
    COLUMN unit_price COMMENT '販売単価（円・税抜）',
    COLUMN discount_amount COMMENT '値引き額（円）',
    COLUMN total_amount COMMENT '支払総額（円・税込）。売上の集計にはこの列を使う',
    COLUMN order_channel COMMENT '販売チャネル（web: Webサイト / app: スマホアプリ / store: 実店舗）',
    COLUMN order_status COMMENT '注文ステータス（completed: 完了 / returned: 返品 / cancelled: キャンセル）',
    COLUMN lot_no COMMENT '商品の入荷ロット番号（例: LOT-2609-B）。記録のある注文のみ値が入り、それ以外は NULL';

SELECT '【Step 7】メタデータの付与が完了しました' AS status;


-- ============================================================================
-- Step 8: Snowflake Intelligence オブジェクトの作成
-- ============================================================================
-- CoWork で Cortex Agent を使うためのオブジェクト（アカウントに1つ）
CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

SELECT '【Step 8】Snowflake Intelligence オブジェクトの作成が完了しました' AS status;


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
テーブル: DIM_PRODUCTS / DIM_CUSTOMERS / FACT_ORDERS（コメント付与済み）、RAW_CALL_TRANSCRIPTS
GitHub 連携: GIT_INTEGRATION_FOR_HANDSON
Snowflake Intelligence: SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT

【次のステップ】
part1_20261216.ipynb を開き、CoCo にデータについて質問してみましょう。
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
' AS "セットアップ完了";
