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
CREATE OR REPLACE STAGE SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.FILE encryption = (type = 'snowflake_sse') DIRECTORY = (ENABLE = TRUE);
CREATE OR REPLACE STAGE SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.PDF encryption = (type = 'snowflake_sse') DIRECTORY = (ENABLE = TRUE);
CREATE OR REPLACE STAGE SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.SEMANTIC_MODEL_STAGE encryption = (type = 'snowflake_sse') DIRECTORY = (ENABLE = TRUE);


// Step3: 公開されているGitからデータとスクリプトを取得 //

-- Git連携のため、API統合を作成する
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/TadakiKanenari/')
  ENABLED = TRUE;

-- GIT統合の作成
CREATE OR REPLACE GIT REPOSITORY GIT_INTEGRATION_FOR_HANDSON
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/TadakiKanenari/cortex_handson.git';

-- チェックする
ls @GIT_INTEGRATION_FOR_HANDSON/branches/main;

-- Githubからファイルを持ってくる
COPY FILES INTO @SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.FILE FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/data/ PATTERN ='.*\\.csv$';
COPY FILES INTO @SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.PDF FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/data/ PATTERN = '.*\\.pdf$';
COPY FILES INTO @SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.SEMANTIC_MODEL_STAGE FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/handson2/sales_analysis_model.yaml;

// Step4: NotebookとStreamlitを作成 //

-- Streamlit in Snowflakeの作成
CREATE OR REPLACE STREAMLIT sis_snowretail_analysis_minimal
    FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/handson2/minimal
    MAIN_FILE = 'mainpage.py'
    QUERY_WAREHOUSE = COMPUTE_WH;

-- Notebookの作成
CREATE OR REPLACE NOTEBOOK cortex_handson_part1_completed
    FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/handson1
    MAIN_FILE = 'cortex_handson_seminar_part1_completed.ipynb'
    QUERY_WAREHOUSE = COMPUTE_WH
    WAREHOUSE = COMPUTE_WH;


// Step5: 基本テーブルの作成とデータ投入 //

-- CUSTOMER_REVIEWSテーブル（Part2で使用）
CREATE OR REPLACE TABLE CUSTOMER_REVIEWS (
    REVIEW_ID VARCHAR(16777216),
    PRODUCT_ID VARCHAR(16777216),
    CUSTOMER_ID VARCHAR(16777216),
    RATING NUMBER(38,1),
    REVIEW_TEXT VARCHAR(16777216),
    REVIEW_DATE TIMESTAMP_NTZ(9),
    PURCHASE_CHANNEL VARCHAR(16777216),
    HELPFUL_VOTES NUMBER(38,0)
);

-- CUSTOMER_REVIEWSにデータ投入
COPY INTO CUSTOMER_REVIEWS 
FROM @FILE 
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"') 
FILES = ('customer_reviews.csv');


// Step6: Cortex Search Service の作成 //

-- Cortex Search用のウェアハウス作成
CREATE OR REPLACE WAREHOUSE cortex_search_wh WITH WAREHOUSE_SIZE='X-SMALL';

-- RAGチャットボット用のドキュメントテーブル作成
CREATE OR REPLACE TABLE SNOW_RETAIL_DOCUMENTS (
    DOCUMENT_ID VARCHAR(16777216),
    TITLE VARCHAR(16777216),
    CONTENT VARCHAR(16777216),
    DOCUMENT_TYPE VARCHAR(16777216),
    DEPARTMENT VARCHAR(16777216),
    CREATED_AT TIMESTAMP_NTZ(9),
    UPDATED_AT TIMESTAMP_NTZ(9),
    VERSION NUMBER(38,1)
);

-- ドキュメントデータを先に投入
COPY INTO SNOW_RETAIL_DOCUMENTS 
FROM @FILE 
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"') 
FILES = ('snow_retail_documents.csv');

-- Cortex Search Service作成（データが既にあるので即座にインデックス構築）
CREATE OR REPLACE CORTEX SEARCH SERVICE snow_retail_search_service
    ON content
    ATTRIBUTES title, document_type, department
    WAREHOUSE = cortex_search_wh
    TARGET_LAG = '1 day'
    EMBEDDING_MODEL = 'voyage-multilingual-2'
    AS (
        SELECT 
            document_id,
            title,
            content,
            document_type,
            department,
            created_at,
            updated_at,
            version
        FROM SNOW_RETAIL_DOCUMENTS
    );


// Step7: Part1完成データの準備（バックアップ用） //

-- Part1をスキップする場合に備えて、完成版データを「_PREBUILT」テーブルとして準備
-- ハンズオン体験者がPart1を実行する場合は、空の状態から始められます

-- バックアップデータ用ステージの作成
CREATE OR REPLACE STAGE BACKUP_STAGE 
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') 
    DIRECTORY = (ENABLE = TRUE);

-- バックアップCSVをステージにコピー
COPY FILES INTO @BACKUP_STAGE FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/data/backup/ PATTERN = '.*_backup\\.csv$';

-- CSVファイルフォーマット
CREATE OR REPLACE FILE FORMAT BACKUP_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '');

-- PRODUCT_MASTER_PREBUILT（Part1の成果物と同一データ）
CREATE OR REPLACE TABLE PRODUCT_MASTER_PREBUILT AS
SELECT
    $1::string AS product_id,
    $2::string AS product_name,
    $3::integer AS unit_price
FROM @BACKUP_STAGE/product_master_backup.csv
(FILE_FORMAT => BACKUP_CSV_FORMAT);

-- PRODUCT_MASTER_EMBED_PREBUILT（ベクトル埋め込み）
CREATE OR REPLACE TABLE PRODUCT_MASTER_EMBED_PREBUILT AS 
SELECT 
    *, 
    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('multilingual-e5-large', product_name) AS product_name_embed 
FROM PRODUCT_MASTER_PREBUILT;

-- EC_DATA_WITH_PRODUCT_MASTER_PREBUILT（Part1の名寄せ結果と同一データ）
CREATE OR REPLACE TABLE EC_DATA_WITH_PRODUCT_MASTER_PREBUILT AS
SELECT
    $1::string AS product_id_master,
    $2::string AS product_name_master,
    $3::integer AS unit_price_master,
    $4::string AS transaction_id,
    $5::date AS transaction_date,
    $6::string AS product_name,
    $7::integer AS quantity,
    $8::integer AS unit_price,
    $9::integer AS total_price,
    $10::float AS similarity
FROM @BACKUP_STAGE/ec_data_with_product_master_backup.csv
(FILE_FORMAT => BACKUP_CSV_FORMAT);

-- RETAIL_DATA_WITH_PRODUCT_MASTER_PREBUILT（Part1の名寄せ結果と同一データ）
CREATE OR REPLACE TABLE RETAIL_DATA_WITH_PRODUCT_MASTER_PREBUILT AS
SELECT
    $1::string AS product_id_master,
    $2::string AS product_name_master,
    $3::integer AS unit_price_master,
    $4::string AS transaction_id,
    $5::date AS transaction_date,
    $6::string AS product_name,
    $7::integer AS quantity,
    $8::integer AS unit_price,
    $9::integer AS total_price,
    $10::float AS similarity
FROM @BACKUP_STAGE/retail_data_with_product_master_backup.csv
(FILE_FORMAT => BACKUP_CSV_FORMAT);

-- Part1で作成されるテーブルの空スキーマを作成（ハンズオン体験用）
CREATE OR REPLACE TABLE PRODUCT_MASTER LIKE PRODUCT_MASTER_PREBUILT;
CREATE OR REPLACE TABLE PRODUCT_MASTER_EMBED LIKE PRODUCT_MASTER_EMBED_PREBUILT;
CREATE OR REPLACE TABLE EC_DATA_WITH_PRODUCT_MASTER LIKE EC_DATA_WITH_PRODUCT_MASTER_PREBUILT;
CREATE OR REPLACE TABLE RETAIL_DATA_WITH_PRODUCT_MASTER LIKE RETAIL_DATA_WITH_PRODUCT_MASTER_PREBUILT;

SELECT 'Prebuilt tables created, empty target tables ready for Part1' AS status;

-- ★ Part1スキップ時のSWAPはStreamlitアプリが自動実行します ★
-- Part2のStreamlitを開くと、テーブルが空かどうかを検知し、
-- 空の場合は自動的に_PREBUILTテーブルとSWAPします


// Step8: Cortex Agent の作成 //

-- Agent作成権限の付与（同一スキーマ内）
GRANT CREATE AGENT ON SCHEMA SNOWRETAIL_DB.SNOWRETAIL_SCHEMA TO ROLE ACCOUNTADMIN;

-- Cortex Agent の作成
CREATE OR REPLACE AGENT SNOW_RETAIL_AGENT
  COMMENT = 'スノーリテール統合AIアシスタント - ドキュメント検索と売上分析を統合'
  PROFILE = '{"display_name": "スノーリテール AIアシスタント", "color": "blue"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: claude-sonnet-4-5

  instructions:
    response: "日本語で丁寧に回答してください。データ分析結果は分かりやすく説明し、ドキュメント検索結果は根拠を明示してください。"
    orchestration: "売上や商品に関する質問にはAnalystを使用し、ポリシーやFAQに関する質問にはSearchを使用してください。"
    system: "あなたはスノーリテール社の統合AIアシスタントです。売上データの分析と社内ドキュメントの検索を通じて、ユーザーの質問に回答します。"
    sample_questions:
      # 売上データ分析（Cortex Analyst）
      - question: "売上TOP10の商品を教えてください"
        answer: "Analystツールを使用して売上ランキングを分析します。"
      - question: "月別の売上推移を時系列で見せて"
        answer: "Analystツールを使用して月別売上トレンドを分析します。"
      - question: "店舗とECの売上を比較して"
        answer: "Analystツールを使用してチャネル別売上を比較分析します。"
      - question: "商品別の売上ランキングを作って"
        answer: "Analystツールを使用して商品別売上をランキング形式で出力します。"
      # 社内ドキュメント検索（Cortex Search）
      - question: "返品ポリシーについて教えてください"
        answer: "Searchツールを使用して社内ドキュメントから返品ポリシーを検索します。"
      - question: "プライベートブランド商品の特徴について教えてください"
        answer: "Searchツールを使用してPB商品に関するドキュメントを検索します。"
      - question: "ポイントカードの有効期限について教えてください"
        answer: "Searchツールを使用してポイントカードのルールを検索します。"
      - question: "ネットスーパーの配送料金と時間帯について教えてください"
        answer: "Searchツールを使用してネットスーパーのサービス情報を検索します。"
      - question: "スノーリテールの基本理念について教えてください"
        answer: "Searchツールを使用して企業理念に関するドキュメントを検索します。"
      - question: "顧客満足度向上のための取り組みについて教えてください"
        answer: "Searchツールを使用してCS向上施策に関するドキュメントを検索します。"

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "SalesAnalyst"
        description: "売上データや商品データに関する質問をSQL分析します"
    - tool_spec:
        type: "cortex_search"
        name: "DocumentSearch"
        description: "社内ドキュメント、FAQ、ポリシーを検索します"

  tool_resources:
    SalesAnalyst:
      semantic_model_file: "@SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.SEMANTIC_MODEL_STAGE/sales_analysis_model.yaml"
    DocumentSearch:
      name: "SNOWRETAIL_DB.SNOWRETAIL_SCHEMA.SNOW_RETAIL_SEARCH_SERVICE"
      max_results: "5"
      title_column: "title"
      id_column: "document_id"
  $$;

SELECT 'setup successfully' AS status;


// =========================================================
// セットアップ完了
// =========================================================
-- 以下の順序でハンズオンを進めてください:
-- 1. Part1: cortex_handson_part1 ノートブックを実行
-- 2. Part2: sis_snowretail_analysis_dev Streamlitアプリを使用
-- 
-- ★Part1をスキップする場合:
-- Part2のアプリは自動的にフォールバックテーブルを参照するため、
-- Part1を実行しなくてもPart2を体験できます。
