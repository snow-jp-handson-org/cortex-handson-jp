-- ============================================================================
-- チェックポイント（12/16 ハンズオン）
-- ============================================================================
-- ノートブックが途中で止まった場合や、AI 関数の結果をそろえたい場合に使います。
-- setup_20261216.sql の実行後なら、どのアカウントでも講師の準備なしで実行できます。
-- 対象のテーブルは作り直されます（CREATE OR REPLACE）。復元したいパートだけを実行してください。
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE GLACIERSTYLE_WH;
USE SCHEMA GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA;

-- 検証済みの結果（CSV）を GitHub からステージにコピー
ALTER GIT REPOSITORY GIT_INTEGRATION_FOR_HANDSON FETCH;
COPY FILES INTO @DATA_STAGE/checkpoint/
  FROM @GIT_INTEGRATION_FOR_HANDSON/branches/pri_20261216/data/checkpoint/
  PATTERN = '.*[.]csv';

-- 1行目をヘッダーとして読み飛ばすファイル形式
CREATE OR REPLACE FILE FORMAT HANDSON_CHECKPOINT_CSV
  TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- ============================================================================
-- Part 1 完了時点（raw 層）
-- ============================================================================
-- 構造化データは part1_20261216.ipynb と同じ手順でロード
CREATE OR REPLACE FILE FORMAT HANDSON_CSV
  TYPE = CSV PARSE_HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"';
CREATE OR REPLACE TABLE PRODUCTS (
  product_id VARCHAR, product_name VARCHAR, category_l1 VARCHAR, category_l2 VARCHAR, category_l3 VARCHAR,
  brand VARCHAR, list_price NUMBER, cost_price NUMBER, launch_date DATE);
CREATE OR REPLACE TABLE CUSTOMERS (
  customer_id VARCHAR, last_name VARCHAR, first_name VARCHAR, gender VARCHAR, birth_date DATE,
  email VARCHAR, phone VARCHAR, prefecture VARCHAR, city VARCHAR, registration_date DATE, membership_tier VARCHAR);
CREATE OR REPLACE TABLE ORDERS (
  order_id VARCHAR, order_datetime TIMESTAMP_NTZ, customer_id VARCHAR, product_id VARCHAR,
  quantity NUMBER, unit_price NUMBER, discount_amount NUMBER, total_amount NUMBER,
  order_channel VARCHAR, order_status VARCHAR, lot_no VARCHAR);
CREATE OR REPLACE TABLE RAW_CALL_TRANSCRIPTS (
  call_id VARCHAR, call_datetime TIMESTAMP_NTZ, customer_id VARCHAR, order_id VARCHAR,
  product_id VARCHAR, agent_id VARCHAR, call_duration_sec NUMBER, transcript_text VARCHAR);
COPY INTO PRODUCTS FROM @DATA_STAGE FILES = ('products.csv')
  FILE_FORMAT = (FORMAT_NAME = HANDSON_CSV) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
COPY INTO CUSTOMERS FROM @DATA_STAGE FILES = ('customers.csv')
  FILE_FORMAT = (FORMAT_NAME = HANDSON_CSV) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
COPY INTO ORDERS FROM @DATA_STAGE FILES = ('orders.csv')
  FILE_FORMAT = (FORMAT_NAME = HANDSON_CSV) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
COPY INTO RAW_CALL_TRANSCRIPTS FROM @DATA_STAGE FILES = ('call_transcripts.csv')
  FILE_FORMAT = (FORMAT_NAME = HANDSON_CSV) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- AI 関数の結果は、検証済みの CSV から復元（AI 関数は実行しない）
CREATE OR REPLACE TABLE CALL_TRANSCRIPTS AS
SELECT $1::VARCHAR AS call_id, $2::TIMESTAMP_NTZ AS call_datetime, $3::VARCHAR AS customer_id,
       $4::VARCHAR AS order_id, $5::VARCHAR AS product_id, $6::VARCHAR AS transcript_text, $7::VARCHAR AS source
FROM @DATA_STAGE/checkpoint/call_transcripts.csv (FILE_FORMAT => 'HANDSON_CHECKPOINT_CSV');

CREATE OR REPLACE TABLE DOC_PARSED AS
SELECT $1::VARCHAR AS file_name, $2::NUMBER AS page_no, $3::VARCHAR AS page_text
FROM @DATA_STAGE/checkpoint/doc_parsed.csv (FILE_FORMAT => 'HANDSON_CHECKPOINT_CSV');

SELECT 'PRODUCTS' AS table_name, COUNT(*) AS record_count, 55 AS expected FROM PRODUCTS
UNION ALL SELECT 'CUSTOMERS', COUNT(*), 300 FROM CUSTOMERS
UNION ALL SELECT 'ORDERS', COUNT(*), 2913 FROM ORDERS
UNION ALL SELECT 'RAW_CALL_TRANSCRIPTS', COUNT(*), 88 FROM RAW_CALL_TRANSCRIPTS
UNION ALL SELECT 'CALL_TRANSCRIPTS', COUNT(*), 98 FROM CALL_TRANSCRIPTS
UNION ALL SELECT 'DOC_PARSED', COUNT(*), 99 FROM DOC_PARSED;

-- ============================================================================
-- Part 2 完了時点（conform 層）
-- ============================================================================
CREATE OR REPLACE TABLE CALL_ANALYSIS AS
SELECT $1::VARCHAR AS call_id, $2::TIMESTAMP_NTZ AS call_datetime, $3::VARCHAR AS customer_id,
       $4::VARCHAR AS order_id, $5::VARCHAR AS product_id, $6::VARCHAR AS source,
       $7::VARCHAR AS inquiry_type, $8::VARCHAR AS product_name, $9::VARCHAR AS product_category,
       $10::VARCHAR AS sentiment, $11::VARCHAR AS churn_risk, $12::VARCHAR AS summary,
       $13::VARCHAR AS transcript_masked
FROM @DATA_STAGE/checkpoint/call_analysis.csv (FILE_FORMAT => 'HANDSON_CHECKPOINT_CSV');

CREATE OR REPLACE TABLE DOC_ATTRIBUTES AS
SELECT $1::VARCHAR AS file_name, $2::VARCHAR AS title, $3::VARCHAR AS doc_type,
       $4::VARCHAR AS department, $5::VARCHAR AS audience, $6::DATE AS doc_date,
       $7::VARCHAR AS product_category, PARSE_JSON($8)::ARRAY AS product_ids
FROM @DATA_STAGE/checkpoint/doc_attributes.csv (FILE_FORMAT => 'HANDSON_CHECKPOINT_CSV');

CREATE OR REPLACE TABLE KNOWLEDGE_CHUNKS AS
SELECT $1::VARCHAR AS chunk_id, $2::VARCHAR AS chunk_text, $3::VARCHAR AS title,
       $4::VARCHAR AS doc_type, $5::VARCHAR AS department, $6::VARCHAR AS audience,
       $7::DATE AS doc_date, $8::VARCHAR AS product_category,
       PARSE_JSON($9)::ARRAY AS product_ids, $10::VARCHAR AS sentiment
FROM @DATA_STAGE/checkpoint/knowledge_chunks.csv (FILE_FORMAT => 'HANDSON_CHECKPOINT_CSV');

SELECT 'CALL_ANALYSIS' AS table_name, COUNT(*) AS record_count, 98 AS expected FROM CALL_ANALYSIS
UNION ALL SELECT 'DOC_ATTRIBUTES', COUNT(*), 11 FROM DOC_ATTRIBUTES
UNION ALL SELECT 'KNOWLEDGE_CHUNKS', COUNT(*), 200 FROM KNOWLEDGE_CHUNKS;
