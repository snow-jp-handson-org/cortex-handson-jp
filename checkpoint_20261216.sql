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
-- Part 1 完了時点（Bronze 層: RAW_CALL_TRANSCRIPTS ・ RAW_DOCUMENTS）
-- ============================================================================
-- DIM_* ・ FACT_ORDERS は setup_20261216.sql でロード・コメント付与済みのため復元しない
-- AI 関数の結果は、検証済みの CSV から復元（AI 関数は実行しない）
CREATE OR REPLACE TABLE RAW_CALL_TRANSCRIPTS AS
SELECT $1::VARCHAR AS call_id, $2::TIMESTAMP_NTZ AS call_datetime, $3::VARCHAR AS customer_id,
       $4::VARCHAR AS order_id, $5::VARCHAR AS product_id, $6::VARCHAR AS agent_id,
       $7::NUMBER AS call_duration_sec, $8::VARCHAR AS transcript_text
FROM @DATA_STAGE/checkpoint/raw_call_transcripts.csv (FILE_FORMAT => 'HANDSON_CHECKPOINT_CSV');

CREATE OR REPLACE TABLE RAW_DOCUMENTS AS
SELECT $1::VARCHAR AS file_name, $2::NUMBER AS page_no, $3::VARCHAR AS page_text
FROM @DATA_STAGE/checkpoint/raw_documents.csv (FILE_FORMAT => 'HANDSON_CHECKPOINT_CSV');

SELECT 'DIM_PRODUCTS' AS table_name, COUNT(*) AS record_count, 55 AS expected FROM DIM_PRODUCTS
UNION ALL SELECT 'DIM_CUSTOMERS', COUNT(*), 300 FROM DIM_CUSTOMERS
UNION ALL SELECT 'FACT_ORDERS', COUNT(*), 2913 FROM FACT_ORDERS
UNION ALL SELECT 'RAW_CALL_TRANSCRIPTS', COUNT(*), 98 FROM RAW_CALL_TRANSCRIPTS
UNION ALL SELECT 'RAW_DOCUMENTS', COUNT(*), 99 FROM RAW_DOCUMENTS;

-- ============================================================================
-- Part 2 完了時点（Silver 層: CALL_ANALYSIS ・ DOC_ATTRIBUTES ／ Gold 層: KNOWLEDGE_CHUNKS）
-- ============================================================================
CREATE OR REPLACE TABLE CALL_ANALYSIS AS
SELECT $1::VARCHAR AS call_id, $2::TIMESTAMP_NTZ AS call_datetime, $3::VARCHAR AS customer_id,
       $4::VARCHAR AS order_id, $5::VARCHAR AS product_id,
       $6::VARCHAR AS inquiry_type, $7::VARCHAR AS product_name, $8::VARCHAR AS product_category,
       $9::VARCHAR AS sentiment, $10::VARCHAR AS churn_risk, $11::VARCHAR AS summary,
       $12::VARCHAR AS transcript_masked
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
