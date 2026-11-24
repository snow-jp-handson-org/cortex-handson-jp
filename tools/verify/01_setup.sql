-- ============================================================
-- 12/16 ハンズオン 検証環境（GLACIERSTYLE_DEC_DB）
-- 既存の GLACIERSTYLE_DB とは分離して検証する
-- ============================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE GLACIERSTYLE_WH;

CREATE DATABASE IF NOT EXISTS GLACIERSTYLE_DEC_DB;
CREATE SCHEMA IF NOT EXISTS GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA;
USE SCHEMA GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA;

CREATE STAGE IF NOT EXISTS DATA_STAGE
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE OR REPLACE FILE FORMAT CSV_FF
  TYPE = CSV PARSE_HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"' ENCODING = 'UTF8';

-- 構造化データ（ノートブックの Part1 と同じテーブル名）
CREATE OR REPLACE TABLE DIM_PRODUCTS (
  product_id VARCHAR, product_name VARCHAR, category_l1 VARCHAR, category_l2 VARCHAR, category_l3 VARCHAR,
  brand VARCHAR, list_price NUMBER, cost_price NUMBER, launch_date DATE);
CREATE OR REPLACE TABLE DIM_CUSTOMERS (
  customer_id VARCHAR, last_name VARCHAR, first_name VARCHAR, gender VARCHAR, birth_date DATE, email VARCHAR,
  phone VARCHAR, prefecture VARCHAR, city VARCHAR, registration_date DATE, membership_tier VARCHAR);
CREATE OR REPLACE TABLE FACT_ORDERS (
  order_id VARCHAR, order_datetime TIMESTAMP_NTZ, customer_id VARCHAR, product_id VARCHAR, quantity NUMBER,
  unit_price NUMBER, discount_amount NUMBER, total_amount NUMBER, order_channel VARCHAR, order_status VARCHAR, lot_no VARCHAR);
CREATE OR REPLACE TABLE RAW_CALL_TRANSCRIPTS (
  call_id VARCHAR, call_datetime TIMESTAMP_NTZ, customer_id VARCHAR, order_id VARCHAR, product_id VARCHAR,
  agent_id VARCHAR, call_duration_sec NUMBER, transcript_text VARCHAR);
CREATE OR REPLACE TABLE DOCUMENT_ATTRIBUTES (
  file_name VARCHAR, doc_type VARCHAR, title VARCHAR, department VARCHAR, audience VARCHAR,
  doc_date DATE, product_category VARCHAR, product_ids VARCHAR);
