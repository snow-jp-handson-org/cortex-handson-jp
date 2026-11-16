-- Part3 相当: 通話と PDF を統合したチャンクテーブルと Cortex Search サービス
-- 属性はすべて AI_EXTRACT の結果（CALL_ANALYSIS / DOC_ATTRIBUTES）。通話の種類・部署・公開範囲はコールセンターの通話という出どころで決まる
USE SCHEMA GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA;
USE WAREHOUSE GLACIERSTYLE_WH;

CREATE OR REPLACE TABLE KNOWLEDGE_CHUNKS AS
-- 通話（1通話 = 1チャンク）
SELECT call_id AS chunk_id,
       '【要約】' || summary || '\n' || transcript_masked AS chunk_text,
       inquiry_type || COALESCE(' / ' || product_name, '') AS title,
       '通話' AS doc_type, 'カスタマーサポート部' AS department, '社内限定' AS audience,
       call_datetime::DATE AS doc_date, product_category,
       IFF(product_id IS NULL, ARRAY_CONSTRUCT(), ARRAY_CONSTRUCT(product_id)) AS product_ids,
       sentiment
FROM CALL_ANALYSIS
UNION ALL
-- PDF（ページ本文を分割）
SELECT d.file_name || '-p' || d.page_no || '-' || c.index AS chunk_id,
       c.value::VARCHAR AS chunk_text,
       a.title, a.doc_type, a.department, a.audience, a.doc_date,
       a.product_category, a.product_ids,
       NULL
FROM DOC_PARSED d
JOIN DOC_ATTRIBUTES a ON a.file_name = d.file_name,
     LATERAL FLATTEN(input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(d.page_text, 'markdown', 800, 100)) c;

CREATE OR REPLACE CORTEX SEARCH SERVICE KNOWLEDGE_SEARCH
  ON chunk_text
  ATTRIBUTES doc_type, department, audience, doc_date, product_category, product_ids, sentiment
  WAREHOUSE = GLACIERSTYLE_WH
  TARGET_LAG = '1 day'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS SELECT chunk_id, chunk_text, title, doc_type, department, audience, doc_date, product_category, product_ids, sentiment
   FROM KNOWLEDGE_CHUNKS;

SELECT doc_type, audience, COUNT(*) n FROM KNOWLEDGE_CHUNKS GROUP BY 1,2 ORDER BY 1;
