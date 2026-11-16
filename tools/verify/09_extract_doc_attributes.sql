-- 検証: PDF の冒頭ページから、Search の属性（文書の種類・部署・公開範囲・発行日・対象商品）を AI_EXTRACT で取り出せるか
USE SCHEMA GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA;
USE WAREHOUSE GLACIERSTYLE_WH;

CREATE OR REPLACE TABLE DOC_ATTR_EXTRACTED AS
SELECT d.file_name,
       AI_EXTRACT(text => d.page_text, responseFormat => {
         'doc_type': '文書の種類を次から1つ: FAQ, マニュアル, 品質報告, 通達, 店舗報告',
         'department': '作成した部署名（例: カスタマーサポート部, 品質管理部, 店舗運営部）',
         'audience': '公開範囲を次から1つ: 社外公開, 社内限定',
         'doc_date': '発行日または最終更新日（YYYY-MM-DD）',
         'product_id': '対象の商品ID（例: PRD-LGT-0001）。なければ空'
       }):response AS r
FROM DOC_PARSED d
WHERE d.page_no = 1;

SELECT e.file_name,
       e.r:doc_type::VARCHAR   AS ext_doc_type,   a.doc_type,
       e.r:department::VARCHAR AS ext_department, a.department,
       e.r:audience::VARCHAR   AS ext_audience,   a.audience,
       e.r:doc_date::VARCHAR   AS ext_doc_date,   a.doc_date,
       e.r:product_id::VARCHAR AS ext_product_id, a.product_ids
FROM DOC_ATTR_EXTRACTED e JOIN DOCUMENT_ATTRIBUTES a USING (file_name)
ORDER BY 1;
