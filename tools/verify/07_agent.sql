-- Part4〜5 相当: セマンティックビュー（検証用の最小構成）と Cortex Agent
USE SCHEMA GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA;
USE WAREHOUSE GLACIERSTYLE_WH;

CREATE OR REPLACE SEMANTIC VIEW CS_QUALITY_SV
  TABLES (
    orders AS FACT_ORDERS PRIMARY KEY (order_id) COMMENT = '注文。order_status=returned が返品',
    products AS DIM_PRODUCTS PRIMARY KEY (product_id) COMMENT = '商品マスタ',
    customers AS DIM_CUSTOMERS PRIMARY KEY (customer_id) COMMENT = '顧客マスタ',
    calls AS CALL_ANALYSIS PRIMARY KEY (call_id) COMMENT = 'コールセンター通話の分析結果（AI_EXTRACT）'
  )
  RELATIONSHIPS (
    orders (product_id) REFERENCES products,
    orders (customer_id) REFERENCES customers,
    calls (customer_id) REFERENCES customers,
    calls (product_id) REFERENCES products
  )
  FACTS (
    orders.total_amount AS total_amount COMMENT = '税込金額',
    orders.is_returned AS IFF(order_status = 'returned', 1, 0) COMMENT = '返品なら1'
  )
  DIMENSIONS (
    orders.order_month AS DATE_TRUNC('MONTH', order_datetime) COMMENT = '注文月',
    orders.order_status AS order_status COMMENT = 'completed / cancelled / returned',
    orders.lot_no AS lot_no COMMENT = '製造ロット番号（例: LOT-2609-B）',
    products.product_name AS product_name COMMENT = '商品名',
    products.category_l2 AS category_l2 WITH SYNONYMS = ('商品カテゴリ', 'カテゴリ') COMMENT = '中カテゴリ（照明、家具など）',
    customers.membership_tier AS membership_tier WITH SYNONYMS = ('会員ランク') COMMENT = 'standard / silver / gold / platinum',
    calls.call_month AS DATE_TRUNC('MONTH', call_datetime) COMMENT = '通話月',
    calls.inquiry_type AS inquiry_type COMMENT = '問い合わせ種別（商品不具合、配送遅延など）',
    calls.sentiment AS sentiment COMMENT = 'ネガティブ / 中立 / ポジティブ',
    calls.churn_risk AS churn_risk WITH SYNONYMS = ('解約リスク') COMMENT = '高 / 中 / 低'
  )
  METRICS (
    orders.sales AS SUM(orders.total_amount) WITH SYNONYMS = ('売上') COMMENT = '売上（税込）',
    orders.order_count AS COUNT(orders.order_id) COMMENT = '注文件数',
    orders.return_rate AS AVG(orders.is_returned) WITH SYNONYMS = ('返品率') COMMENT = '返品率（返品件数 / 注文件数）',
    calls.call_count AS COUNT(calls.call_id) WITH SYNONYMS = ('問い合わせ件数', '通話件数') COMMENT = '通話件数',
    calls.customer_count AS COUNT(DISTINCT calls.customer_id) COMMENT = '通話した顧客数'
  )
  COMMENT = 'GlacierStyle CS・品質分析（12/16ハンズオン検証用）';

CREATE OR REPLACE AGENT CS_QUALITY_AGENT
  COMMENT = '12/16ハンズオン検証用'
  PROFILE = '{"display_name": "CS・品質アシスタント"}'
FROM SPECIFICATION $$
models:
  orchestration: auto
instructions:
  orchestration: |
    数値（売上・返品率・通話件数・解約リスク）は CS_Quality_Analyst を使う。
    通話の内容や社内文書（品質報告・通達・マニュアル）は Knowledge_Search を使い、質問に合わせて属性でフィルタする。
    お客様向けの案内は Customer_FAQ_Search を使う。
    データの期間は2026年6月〜11月。
  response: |
    日本語の丁寧語で、結論から簡潔に答える。お客様向けの案内と社内の手順は分けて書く。
tools:
  - tool_spec: {type: cortex_analyst_text_to_sql, name: CS_Quality_Analyst, description: "注文・返品・商品・顧客・通話分析の数値を集計する"}
  - tool_spec: {type: cortex_search, name: Knowledge_Search, description: "通話記録と社内文書（FAQ・マニュアル・品質報告・通達・店舗報告）を横断検索する"}
  - tool_spec: {type: cortex_search, name: Customer_FAQ_Search, description: "お客様に公開しているFAQだけを検索する"}
tool_resources:
  CS_Quality_Analyst:
    semantic_view: GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA.CS_QUALITY_SV
    execution_environment: {type: warehouse, warehouse: GLACIERSTYLE_WH}
  Knowledge_Search:
    search_service: GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA.KNOWLEDGE_SEARCH
    max_results: 5
    title_column: TITLE
    id_column: CHUNK_ID
    columns_and_descriptions:
      CHUNK_TEXT: {description: "本文", type: string, searchable: true, filterable: false}
      DOC_TYPE: {description: "文書の種類。値: 通話, FAQ, マニュアル, 品質報告, 通達, 店舗報告", type: string, searchable: false, filterable: true}
      DEPARTMENT: {description: "作成部署。値: カスタマーサポート部, 品質管理部, 店舗運営部", type: string, searchable: false, filterable: true}
      AUDIENCE: {description: "公開範囲。値: 社外公開, 社内限定", type: string, searchable: false, filterable: true}
      DOC_DATE: {description: "通話日または文書の発行日（2026-06〜2026-11）", type: datetime, searchable: false, filterable: true}
      PRODUCT_CATEGORY: {description: "商品の中カテゴリ。値: 照明, 家具, 収納, 調理器具, 食器 など", type: string, searchable: false, filterable: true}
      SENTIMENT: {description: "通話の顧客感情。値: ネガティブ, 中立, ポジティブ（通話のみ）", type: string, searchable: false, filterable: true}
  Customer_FAQ_Search:
    search_service: GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA.KNOWLEDGE_SEARCH
    max_results: 5
    title_column: TITLE
    id_column: CHUNK_ID
    filter: {"@eq": {"AUDIENCE": "社外公開"}}
$$;
