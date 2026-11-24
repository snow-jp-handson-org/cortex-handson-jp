-- Part1〜2 相当: 文字起こし・PDF解析・通話分析
-- Cortex Search の属性は、すべて非構造化データ（PDF・通話）の中身から AI_EXTRACT で取り出す
USE SCHEMA GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA;
USE WAREHOUSE GLACIERSTYLE_WH;

-- Part1: 音声10件を文字起こしして RAW_VOICE_LOGS に保存（文字起こし済み88件は RAW_CALL_TRANSCRIPTS）
CREATE TABLE IF NOT EXISTS RAW_VOICE_LOGS AS
WITH transcribed AS (
  SELECT SPLIT_PART(relative_path, '/', -1) AS audio_file,
         AI_TRANSCRIBE(TO_FILE('@DATA_STAGE', relative_path)) AS result
  FROM DIRECTORY(@DATA_STAGE) WHERE relative_path LIKE 'voice/%.mp3'
)
SELECT 'VOICE-' || REPLACE(audio_file, '.mp3', '') AS call_id, audio_file,
       result:audio_duration::NUMBER(10, 1) AS audio_duration_sec,
       result:text::VARCHAR AS transcript_text
FROM transcribed;

-- Part1: PDF をページ単位で解析する
CREATE OR REPLACE TABLE RAW_DOCUMENTS AS
WITH parsed AS (
  SELECT SPLIT_PART(relative_path, '/', 2) AS file_name,
         AI_PARSE_DOCUMENT(TO_FILE('@DATA_STAGE', relative_path), {'mode': 'LAYOUT', 'page_split': true}) AS doc
  FROM DIRECTORY(@DATA_STAGE) WHERE relative_path LIKE 'documents/%.pdf'
)
SELECT p.file_name, pg.index + 1 AS page_no, pg.value:content::VARCHAR AS page_text
FROM parsed p
JOIN LATERAL FLATTEN(input => p.doc:pages) pg;

-- Part2: PDF の1ページ目（表紙・文書情報）から、検索の属性を AI_EXTRACT で取り出す
CREATE OR REPLACE TABLE DOC_ATTRIBUTES AS
WITH ex AS (
  SELECT file_name,
         AI_EXTRACT(text => page_text, responseFormat => {
           'title': '文書のタイトル',
           'doc_type': '文書の種類を次から1つ: FAQ, マニュアル, 品質報告, 通達, 店舗報告',
           'department': '作成した部署を次から1つ: カスタマーサポート部, 品質管理部, 店舗運営部',
           'audience': '公開範囲を次から1つ: 社外公開, 社内限定。お客様向けの文書は社外公開',
           'doc_date': '発行日または最終更新日（YYYY-MM-DD）',
           'product_category': '対象の商品カテゴリを次から1つ: 照明, 家具, 収納, 調理器具, 食器, 文具, トラベル, ギフト, ファッション小物, ルームウェア。特定の商品についての文書でなければ空',
           'product_ids': '対象の商品IDをすべて（例: PRD-LGT-0001）'
         }):response AS r
  FROM RAW_DOCUMENTS
  WHERE page_no = 1
)
-- 特定の商品を扱わない文書では AI が余計な値（「None」「空」など）を返すため、
-- 商品IDが取れた文書（PRD- で始まる）にだけ商品カテゴリと商品IDを付ける
, cleaned AS (
  SELECT file_name, r,
         ARRAY_COMPACT(TRANSFORM(
           COALESCE(TRY_PARSE_JSON(r:product_ids::VARCHAR), ARRAY_CONSTRUCT(r:product_ids)),
           x -> IFF(STARTSWITH(x::VARCHAR, 'PRD-'), x::VARCHAR, NULL))) AS ids
  FROM ex
)
SELECT file_name,
       r:title::VARCHAR AS title,
       r:doc_type::VARCHAR AS doc_type,
       r:department::VARCHAR AS department,
       r:audience::VARCHAR AS audience,
       TRY_TO_DATE(r:doc_date::VARCHAR) AS doc_date,
       IFF(ARRAY_SIZE(ids) > 0, r:product_category::VARCHAR, NULL) AS product_category,
       ids AS product_ids
FROM cleaned;

-- Part2: 通話から項目を抽出し、個人情報をマスクする（商品カテゴリも通話の中身から取り出す）
CREATE OR REPLACE TABLE CALL_ANALYSIS AS
WITH calls AS (
  SELECT call_id, call_datetime, customer_id, order_id, product_id, transcript_text, '文字起こし済み' AS source
  FROM RAW_CALL_TRANSCRIPTS
  UNION ALL
  SELECT call_id, NULL::TIMESTAMP_NTZ, NULL, NULL, NULL, transcript_text, '音声'
  FROM RAW_VOICE_LOGS
),
ex AS (
  SELECT t.*,
         AI_EXTRACT(text => transcript_text, responseFormat => {
           'inquiry_type': '問い合わせ種別を次から1つ: 商品不具合, 配送遅延, 返品・交換, 注文キャンセル, 商品問い合わせ, 支払い, 会員・ポイント, その他',
           'product_name': '話題になっている商品名。なければ空',
           'product_category': '話題になっている商品のカテゴリを次から1つ: 照明, 家具, 収納, 調理器具, 食器, 文具, トラベル, ギフト, ファッション小物, ルームウェア。商品の話でなければ空',
           'sentiment': '顧客の感情を次から1つ: ネガティブ, 中立, ポジティブ',
           'churn_risk': '顧客が他社に乗り換えたり利用をやめたりする可能性を次から1つ: 高, 中, 低',
           'summary': '通話内容の要約（60文字以内）'
         }):response AS r
  FROM calls t
)
SELECT call_id, call_datetime, customer_id, order_id, product_id, source,
       r:inquiry_type::VARCHAR AS inquiry_type, r:product_name::VARCHAR AS product_name,
       -- AI が選択肢にない値（商品名など）を返すことがあるため、選択肢以外は空にする
       IFF(r:product_category::VARCHAR IN ('照明', '家具', '収納', '調理器具', '食器', '文具', 'トラベル', 'ギフト', 'ファッション小物', 'ルームウェア'),
           r:product_category::VARCHAR, NULL) AS product_category,
       r:sentiment::VARCHAR AS sentiment, r:churn_risk::VARCHAR AS churn_risk, r:summary::VARCHAR AS summary,
       AI_REDACT(transcript_text)::VARCHAR AS transcript_masked
FROM ex;

SELECT 'RAW_VOICE_LOGS' t, COUNT(*) n FROM RAW_VOICE_LOGS
UNION ALL SELECT 'RAW_DOCUMENTS', COUNT(*) FROM RAW_DOCUMENTS
UNION ALL SELECT 'DOC_ATTRIBUTES', COUNT(*) FROM DOC_ATTRIBUTES
UNION ALL SELECT 'CALL_ANALYSIS', COUNT(*) FROM CALL_ANALYSIS;
