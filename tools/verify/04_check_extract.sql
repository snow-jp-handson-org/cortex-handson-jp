USE SCHEMA GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA;
CREATE OR REPLACE TEMP TABLE EXPECTED (call_id VARCHAR, inquiry VARCHAR, sentiment VARCHAR);
COPY INTO EXPECTED FROM @DATA_STAGE/check/expected_call_labels.csv FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
-- 種別・感情の一致率
SELECT COUNT(*) n, AVG(IFF(a.inquiry_type = e.inquiry, 1, 0)) inquiry_acc, AVG(IFF(a.sentiment = e.sentiment, 1, 0)) sentiment_acc
FROM CALL_ANALYSIS a JOIN EXPECTED e USING (call_id);
-- 種別の取り違え
SELECT e.inquiry expected, a.inquiry_type got, COUNT(*) n FROM CALL_ANALYSIS a JOIN EXPECTED e USING (call_id)
WHERE a.inquiry_type <> e.inquiry GROUP BY 1,2 ORDER BY 3 DESC;
-- 解約リスク
SELECT churn_risk, inquiry_type, COUNT(*) FROM CALL_ANALYSIS GROUP BY 1,2 HAVING churn_risk = '高' ORDER BY 3 DESC;
-- 音声10件の抽出結果
SELECT call_id, inquiry_type, sentiment, LEFT(summary, 40) s FROM CALL_ANALYSIS WHERE source = '音声' ORDER BY 1;
-- マスクの確認
SELECT LEFT(transcript_masked, 160) FROM CALL_ANALYSIS WHERE inquiry_type = '商品不具合' LIMIT 1;
