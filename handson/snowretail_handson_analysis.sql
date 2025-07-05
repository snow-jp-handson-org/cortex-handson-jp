-- =========================================================
-- Snowflake Discover
-- Snowflake Cortex AI で実現する次世代の VoC (顧客の声) 分析ワークシート
-- =========================================================
-- Created by Tsubasa Kanno @Snowflake
-- 最終更新: 2025/07/06
-- =========================================================

-- ロール、DB、スキーマ、ウェアハウスの設定
USE ROLE accountadmin;
USE DATABASE snowretail_db;
USE SCHEMA snowretail_schema;
USE warehouse compute_wh;



-- =========================================================
-- Step1: データ準備
-- =========================================================

-- TRANSLATE関数
SELECT SNOWFLAKE.CORTEX.TRANSLATE('この食品は価格が高いです。一方で非常においしいのでもっと価格が低いと嬉しいです。', '', 'en') as translated;

-- ※ハンズオン※
-- Streamlitの112行目付近の『★★★修正対象★★★』を書き換えてみましょう



-- SENTIMENT関数
SELECT SNOWFLAKE.CORTEX.SENTIMENT('This is really the best!') as basic_sentiment;

-- ※ハンズオン※
-- Streamlitの116行目付近の『★★★修正対象★★★』を書き換えてみましょう



-- ENTITY_SENTIMENT関数
SELECT SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(
    'The restaurant food was excellent and the price was reasonable, but the waiting time was too long.',
    ['food', 'price', 'waiting time']  -- 最大10個のエンティティを指定可能
) as entity_sentiment;



-- SPLIT_TEXT_RECURSIVE_CHARACTER関数
SELECT SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
    'スノーリテールは食品スーパーマーケットチェーンです。全国に150店舗を展開し年間売上高は2500億円です。',  -- 対象テキスト
    'none',  -- 区切り方法 （通常の文章かmarkdown）
    15,     -- 最大チャンクサイズ（文字数）
    5        -- オーバーラップの文字数
) as split_result;

-- ※ハンズオン※
-- Streamlitの123行目付近の『★★★修正対象★★★』を書き換えてみましょう



-- Embedding関数
SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('multilingual-e5-large', 'ECでの買い物体験は素晴らしかったです。');

-- ※ハンズオン※
-- Streamlitの140行目付近の『★★★修正対象★★★』を書き換えてみましょう



-- =========================================================
-- Step2: 顧客の声分析
-- =========================================================

-- AI_CLASSIFY関数によるマルチラベル分類
SELECT 
    AI_CLASSIFY(
        '週末は観光を楽しんできました。美味しいレストランで食事もできて最高でした。',  -- 分類するテキスト
        ['休暇', '仕事', '家事', '食事', '旅行', 'スポーツ'],  -- 分類カテゴリのリスト
        {'output_mode': 'multi'} -- シングルラベル分類かマルチラベル分類かを指定
    ) as classification;

-- ※ハンズオン※
-- Streamlitの173行目付近の『★★★修正対象★★★』を書き換えてみましょう



-- AI_FILTER関数による条件フィルタリング
SELECT 
    review_id,
    review_text,
    AI_FILTER(CONCAT('この商品に関するポジティブな評価が含まれているか？: ', review_text)) as is_positive_review
FROM CUSTOMER_REVIEWS
WHERE AI_FILTER(CONCAT('商品の品質について言及されているか？: ', review_text)) = TRUE
LIMIT 10;

-- ※ハンズオン※
-- Streamlitの314行目付近の『★★★修正対象★★★』を書き換えてみましょう



-- AI_AGG関数による集約分析 (購入チャネル別)
SELECT 
    purchase_channel,
    AI_AGG(
        review_text, 
        '価格に関する内容を抽出して日本語1行で要約してください。'
    ) as channel_insights
FROM CUSTOMER_REVIEWS
GROUP BY purchase_channel;

-- ※ハンズオン※
-- Streamlitの415行目付近の『★★★修正対象★★★』を書き換えてみましょう



-- AI_SUMMARIZE_AGG関数による要約 (購入チャネル別)
SELECT 
    purchase_channel,
    AI_SUMMARIZE_AGG(review_text) as review_summary
FROM CUSTOMER_REVIEWS
GROUP BY purchase_channel;



-- AI_SIMILARITY関数による類似度計算
SELECT 
    '今日は良い天気です' as text1,
    '天候が素晴らしいですね' as text2,
    AI_SIMILARITY('今日は良い天気です', '天候が素晴らしいですね') as similarity_score;

-- ※ハンズオン※
-- Streamlitの479行目付近の『★★★修正対象★★★』を書き換えてみましょう



-- =========================================================
-- (Option) Step2': 高度なレビュー分析とフィルタリング
-- =========================================================

-- レビュー内容と売上データを自然言語でマッチング
-- 低評価レビューがどの商品と売上に影響しているのかを分析する
WITH smart_review_sales_matching AS (
    SELECT 
        r.REVIEW_ID,
        r.PRODUCT_ID,
        r.RATING,
        r.REVIEW_TEXT,
        r.PURCHASE_CHANNEL,
        rt.TRANSACTION_ID,
        rt.PRODUCT_NAME_MASTER,
        rt.TOTAL_PRICE
    FROM CUSTOMER_REVIEWS r
    LEFT JOIN RETAIL_DATA_WITH_PRODUCT_MASTER rt 
        ON r.PRODUCT_ID = rt.PRODUCT_ID_MASTER
        AND AI_FILTER(prompt('この商品レビューは品質や満足度に関する具体的な問題を含んでいますか？ レビュー内容: {0}', r.REVIEW_TEXT))
    WHERE r.RATING <= 3  -- 低評価に絞る
    LIMIT 50  -- ハンズオン用に制限
),
sentiment_analysis AS (
    SELECT 
        *,
        SNOWFLAKE.CORTEX.SENTIMENT(REVIEW_TEXT) as SENTIMENT_SCORE
    FROM smart_review_sales_matching
)
SELECT 
    PURCHASE_CHANNEL,
    PRODUCT_NAME_MASTER,
    COUNT(*) as PROBLEM_REVIEW_COUNT,
    AVG(SENTIMENT_SCORE) as AVG_SENTIMENT,
    AVG(TOTAL_PRICE) as AVG_TRANSACTION_AMOUNT,
    '具体的な問題を含む低評価レビューと売上の関連性分析' as ANALYSIS_TYPE
FROM sentiment_analysis
WHERE PRODUCT_NAME_MASTER IS NOT NULL
GROUP BY PURCHASE_CHANNEL, PRODUCT_NAME_MASTER
ORDER BY PROBLEM_REVIEW_COUNT DESC;



-- 販売チャネル別の顧客満足度を集約してグループごとのインサイトを抽出
-- AI_AGG関数を用いて複数レビューを要約し、改善提案を生成
WITH channel_review_aggregation AS (
    SELECT 
        r.PURCHASE_CHANNEL,
        COUNT(*) as TOTAL_REVIEWS,
        AVG(r.RATING) as AVG_RATING,
        COUNT(CASE WHEN r.RATING >= 4 THEN 1 END) as POSITIVE_REVIEWS,
        COUNT(CASE WHEN r.RATING <= 2 THEN 1 END) as NEGATIVE_REVIEWS,
        AI_AGG(r.REVIEW_TEXT, 'このチャネルの顧客レビューを分析し、主要な満足要因と改善すべき課題を3つずつ日本語で抽出してください') as CUSTOMER_INSIGHTS
    FROM CUSTOMER_REVIEWS r
    GROUP BY r.PURCHASE_CHANNEL
    LIMIT 100  -- ハンズオン用に制限
)
SELECT 
    PURCHASE_CHANNEL,
    TOTAL_REVIEWS,
    ROUND(AVG_RATING, 2) as AVG_RATING,
    POSITIVE_REVIEWS,
    NEGATIVE_REVIEWS,
    CUSTOMER_INSIGHTS
FROM channel_review_aggregation
ORDER BY TOTAL_REVIEWS DESC;



-- レビュー内容をAI_CLASSIFY関数で分類し商品特性を分析
WITH classified_reviews AS (
    SELECT 
        r.REVIEW_ID,
        r.PRODUCT_ID,
        r.RATING,
        r.REVIEW_TEXT,
        r.PURCHASE_CHANNEL,
        -- レビュー内容を商品特性で分類
        AI_CLASSIFY(
            r.REVIEW_TEXT, 
            ['価格重視', '品質重視', '利便性重視', 'デザイン重視', 'サービス重視']
        ) as REVIEW_CATEGORY,
        SNOWFLAKE.CORTEX.SENTIMENT(r.REVIEW_TEXT) as SENTIMENT_SCORE
    FROM CUSTOMER_REVIEWS r
    LIMIT 100  -- ハンズオン用に制限
),
category_insights AS (
    SELECT 
        REVIEW_CATEGORY['labels'][0]::string as PRIMARY_CATEGORY,
        PURCHASE_CHANNEL,
        COUNT(*) as REVIEW_COUNT,
        AVG(RATING) as AVG_RATING,
        AVG(SENTIMENT_SCORE) as AVG_SENTIMENT,
        -- カテゴリ別の代表的なレビューを要約
        SNOWFLAKE.CORTEX.SUMMARIZE(
            LISTAGG(CASE 
                WHEN LEN(REVIEW_TEXT) <= 200 THEN REVIEW_TEXT 
                ELSE LEFT(REVIEW_TEXT, 200) || '...' 
            END, ' | ') WITHIN GROUP (ORDER BY RATING DESC)
        ) as CATEGORY_SUMMARY
    FROM classified_reviews
    WHERE REVIEW_CATEGORY['labels'][0] IS NOT NULL
    GROUP BY REVIEW_CATEGORY['labels'][0]::string, PURCHASE_CHANNEL
)
SELECT 
    PRIMARY_CATEGORY,
    PURCHASE_CHANNEL,
    REVIEW_COUNT,
    ROUND(AVG_RATING, 2) as AVG_RATING,
    ROUND(AVG_SENTIMENT, 3) as AVG_SENTIMENT,
    CATEGORY_SUMMARY,
    '🎯 ' || PRIMARY_CATEGORY || 'の顧客は' || PURCHASE_CHANNEL || 'で' || 
    ROUND(AVG_RATING, 1) || '点の評価' as INSIGHT_SUMMARY
FROM category_insights
ORDER BY REVIEW_COUNT DESC, AVG_RATING DESC;



-- =========================================================
-- Step3: シンプルチャットボット
-- =========================================================

-- AI_COMPLETE関数によるチャットボット応答
SELECT AI_COMPLETE('llama4-maverick', 'Snowflakeの特徴を端的に教えてください。');

-- ※ハンズオン※
-- Streamlitの50行目付近の『★★★修正対象★★★』を書き換えてみましょう

-- ※(Option) ハンズオン※
-- Streamlitの29行目付近を書き換えて他のLLMモデルを追加してみましょう
-- <https://docs.snowflake.com/en/sql-reference/functions/ai_complete-single-string#arguments>



-- =========================================================
-- Step4: RAGチャットボット
-- =========================================================

-- 社内ドキュメントの確認
SELECT * FROM SNOW_RETAIL_DOCUMENTS LIMIT 10;

-- AI/ML StudioでCortex Searchサービスを作成する
-- 以下のコマンドベースでCortex Searchサービスを作成してもOK

-- (Option) Cortex Search Service作成
CREATE OR REPLACE CORTEX SEARCH SERVICE snow_retail_search_service
    ON content
    ATTRIBUTES title, document_type, department
    WAREHOUSE = 'COMPUTE_WH'
    TARGET_LAG = '1 day'
    EMBEDDING_MODEL = 'voyage-multilingual-2'
    AS
        SELECT 
        document_id,
        title,
        content,
        document_type,
        department,
        created_at,
        updated_at,
        version
    FROM SNOW_RETAIL_DOCUMENTS;



-- =========================================================
-- Step5: Cortex Analyst分析
-- =========================================================

-- StudioからCortex分析を開いてセマンティックモデルを確認しましょう



-- お疲れ様でした！
