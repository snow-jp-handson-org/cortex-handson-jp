-- =========================================================
-- Snowflake Cortex Handson シナリオ#2
-- AIを用いた顧客の声分析ワークシート
-- =========================================================
-- Created by Tsubasa Kanno @Snowflake
-- 最終更新: 2025/06/16
-- =========================================================

-- ロール、DB、スキーマ、ウェアハウスの設定
USE ROLE accountadmin;
USE DATABASE snowretail_db;
USE SCHEMA snowretail_schema;
USE warehouse compute_wh;

-- =========================================================
-- Step1: データ準備
-- =========================================================

-- 翻訳機能
SELECT SNOWFLAKE.CORTEX.TRANSLATE('こんにちは！あなたは誰ですか？', '', 'en') as translated;

-- ※ハンズオン※
-- Streamlitの112行目付近の『★★★修正対象★★★』を書き換えてみましょう

-- 感情分析機能（基本版）
SELECT SNOWFLAKE.CORTEX.SENTIMENT('This is really the best!') as basic_sentiment;

-- ※ハンズオン※
-- Streamlitの116行目付近の『★★★修正対象★★★』を書き換えてみましょう

-- 感情分析機能（エンティティ対応高精度版）- 特定の観点での感情分析が可能
SELECT SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(
    'The restaurant food was excellent and the price was reasonable, but the waiting time was too long.',
    ['food', 'price', 'waiting time']  -- 最大10個のエンティティを指定可能
) as entity_sentiment;

-- テキスト分割機能
SELECT SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
    'スノーリテールは食品スーパーマーケットチェーンです。全国に150店舗を展開し年間売上高は2500億円です。',  -- 対象テキスト
    'none',  -- 区切り方法（段落や文など）
    15,     -- 最大チャンクサイズ（文字数）
    5        -- オーバーラップの文字数
) as split_result;

-- ※ハンズオン※
-- Streamlitの123行目付近の『★★★修正対象★★★』を書き換えてみましょう

-- 埋め込み機能（ベクトル検索用）
SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('multilingual-e5-large', '今日は仕事が忙しいですね。');

-- ※ハンズオン※
-- Streamlitの140行目付近の『★★★修正対象★★★』を書き換えてみましょう

-- =========================================================
-- Step2: 顧客の声分析
-- =========================================================

-- AI_CLASSIFY関数によるマルチラベル分類
SELECT 
    AI_CLASSIFY(
        '週末は観光を楽しんできました。美味しいレストランで食事もできて最高でした。',  -- 分類するテキスト
        ['食事', '休暇', '仕事', '家事', '娯楽', '旅行']  -- 分類カテゴリのリスト
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
        'レビューの中で特に言及されていることは何か、単語で答えてください。'
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

-- 複数条件でのスマートフィルタリング
SELECT 
    review_id,
    product_id,
    rating,
    review_text,
    AI_FILTER(CONCAT('配送や梱包に関する問題が言及されているか？: ', review_text)) as has_shipping_issue,
    AI_FILTER(CONCAT('商品の品質に満足している表現が含まれるか？: ', review_text)) as is_quality_satisfied
FROM CUSTOMER_REVIEWS
WHERE AI_FILTER(CONCAT('レビューが具体的で詳細な内容を含んでいるか？: ', review_text)) = TRUE
LIMIT 20;

-- 感情とカテゴリによる複合分析
SELECT 
    review_id,
    product_id,
    purchase_channel,
    AI_CLASSIFY(review_text, ['品質', '価格', '配送', 'サービス', 'その他']) as category,
    SNOWFLAKE.CORTEX.SENTIMENT(review_text) as basic_sentiment,
    SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(SNOWFLAKE.CORTEX.TRANSLATE(review_text, '', 'en')) as entity_sentiment_score,
    AI_FILTER(CONCAT('このレビューは他の顧客の購入判断に影響を与える可能性が高いか？: ', review_text)) as is_influential
FROM CUSTOMER_REVIEWS
WHERE rating >= 4 OR rating <= 2  -- 極端な評価のみ対象
LIMIT 30;

-- 購入チャネル別の詳細分析
SELECT 
    purchase_channel,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    AI_AGG(
        review_text,
        '特徴を一言で表現してください。'
    ) as channel_insights,
    AI_SUMMARIZE_AGG(review_text) as channel_summary
FROM CUSTOMER_REVIEWS
GROUP BY purchase_channel;

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
