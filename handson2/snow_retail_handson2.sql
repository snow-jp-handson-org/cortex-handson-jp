-- =========================================================
-- Snowflake Cortex Handson シナリオ#2
-- AIを用いた顧客の声分析ワークシート
-- =========================================================
-- Created by Takuya Shoji @Snowflake
-- 最終更新: 2025/06/17
-- =========================================================

-- ロール、DB、スキーマ、ウェアハウスの設定
USE ROLE accountadmin;
USE DATABASE snowretail_db;
USE SCHEMA snowretail_schema;
USE WAREHOUSE compute_wh;


-- =========================================================
-- Step1: データ準備
-- =========================================================

-- SPLIT_TEXT_RECURSIVE_CHARACTER関数
SELECT SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
    'こんにちは、今日は良い天気ですね。昨日の雨が嘘みたいです。',  -- 対象テキスト
    'none',  -- 区切り方法（段落や文など）
    10,     -- 最大チャンクサイズ（文字数）
    2        -- オーバーラップの文字数
) as split_result;

-- Streamlitの260行目付近の『★★★修正対象★★★』を書き換えてみましょう


-- TRANSLATE関数
SELECT SNOWFLAKE.CORTEX.TRANSLATE('こんにちは！あなたは誰ですか？', '', 'en') as translated;

-- Streamlitの245行目付近の『★★★修正対象★★★』を書き換えてみましょう


-- SENTIMENT関数
SELECT SNOWFLAKE.CORTEX.SENTIMENT('This is really the best!') as score;

-- Streamlitの250行目付近の『★★★修正対象★★★』を書き換えてみましょう


-- 参考: ENTITY_SENTIMENT関数（エンティティ対応版）- 特定の観点での感情分析が可能
SELECT SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(
    'The restaurant food was excellent and the price was reasonable, but the waiting time was too long.',
    ['food', 'price', 'waiting time']  -- 最大10個のエンティティを指定可能
) as entity_sentiment;


-- EMBEDDING関数
-- (EMBED_TEXT_1024関数)
SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('multilingual-e5-large', '今日は仕事が忙しいですね。');

-- Streamlitの294行目付近の『★★★修正対象★★★』を書き換えてみましょう

-- Streamlitで実際にデータ準備をしてみましょう


-- =========================================================
-- Step2: 顧客の声分析
-- =========================================================

-- CLASSIFY_TEXT関数
SELECT 
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        '週末は観光を楽しんできました。美味しいレストランで食事もできて最高でした。',  -- 分類するテキスト
        ['食事', '休暇', '仕事', '家事', '娯楽', '旅行']  -- 分類カテゴリのリスト
    ) as classification;

-- Streamlitの578行目付近の『★★★修正対象★★★』を書き換えてみましょう


-- AI_COMPLETE関数による構造化出力
SELECT AI_COMPLETE(
    'llama4-maverick',  -- 使用するLLMモデル
    'テキストから重要な単語を抽出し、品詞と出現回数を分析してください。対象テキスト：明日の東日本は広い範囲で大雪となるでしょう。',  -- プロンプト
    {
        'temperature': 0,  -- 生成結果の多様性
        'max_tokens': 1000  -- 最大応答トークン数
    },
    {
        'type': 'json',
        'schema': {
            'type': 'object',
            'properties': {
                'words': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'word': {
                                'type': 'string',
                                'description': '抽出された単語'
                            },
                            'type': {
                                'type': 'string',
                                'enum': ['名詞', '動詞', '形容詞'],
                                'description': '品詞（名詞、動詞、形容詞のいずれか）'
                            },
                            'frequency': {
                                'type': 'integer',
                                'description': '単語の出現回数'
                            }
                        },
                        'required': ['word', 'type', 'frequency']
                    }
                }
            },
            'required': ['words']
        }
    }
) as result;

-- Streamlitの694行目付近の『★★★修正対象★★★』を書き換えてみましょう



-- ベクトル類似度関数
SELECT VECTOR_COSINE_SIMILARITY([0.1, 0.5, 0.2, 0.8]::VECTOR(FLOAT, 4), [-0.1, 0.7, 0.3, -0.4]::VECTOR(FLOAT, 4)) AS similarity;

-- Streamlitの1546行目付近の『★★★修正対象★★★』を書き換えてみましょう

-- Streamlitで実際に顧客の声分析を使ってみましょう


-- =========================================================
-- Step3: シンプルチャットボット
-- =========================================================

-- COMPLETE関数
SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-4-sonnet', 'あなたは何ができますか？');

-- Streamlitの568行目付近のCOMPLETE関数を見てみましょう
-- 修正は不要です

-- Streamlitで実際にシンプルチャットボットを使ってみましょう


-- =========================================================
-- Step4: RAGチャットボット
-- =========================================================

-- 社内ドキュメントの確認
SELECT * FROM SNOW_RETAIL_DOCUMENTS;

-- CORTEX SEARCH
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

-- Streamlitで実際にRAGチャットボットを使ってみましょう


-- =========================================================
-- Step5: 分析チャットボット
-- =========================================================

-- StudioからCortex分析を開いてセマンティックモデルを確認しましょう

-- Streamlitで実際に分析チャットボットを使ってみましょう

-- お疲れ様でした！