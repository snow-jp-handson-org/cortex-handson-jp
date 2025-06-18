# =========================================================
# Snowflake Cortex AI ワークショップ
# 顧客の声分析アプリケーション - MVP
# =========================================================
# 概要: 
# このアプリケーションは、Snowflake Cortex AIとStreamlitを使用して、
# 顧客レビューの分析と社内文書の検索を行うデモアプリケーションです。
#
# 機能:
# - 顧客レビューの感情分析と可視化
# - ベクトル検索によるレビュー検索
# - シンプルなチャットボット
# - Cortex Searchを用いたRAGチャットボットによる社内文書Q&A
# - Cortex Analystによる自然言語分析
#
# Created by Tsubasa Kanno @Snowflake
# 最終更新: 2025/03/29
# =========================================================

# =========================================================
# 必要なライブラリのインポート
# =========================================================
# 基本ライブラリ
import streamlit as st
import pandas as pd
import json
import time
import requests

# Streamlitの設定
st.set_page_config(layout="wide")

# 可視化ライブラリ
import plotly.express as px

# Snowflake関連ライブラリ
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete as CompleteText
from snowflake.core import Root

# =========================================================
# 定数定義
# =========================================================
# Cortex Analyst APIの設定
ANALYST_API_ENDPOINT = "/api/v2/cortex/analyst/message"
ANALYST_API_TIMEOUT = 50  # 秒

# モデル設定
# 埋め込みモデル選択肢
EMBEDDING_MODELS = [
    "multilingual-e5-large",
    "voyage-multilingual-2",
    "snowflake-arctic-embed-l-v2.0",
    "nv-embed-qa-4"
]

# COMPLETE関数用のLLMモデル選択肢
COMPLETE_MODELS = [
    "claude-3-5-sonnet",
    "deepseek-r1",
    "mistral-large2",
    "llama3.3-70b",
    "snowflake-llama-3.3-70b"
]

# Cortex Search Service用のベクトル埋め込みモデル選択肢
SEARCH_MODELS = [
    "voyage-multilingual-2",
    "snowflake-arctic-embed-m-v1.5",
    "snowflake-arctic-embed-l-v2.0"
]

# デフォルトのレビューカテゴリ
DEFAULT_CATEGORIES = [
    "商品の品質",
    "価格",
    "接客サービス",
    "店舗環境",
    "配送・梱包",
    "品揃え",
    "使いやすさ",
    "鮮度",
    "その他"
]

# =========================================================
# Snowflake接続と共通ユーティリティ関数
# =========================================================

# Snowflakeセッションの取得
snowflake_session = get_active_session()

def check_table_exists(table_name: str) -> bool:
    """指定されたテーブルが存在するかチェックします。
    
    Args:
        table_name (str): チェックするテーブル名
    
    Returns:
        bool: テーブルが存在する場合はTrue、存在しない場合はFalse
    """
    try:
        snowflake_session.sql(f"DESC {table_name}").collect()
        return True
    except:
        return False

def get_table_count(table_name: str) -> int:
    """指定されたテーブルのレコード数を取得します。
    
    Args:
        table_name (str): レコード数を取得するテーブル名
    
    Returns:
        int: テーブル内のレコード数（テーブルが存在しない場合は0）
    """
    try:
        result = snowflake_session.sql(f"""
            SELECT COUNT(*) as count FROM {table_name}
        """).collect()
        return result[0]['COUNT']
    except:
        return 0

def get_available_warehouses() -> list:
    """利用可能なSnowflakeウェアハウスの一覧を取得します。
    
    Returns:
        list: ウェアハウス名のリスト（取得失敗時は空リスト）
    """
    try:
        result = snowflake_session.sql("SHOW WAREHOUSES").collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"ウェアハウスの取得に失敗しました: {str(e)}")
        return []

# =========================================================
# データ処理関数
# =========================================================

# =========================================================
# データ初期化・準備
# =========================================================

def create_customer_analysis_table() -> bool:
    """
    顧客レビュー分析用のテーブルを作成します。
    
    CUSTOMER_ANALYSISテーブルは、レビューテキストのチャンキング、
    ベクトル化、感情分析の結果を格納するために使用されます。
    テーブルは既存の場合は再作成せず、存在しない場合のみ作成します（CREATE IF NOT EXISTS）。
    
    テーブル構造:
    - analysis_id: 自動採番のID
    - review_id: 元レビューの参照ID
    - product_id: 製品ID
    - customer_id: 顧客ID
    - rating: 評価（星の数）
    - review_text: 元のレビューテキスト全文
    - review_date: レビュー日時
    - purchase_channel: 購入チャネル
    - helpful_votes: 参考になった投票数
    - chunked_text: チャンキングされたテキスト断片
    - embedding: テキスト断片のベクトル表現 (VECTOR型)
    - sentiment_score: 感情分析スコア (-1〜1の範囲)
    - updated_at: 最終更新日時
    
    Returns:
        bool: テーブル作成に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        st.info("顧客分析テーブルを作成しています...")
        snowflake_session.sql("""
        CREATE TABLE IF NOT EXISTS CUSTOMER_ANALYSIS (
            analysis_id NUMBER AUTOINCREMENT,
            review_id VARCHAR(20),
            product_id VARCHAR(10),
            customer_id VARCHAR(10),
            rating NUMBER(2,1),
            review_text TEXT,
            review_date TIMESTAMP_NTZ,
            purchase_channel VARCHAR(20),
            helpful_votes NUMBER(5),
            chunked_text TEXT,
            embedding VECTOR(FLOAT, 1024),
            sentiment_score FLOAT,
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """).collect()
        
        st.success("顧客分析テーブルの作成が完了しました")
        
        # テーブルの内容を確認
        count = get_table_count("CUSTOMER_ANALYSIS")
        if count > 0:
            st.info(f"既存の分析データが {count} 件あります")
        
        return True
    except Exception as e:
        st.error(f"テーブルの作成に失敗しました: {str(e)}")
        return False

def process_review_chunks() -> bool:
    """
    レビューテキストの処理を実行します。
    
    このプロセスでは以下の処理を順番に行います：
    1. RETAIL_DATA_WITH_PRODUCT_MASTERとEC_DATA_WITH_PRODUCT_MASTERから未処理のデータを取得
    2. CUSTOMER_REVIEWSテーブルからレビューテキストと関連メタデータを取得
    3. レビューテキストをチャンクに分割
    4. レビュー全体を英語に翻訳して感情分析を実行
    5. 各チャンクをベクトル化（EMBED_TEXT_1024を使用）
    6. すべての結果をCUSTOMER_ANALYSISテーブルに保存
    
    チャンキングには、SPLIT_TEXT_RECURSIVE_CHARACTER関数を使用し、
    最大300文字のチャンクサイズでテキストを分割します。
    
    感情分析は、SENTIMENT関数を使用して-1（ネガティブ）〜1（ポジティブ）の
    スコアを計算します。日本語を正確に分析するために、まずSNOWFLAKE.CORTEX.TRANSLATE
    関数で英語に翻訳してから感情分析を行います。感情分析はレビュー全体で行い、
    同一レビューのすべてのチャンクに同じスコアを適用します。
    
    ベクトル化はEMBED_TEXT_1024関数を使用し、選択されたモデルで
    テキストを1024次元のベクトルに変換します。
    
    Returns:
        bool: 処理に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        # ステップ1: 未処理のレビューを取得
        # RETAIL_DATA_WITH_PRODUCT_MASTERとEC_DATA_WITH_PRODUCT_MASTERを使用しつつ
        # Review_IDやレビューテキストなどのレビュー情報はCUSTOMER_REVIEWSテーブルから取得する
        reviews = snowflake_session.sql("""
            SELECT r.*
            FROM CUSTOMER_REVIEWS r
            LEFT JOIN CUSTOMER_ANALYSIS a
            ON r.review_id = a.review_id
            WHERE a.review_id IS NULL
            -- 全件処理するために制限を削除
        """).collect()
        
        if not reviews:
            st.info("処理が必要なレビューはありません。")
            return True
        
        # 進捗バーとテキスト表示の初期化
        st.write(f"**合計 {len(reviews)} 件のレビューを処理します**")
        progress_bar = st.progress(0)
        progress_text = st.empty()
        
        # 処理済みの総チャンク数を追跡
        total_chunks_processed = 0
        
        for i, review in enumerate(reviews):
            # ステップ3: レビュー全体の感情分析
            # 全体のレビューテキストを英語に翻訳
            translated_full_text = snowflake_session.sql("""
                SELECT SNOWFLAKE.CORTEX.『★★★修正対象★★★』(?, '', '『★★★修正対象★★★』') as translated
            """, params=[review['REVIEW_TEXT']]).collect()[0]['TRANSLATED']
            
            # レビュー全体の感情分析スコアの計算（英訳したテキストを使用）
            sentiment_score = snowflake_session.sql("""
                SELECT SNOWFLAKE.CORTEX.『★★★修正対象★★★』(?) as score
            """, params=[translated_full_text]).collect()[0]['SCORE']
            
            # ステップ4: テキストをチャンクに分割
            chunks = snowflake_session.sql("""
                SELECT t.value as chunk
                FROM (
                    SELECT SNOWFLAKE.CORTEX.★★★修正対象★★★(
                        ?,
                        'none',  -- 区切り方法（段落や文など）
                        ★★★修正対象★★★,     -- 最大チャンクサイズ（文字数）
                        ★★★修正対象★★★        -- オーバーラップの文字数
                    ) as split_result
                ),
                LATERAL FLATTEN(input => split_result) t
            """, params=[review['REVIEW_TEXT']]).collect()
            
            # 各チャンクに対する処理
            for chunk_idx, chunk in enumerate(chunks):
                # ステップ5: ベクトル化して結果を挿入
                snowflake_session.sql("""
                    INSERT INTO CUSTOMER_ANALYSIS (
                        review_id,
                        product_id,
                        customer_id,
                        rating,
                        review_text,
                        review_date,
                        purchase_channel,
                        helpful_votes,
                        chunked_text,
                        embedding,
                        sentiment_score
                    )
                    SELECT 
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        SNOWFLAKE.CORTEX.★★★修正対象★★★(?, ?),
                        ?
                """, params=[
                    review['REVIEW_ID'],
                    review['PRODUCT_ID'],
                    review['CUSTOMER_ID'],
                    review['RATING'],
                    review['REVIEW_TEXT'],
                    review['REVIEW_DATE'],
                    review['PURCHASE_CHANNEL'],
                    review['HELPFUL_VOTES'],
                    chunk['CHUNK'],
                    embedding_model,
                    chunk['CHUNK'],
                    sentiment_score  # レビュー全体の感情スコアを各チャンクに適用
                ]).collect()
                
                total_chunks_processed += 1
            
            # 進捗状況の更新
            progress = (i + 1) / len(reviews)
            progress_bar.progress(progress)
            progress_text.text(f"処理進捗: {i + 1}/{len(reviews)} 件完了 (合計 {total_chunks_processed} チャンク処理済み)")
        
        st.success(f"レビュー処理が完了しました。{len(reviews)} 件のレビューから合計 {total_chunks_processed} チャンクを処理しました。")
        return True
    except Exception as e:
        st.error(f"レビューの処理中にエラーが発生しました: {str(e)}")
        st.code(str(e))  # エラーの詳細をコードブロックに表示
        return False

# =========================================================
# レビュー管理テーブル操作
# =========================================================

def create_review_management_tables() -> bool:
    """
    レビュー管理用のテーブルを作成します。
    
    以下の3つのテーブルを作成します：
    1. REVIEW_CATEGORIES: レビューカテゴリのマスターテーブル
    2. REVIEW_TAGS: レビューに付与されたカテゴリタグの情報
    3. REVIEW_WORDS: レビューから抽出された重要単語情報
    
    テーブルは存在しない場合のみ作成され（CREATE IF NOT EXISTS）、
    初期データとしてデフォルトカテゴリがREVIEW_CATEGORIESに登録されます。
    
    テーブル構造:
    REVIEW_CATEGORIES:
    - category_id: 自動採番のカテゴリID
    - category_name: カテゴリ名
    - description: カテゴリの説明
    - created_at: 作成日時
    - updated_at: 更新日時

    REVIEW_TAGS:
    - tag_id: 自動採番のタグID
    - review_id: レビューID（RETAIL_DATA_WITH_PRODUCT_MASTERとEC_DATA_WITH_PRODUCT_MASTERから生成されたIDを参照）
    - category_name: カテゴリ名
    - confidence_score: 分類の確信度スコア
    - created_at: 作成日時
    - updated_at: 更新日時
    
    REVIEW_WORDS:
    - word_id: 自動採番の単語ID
    - review_id: レビューID（RETAIL_DATA_WITH_PRODUCT_MASTERとEC_DATA_WITH_PRODUCT_MASTERから生成されたIDを参照）
    - word: 抽出された単語
    - word_type: 品詞（「名詞」「動詞」「形容詞」など）
    - frequency: レビュー内での出現回数
    - created_at: 作成日時
    - updated_at: 更新日時
    
    Returns:
        bool: 全テーブルの作成に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        st.info("レビュー管理用テーブルを作成しています...")
        
        # ステップ1: カテゴリマスタテーブル（REVIEW_CATEGORIES）の作成
        st.write("カテゴリマスタテーブルを作成中...")
        snowflake_session.sql("""
        CREATE TABLE IF NOT EXISTS REVIEW_CATEGORIES (
            category_id NUMBER AUTOINCREMENT,
            category_name VARCHAR(100),
            description TEXT,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """).collect()

        # ステップ2: レビュータグテーブル（REVIEW_TAGS）の作成
        st.write("レビュータグテーブルを作成中...")
        snowflake_session.sql("""
        CREATE TABLE IF NOT EXISTS REVIEW_TAGS (
            tag_id NUMBER AUTOINCREMENT,
            review_id VARCHAR(20),
            category_name VARCHAR(100),
            confidence_score FLOAT,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """).collect()
        
        # ステップ3: 重要単語テーブル（REVIEW_WORDS）の作成
        st.write("重要単語テーブルを作成中...")
        snowflake_session.sql("""
        CREATE TABLE IF NOT EXISTS REVIEW_WORDS (
            word_id NUMBER AUTOINCREMENT,
            review_id VARCHAR(20),
            word VARCHAR(100),
            word_type VARCHAR(50),
            frequency NUMBER,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """).collect()

        # ステップ4: デフォルトカテゴリの登録（存在しない場合のみ）
        st.write("デフォルトカテゴリを登録中...")
        categories_values = ", ".join([f"('{category}')" for category in DEFAULT_CATEGORIES])
        
        # カテゴリが存在しない場合のみ登録するSQL
        insert_result = snowflake_session.sql(f"""
        INSERT INTO REVIEW_CATEGORIES (category_name)
        SELECT category FROM (VALUES {categories_values}) AS v(category)
        WHERE category NOT IN (SELECT category_name FROM REVIEW_CATEGORIES)
        """).collect()
        
        # テーブル作成後の状況を確認
        categories_count = get_table_count("REVIEW_CATEGORIES")
        tags_count = get_table_count("REVIEW_TAGS")
        words_count = get_table_count("REVIEW_WORDS")
        
        st.success(f"""
        レビュー管理用テーブルの作成が完了しました:
        - カテゴリ: {categories_count} 件
        - タグ: {tags_count} 件
        - 単語: {words_count} 件
        """)
            
        return True
    except Exception as e:
        st.error(f"テーブルの作成に失敗しました: {str(e)}")
        st.code(str(e))
        return False

def get_review_categories() -> list:
    """
    登録されているレビューカテゴリの一覧を取得します。
    
    REVIEW_CATEGORIESテーブルから全カテゴリ名を取得し、
    アルファベット順にソートして返します。
    
    Returns:
        list: カテゴリ名のリスト。テーブルが存在しない場合や
              エラーが発生した場合は空リストを返します。
    
    Example:
        categories = get_review_categories()
        for category in categories:
            print(category)
    """
    try:
        result = snowflake_session.sql("""
            SELECT category_name
            FROM REVIEW_CATEGORIES
            ORDER BY category_name
        """).collect()
        return [row['CATEGORY_NAME'] for row in result]
    except Exception as e:
        st.error(f"カテゴリ一覧の取得に失敗しました: {str(e)}")
        return []

def add_review_category(category_name: str, description: str = None) -> bool:
    """
    新しいレビューカテゴリを追加します。
    
    既に同名のカテゴリが存在する場合は追加されません。
    カテゴリ名が空文字や無効な値の場合もエラーとなります。
    
    Args:
        category_name (str): カテゴリ名
        description (str, optional): カテゴリの説明
    
    Returns:
        bool: 追加に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        snowflake_session.sql("""
            INSERT INTO REVIEW_CATEGORIES (category_name, description)
            SELECT ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM REVIEW_CATEGORIES WHERE category_name = ?
            )
        """, params=[category_name, description, category_name]).collect()
        return True
    except Exception as e:
        st.error(f"カテゴリの追加に失敗しました: {str(e)}")
        return False

def delete_review_category(category_name: str) -> bool:
    """
    レビューカテゴリを削除します。
    
    Args:
        category_name (str): 削除するカテゴリ名
    
    Returns:
        bool: 削除に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        # カテゴリの削除
        snowflake_session.sql("""
            DELETE FROM REVIEW_CATEGORIES
            WHERE category_name = ?
        """, params=[category_name]).collect()
        
        # 関連するタグの削除
        snowflake_session.sql("""
            DELETE FROM REVIEW_TAGS
            WHERE category_name = ?
        """, params=[category_name]).collect()
        
        return True
    except Exception as e:
        st.error(f"カテゴリの削除に失敗しました: {str(e)}")
        return False

def generate_review_tags() -> bool:
    """
    未分類のレビューに対してタグを自動生成します。
    
    このプロセスでは以下の処理を行います：
    1. 登録されているカテゴリ一覧を取得
    2. まだタグ付けされていないレビューを取得
    3. CLASSIFY_TEXT関数を使用して各レビューを適切なカテゴリに分類
    4. 分類結果をREVIEW_TAGSテーブルに保存
    
    CLASSIFY_TEXT関数はLLMを使用して、テキストを登録済みの
    カテゴリのいずれかに分類します。これはゼロショット分類であり、
    事前の学習データは不要です。
    
    Returns:
        bool: タグ生成処理に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        # ステップ1: カテゴリ一覧の取得
        categories = get_review_categories()
        if not categories:
            st.warning("カテゴリが登録されていません。まずはカテゴリを追加してください。")
            return False
        
        # カテゴリ情報をJSON形式で準備
        categories_json = json.dumps(categories, ensure_ascii=False)
        st.write(f"**登録済みカテゴリ**: {', '.join(categories)}")
        
        # ステップ2: 未分類のレビューを取得
        reviews = snowflake_session.sql("""
            SELECT r.*
            FROM CUSTOMER_REVIEWS r
            LEFT JOIN REVIEW_TAGS t
            ON r.review_id = t.review_id
            WHERE t.review_id IS NULL
            -- 全件処理するために制限を削除
        """).collect()
        
        if not reviews:
            st.info("分類が必要なレビューはありません。")
            return True
        
        st.write(f"**合計 {len(reviews)} 件のレビューを分類します**")
        progress_bar = st.progress(0)
        progress_text = st.empty()
        
        # 処理状況をトラッキングする変数
        processed_count = 0
        success_count = 0
        
        for i, review in enumerate(reviews):
            try:
                # ステップ3: レビューテキストを分類
                # CLASSIFY_TEXT関数を使用してレビューテキストを特定のカテゴリに分類
                result = snowflake_session.sql("""
                    SELECT 
                        SNOWFLAKE.CORTEX.★★★修正対象★★★(
                            ?,  -- 分類するテキスト
                            PARSE_JSON(?),  -- 分類カテゴリのリスト
                        ) as classification
                """, params=[
                    review['REVIEW_TEXT'],
                    categories_json
                ]).collect()[0]['CLASSIFICATION']
                
                # 結果をJSONとしてパース
                classification = json.loads(result)
                assigned_category = classification.get('label', 'その他')
                
                # ステップ4: タグ情報を保存
                snowflake_session.sql("""
                    INSERT INTO REVIEW_TAGS (
                        review_id,
                        category_name,
                        confidence_score
                    )
                    VALUES (?, ?, 1.0)
                """, params=[
                    review['REVIEW_ID'],
                    assigned_category
                ]).collect()
                
                success_count += 1
                
            except Exception as e:
                st.error(f"レビューID {review['REVIEW_ID']} の分類中にエラーが発生しました: {str(e)}")
            
            processed_count += 1
            
            # 進捗状況の更新
            progress = (i + 1) / len(reviews)
            progress_bar.progress(progress)
            progress_text.text(f"処理進捗: {i + 1}/{len(reviews)} 件完了（成功: {success_count}件）")
        
        st.success(f"レビュータグ生成が完了しました。{success_count}/{processed_count} 件を正常に処理しました。")
        return True
    
    except Exception as e:
        st.error(f"レビューの分類中にエラーが発生しました: {str(e)}")
        st.code(str(e))
        return False

def extract_important_words() -> bool:
    """
    レビューから重要な単語を抽出し、分析結果を保存します。
    
    このプロセスでは以下の処理を行います：
    1. 単語抽出がまだ行われていないレビューを取得
    2. レビューを10件ずつのバッチに分割
    3. 各バッチ内の複数レビューを一度のCOMPLETE関数呼び出しでまとめて処理
    4. 単語の品詞と出現頻度を分析
    5. 結果をREVIEW_WORDSテーブルに保存
    
    COMPLETE関数は構造化された出力形式（JSON）を指定して実行され、
    テキスト内の重要な単語、その品詞、出現頻度を抽出します。
    これにより、頻出単語や特徴的な表現を分析できます。
    
    抽出される品詞:
    - 名詞: 製品名、特徴、部品名など
    - 動詞: 操作や動作を表す語
    - 形容詞: 評価や感想を表す語
    
    Returns:
        bool: 単語抽出処理に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        # ステップ1: 未処理のレビューを取得
        reviews = snowflake_session.sql("""
            SELECT r.*
            FROM CUSTOMER_REVIEWS r
            LEFT JOIN REVIEW_WORDS w
            ON r.review_id = w.review_id
            WHERE w.review_id IS NULL
            -- 全件処理するために制限を削除
        """).collect()
        
        if not reviews:
            st.info("処理が必要なレビューはありません。")
            return True
        
        st.write(f"**合計 {len(reviews)} 件のレビューから単語を抽出します**")
        progress_bar = st.progress(0)
        progress_text = st.empty()
        
        # 処理状況をトラッキングする変数
        processed_count = 0
        words_extracted = 0
        
        # バッチサイズを設定
        batch_size = 10
        
        # レビューをバッチに分割
        for i in range(0, len(reviews), batch_size):
            batch = reviews[i:i+batch_size]
            batch_reviews_ids = [review['REVIEW_ID'] for review in batch]
            
            try:
                st.write(f"バッチ {i//batch_size + 1}: {len(batch)} 件のレビューを一度に処理中...")
                
                # 複数レビューのテキストを準備
                combined_reviews = []
                for idx, review in enumerate(batch):
                    combined_reviews.append({
                        "id": review['REVIEW_ID'],
                        "text": review['REVIEW_TEXT']
                    })
                
                # ステップ2: 複数レビューを一度のCOMPLETE呼び出しで処理
                result = snowflake_session.sql("""
                    SELECT SNOWFLAKE.CORTEX.★★★修正対象★★★(
                        ?,  -- 使用するLLMモデル
                        [
                            {
                                'role': 'system',
                                'content': 'テキストから重要な単語を抽出し、品詞と出現回数を分析してください。対象テキスト：明日の東日本は広い範囲で大雪となるでしょう。'
                            },
                            {
                                'role': 'user',
                                'content': ?  -- 分析する複数レビューテキスト（JSONフォーマット）
                            }
                        ],
                        {
                            'temperature': 0,  -- 生成結果の多様性（0=決定的な出力）
                            'max_tokens': 2000,  -- 最大応答トークン数を増やす
                            'response_format': {
                                'type': 'json',
                                'schema': {
                                    'type': 'object',
                                    'properties': {
                                        'reviews_analysis': {
                                            'type': 'array',
                                            'items': {
                                                'type': 'object',
                                                'properties': {
                                                    'review_id': {
                                                        'type': 'string',
                                                        'description': 'レビューのID'
                                                    },
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
                                                'required': ['review_id', 'words']
                                            }
                                        }
                                    },
                                    'required': ['reviews_analysis']
                                }
                            }
                        }
                    ) as result
                """, params=[
                    complete_model,
                    json.dumps(combined_reviews)  # JSONとして複数レビューを渡す
                ]).collect()[0]['RESULT']
                
                # ステップ3: 結果をJSONとしてパース
                response = json.loads(result)
                
                # Snowflake Cortexの出力形式に対応するための処理
                reviews_data = []
                if 'structured_output' in response:
                    # 新形式
                    structured_output = response.get('structured_output', [{}])[0].get('raw_message', {})
                    reviews_data = structured_output.get('reviews_analysis', [])
                else:
                    # 旧形式（直接JSON）
                    reviews_data = response.get('reviews_analysis', [])
                
                # ステップ4: バッチ内の各レビューの単語情報を処理
                local_processed = 0
                for review_data in reviews_data:
                    review_id = review_data.get('review_id')
                    words_data = review_data.get('words', [])
                    
                    # review_idが実際のレビューIDと一致しない場合の対応
                    # 一致しない場合は、バッチ内のレビューIDを順番に割り当てる
                    if not review_id or review_id not in batch_reviews_ids:
                        if local_processed < len(batch):
                            review_id = batch[local_processed]['REVIEW_ID']
                    
                    # 単語情報をテーブルに保存
                    local_words_count = 0
                    for word in words_data:
                        # 単語データの検証
                        if not all(k in word for k in ['word', 'type', 'frequency']):
                            continue
                            
                        # テーブルに挿入
                        snowflake_session.sql("""
                            INSERT INTO REVIEW_WORDS (
                                review_id,
                                word,
                                word_type,
                                frequency
                            )
                            VALUES (?, ?, ?, ?)
                        """, params=[
                            review_id,
                            word['word'],
                            word['type'],
                            word['frequency']
                        ]).collect()
                        
                        local_words_count += 1
                    
                    words_extracted += local_words_count
                    local_processed += 1
                
                # もしAIの出力が不完全だった場合、残りのレビューも処理済みとしてカウント
                processed_count += len(batch)
                
                # バッチごとの進捗状況の更新
                progress = min(1.0, processed_count / len(reviews))
                progress_bar.progress(progress)
                progress_text.text(f"処理進捗: {processed_count}/{len(reviews)} 件完了 (合計 {words_extracted} 単語抽出済み)")
                
            except Exception as e:
                st.error(f"バッチ処理中にエラーが発生しました: {str(e)}")
                # エラーが発生しても処理カウントを進める
                processed_count += len(batch)
        
        st.success(f"単語抽出が完了しました。{processed_count} 件のレビューから合計 {words_extracted} 単語を抽出しました。")
        return True
    except Exception as e:
        st.error(f"単語抽出中にエラーが発生しました: {str(e)}")
        st.code(str(e))
        return False

# =========================================================
# Cortex Search Service 操作
# =========================================================

def check_search_service_exists() -> bool:
    """Cortex Search Serviceが存在するかチェックします。
    
    Returns:
        bool: 検索サービスが存在する場合はTrue、存在しない場合はFalse
    """
    try:
        # 現在のデータベースとスキーマを取得
        current_db_schema = snowflake_session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
        
        # サービスの存在確認
        result = snowflake_session.sql(f"""
            SHOW CORTEX SEARCH SERVICES LIKE 'snow_retail_search_service'
        """).collect()
        
        return len(result) > 0
    
    except Exception:
        return False  # エラーが発生した場合は存在しないと判断

def create_snow_retail_search_service(warehouse, model) -> bool:
    """Cortex Search Serviceを作成します。
    
    Args:
        warehouse (str): 使用するSnowflakeウェアハウス名
        model (str): 使用する埋め込みモデル名
    
    Returns:
        bool: サービス作成に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        # 現在のデータベースとスキーマを取得
        current_db_schema = snowflake_session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
        
        # サービスの作成
        try:
            snowflake_session.sql(f"""
            CREATE OR REPLACE CORTEX SEARCH SERVICE snow_retail_search_service
            ON content
            ATTRIBUTES title, document_type, department
            WAREHOUSE = '{warehouse}'
            TARGET_LAG = '1 day'
            EMBEDDING_MODEL = '{model}'
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
            FROM SNOW_RETAIL_DOCUMENTS
            """).collect()
        except Exception as sql_error:
            # SQL実行中にエラーが発生した場合でも、サービスが作成されている可能性を確認
            if check_search_service_exists():
                st.success("Cortex Search Serviceは正常に作成されました。")
                
                # アクセス権限の付与
                snowflake_session.sql(f"""
                    GRANT USAGE ON CORTEX SEARCH SERVICE snow_retail_search_service TO ROLE CURRENT_ROLE()
                """).collect()
                
                return True
            else:
                # 本当にエラーがあった場合
                raise sql_error
        
        # アクセス権限の付与
        snowflake_session.sql(f"""
            GRANT USAGE ON CORTEX SEARCH SERVICE snow_retail_search_service TO ROLE CURRENT_ROLE()
        """).collect()
        
        st.success("Cortex Search Serviceを作成しました。")
        return True
    
    except Exception as e:
        # サービス作成中に例外が発生してもう一度存在確認
        if check_search_service_exists():
            st.success("Cortex Search Serviceは正常に作成されました。")
            return True
        
        st.error(f"Cortex Search Serviceの作成に失敗しました: {str(e)}")
        return False

def delete_snow_retail_search_service() -> bool:
    """スノーリテールのCortex Search Serviceを削除します。
    
    Returns:
        bool: 削除に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        # 現在のデータベースとスキーマを取得
        current_db_schema = snowflake_session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
        current_database = current_db_schema['CURRENT_DATABASE()']
        current_schema = current_db_schema['CURRENT_SCHEMA()']
        
        snowflake_session.sql(f"""
            DROP CORTEX SEARCH SERVICE {current_database}.{current_schema}.snow_retail_search_service
        """).collect()
        st.success("Cortex Search Serviceを削除しました。")
        return True
    except Exception as e:
        st.error(f"Cortex Search Serviceの削除に失敗しました: {str(e)}")
        return False

# =========================================================
# UI関数
# =========================================================

def render_data_preparation_page():
    """データ準備ページを表示します。"""
    st.header("データ準備")
    
    # データ準備機能の概要説明
    st.info("""
    ## 🔍 データ準備機能について
    
    このページでは、レビューデータの分析準備を行います。以下の処理が実行されます：
    
    ### 1. テーブル作成と初期設定
    * 分析用テーブル (CUSTOMER_ANALYSIS) の作成
    
    ### 2. レビューテキスト処理
    * **テキスト分割**: SPLIT_TEXT_RECURSIVE_CHARACTER関数を使用して、レビューテキストを300文字以内のチャンクに分割
    * **翻訳処理**: TRANSLATE関数を使用して、日本語のテキストを英語に翻訳
    * **感情分析**: SENTIMENT関数を使用して、翻訳されたテキストの感情スコア (-1〜1) を算出
    * **ベクトル化**: EMBED_TEXT_1024関数を使用して、テキストを1024次元のベクトルデータに変換
    """)
    
    # 分析用テーブルが存在しない場合は作成を促す
    if not check_table_exists("CUSTOMER_ANALYSIS"):
        st.warning("分析用テーブルが存在しません。まずはテーブルを作成してください。")
        if st.button("分析用テーブルを作成"):
            if create_customer_analysis_table():
                st.success("分析用テーブルの作成が完了しました。")
                st.rerun()
        return
    
    # データ処理ボタン
    if st.button("レビューデータの処理を実行"):
        with st.expander("処理の詳細", expanded=True):
            st.info("以下の処理が実行されます：\n"
                   "1. 未処理のレビューテキストを取得\n"
                   "2. レビューをチャンクに分割 (SPLIT_TEXT_RECURSIVE_CHARACTER関数)\n"
                   "3. 感情分析とベクトル化を実行 (TRANSRATE関数、SENTIMENT関数、EMBED_TEXT_1024関数)\n"
                   "4. 分析結果をテーブル (CUSTOMER_ANALYSIS) に保存")
            process_review_chunks()
    
    # データ状況のダッシュボード表示
    st.subheader("📊 データ状況")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("### 店舗データ")
        retail_count = get_table_count("RETAIL_DATA_WITH_PRODUCT_MASTER")
        st.metric("店舗データ数", retail_count)
        
        # サンプルデータの表示
        if retail_count > 0:
            with st.expander("店舗データサンプル"):
                sample = snowflake_session.sql("""
                    SELECT 
                        transaction_id, 
                        product_id_master, 
                        product_name_master, 
                        transaction_date, 
                        quantity 
                    FROM RETAIL_DATA_WITH_PRODUCT_MASTER 
                    LIMIT 3
                """).collect()
                for item in sample:
                    st.write(f"**ID**: {item['TRANSACTION_ID']}, **商品**: {item['PRODUCT_NAME_MASTER']}")
                    st.write(f"**日付**: {item['TRANSACTION_DATE']}, **数量**: {item['QUANTITY']}")
                    st.write("---")
    
    with col2:
        st.info("### ECデータ")
        ec_count = get_table_count("EC_DATA_WITH_PRODUCT_MASTER")
        st.metric("ECデータ数", ec_count)
        
        # サンプルデータの表示
        if ec_count > 0:
            with st.expander("ECデータサンプル"):
                sample = snowflake_session.sql("""
                    SELECT 
                        transaction_id, 
                        product_id_master, 
                        product_name_master, 
                        transaction_date, 
                        quantity 
                    FROM EC_DATA_WITH_PRODUCT_MASTER 
                    LIMIT 3
                """).collect()
                for item in sample:
                    st.write(f"**ID**: {item['TRANSACTION_ID']}, **商品**: {item['PRODUCT_NAME_MASTER']}")
                    st.write(f"**日付**: {item['TRANSACTION_DATE']}, **数量**: {item['QUANTITY']}")
                    st.write("---")
    
    with col3:
        st.info("### 社内文書データ")
        document_count = get_table_count("SNOW_RETAIL_DOCUMENTS")
        st.metric("文書総数", document_count)
        
        # 文書タイプの分布
        if document_count > 0:
            with st.expander("文書タイプの分布"):
                doc_types = snowflake_session.sql("""
                    SELECT document_type, COUNT(*) as count
                    FROM SNOW_RETAIL_DOCUMENTS
                    GROUP BY document_type
                    ORDER BY count DESC
                """).collect()
                for dt in doc_types:
                    st.write(f"**{dt['DOCUMENT_TYPE']}**: {dt['COUNT']} 件")
                    
        st.info("### 処理済みデータ")
        analysis_count = get_table_count("CUSTOMER_ANALYSIS")
        st.metric("処理済みチャンク数", analysis_count)
        
        # 感情スコアの分布
        if analysis_count > 0:
            with st.expander("感情スコアの分布"):
                sentiment_stats = snowflake_session.sql("""
                    SELECT 
                        MIN(sentiment_score) as min_score,
                        MAX(sentiment_score) as max_score,
                        AVG(sentiment_score) as avg_score
                    FROM CUSTOMER_ANALYSIS
                """).collect()[0]
                st.write(f"**最小スコア**: {sentiment_stats['MIN_SCORE']:.2f}")
                st.write(f"**最大スコア**: {sentiment_stats['MAX_SCORE']:.2f}")
                st.write(f"**平均スコア**: {sentiment_stats['AVG_SCORE']:.2f}")

def render_voice_analysis_page():
    """顧客の声分析ページを表示します。"""
    st.header("顧客の声分析")
    
    # 必要なテーブルが存在しない場合は作成を促す
    if not check_table_exists("REVIEW_CATEGORIES") or not check_table_exists("REVIEW_TAGS"):
        st.warning("レビュー管理用のテーブルが存在しません。まずはテーブルを作成してください。")
        if st.button("レビュー管理用テーブルを作成"):
            if create_review_management_tables():
                st.success("レビュー管理用テーブルの作成が完了しました。")
                st.rerun()
        return
    
    # 分析ダッシュボード
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "⚙️ 管理",
        "📊 全体概要",
        "😊 感情分析",
        "🔤 単語分析",
        "📝 詳細分析",
        "🔍 顧客の声検索"
    ])
    
    with tab1:
        render_management_page()
    
    with tab2:
        render_overview_dashboard()
    
    with tab3:
        render_sentiment_analysis()
    
    with tab4:
        render_word_analysis()
    
    with tab5:
        render_detail_analysis()
    
    with tab6:
        render_vector_search()

def render_overview_dashboard():
    """全体概要ダッシュボードを表示します。"""
    # ヘッダーをサブヘッダーに変更して視覚的階層を整理
    st.subheader("全体概要")
    
    # データの取得
    df = pd.DataFrame(snowflake_session.sql("""
        WITH review_stats AS (
            SELECT 
                r.review_id,
                r.rating,
                r.review_text,
                r.helpful_votes,
                t.category_name,
                a.sentiment_score,
                DATE_TRUNC('month', r.review_date) as review_month
            FROM CUSTOMER_REVIEWS r
            LEFT JOIN REVIEW_TAGS t ON r.review_id = t.review_id
            LEFT JOIN CUSTOMER_ANALYSIS a ON r.review_id = a.review_id
        )
        SELECT * FROM review_stats
    """).collect())
    
    if df.empty:
        st.info("分析可能なレビューデータがありません。")
        return
    
    # 上部メトリクス
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("総レビュー数", len(df))
    with col2:
        st.metric("平均評価", f"{df['RATING'].mean():.1f}")
    with col3:
        st.metric("平均感情スコア", f"{df['SENTIMENT_SCORE'].mean():.2f}")
    with col4:
        st.metric("総参考になった数", df['HELPFUL_VOTES'].sum())
    
    # グラフ表示エリア
    col1, col2 = st.columns(2)
    
    with col1:
        # 評価分布
        fig_rating = px.histogram(
            df,
            x="RATING",
            title="評価分布",
            labels={"RATING": "評価", "count": "件数"},
            nbins=10
        )
        st.plotly_chart(fig_rating, use_container_width=True, key="overview_rating_hist")
        
        # カテゴリ別レビュー数
        if 'CATEGORY_NAME' in df.columns:
            fig_category = px.bar(
                df["CATEGORY_NAME"].value_counts().reset_index(),
                x="CATEGORY_NAME",
                y="count",
                title="カテゴリ別レビュー数",
                labels={"CATEGORY_NAME": "カテゴリ", "count": "レビュー数"}
            )
            fig_category.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig_category, use_container_width=True, key="overview_category_bar")
    
    with col2:
        # 感情スコア分布
        fig_sentiment = px.histogram(
            df,
            x="SENTIMENT_SCORE",
            title="感情スコア分布",
            labels={"SENTIMENT_SCORE": "感情スコア", "count": "件数"},
            nbins=20
        )
        st.plotly_chart(fig_sentiment, use_container_width=True, key="overview_sentiment_hist")
        
        # 月別レビュー数推移
        monthly_reviews = df.groupby("REVIEW_MONTH").size().reset_index(name="count")
        fig_trend = px.line(
            monthly_reviews,
            x="REVIEW_MONTH",
            y="count",
            title="月別レビュー数推移",
            labels={"REVIEW_MONTH": "月", "count": "レビュー数"}
        )
        st.plotly_chart(fig_trend, use_container_width=True, key="overview_monthly_trend")

def render_sentiment_analysis():
    """感情分析ページを表示します。"""
    
    # データの取得
    df = pd.DataFrame(snowflake_session.sql("""
        SELECT 
            r.review_id,
            r.rating,
            r.review_text,
            r.helpful_votes,
            t.category_name,
            a.sentiment_score,
            TO_VARCHAR(DATE_TRUNC('month', r.review_date)) as review_month
        FROM CUSTOMER_REVIEWS r
        LEFT JOIN REVIEW_TAGS t ON r.review_id = t.review_id
        LEFT JOIN (
            SELECT review_id, MIN(sentiment_score) as sentiment_score
            FROM CUSTOMER_ANALYSIS
            GROUP BY review_id
        ) a ON r.review_id = a.review_id
        WHERE a.sentiment_score IS NOT NULL
    """).collect())
    
    if df.empty:
        st.info("感情分析が完了したレビューデータがありません。")
        return
    
    # カテゴリフィルター
    if 'CATEGORY_NAME' in df.columns and not df['CATEGORY_NAME'].isna().all():
        categories = [cat for cat in df["CATEGORY_NAME"].unique() if cat is not None]
        selected_categories = st.multiselect(
            "カテゴリでフィルター",
            categories,
            default=categories
        )
        filtered_df = df[df["CATEGORY_NAME"].isin(selected_categories)]
    else:
        filtered_df = df
    
    # カテゴリ別感情スコアと評価の相関（重要な感情分析固有の内容）
    st.subheader("感情スコア分析")
    if 'CATEGORY_NAME' in filtered_df.columns and not filtered_df['CATEGORY_NAME'].isna().all():
        col1, col2 = st.columns(2)
        
        with col1:
            # カテゴリ別平均感情スコア
            category_sentiment = filtered_df.groupby("CATEGORY_NAME")["SENTIMENT_SCORE"].mean().reset_index()
            fig_category_sentiment = px.bar(
                category_sentiment,
                x="CATEGORY_NAME",
                y="SENTIMENT_SCORE",
                title="カテゴリ別平均感情スコア",
                labels={"CATEGORY_NAME": "カテゴリ", "SENTIMENT_SCORE": "平均感情スコア"}
            )
            fig_category_sentiment.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig_category_sentiment, use_container_width=True, key="sentiment_category_score_bar")
        
        with col2:
            # 評価と感情スコアの相関
            fig_correlation = px.scatter(
                filtered_df,
                x="RATING",
                y="SENTIMENT_SCORE",
                color="CATEGORY_NAME",
                title="評価と感情スコアの相関",
                labels={
                    "RATING": "評価",
                    "SENTIMENT_SCORE": "感情スコア",
                    "CATEGORY_NAME": "カテゴリ"
                }
            )
            st.plotly_chart(fig_correlation, use_container_width=True, key="sentiment_correlation_scatter")
    
    # 感情スコアの高い/低いレビューの表示
    st.subheader("感情スコアによるレビュー分析")
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("#### 最も肯定的なレビュー")
        positive_reviews = filtered_df.nlargest(5, "SENTIMENT_SCORE")
        for _, review in positive_reviews.iterrows():
            with st.expander(f"感情スコア: {review['SENTIMENT_SCORE']:.2f}"):
                st.write(review["REVIEW_TEXT"])
    
    with col2:
        st.write("#### 最も否定的なレビュー")
        negative_reviews = filtered_df.nsmallest(5, "SENTIMENT_SCORE")
        for _, review in negative_reviews.iterrows():
            with st.expander(f"感情スコア: {review['SENTIMENT_SCORE']:.2f}"):
                st.write(review["REVIEW_TEXT"])

def render_word_analysis():
    """単語分析ページを表示します。"""
    
    # レビュータグテーブルから単語データを取得
    if not check_table_exists("REVIEW_WORDS"):
        st.warning("重要単語テーブルが存在しません。まずはレビュー管理テーブルを作成し、単語抽出を実行してください。")
        return
    
    # カテゴリ一覧の取得
    categories = get_review_categories()
    if not categories:
        st.warning("カテゴリが登録されていません。")
        return
    
    # カテゴリフィルター
    selected_category = st.selectbox(
        "カテゴリでフィルター",
        ["すべて"] + categories
    )
    
    # 単語タイプフィルター
    word_types = snowflake_session.sql("""
        SELECT DISTINCT word_type FROM REVIEW_WORDS
        ORDER BY word_type
    """).collect()
    word_types = [row['WORD_TYPE'] for row in word_types]
    selected_word_types = st.multiselect(
        "単語タイプでフィルター",
        word_types,
        default=word_types
    )
    
    # フィルター条件の構築
    category_condition = f"t.category_name = '{selected_category}'" if selected_category != "すべて" else "1=1"
    word_type_condition = "w.word_type IN (" + ", ".join([f"'{wt}'" for wt in selected_word_types]) + ")" if selected_word_types else "1=1"
    
    # データの取得
    df = pd.DataFrame(snowflake_session.sql(f"""
        WITH product_data AS (
            -- 店舗データ
            SELECT 
                r.product_id_master as product_id,
                r.product_name_master
            FROM RETAIL_DATA_WITH_PRODUCT_MASTER r
            
            UNION ALL
            
            -- ECデータ
            SELECT 
                e.product_id_master as product_id,
                e.product_name_master
            FROM EC_DATA_WITH_PRODUCT_MASTER e
        ),
        -- 正確な単語出現回数を計算
        word_frequency AS (
            SELECT 
                word,
                word_type,
                COUNT(DISTINCT review_id) as review_count,
                -- 各レビューでの実際の出現回数を合計
                SUM(frequency) as actual_total
            FROM (
                -- 各レビューで各単語は1回だけカウント（重複を排除）
                SELECT 
                    word,
                    word_type,
                    review_id,
                    frequency
                FROM REVIEW_WORDS
                QUALIFY ROW_NUMBER() OVER (PARTITION BY word, word_type, review_id ORDER BY frequency DESC) = 1
            )
            GROUP BY word, word_type
        )
        
        SELECT 
            w.word,
            w.word_type,
            w.review_count,
            -- 総出現回数は単語の全レビューでの出現回数
            ROUND(w.actual_total) as total_mentions,
            -- 平均出現回数は総出現回数をレビュー数で割った値
            ROUND(w.actual_total / w.review_count, 2) as avg_frequency,
            LISTAGG(DISTINCT p.product_name_master, ', ') WITHIN GROUP (ORDER BY p.product_name_master) as products
        FROM word_frequency w
        LEFT JOIN REVIEW_WORDS rw ON w.word = rw.word AND w.word_type = rw.word_type
        LEFT JOIN REVIEW_TAGS t ON rw.review_id = t.review_id
        LEFT JOIN CUSTOMER_ANALYSIS a ON rw.review_id = a.review_id
        LEFT JOIN product_data p ON p.product_id = a.product_id
        WHERE {category_condition} AND {word_type_condition}
        AND w.review_count > 1
        GROUP BY w.word, w.word_type, w.review_count, w.actual_total
        ORDER BY total_mentions DESC
        LIMIT 100
    """).collect())
    
    if df.empty:
        st.info("条件に合う単語データがありません。")
        return
    
    # 単語の出現状況をテーブルで表示
    st.subheader("単語出現状況")
    
    # 列の表示形式をカスタマイズ
    formatted_df = df.copy()
    
    def format_product_list(product_text):
        """商品リストを整形する関数"""
        if product_text is None or product_text == '':
            return '関連商品なし'
        
        # 長すぎる場合は切り詰める
        if len(product_text) > 100:
            return product_text[:100] + '...'
        return product_text
    
    # 商品リストの整形
    formatted_df['PRODUCTS'] = formatted_df['PRODUCTS'].apply(format_product_list)
    
    # 出現回数の説明文を追加
    st.info("""
    ※ 出現データについて:
    - 「データ件数」: この単語が出現したレビューの件数です
    - 「総出現回数」: すべてのレビュー内でのこの単語の合計出現回数です (各レビュー内での出現回数の合計)
    - 「平均出現回数」: 1レビューあたりの平均出現回数です
    """)
    
    st.dataframe(
        formatted_df.rename(columns={
            "WORD": "単語",
            "WORD_TYPE": "品詞",
            "REVIEW_COUNT": "データ件数",
            "TOTAL_MENTIONS": "総出現回数",
            "AVG_FREQUENCY": "平均出現回数",
            "PRODUCTS": "関連商品"
        }),
        use_container_width=True
    )
    
    # ワードクラウドの表示
    st.subheader("ワードクラウド")
    # ここでは単純な頻度テーブルを表示
    # ワードクラウドライブラリを使うことも可能だが今回は簡略化
    
    col1, col2 = st.columns(2)
    
    with col1:
        words_by_type = df.groupby('WORD_TYPE')['TOTAL_MENTIONS'].sum().reset_index()
        fig = px.pie(
            words_by_type,
            values='TOTAL_MENTIONS',
            names='WORD_TYPE',
            title='品詞別の単語出現回数の割合'
        )
        st.plotly_chart(fig, use_container_width=True, key="word_analysis_pie")
    
    with col2:
        top_words = df.head(20)
        fig = px.bar(
            top_words,
            x='WORD',
            y='TOTAL_MENTIONS',
            title='総出現回数TOP20の単語',
            labels={"WORD": "単語", "TOTAL_MENTIONS": "総出現回数"}
        )
        fig.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True, key="word_analysis_top20")

def render_detail_analysis():
    """詳細分析（カテゴリ別レビュー一覧）を表示します。"""
    
    categories = get_review_categories()
    if not categories:
        st.info("カテゴリが登録されていません。")
        return
    
    selected_category = st.selectbox(
        "分析するカテゴリを選択",
        categories
    )
    
    # 選択されたカテゴリのレビュー一覧を表示
    reviews = snowflake_session.sql("""
        SELECT 
            r.*,
            t.confidence_score,
            a.sentiment_score
        FROM REVIEW_TAGS t
        JOIN CUSTOMER_REVIEWS r ON t.review_id = r.review_id
        LEFT JOIN (
            SELECT review_id, MIN(sentiment_score) as sentiment_score
            FROM CUSTOMER_ANALYSIS
            GROUP BY review_id
        ) a ON r.review_id = a.review_id
        WHERE t.category_name = ?
        ORDER BY r.review_date DESC
    """, params=[selected_category]).collect()
    
    if reviews:
        for review in reviews:
            with st.expander(f"レビュー: {review['REVIEW_TEXT'][:100]}..."):
                st.write(f"**感情スコア**: {review['SENTIMENT_SCORE']:.2f}")
                st.write(f"**評価**: {review['RATING']}")
                st.write(f"**投稿日**: {review['REVIEW_DATE']}")
                st.write(f"**参考になった数**: {review['HELPFUL_VOTES']}")
    else:
        st.info(f"カテゴリ '{selected_category}' のレビューはまだありません。")

def render_analyst_chatbot_page():
    """Cortex Analystを使用した分析チャットボットページを表示します。"""
    st.header("分析チャットボット")
    
    # ワークショップ向けの説明
    st.info("""
    ## 📊 分析チャットボットについて
    
    このページでは、Snowflake Cortex Analystを使用したデータ分析チャットボットを体験できます。
    
    ### 主な機能
    * **自然言語でのデータ分析**: 質問をSQLに自動変換し、データベースに対して実行
    * **視覚化**: 分析結果を自動的にグラフ化して表示
    * **日本語対応**: 英語で返される分析結果を自動的に日本語に翻訳
    
    ### 使い方のヒント
    * 今回は店舗とECの取引データを元にセマンティックモデルを作成しているため、販売数量や売上金額などの分析に適しています。
    * データに関する質問を具体的に記述してください（例：「2023年の四半期ごとの売上推移を教えて」）
    * 質問はデータに関連するものに限定されます（一般的な会話ではなく、データ分析のクエリとして解釈されます）
    * 分析結果はグラフと表形式で表示され、生成されたSQLも確認できます
    
    ### セマンティックモデル
    このチャットボットは、選択したセマンティックモデルを使用してデータベースのスキーマを理解しています。
    """)
    
    # セマンティックモデルの選択
    semantic_model_files = get_semantic_model_files()
    
    if not semantic_model_files:
        st.error(f"""
        セマンティックモデルファイルが見つかりません。
        ステージ 'SEMANTIC_MODEL_STAGE' にセマンティックモデルファイル（.yamlまたは.yml）をアップロードしてください。
        """)
        return
    
    SEMANTIC_MODEL_STAGE = "SEMANTIC_MODEL_STAGE"
    selected_model_file = st.selectbox(
        "使用するセマンティックモデルを選択してください",
        semantic_model_files,
        index=0
    )
    
    # 選択されたセマンティックモデルのパス
    full_stage_path = f"@{SEMANTIC_MODEL_STAGE}/{selected_model_file}"
    
    # セッション状態の初期化
    if "analyst_messages" not in st.session_state:
        st.session_state.analyst_messages = []
    
    # チャット履歴のクリアボタン
    if st.button("チャット履歴をクリア"):
        st.session_state.analyst_messages = []
        st.rerun()
    
    # チャット履歴の表示
    for message in st.session_state.analyst_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if "result" in message and message["result"] is not None:
                # 結果が DataFrame であれば表示
                if isinstance(message["result"], pd.DataFrame) and not message["result"].empty:
                    st.dataframe(message["result"])
                
                # SQLクエリが含まれていれば表示
                if "sql" in message and message["sql"]:
                    with st.expander("生成されたSQL"):
                        st.code(message["sql"], language="sql")
                
                # グラフが含まれていれば表示
                if "chart" in message and message["chart"]:
                    st.plotly_chart(message["chart"], use_container_width=True, key=f"analyst_chart_{st.session_state.analyst_messages.index(message)}")
    
    # ユーザー入力の処理
    if prompt := st.chat_input("データについて質問してください"):
        # ユーザーメッセージの表示
        st.session_state.analyst_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # 回答生成の処理
        with st.spinner("回答を生成中..."):
            try:
                # メッセージの準備
                messages = [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": prompt}]
                    }
                ]
                
                # リクエストボディの準備
                request_body = {
                    "messages": messages,
                    "semantic_model_file": full_stage_path,
                }
                
                # Cortex Analyst API呼び出し
                try:
                    import _snowflake
                    # Snowflake内部APIを使用
                    resp = _snowflake.send_snow_api_request(
                        "POST",
                        ANALYST_API_ENDPOINT,
                        {},  # headers
                        {},  # params
                        request_body,
                        None,  # request_guid
                        ANALYST_API_TIMEOUT * 1000,  # ミリ秒に変換
                    )
                    
                    # レスポンスの処理
                    if resp["status"] < 400:
                        response_data = json.loads(resp["content"])
                        if "message" in response_data and "content" in response_data["message"]:
                            content_list = response_data["message"]["content"]
                            
                            # テキストとSQLを抽出
                            response_text = ""
                            sql_query = ""
                            result_data = None
                            chart = None
                            
                            for item in content_list:
                                if item["type"] == "text":
                                    response_text += item["text"] + "\n\n"
                                elif item["type"] == "sql":
                                    sql_query = item["statement"]
                            
                            # 英語のレスポンスを日本語に翻訳
                            if response_text:
                                try:
                                    translated_response = snowflake_session.sql("""
                                        SELECT SNOWFLAKE.CORTEX.TRANSLATE(?, 'en', 'ja') as translated
                                    """, params=[response_text.strip()]).collect()[0]['TRANSLATED']
                                    response_text = translated_response
                                except Exception as translate_error:
                                    st.warning(f"翻訳中にエラーが発生しました。元の英語レスポンスを表示します: {str(translate_error)}")
                            
                            # SQLを実行してデータフレームを取得
                            try:
                                # SQLクエリが存在し、空でない場合のみ実行
                                if sql_query and sql_query.strip():
                                    result_data = snowflake_session.sql(sql_query).to_pandas()
                                else:
                                    # SQLが生成されなかった場合
                                    result_data = None
                                    chart = None
                                
                                # シンプルなグラフを作成（データに基づいて）
                                if result_data is not None and not result_data.empty and len(result_data.columns) >= 2:
                                    x_col = result_data.columns[0]
                                    y_col = result_data.columns[1]
                                    
                                    # データタイプに基づいて適切なグラフを選択
                                    if result_data[x_col].dtype == 'object':  # カテゴリデータ
                                        chart = px.bar(
                                            result_data,
                                            x=x_col,
                                            y=y_col,
                                            title="分析結果"
                                        )
                                    else:  # 数値データ
                                        chart = px.line(
                                            result_data,
                                            x=x_col,
                                            y=y_col,
                                            title="分析結果"
                                        )
                            except Exception as sql_error:
                                st.error(f"SQL実行エラー: {str(sql_error)}")
                                result_data = None
                                chart = None
                            
                            # 応答をチャット履歴に追加
                            st.session_state.analyst_messages.append({
                                "role": "assistant", 
                                "content": response_text.strip(),
                                "result": result_data,
                                "sql": sql_query,
                                "chart": chart
                            })
                            
                            # 応答を表示
                            with st.chat_message("assistant"):
                                st.markdown(response_text.strip())
                                
                                if result_data is not None and not result_data.empty:
                                    st.dataframe(result_data)
                                
                                if chart:
                                    st.plotly_chart(chart, use_container_width=True)
                                
                                if sql_query:
                                    with st.expander("生成されたSQL"):
                                        st.code(sql_query, language="sql")
                        else:
                            raise Exception("APIレスポンスの形式が不正です")
                    else:
                        error_content = json.loads(resp["content"])
                        error_msg = f"""
                        🚨 APIエラーが発生しました 🚨
                        
                        * ステータスコード: `{resp['status']}`
                        * リクエストID: `{error_content.get('request_id', 'N/A')}`
                        * エラーコード: `{error_content.get('error_code', 'N/A')}`
                        
                        メッセージ:
                        ```
                        {error_content.get('message', '不明なエラー')}
                        ```
                        """
                        raise Exception(error_msg)
                
                except ImportError:
                    # Snowflake内部APIが使用できない場合
                    st.error("Snowflakeの内部APIにアクセスできません。Streamlit in Snowflake環境で実行してください。")
                    
                    # 代替のモック応答を生成（デモ用）
                    response_text = f"質問: {prompt}\n\n申し訳ありませんが、Cortex Analystへのアクセスが現在利用できません。本来であれば、ここにデータ分析の結果が日本語で表示されます。"
                    result_data = pd.DataFrame({
                        "カテゴリ": ["商品の品質", "価格", "接客サービス", "店舗環境", "その他"],
                        "レビュー数": [45, 32, 28, 15, 10]
                    })
                    
                    # モックチャート
                    chart = px.bar(
                        result_data,
                        x="カテゴリ",
                        y="レビュー数",
                        title="カテゴリ別レビュー数（モックデータ）"
                    )
                    
                    # 応答をチャット履歴に追加
                    st.session_state.analyst_messages.append({
                        "role": "assistant", 
                        "content": response_text,
                        "result": result_data,
                        "sql": "-- モックSQL\nSELECT category_name, COUNT(*) as count\nFROM REVIEW_TAGS\nGROUP BY category_name\nORDER BY count DESC",
                        "chart": chart
                    })
                    
                    # 応答を表示
                    with st.chat_message("assistant"):
                        st.markdown(response_text)
                        st.dataframe(result_data)
                        st.plotly_chart(chart, use_container_width=True)
                        
                        with st.expander("モックSQL"):
                            st.code("-- モックSQL\nSELECT category_name, COUNT(*) as count\nFROM REVIEW_TAGS\nGROUP BY category_name\nORDER BY count DESC", language="sql")
                
            except Exception as e:
                error_msg = f"""
                Cortex Analystにアクセスできません。
                エラー: {str(e)}
                
                **確認事項:**
                1. セマンティックモデルファイル '{selected_model_file}' がステージ '{SEMANTIC_MODEL_STAGE}' に存在するか確認してください。
                2. Cortex Analystサービスが有効になっているか確認してください。
                3. 必要な権限が付与されているか確認してください。
                
                **セマンティックモデルのパス:** {full_stage_path}
                """
                
                st.error(error_msg)
                st.code(str(e))
                
                # エラーメッセージをチャット履歴に追加
                st.session_state.analyst_messages.append({
                    "role": "assistant", 
                    "content": f"エラーが発生しました: {str(e)}",
                    "result": None
                })

def render_management_page():
    """管理ページを表示します。"""
    # タブの内容なのでヘッダーは不要
    
    st.info("""
    ## 🛠 顧客の声分析について
    
    このページでは、レビューデータを分析するための機能を提供します。
    
    まずはタグの一括生成と単語の抽出を実施してください。
    """)
    
    # カテゴリの管理
    render_category_management()
    
    st.markdown("---")
    
    # タグの一括生成
    st.subheader("タグの一括生成")
    st.info("""
    レビューテキストを自動的に分析し、内容に基づいてカテゴリを設定します。
    
    **使用AI機能**: `CLASSIFY_TEXT関数`
    
    このプロセスでは、CLASSIFY_TEXT関数を使用して各レビューの内容を分析し、事前に定義されたカテゴリから最も関連性の高いものを設定します。
    
    処理結果はREVIEW_TAGSテーブルに保存されます。
    """)
    if st.button("タグを一括生成", key="page_generate_tags"):
        with st.expander("処理の詳細", expanded=True):
            st.info("処理中はこちらに進捗状況が表示されます。")
            generate_review_tags()
    
    st.markdown("---")
    
    # 単語の抽出
    st.subheader("単語の抽出")
    st.info("""
    レビューテキストから重要な単語を抽出し、その品詞や出現頻度を分析します。
    
    **使用AI機能**: `COMPLETE関数の構造化出力機能`
    
    このプロセスでは、COMPLETE関数の構造化出力機能を使用して各レビューから重要な単語 (名詞、動詞、形容詞) を抽出し、
    それぞれの出現回数をカウントします。抽出された単語は「単語分析」タブで確認できます。
    
    **処理内容**:
    1. 未処理のレビューデータを取得
    2. 10件ずつのバッチで処理を実行 (COMPLETE関数の構造化出力機能)
    3. 重要単語の抽出とその品詞の判定
    4. 単語の出現回数の集計
    5. 結果をREVIEW_WORDSテーブルに保存
    """)
    if st.button("単語を抽出", key="page_extract_words"):
        with st.expander("処理の詳細", expanded=True):
            st.info("処理中はこちらに進捗状況が表示されます。")
            extract_important_words()

def render_category_management():
    """カテゴリ管理機能を表示します。"""
    st.subheader("カテゴリの管理")
    
    st.info("""
    レビューを分類するためのカテゴリを作成・管理します。カテゴリはタグ生成時に使用されます。
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("#### 現在のカテゴリ一覧")
        categories = get_review_categories()
        if categories:
            selected = st.selectbox(
                "カテゴリを選択",
                categories,
                key="category_select"
            )
            if st.button("選択したカテゴリを削除", key="delete_category"):
                if delete_review_category(selected):
                    st.success(f"カテゴリ '{selected}' を削除しました。")
                    st.rerun()
        else:
            st.info("登録されているカテゴリがありません。")
    
    with col2:
        st.write("#### 新しいカテゴリの追加")
        with st.form("add_category_form", clear_on_submit=True):
            new_category = st.text_input("カテゴリ名", key="new_category_name")
            description = st.text_area("説明（オプション）", key="new_category_desc")
            submitted = st.form_submit_button("追加")
            if submitted and new_category:
                if add_review_category(new_category, description):
                    st.success(f"カテゴリ '{new_category}' を追加しました。")
                    st.rerun()

def render_vector_search():
    """顧客の声検索機能を表示します。ベクトル検索（コサイン類似度）を使用します。"""
    # すでにタブがタイトルを持っているため、サブヘッダーは不要
    
    with st.expander("🔍 ベクトル検索について", expanded=False):
        st.markdown("""
        ### ベクトル検索の仕組み
        
        このページでは**ベクトル検索**を使って、顧客の声を曖昧検索できます。
        
        **仕組み**:
        1. 検索文字列をベクトル (1024次元の数値配列) に変換
        2. 各レビューテキストとのコサイン類似度を計算
        3. 類似度の高い順に結果を表示
        
        コサイン類似度は、2つのベクトル間の角度のコサインを測定し、
        [-1, 1]の範囲で類似度を返します。値が1に近いほど、より類似していることを示します。
        
        **特長**:
        - キーワードの完全一致だけでなく、意味的に関連する内容も検索可能
        - 類義語や関連概念も検索結果に含まれる
        """)
    
    # 検索UI
    st.write("### 検索キーワードを入力")
    
    # 検索オプション
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # 検索クエリの入力
        search_query = st.text_input(
            "検索キーワード", 
            placeholder="例: 「商品の品質について不満」「配送が早くて満足」など"
        )
    
    with col2:
        # オプション設定
        top_k = st.slider("表示件数", min_value=1, max_value=20, value=5)
        min_score = st.slider("最小類似度", min_value=0.0, max_value=1.0, value=0.2, step=0.05)
    
    # 検索ボタン
    search_button = st.button("検索", type="primary", use_container_width=True)
    
    # 検索実行
    if search_query and search_button:
        # 処理中表示
        with st.spinner("ベクトル検索を実行中..."):
            try:
                # ベクトル検索の実行（コサイン類似度を使用）
                # CTE (Common Table Expression) を使ってベクトルを一時的に保存し、
                search_results = snowflake_session.sql(f"""
                    WITH query_embedding AS (
                        SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('{embedding_model}', ?) AS vector
                    )
                    SELECT 
                        ca.review_id,
                        r.product_id,
                        r.rating,
                        r.review_text,
                        r.review_date,
                        r.purchase_channel,
                        r.helpful_votes,
                        ca.chunked_text,
                        ca.sentiment_score,
                        t.category_name,
                        ★★★修正対象★★★(ca.embedding, (SELECT vector FROM query_embedding)) as similarity_score
                    FROM CUSTOMER_ANALYSIS ca
                    JOIN CUSTOMER_REVIEWS r ON ca.review_id = r.review_id
                    LEFT JOIN REVIEW_TAGS t ON r.review_id = t.review_id
                    WHERE ca.embedding IS NOT NULL
                    AND ★★★修正対象★★★(ca.embedding, (SELECT vector FROM query_embedding)) >= ?
                    ORDER BY similarity_score DESC
                    LIMIT ?
                """, params=[search_query, min_score, top_k]).collect()
                
                # 検索結果の表示
                if search_results:
                    st.success(f"検索結果: {len(search_results)}件")
                    
                    # 結果の概要
                    avg_similarity = sum(r['SIMILARITY_SCORE'] for r in search_results) / len(search_results)
                    st.info(f"平均類似度: {avg_similarity:.2f}")
                    
                    # タブで各結果を表示
                    tabs = st.tabs([f"結果 {i+1} ({r['SIMILARITY_SCORE']:.2f})" for i, r in enumerate(search_results)])
                    
                    for i, (tab, result) in enumerate(zip(tabs, search_results)):
                        with tab:
                            similarity = result['SIMILARITY_SCORE']
                            
                            # カード形式で結果を表示
                            col1, col2 = st.columns([3, 1])
                            
                            with col1:
                                # レビュー内容
                                st.markdown(f"#### レビュー")
                                st.markdown(f"{result['REVIEW_TEXT']}")
                            
                            with col2:
                                # メタデータ
                                st.metric("類似度", f"{similarity:.2f}")
                                st.metric("評価", f"{result['RATING']}")
                                st.metric("感情スコア", f"{result['SENTIMENT_SCORE']:.2f}")
                            
                            # 詳細情報
                            st.markdown("#### 詳細情報")
                            st.markdown(f"""
                            | 項目 | 内容 |
                            | --- | --- |
                            | **レビューID** | {result['REVIEW_ID']} |
                            | **カテゴリ** | {result['CATEGORY_NAME'] or '未分類'} |
                            | **投稿日** | {result['REVIEW_DATE']} |
                            | **購入チャネル** | {result['PURCHASE_CHANNEL']} |
                            | **参考になった数** | {result['HELPFUL_VOTES']} |
                            """)
                else:
                    st.warning(f"""
                    検索結果がありません。以下を試してみてください：
                    - 別のキーワードで検索する
                    - より一般的な表現を使う
                    - 最小類似度のしきい値を下げる（現在: {min_score}）
                    """)
                    
            except Exception as e:
                st.error(f"検索中にエラーが発生しました: {str(e)}")
                st.code(str(e))
    elif not search_query and search_button:
        st.warning("検索キーワードを入力してください。")
    
    # 使い方ガイド（検索実行前のみ表示）
    if not search_query or not search_button:
        st.markdown("""
        ### 使い方
        1. 検索したい内容やキーワードを入力してください
        2. 必要に応じて表示件数や最小類似度を調整
        3. 「検索」ボタンをクリックして結果を表示
        
        #### 検索のヒント
        - **自然文で入力**: キーワードだけでなく、文章で入力すると関連性の高い結果が得られやすいです
        - **具体的に**: 「品質」よりも「商品の耐久性について」のように具体的に書くと良い結果が得られます
        - **否定表現も有効**: 「〜について不満」のように否定的な内容も検索できます
        """)

def render_simple_chatbot_page():
    """シンプルチャットボットページを表示します。"""
    st.header("シンプルチャットボット")
    
    # ワークショップ向けの説明
    st.info("""
    ## 🤖 シンプルチャットボットについて
    
    このページでは、Snowflake Cortexの生成AIモデルを使用した基本的なチャットボットを体験できます。
    
    ### 主な機能
    * **テキスト生成**: COMPLETE関数を使用して、入力プロンプトに基づいた応答を生成
    * **チャット履歴の保持**: 会話の文脈を保持し、より自然な対話を実現
    
    ### 使い方のヒント
    * 質問や指示を自然な文章で入力してください
    * 複雑な質問の場合は、具体的に詳細を記述するとより良い応答が得られます
    * このシンプルなチャットボットは外部データを参照せず、モデルの知識だけで応答を生成します
    """)
    
    # セッション状態の初期化
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.chat_history = ""
    
    # チャット履歴のクリアボタン
    if st.button("チャット履歴をクリア"):
        st.session_state.messages = []
        st.session_state.chat_history = ""
        st.rerun()
    
    # チャット履歴の表示
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # ユーザー入力の処理
    if prompt := st.chat_input("メッセージを入力してください"):
        # ユーザーメッセージの表示と履歴の更新
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.session_state.chat_history += f"User: {prompt}\n"
        with st.chat_message("user"):
            st.markdown(prompt)
        
        try:
            # Cortex Completeを使用して応答を生成
            full_prompt = st.session_state.chat_history + "AI: "
            response = CompleteText(complete_model, full_prompt)
            
            # 応答の表示と履歴の更新
            st.session_state.messages.append({"role": "assistant", "content": response})
            st.session_state.chat_history += f"AI: {response}\n"
            with st.chat_message("assistant"):
                st.markdown(response)
            
        except Exception as e:
            st.error(f"応答の生成中にエラーが発生しました: {str(e)}")

def render_rag_chatbot_page():
    """RAGチャットボットページを表示します。"""
    st.header("RAGチャットボット")
    
    # ワークショップ向けの説明
    st.info("""
    ## 📚 RAGチャットボットについて
    
    このページでは、Cortex Searchを用いたRetrieval-Augmented Generation (RAG) フレームワークの高度なチャットボットを体験できます。
    
    ### 主な機能
    * **多言語対応**: Cortex Searchは日本語を含む複数の言語に対応しているため、自然な日本語での質問が可能
    * **検索対象の自動リフレッシュ機能**: Cortex Searchを使用して検索対象ドキュメントを定期的に最新化
    * **ハイブリッド検索**: キーワード検索と曖昧検索の両方からドキュメントを検索することが可能
    
    ### 使い方のヒント
    * 社内文書に関する質問や、製品・サービスに関する具体的な質問を日本語で尋ねてみてください
    * 質問が具体的であるほど、より関連性の高いドキュメントが検索されます
    * 参考ドキュメントを展開すると、応答の生成に使用されたドキュメントを確認できます
    
    ### 注意事項
    Cortex Search Serviceはドキュメントの更新に伴うコンピューティングコスト以外にも、インデックス化されたデータサイズに対しての料金も発生します。長期間使用しない場合はCortex Search Serviceを削除するなどをご検討ください。
    """)
    
    # Snowflake Root オブジェクトの初期化
    root = Root(snowflake_session)
    
    # 現在のデータベースとスキーマを取得
    current_db_schema = snowflake_session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
    current_database = current_db_schema['CURRENT_DATABASE()']
    current_schema = current_db_schema['CURRENT_SCHEMA()']
    
    # 部署とドキュメントタイプの取得
    try:
        departments = snowflake_session.sql("""
            SELECT DISTINCT department FROM snow_retail_documents
            ORDER BY department
        """).collect()
        department_list = [row['DEPARTMENT'] for row in departments]
        
        document_types = snowflake_session.sql("""
            SELECT DISTINCT document_type FROM snow_retail_documents
            ORDER BY document_type
        """).collect()
        document_type_list = [row['DOCUMENT_TYPE'] for row in document_types]
    except Exception as e:
        st.warning("部署とドキュメントタイプの取得に失敗しました。フィルター機能は使用できません。")
        department_list = []
        document_type_list = []
    
    # Cortex Search Serviceの管理
    st.subheader("Cortex Search Serviceの管理")
    
    # サービスの存在確認
    service_exists = check_search_service_exists()
    
    if service_exists:
        st.success("Cortex Search Serviceが利用可能です。")
        if st.button("Cortex Search Serviceを削除"):
            if delete_snow_retail_search_service():
                st.rerun()
    else:
        st.error("Cortex Search Serviceが見つかりません。ワークショップの準備ステップでCortex Search Serviceが正しく作成されているか確認してください。")
        st.info("Cortex Search Serviceはワークショップの前段階で作成される必要があります。")
        return
    
    st.markdown("---")
    
    # 検索フィルターの設定
    with st.expander("検索フィルター設定", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            selected_departments = st.multiselect(
                "部署で絞り込み",
                options=department_list,
                default=[]
            )
        
        with col2:
            selected_document_types = st.multiselect(
                "ドキュメントタイプで絞り込み",
                options=document_type_list,
                default=[]
            )
    
    # セッション状態の初期化
    if "rag_messages" not in st.session_state:
        st.session_state.rag_messages = []
        st.session_state.rag_chat_history = ""
    
    # チャット履歴のクリアボタン
    if st.button("チャット履歴をクリア"):
        st.session_state.rag_messages = []
        st.session_state.rag_chat_history = ""
        st.rerun()
    
    # チャット履歴の表示
    for message in st.session_state.rag_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if "relevant_docs" in message:
                with st.expander("参考ドキュメント"):
                    for doc in message["relevant_docs"]:
                        st.markdown(f"""
                        **タイトル**: {doc['title']}  
                        **種類**: {doc['document_type']}  
                        **部署**: {doc['department']}  
                        **内容**: {doc['content'][:200]}...
                        """)
    
    # ユーザー入力の処理
    if prompt := st.chat_input("質問を入力してください"):
        # ユーザーメッセージの表示と履歴の更新
        st.session_state.rag_messages.append({"role": "user", "content": prompt})
        st.session_state.rag_chat_history += f"User: {prompt}\n"
        with st.chat_message("user"):
            st.markdown(prompt)
        
        try:
            # Cortex Search Serviceの取得
            search_service = (
                root.databases[current_database]
                .schemas[current_schema]
                .cortex_search_services["snow_retail_search_service"]
            )
            
            # フィルターの構築
            filter_conditions = []
            
            # 部署フィルターの追加
            if selected_departments:
                dept_conditions = []
                for dept in selected_departments:
                    dept_conditions.append({"@eq": {"department": dept}})
                
                if len(dept_conditions) == 1:
                    filter_conditions.append(dept_conditions[0])
                else:
                    filter_conditions.append({"@or": dept_conditions})
            
            # ドキュメントタイプフィルターの追加
            if selected_document_types:
                type_conditions = []
                for doc_type in selected_document_types:
                    type_conditions.append({"@eq": {"document_type": doc_type}})
                
                if len(type_conditions) == 1:
                    filter_conditions.append(type_conditions[0])
                else:
                    filter_conditions.append({"@or": type_conditions})
            
            # 最終的なフィルターの組み立て
            search_filter = None
            if filter_conditions:
                if len(filter_conditions) == 1:
                    search_filter = filter_conditions[0]
                else:
                    search_filter = {"@and": filter_conditions}
            
            # フィルター情報の表示
            if selected_departments or selected_document_types:
                filter_info = []
                if selected_departments:
                    filter_info.append(f"部署: {', '.join(selected_departments)}")
                if selected_document_types:
                    filter_info.append(f"ドキュメントタイプ: {', '.join(selected_document_types)}")
                st.info(f"以下の条件で検索します: {' / '.join(filter_info)}")
            
            # 検索の実行
            search_args = {
                "query": prompt,
                "columns": ["title", "content", "document_type", "department"],
                "limit": 3
            }
            
            # フィルターがある場合は追加
            if search_filter:
                search_args["filter"] = search_filter
            
            search_results = search_service.search(**search_args)
            
            # 検索結果の取得
            relevant_docs = [
                {
                    "title": result["title"],
                    "content": result["content"],
                    "document_type": result["document_type"],
                    "department": result["department"]
                }
                for result in search_results.results
            ]
            
            # 検索結果をコンテキストとして使用
            context = "参考文書:\n"
            for doc in relevant_docs:
                context += f"""
                タイトル: {doc['title']}
                種類: {doc['document_type']}
                部署: {doc['department']}
                内容: {doc['content']}
                ---
                """
            
            # COMPLETEを使用して応答を生成
            prompt_template = f"""
            あなたはスノーリテールの社内アシスタントです。
            以下の文脈を参考に、ユーザーからの質問に日本語で回答してください。
            わからない場合は、その旨を正直に伝えてください。

            文脈:
            {context}

            質問: {prompt}
            """
            
            response = CompleteText(complete_model, prompt_template)
            
            # アシスタントの応答を表示
            with st.chat_message("assistant"):
                st.markdown(response)
                with st.expander("参考ドキュメント"):
                    for doc in relevant_docs:
                        st.markdown(f"""
                        **タイトル**: {doc['title']}  
                        **種類**: {doc['document_type']}  
                        **部署**: {doc['department']}  
                        **内容**: {doc['content'][:200]}...
                        """)
            
            # チャット履歴に追加
            st.session_state.rag_messages.append({
                "role": "assistant",
                "content": response,
                "relevant_docs": relevant_docs
            })
            st.session_state.rag_chat_history += f"AI: {response}\n"
            
        except Exception as e:
            st.error(f"応答の生成中にエラーが発生しました: {str(e)}")
            st.code(str(e))

def get_semantic_model_files() -> list:
    """ステージ内のセマンティックモデルファイル一覧を取得します。
    
    Returns:
        list: ファイル名のリスト（取得失敗時は空リスト）
    """
    try:
        SEMANTIC_MODEL_STAGE = "SEMANTIC_MODEL_STAGE"
        stage_files = snowflake_session.sql(f"""
            LIST @{SEMANTIC_MODEL_STAGE}
        """).collect()
        
        # YAMLファイルだけをフィルタリング
        yaml_files = []
        for file in stage_files:
            filename = file['name']
            # ステージ名が含まれている場合は削除
            if '/' in filename:
                filename = filename.split('/')[-1]
            
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                yaml_files.append(filename)
        
        return yaml_files
    except Exception as e:
        st.error(f"セマンティックモデルファイルの取得に失敗しました: {str(e)}")
        return []

# =========================================================
# メイン処理
# =========================================================

# サイドバーでの機能選択
st.sidebar.title("機能選択")
selected_function = st.sidebar.radio(
    "機能を選択してください",
    ["データ準備", "顧客の声分析", "シンプルチャットボット", "RAGチャットボット", "分析チャットボット"]
)

# モデル設定
st.sidebar.title("モデル設定")

# モデル選択UI
embedding_model = st.sidebar.selectbox(
    "Embeddingモデルを選択してください",
    EMBEDDING_MODELS,
    index=0
)

complete_model = st.sidebar.selectbox(
    "Completeモデルを選択してください",
    COMPLETE_MODELS,
    index=0
)

# メインコンテンツ
st.title("🏪 スノーリテール 顧客の声分析アプリ")
st.markdown("---")

# 選択された機能に応じた処理
if selected_function == "データ準備":
    render_data_preparation_page()
elif selected_function == "顧客の声分析":
    render_voice_analysis_page()
elif selected_function == "シンプルチャットボット":
    render_simple_chatbot_page()
elif selected_function == "RAGチャットボット":
    render_rag_chatbot_page()
elif selected_function == "分析チャットボット":
    render_analyst_chatbot_page() 
