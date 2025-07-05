# =========================================================
# Snowflake Discover
# Snowflake Cortex AI で実現する次世代の VoC (顧客の声) アプリケーション
# Step1: データ準備ページ
# =========================================================
# 概要: 既存データの確認とレビューデータの前処理
# 使用する機能: SPLIT_TEXT_RECURSIVE_CHARACTER, TRANSLATE, SENTIMENT, EMBED_TEXT_1024
# =========================================================
# Created by Tsubasa Kanno @Snowflake
# 最終更新: 2025/07/06
# =========================================================

import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# ページ設定
st.set_page_config(layout="wide")

# Snowflakeセッション取得
@st.cache_resource
def get_snowflake_session():
    """Snowflakeセッションを取得"""
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# 定数設定
# =========================================================
# 埋め込みモデル選択肢
EMBEDDING_MODELS = [
    "multilingual-e5-large",
    "voyage-multilingual-2", 
    "snowflake-arctic-embed-l-v2.0",
    "nv-embed-qa-4"
]

# session_stateで選択されたembeddingモデルを初期化
if 'selected_embedding_model' not in st.session_state:
    st.session_state.selected_embedding_model = EMBEDDING_MODELS[0]

# =========================================================
# ユーティリティ関数
# =========================================================
def check_table_exists(table_name: str) -> bool:
    """テーブルの存在確認"""
    try:
        session.sql(f"SELECT 1 FROM {table_name} LIMIT 1").collect()
        return True
    except:
        return False

def get_table_count(table_name: str) -> int:
    """テーブルのレコード数を取得"""
    try:
        result = session.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()
        return result[0]['COUNT']
    except:
        return 0

def process_reviews(embedding_model: str, limit: int = 10):
    """レビューデータの前処理を実行"""
    # 未処理のレビューを取得
    limit_clause = f"LIMIT {limit}" if limit else ""
    reviews = session.sql(f"""
        SELECT r.*
        FROM CUSTOMER_REVIEWS r
        LEFT JOIN CUSTOMER_ANALYSIS a ON r.review_id = a.review_id
        WHERE a.review_id IS NULL
        {limit_clause}
    """).collect()
    
    if not reviews:
        st.info("処理が必要なレビューはありません。")
        return
    
    # 進捗表示の初期化
    progress_bar = st.progress(0)
    progress_text = st.empty()
    
    # 各レビューを処理
    for i, review in enumerate(reviews):
        # 進捗表示
        progress = (i + 1) / len(reviews)
        progress_bar.progress(progress)
        progress_text.text(f"処理中: {i + 1}/{len(reviews)} 件")
        
        # レビュー全体の感情分析（英語翻訳してから実行）
        translated_text = session.sql("""
            SELECT SNOWFLAKE.CORTEX.『★★★修正対象★★★』(?, '', 'en') as translated
        """, params=[review['REVIEW_TEXT']]).collect()[0]['TRANSLATED']
        
        sentiment_score = session.sql("""
            SELECT SNOWFLAKE.CORTEX.『★★★修正対象★★★』(?) as score
        """, params=[translated_text]).collect()[0]['SCORE']
        
        # テキストをチャンクに分割
        chunks = session.sql("""
            SELECT t.value as chunk
            FROM (
                SELECT SNOWFLAKE.CORTEX.『★★★修正対象★★★』(
                    ?, 'none', 300, 30
                ) as split_result
            ),
            LATERAL FLATTEN(input => split_result) t
        """, params=[review['REVIEW_TEXT']]).collect()
        
        # 各チャンクを処理してCUSTOMER_ANALYSISに挿入
        for chunk in chunks:
            session.sql("""
                INSERT INTO CUSTOMER_ANALYSIS (
                    review_id, product_id, customer_id, rating, review_text,
                    review_date, purchase_channel, helpful_votes,
                    chunked_text, embedding, sentiment_score
                )
                SELECT 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    SNOWFLAKE.CORTEX.『★★★修正対象★★★』(?, ?),
                    ?
            """, params=[
                review['REVIEW_ID'], review['PRODUCT_ID'], review['CUSTOMER_ID'],
                review['RATING'], review['REVIEW_TEXT'], review['REVIEW_DATE'],
                review['PURCHASE_CHANNEL'], review['HELPFUL_VOTES'],
                chunk['CHUNK'], embedding_model, chunk['CHUNK'], sentiment_score
            ]).collect()
    
    progress_text.text(f"完了: {len(reviews)} 件のレビューを処理しました")

# =========================================================
# メインページタイトル
# =========================================================
st.title("📊 Step1: データ準備")
st.header("既存データの確認とレビューデータの前処理")

# =========================================================
# サイドバー設定
# =========================================================
st.sidebar.header("⚙️ 設定")

# 埋め込みモデルの選択
selected_embedding_model = st.sidebar.selectbox(
    "埋め込みモデルを選択:",
    EMBEDDING_MODELS,
    index=EMBEDDING_MODELS.index(st.session_state.selected_embedding_model),
    key="embedding_model_selectbox",
    help="ベクトル化に使用する埋め込みモデルを選択してください"
)

# 選択が変更された場合、session_stateを更新
if selected_embedding_model != st.session_state.selected_embedding_model:
    st.session_state.selected_embedding_model = selected_embedding_model

st.sidebar.info(f"""
**選択中のモデル:**
- 埋め込みモデル: `{st.session_state.selected_embedding_model}`

このモデルがテキストのベクトル化に使用されます。
""")

st.markdown("---")

# =========================================================
# セクション1: 既存データの確認
# =========================================================
st.subheader("🗄️ セクション1: 既存データの確認")
st.markdown("ワークショップで使用する既存のテーブルを確認しましょう。")

# 既存テーブルのリスト
existing_tables = {
    "RETAIL_DATA_WITH_PRODUCT_MASTER": "クレンジング済み店舗データ",
    "EC_DATA_WITH_PRODUCT_MASTER": "クレンジング済みECデータ", 
    "CUSTOMER_REVIEWS": "顧客レビューデータ",
    "SNOW_RETAIL_DOCUMENTS": "社内ドキュメント"
}

tab1, tab2 = st.tabs(["📋 テーブル確認", "🔍 データサンプル"])

with tab1:
    st.markdown("#### 📋 既存テーブルの状況確認")
    
    # テーブル存在確認
    table_status = {}
    for table_name, description in existing_tables.items():
        exists = check_table_exists(table_name)
        count = get_table_count(table_name) if exists else 0
        table_status[table_name] = {"exists": exists, "count": count, "description": description}
        
        status_icon = "✅" if exists else "❌"
        st.write(f"{status_icon} **{table_name}** ({description}): {count:,}件")
    
    # 全テーブルが存在するかチェック
    all_tables_exist = all(status["exists"] for status in table_status.values())
    
    if all_tables_exist:
        st.success("✅ 全ての必要なテーブルが確認されました！")
    else:
        st.warning("⚠️ 一部のテーブルが見つかりません。ワークショップの前準備を確認してください。")

with tab2:
    st.markdown("#### 🔍 データサンプルの確認")
    
    # テーブル選択
    available_tables = [name for name, status in table_status.items() if status["exists"]]
    
    if available_tables:
        selected_table = st.selectbox(
            "確認するテーブルを選択:",
            available_tables,
            format_func=lambda x: f"{x} ({existing_tables[x]})"
        )
        
        @st.fragment
        def show_sample_data():
            """サンプルデータ表示のフラグメント"""
            if st.button("📄 サンプルデータ表示"):
                try:
                    sample_data = session.sql(f"SELECT * FROM {selected_table} LIMIT 5").collect()
                    if sample_data:
                        df_sample = pd.DataFrame([row.as_dict() for row in sample_data])
                        st.dataframe(df_sample, use_container_width=True)
                    else:
                        st.info("データが見つかりませんでした。")
                except Exception as e:
                    st.error(f"❌ データ取得エラー: {str(e)}")
        
        show_sample_data()
    else:
        st.warning("利用可能なテーブルがありません。")

# =========================================================
# セクション2: レビューデータの前処理
# =========================================================
st.markdown("---")
st.subheader("🔄 セクション2: レビューデータの前処理")
st.markdown("顧客レビューデータに対してCortex AI機能を使用した前処理を実行します。")

if not check_table_exists("CUSTOMER_REVIEWS"):
    st.error("CUSTOMER_REVIEWSテーブルが見つかりません。前準備を確認してください。")
else:
    # 前処理の説明
    st.info("""
    **前処理で実行される処理：**
    1. **翻訳・感情分析**: レビューテキスト全体を英語に翻訳し、感情スコアを算出（TRANSLATE, SENTIMENT）
    2. **テキスト分割**: レビューテキストをチャンクに分割（SPLIT_TEXT_RECURSIVE_CHARACTER）
    3. **ベクトル化**: 分割されたチャンクテキストを1024次元のベクトルに変換（EMBED_TEXT_1024）
    """)
    
    # 前処理テーブルの存在確認
    analysis_table_exists = check_table_exists("CUSTOMER_ANALYSIS")
    
    if not analysis_table_exists:
        st.warning("前処理用テーブル（CUSTOMER_ANALYSIS）が存在しません。")
        if st.button("🔧 前処理用テーブルを作成", type="primary"):
            with st.spinner("前処理用テーブルを作成中..."):
                try:
                    session.sql("""
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
                    st.success("✅ 前処理用テーブルを作成しました！")
                    st.rerun()
                        
                except Exception as e:
                    st.error(f"❌ テーブル作成エラー: {str(e)}")
    else:
        # 前処理用テーブルが存在する場合
        st.success("✅ 前処理用テーブル（CUSTOMER_ANALYSIS）が存在します。")
        
        col1, col2 = st.columns(2)
        
        with col1:
            processed_count = get_table_count("CUSTOMER_ANALYSIS")
            st.metric("処理済みチャンク数", f"{processed_count:,}件")
        
        with col2:
            # 未処理レビュー数の確認
            try:
                unprocessed_count = session.sql("""
                    SELECT COUNT(*) as count
                    FROM CUSTOMER_REVIEWS r
                    LEFT JOIN CUSTOMER_ANALYSIS a ON r.review_id = a.review_id
                    WHERE a.review_id IS NULL
                """).collect()[0]['COUNT']
                
                st.metric("未処理レビュー数", f"{unprocessed_count:,}件")
                
                if unprocessed_count > 0:
                    # 10件処理ボタン
                    if st.button("🧪 10件ずつ処理", type="secondary", use_container_width=True):
                        with st.spinner("レビューデータを前処理中（10件）..."):
                            try:
                                process_reviews(st.session_state.selected_embedding_model, limit=10)
                                st.success("✅ 10件のレビューデータの前処理が完了しました！")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 前処理エラー: {str(e)}")
                    
                    # 全件処理ボタン
                    if st.button("🚀 全件処理", type="primary", use_container_width=True):
                        with st.spinner("レビューデータを前処理中（全件）..."):
                            try:
                                process_reviews(st.session_state.selected_embedding_model, limit=None)
                                st.success("✅ 全件のレビューデータの前処理が完了しました！")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 前処理エラー: {str(e)}")
                else:
                    st.info("すべてのレビューが処理済みです。")
                    
            except Exception as e:
                st.error(f"❌ 前処理状況の確認でエラー: {str(e)}")

# =========================================================
# セクション3: 前処理結果の確認
# =========================================================
if check_table_exists("CUSTOMER_ANALYSIS"):
    st.markdown("---")
    st.subheader("📈 セクション3: 前処理結果の確認")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 感情スコア分布（レビュー単位で表示）
        try:
            sentiment_stats = session.sql("""
                SELECT 
                    sentiment_score,
                    COUNT(DISTINCT review_id) as review_count
                FROM CUSTOMER_ANALYSIS
                GROUP BY sentiment_score
                ORDER BY sentiment_score
            """).collect()
            
            if sentiment_stats:
                sentiment_df = pd.DataFrame([row.as_dict() for row in sentiment_stats])
                fig = px.histogram(sentiment_df, x='SENTIMENT_SCORE', y='REVIEW_COUNT',
                                 title='感情スコア分布（レビュー単位）', nbins=20,
                                 labels={'REVIEW_COUNT': 'レビュー数', 'SENTIMENT_SCORE': '感情スコア'})
                st.plotly_chart(fig, use_container_width=True)
        except:
            st.info("感情スコア分布データを取得できませんでした。")
    
    with col2:
        # 処理統計
        try:
            stats = session.sql("""
                SELECT 
                    COUNT(DISTINCT review_id) as unique_reviews,
                    COUNT(*) as total_chunks,
                    AVG(sentiment_score) as avg_sentiment,
                    MIN(sentiment_score) as min_sentiment,
                    MAX(sentiment_score) as max_sentiment
                FROM CUSTOMER_ANALYSIS
            """).collect()[0]
            
            st.metric("処理済みレビュー数", f"{stats['UNIQUE_REVIEWS']:,}件")
            st.metric("総チャンク数", f"{stats['TOTAL_CHUNKS']:,}件")
            st.metric("平均感情スコア", f"{stats['AVG_SENTIMENT']:.3f}")
            
        except:
            st.info("処理統計を取得できませんでした。")

# =========================================================
# 次のステップ
# =========================================================
st.markdown("---")
st.subheader("🎯 Step1 完了！")
st.success("""
✅ **データ準備とテキスト処理の基盤確認が完了しました！**

**確認した内容:**
- 既存テーブルの状況確認
- `SPLIT_TEXT_RECURSIVE_CHARACTER`: テキスト分割
- `TRANSLATE`: 多言語翻訳
- `SENTIMENT`: 感情分析
- `EMBED_TEXT_1024`: ベクトル埋め込み

**確認したテーブル:**
- RETAIL_DATA_WITH_PRODUCT_MASTER: 店舗データ
- EC_DATA_WITH_PRODUCT_MASTER: ECデータ
- CUSTOMER_REVIEWS: 顧客レビューデータ
- SNOW_RETAIL_DOCUMENTS: 社内ドキュメント
""")

st.info("💡 **次のステップ**: Step2では、AI_CLASSIFY、AI_FILTER、AI_AGGなどのAI関数を使った高度な分析を学習します。")

st.markdown("---")
st.markdown("**Snowflake Cortex AI で実現する次世代の VoC (顧客の声) アプリケーション | Step1: データ準備**") 