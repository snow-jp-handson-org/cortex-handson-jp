# =========================================================
# GLACIER CREATIVE STUDIO
# SNS分析ページ
# =========================================================
# 概要: SNS投稿からトレンドを分析し、AI関数を活用したインサイト抽出
# =========================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
from collections import Counter
import re
import json

# ページ設定
st.set_page_config(layout="wide")

# =========================================================
# ユーティリティ関数
# =========================================================
@st.cache_resource
def get_session():
    """Snowflakeセッションを取得"""
    return get_active_session()


def process_ai_response(response: str) -> str:
    """AI応答の後処理（エスケープ文字の変換）"""
    if not response:
        return ""
    
    if response.startswith('"') and response.endswith('"'):
        response = response[1:-1]
    
    response = response.replace('\\n', '\n')
    response = response.replace('\\t', '\t')
    response = response.replace('\\"', '"')
    response = response.replace("\\'", "'")
    response = response.replace('\\\\', '\\')
    
    return response


@st.cache_data(ttl=600)
def get_sns_data() -> pd.DataFrame:
    """SNS投稿データを取得（正しいカラム名を使用）"""
    session = get_session()
    
    # GOLD_SNS_MENTIONS_ANALYZEDテーブルの実際のカラム名を使用
    query = """
    SELECT 
        POST_ID,
        PLATFORM,
        POST_TYPE,
        USERNAME,
        DISPLAY_NAME,
        CONTENT,
        POSTED_AT,
        LIKES,
        RETWEETS,
        REPLIES,
        HASHTAGS,
        MENTIONED_PRODUCTS,
        EXTRACTED_PRODUCT_NAME,
        EXTRACTED_CATEGORY,
        INQUIRY_TYPE,
        OVERALL_SENTIMENT,
        SENTIMENT,
        POST_CATEGORY,
        PROCESSED_AT
    FROM GOLD_SNS_MENTIONS_ANALYZED
    ORDER BY POSTED_AT DESC
    LIMIT 1000
    """
    
    return session.sql(query).to_pandas()


def call_cortex_complete(prompt: str, model: str = "claude-sonnet-4-5") -> str:
    """Cortex AIのAI_COMPLETE関数を呼び出し"""
    session = get_session()
    
    escaped_prompt = prompt.replace("'", "''").replace("\\", "\\\\")
    
    query = f"""
    SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
        '{model}',
        '{escaped_prompt}'
    ) AS RESPONSE
    """
    
    try:
        result = session.sql(query).collect()
        if result and result[0]['RESPONSE']:
            return process_ai_response(result[0]['RESPONSE'])
        return "応答を取得できませんでした。"
    except Exception as e:
        return f"エラーが発生しました: {str(e)}"


def call_ai_aggregate_with_complete(texts: list, question: str, model: str = "claude-sonnet-4-5") -> str:
    """AI_COMPLETE + LISTAGGで複数テキストを集約分析（AI_AGGの代替）"""
    session = get_session()
    
    # テキストを結合（トークン制限を考慮して最大30件）
    limited_texts = texts[:30]
    combined_text = "\n---\n".join([str(t)[:200] for t in limited_texts if pd.notna(t)])
    
    escaped_question = question.replace("'", "''").replace("\\", "\\\\")
    escaped_text = combined_text.replace("'", "''").replace("\\", "\\\\")
    
    prompt = f"""以下のSNS投稿（{len(limited_texts)}件）を分析し、質問に回答してください。

【質問】
{escaped_question}

【SNS投稿データ】
{escaped_text}

日本語で箇条書きで回答してください。"""
    
    query = f"""
    SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
        '{model}',
        '{prompt.replace("'", "''")}'
    ) AS RESPONSE
    """
    
    try:
        result = session.sql(query).collect()
        if result and result[0]['RESPONSE']:
            return process_ai_response(str(result[0]['RESPONSE']))
        return "集約結果を取得できませんでした。"
    except Exception as e:
        return f"エラーが発生しました: {str(e)}"


def call_ai_classify(text: str, categories: list) -> str:
    """AI_CLASSIFY関数を呼び出してテキストを分類"""
    session = get_session()
    
    escaped_text = text.replace("'", "''").replace("\\", "\\\\")
    # カテゴリを配列形式で渡す
    categories_array = ", ".join([f"'{c.replace(chr(39), chr(39)+chr(39))}'" for c in categories])
    
    query = f"""
    SELECT SNOWFLAKE.CORTEX.AI_CLASSIFY(
        '{escaped_text}',
        ARRAY_CONSTRUCT({categories_array})
    ) AS RESULT
    """
    
    try:
        result = session.sql(query).collect()
        if result and result[0]['RESULT']:
            # 結果は {"labels": ["category"]} 形式
            result_obj = result[0]['RESULT']
            if isinstance(result_obj, str):
                result_obj = json.loads(result_obj)
            if isinstance(result_obj, dict) and 'labels' in result_obj:
                labels = result_obj['labels']
                if labels and len(labels) > 0:
                    return labels[0]
            return "分類不能"
        return "分類不能"
    except Exception as e:
        return f"エラー: {str(e)}"


def call_ai_similarity(text1: str, text2: str) -> float:
    """AI_SIMILARITY関数を呼び出してテキスト類似度を計算"""
    session = get_session()
    
    escaped_text1 = text1.replace("'", "''").replace("\\", "\\\\")
    escaped_text2 = text2.replace("'", "''").replace("\\", "\\\\")
    
    query = f"""
    SELECT VECTOR_COSINE_SIMILARITY(
        SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', '{escaped_text1}'),
        SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', '{escaped_text2}')
    ) AS SIMILARITY
    """
    
    try:
        result = session.sql(query).collect()
        if result and result[0]['SIMILARITY']:
            return float(result[0]['SIMILARITY'])
        return 0.0
    except Exception as e:
        return 0.0


def extract_keywords(texts: list, top_n: int = 30) -> list:
    """テキストからキーワードを抽出"""
    # 日本語の一般的なストップワード
    stop_words = {'の', 'に', 'は', 'を', 'た', 'が', 'で', 'て', 'と', 'し', 'れ', 'さ', 
                  'ある', 'いる', 'も', 'な', 'こと', 'する', 'から', 'ない', 'この', 'ため',
                  'その', 'よう', 'など', 'として', 'です', 'ます', 'という', 'これ', 'それ',
                  'あと', 'また', 'へ', 'だ', 'られ', 'なる', 'ん', 'や', 'お', 'ね', 'よ',
                  'https', 'http', 'www', 'co', 'jp', 'com', 't', 'RT', 'amp'}
    
    all_words = []
    for text in texts:
        if pd.isna(text):
            continue
        # URLを除去
        text = re.sub(r'https?://\S+', '', str(text))
        # メンションを除去
        text = re.sub(r'@\w+', '', text)
        # ハッシュタグはキーワードとして残す
        text = text.replace('#', '')
        # 英数字と日本語を抽出（2文字以上）
        words = re.findall(r'[A-Za-z]{2,}|[ぁ-んァ-ヶー一-龥]{2,}', text)
        all_words.extend([w for w in words if w.lower() not in stop_words and len(w) >= 2])
    
    word_counts = Counter(all_words)
    return word_counts.most_common(top_n)


def extract_hashtags(df: pd.DataFrame) -> pd.DataFrame:
    """ハッシュタグを抽出して集計"""
    all_hashtags = []
    
    for _, row in df.iterrows():
        if pd.notna(row.get('HASHTAGS')):
            hashtags_str = str(row['HASHTAGS'])
            # JSON配列形式の場合
            try:
                if hashtags_str.startswith('['):
                    tags = json.loads(hashtags_str)
                    all_hashtags.extend(tags)
            except:
                # カンマ区切りなどの場合
                tags = [t.strip() for t in hashtags_str.replace('[', '').replace(']', '').replace('"', '').split(',')]
                all_hashtags.extend([t for t in tags if t])
    
    hashtag_counts = Counter(all_hashtags)
    return pd.DataFrame(hashtag_counts.most_common(20), columns=['ハッシュタグ', '件数'])


def extract_mentioned_products(df: pd.DataFrame) -> pd.DataFrame:
    """メンション商品を抽出して集計"""
    all_products = []
    
    for _, row in df.iterrows():
        if pd.notna(row.get('EXTRACTED_PRODUCT_NAME')):
            all_products.append(str(row['EXTRACTED_PRODUCT_NAME']))
    
    product_counts = Counter(all_products)
    return pd.DataFrame(product_counts.most_common(15), columns=['商品名', 'メンション数'])


# =========================================================
# セッションステートの初期化
# =========================================================
if "selected_model" not in st.session_state:
    st.session_state.selected_model = "claude-sonnet-4-5"

if "trend_analysis_result" not in st.session_state:
    st.session_state.trend_analysis_result = None

if "ai_agg_result" not in st.session_state:
    st.session_state.ai_agg_result = None

if "similarity_results" not in st.session_state:
    st.session_state.similarity_results = None

# =========================================================
# メインコンテンツ
# =========================================================

st.title("📱 SNS分析")
st.markdown("SNS投稿データからトレンドを分析し、Snowflake AI関数を活用した高度なインサイト抽出を行います。")
st.markdown("---")

# =========================================================
# サイドバー設定
# =========================================================
st.sidebar.subheader("🤖 AI設定")

model_options = {
    "Claude Sonnet 4.5": "claude-sonnet-4-5",
    "OpenAI GPT-5": "openai-gpt-5",
    "Llama 4 Maverick": "llama4-maverick",
    "OpenAI GPT OSS 120B": "openai-gpt-oss-120b",
}

model_names = list(model_options.keys())

def update_sns_model():
    st.session_state.selected_model = model_options[st.session_state.sns_model_selector]

current_model_name = [k for k, v in model_options.items() if v == st.session_state.selected_model]
default_index = model_names.index(current_model_name[0]) if current_model_name else 0

st.sidebar.selectbox(
    "モデル選択", 
    model_names, 
    index=default_index,
    key="sns_model_selector",
    on_change=update_sns_model
)

st.sidebar.info(f"""
**選択中の設定:**
- モデル: `{st.session_state.selected_model}`
""")

# データ取得
df = get_sns_data()

if df.empty:
    st.error("SNSデータが見つかりません。GOLD_SNS_MENTIONS_ANALYZEDテーブルを確認してください。")
    st.info("💡 setup.sqlを実行してデータをロードしてください。")
    st.stop()

# =========================================================
# タブ構成
# =========================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 概要・トレンド",
    "☁️ ワードクラウド",
    "#️⃣ ハッシュタグ・商品分析",
    "🔬 AI集約分析",
    "🏷️ AI分類"
])

# =========================================================
# タブ1: 概要・トレンド
# =========================================================
with tab1:
    st.subheader("📊 SNS投稿概要")
    
    # KPIサマリー
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("総投稿数", f"{len(df):,}")
    
    with col2:
        total_likes = df['LIKES'].sum() if 'LIKES' in df.columns else 0
        st.metric("総いいね数", f"{total_likes:,}")
    
    with col3:
        total_retweets = df['RETWEETS'].sum() if 'RETWEETS' in df.columns else 0
        st.metric("総リツイート", f"{total_retweets:,}")
    
    with col4:
        if 'SENTIMENT' in df.columns:
            positive_rate = (df['SENTIMENT'] == 'positive').mean() * 100
            st.metric("ポジティブ率", f"{positive_rate:.1f}%")
        else:
            st.metric("ポジティブ率", "-")
    
    with col5:
        if 'PLATFORM' in df.columns:
            unique_platforms = df['PLATFORM'].nunique()
            st.metric("プラットフォーム数", f"{unique_platforms}")
        else:
            st.metric("プラットフォーム数", "-")
    
    st.markdown("---")
    
    # 感情分析分布
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown("#### 😊 感情分布")
        
        if 'SENTIMENT' in df.columns:
            sentiment_counts = df['SENTIMENT'].value_counts()
            
            colors = {'positive': '#10B981', 'negative': '#EF4444', 'neutral': '#6B7280', 'mixed': '#F59E0B'}
            
            fig_sentiment = go.Figure(data=[go.Pie(
                labels=sentiment_counts.index.tolist(),
                values=sentiment_counts.values.tolist(),
                hole=0.4,
                marker_colors=[colors.get(s, '#3B82F6') for s in sentiment_counts.index]
            )])
            
            fig_sentiment.update_layout(
                height=300,
                margin=dict(l=20, r=20, t=20, b=20)
            )
            
            st.plotly_chart(fig_sentiment, use_container_width=True)
        else:
            st.info("感情データがありません。")
    
    with col_right:
        st.markdown("#### 📈 プラットフォーム別投稿数")
        
        if 'PLATFORM' in df.columns:
            platform_counts = df['PLATFORM'].value_counts()
            
            fig_platform = go.Figure(data=[go.Bar(
                x=platform_counts.index.tolist(),
                y=platform_counts.values.tolist(),
                marker_color='#3B82F6'
            )])
            
            fig_platform.update_layout(
                height=300,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="",
                yaxis_title="投稿数"
            )
            
            st.plotly_chart(fig_platform, use_container_width=True)
        else:
            st.info("プラットフォームデータがありません。")
    
    st.markdown("---")
    
    # カテゴリ分布
    st.markdown("#### 📂 投稿カテゴリ分布")
    
    col_cat1, col_cat2 = st.columns(2)
    
    with col_cat1:
        if 'POST_CATEGORY' in df.columns:
            category_counts = df['POST_CATEGORY'].value_counts().head(10)
            
            fig_category = go.Figure(data=[go.Bar(
                x=category_counts.values.tolist(),
                y=category_counts.index.tolist(),
                orientation='h',
                marker_color='#10B981'
            )])
            
            fig_category.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="投稿数",
                yaxis_title=""
            )
            
            st.plotly_chart(fig_category, use_container_width=True)
    
    with col_cat2:
        if 'EXTRACTED_CATEGORY' in df.columns:
            product_category_counts = df['EXTRACTED_CATEGORY'].value_counts().head(10)
            
            fig_prod_category = go.Figure(data=[go.Bar(
                x=product_category_counts.values.tolist(),
                y=product_category_counts.index.tolist(),
                orientation='h',
                marker_color='#F59E0B'
            )])
            
            fig_prod_category.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="投稿数",
                yaxis_title=""
            )
            
            st.plotly_chart(fig_prod_category, use_container_width=True)
    
    st.markdown("---")
    
    # トレンド分析
    st.markdown("#### 📈 投稿トレンド")
    
    @st.fragment
    def trend_analysis():
        """トレンド分析（Fragmentで部分更新）"""
        if st.button("🔍 トレンドをAI分析", type="primary"):
            with st.spinner("🤔 AIがトレンドを分析中..."):
                # 最新の投稿を抽出（CONTENTカラムを使用）
                recent_posts = df.head(50)['CONTENT'].tolist()
                posts_summary = "\n".join([f"- {p[:100]}..." if len(str(p)) > 100 else f"- {p}" for p in recent_posts if pd.notna(p)])
                
                trend_prompt = f"""あなたはSNSマーケティングの専門家です。以下のSNS投稿から、トレンドとインサイトを分析してください。

【最新のSNS投稿（50件）】
{posts_summary}

以下の観点で分析し、箇条書きで回答してください：

1. 【主要トピック】最も話題になっているトピック（5つ）
2. 【感情傾向】全体的な感情傾向と特徴的な意見
3. 【ポジティブな声】製品・サービスへのポジティブなフィードバック（3点）
4. 【改善要望】ユーザーからの改善要望や不満（3点）
5. 【マーケティング示唆】今後のマーケティング施策への示唆（3点）

日本語で回答してください。"""
                
                st.session_state.trend_analysis_result = call_cortex_complete(trend_prompt, st.session_state.selected_model)
        
        if st.session_state.trend_analysis_result:
            with st.expander("📊 トレンド分析結果", expanded=True):
                st.markdown(st.session_state.trend_analysis_result)
    
    trend_analysis()

# =========================================================
# タブ2: ワードクラウド
# =========================================================
with tab2:
    st.subheader("☁️ ワードクラウド分析")
    st.markdown("SNS投稿から頻出キーワードを抽出・可視化します。")
    
    # フィルタ（formで囲んで再描画防止）
    with st.form("wordcloud_filter_form"):
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            if 'SENTIMENT' in df.columns:
                sentiment_filter = st.selectbox(
                    "感情でフィルタ",
                    ["すべて", "positive", "negative", "neutral", "mixed"],
                    key="wc_sentiment_form"
                )
            else:
                sentiment_filter = "すべて"
        
        with col2:
            if 'PLATFORM' in df.columns:
                platforms = ['すべて'] + df['PLATFORM'].dropna().unique().tolist()
                platform_filter = st.selectbox("プラットフォーム", platforms, key="wc_platform_form")
            else:
                platform_filter = "すべて"
        
        with col3:
            st.write("")  # スペーサー
            filter_submitted = st.form_submit_button("🔍 フィルタ適用", use_container_width=True)
    
    # フィルタ適用（フォーム送信後のみ）
    filtered_df = df.copy()
    
    if filter_submitted or 'wc_sentiment_form' in st.session_state:
        if st.session_state.get('wc_sentiment_form', 'すべて') != "すべて" and 'SENTIMENT' in df.columns:
            filtered_df = filtered_df[filtered_df['SENTIMENT'] == st.session_state.wc_sentiment_form]
        
        if st.session_state.get('wc_platform_form', 'すべて') != "すべて" and 'PLATFORM' in df.columns:
            filtered_df = filtered_df[filtered_df['PLATFORM'] == st.session_state.wc_platform_form]
    
    st.info(f"📊 分析対象: {len(filtered_df)}件の投稿")
    
    # キーワード抽出（CONTENTカラムを使用）
    if 'CONTENT' in filtered_df.columns:
        texts = filtered_df['CONTENT'].tolist()
        keywords = extract_keywords(texts, top_n=30)
        
        if keywords:
            st.markdown("---")
            
            # 棒グラフでワードクラウド風に表示
            col_chart, col_table = st.columns([2, 1])
            
            with col_chart:
                st.markdown("#### 📊 頻出キーワードTOP30")
                
                words = [k[0] for k in keywords]
                counts = [k[1] for k in keywords]
                
                fig_keywords = go.Figure(data=[go.Bar(
                    x=counts[::-1],
                    y=words[::-1],
                    orientation='h',
                    marker_color='#3B82F6'
                )])
                
                fig_keywords.update_layout(
                    height=600,
                    margin=dict(l=20, r=20, t=20, b=20),
                    xaxis_title="出現回数",
                    yaxis_title=""
                )
                
                st.plotly_chart(fig_keywords, use_container_width=True)
            
            with col_table:
                st.markdown("#### 📋 キーワード一覧")
                
                keywords_df = pd.DataFrame(keywords, columns=['キーワード', '出現回数'])
                st.dataframe(keywords_df, use_container_width=True, hide_index=True, height=600)
        else:
            st.info("キーワードを抽出できませんでした。")
    else:
        st.info("投稿テキストデータがありません。")

# =========================================================
# タブ3: ハッシュタグ・商品分析（新機能）
# =========================================================
with tab3:
    st.subheader("#️⃣ ハッシュタグ・商品メンション分析")
    st.markdown("SNS投稿で使用されているハッシュタグと商品メンションを分析します。")
    
    col_hash, col_product = st.columns(2)
    
    with col_hash:
        st.markdown("#### #️⃣ 人気ハッシュタグTOP20")
        
        hashtag_df = extract_hashtags(df)
        
        if not hashtag_df.empty:
            fig_hashtags = go.Figure(data=[go.Bar(
                x=hashtag_df['件数'].tolist()[::-1],
                y=hashtag_df['ハッシュタグ'].tolist()[::-1],
                orientation='h',
                marker_color='#8B5CF6'
            )])
            
            fig_hashtags.update_layout(
                height=500,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="使用回数",
                yaxis_title=""
            )
            
            st.plotly_chart(fig_hashtags, use_container_width=True)
            
            with st.expander("📋 ハッシュタグ一覧"):
                st.dataframe(hashtag_df, use_container_width=True, hide_index=True)
        else:
            st.info("ハッシュタグデータがありません。")
    
    with col_product:
        st.markdown("#### 🏷️ 商品メンションTOP15")
        
        product_df = extract_mentioned_products(df)
        
        if not product_df.empty:
            fig_products = go.Figure(data=[go.Bar(
                x=product_df['メンション数'].tolist()[::-1],
                y=product_df['商品名'].tolist()[::-1],
                orientation='h',
                marker_color='#EC4899'
            )])
            
            fig_products.update_layout(
                height=500,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="メンション数",
                yaxis_title=""
            )
            
            st.plotly_chart(fig_products, use_container_width=True)
            
            with st.expander("📋 商品メンション一覧"):
                st.dataframe(product_df, use_container_width=True, hide_index=True)
        else:
            st.info("商品メンションデータがありません。")
    
    st.markdown("---")
    
    # 商品別感情分析
    st.markdown("#### 📊 商品別 感情分布")
    
    if 'EXTRACTED_PRODUCT_NAME' in df.columns and 'SENTIMENT' in df.columns:
        product_sentiment = df.groupby(['EXTRACTED_PRODUCT_NAME', 'SENTIMENT']).size().unstack(fill_value=0)
        
        if not product_sentiment.empty:
            # 上位10商品に絞る
            top_products = df['EXTRACTED_PRODUCT_NAME'].value_counts().head(10).index.tolist()
            product_sentiment_top = product_sentiment.loc[product_sentiment.index.isin(top_products)]
            
            if not product_sentiment_top.empty:
                colors = {'positive': '#10B981', 'negative': '#EF4444', 'neutral': '#6B7280', 'mixed': '#F59E0B'}
                
                fig_prod_sent = go.Figure()
                
                for sentiment in product_sentiment_top.columns:
                    fig_prod_sent.add_trace(go.Bar(
                        name=sentiment,
                        x=product_sentiment_top.index.tolist(),
                        y=product_sentiment_top[sentiment].tolist(),
                        marker_color=colors.get(sentiment, '#3B82F6')
                    ))
                
                fig_prod_sent.update_layout(
                    barmode='stack',
                    height=400,
                    margin=dict(l=20, r=20, t=20, b=80),
                    xaxis_title="",
                    yaxis_title="投稿数",
                    xaxis_tickangle=-45,
                    legend_title="感情"
                )
                
                st.plotly_chart(fig_prod_sent, use_container_width=True)
    else:
        st.info("商品別感情データがありません。")

# =========================================================
# タブ4: AI集約分析（AI_AGGの代替）
# =========================================================
with tab4:
    st.subheader("🔬 AI集約分析")
    st.markdown("""
    複数のSNS投稿をAIがまとめて分析し、統合的なインサイトを抽出します。
    （`AI_COMPLETE` + `LISTAGG` を使用 - AI_AGGはプレビュー機能のため代替手法を採用）
    """)
    
    with st.expander("💡 この機能について", expanded=False):
        st.markdown("""
        **集約分析とは？**
        
        個別の投稿を一つずつ分析するのではなく、複数の投稿をまとめてAIに渡し、
        全体的な傾向やパターンを抽出する手法です。
        
        **活用例:**
        - 大量のレビューから共通の傾向を抽出
        - SNS投稿から主要なトピックを特定
        - 顧客フィードバックから改善ポイントを集約
        
        **技術的な注意:**
        - 最大30件の投稿を対象（トークン制限のため）
        - 各投稿は200文字に制限
        """)
    
    st.markdown("---")
    
    # フィルタ（formで囲んで再描画防止）
    with st.form("agg_filter_form"):
        st.markdown("#### 📝 分析対象の選択")
        
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            if 'SENTIMENT' in df.columns:
                agg_sentiment = st.selectbox(
                    "感情でフィルタ",
                    ["すべて", "positive", "negative", "neutral", "mixed"],
                    key="agg_sentiment_form"
                )
            else:
                agg_sentiment = "すべて"
        
        with col2:
            agg_limit = st.slider("分析する投稿数（最大30件）", 10, 30, 20, key="agg_limit_form")
        
        with col3:
            st.write("")
            agg_filter_submitted = st.form_submit_button("適用", use_container_width=True)
    
    # フィルタ適用
    agg_df = df.copy()
    if st.session_state.get('agg_sentiment_form', 'すべて') != "すべて" and 'SENTIMENT' in df.columns:
        agg_df = agg_df[agg_df['SENTIMENT'] == st.session_state.agg_sentiment_form]
    
    agg_limit_val = st.session_state.get('agg_limit_form', 20)
    agg_texts = agg_df.head(agg_limit_val)['CONTENT'].dropna().tolist()
    
    st.info(f"📊 分析対象: {len(agg_texts)}件の投稿")
    
    st.markdown("---")
    st.markdown("#### ❓ 質問を入力")
    
    question_options = [
        "この投稿群に共通する主要なトピックは何ですか？",
        "ユーザーが最も評価しているポイントは何ですか？",
        "ユーザーが改善を求めているポイントは何ですか？",
        "ブランドに対する全体的な印象はどうですか？",
        "カスタム質問を入力..."
    ]
    
    selected_question = st.selectbox("質問を選択", question_options, key="agg_question")
    
    if selected_question == "カスタム質問を入力...":
        custom_question = st.text_input("カスタム質問", placeholder="例: 季節に関する言及はありますか？")
        final_question = custom_question
    else:
        final_question = selected_question
    
    @st.fragment
    def ai_agg_analysis():
        """AI集約分析（Fragmentで部分更新）"""
        if st.button("🔬 AIで集約分析", type="primary", disabled=not final_question):
            if agg_texts:
                with st.spinner("🤔 AIが複数投稿を集約分析中..."):
                    st.session_state.ai_agg_result = call_ai_aggregate_with_complete(
                        agg_texts, final_question, st.session_state.selected_model
                    )
            else:
                st.warning("分析対象の投稿がありません。")
        
        if st.session_state.ai_agg_result:
            with st.expander("🔬 集約分析結果", expanded=True):
                st.markdown(st.session_state.ai_agg_result)
    
    ai_agg_analysis()

# =========================================================
# タブ5: AI分類（AI_CLASSIFY）
# =========================================================
with tab5:
    st.subheader("🏷️ AI分類（AI_CLASSIFY）")
    st.markdown("""
    Snowflakeの`AI_CLASSIFY`関数を使用して、テキストを指定したカテゴリに自動分類します。
    これはSnowflake Intelligenceでは直接利用できない高度な機能です。
    """)
    
    with st.expander("💡 AI_CLASSIFYとは？", expanded=False):
        st.markdown("""
        **AI_CLASSIFY**は、テキストを指定したカテゴリに自動分類するSnowflake Cortex AI関数です。
        
        **構文:**
        ```sql
        AI_CLASSIFY(text, ARRAY_CONSTRUCT('カテゴリ1', 'カテゴリ2', ...))
        ```
        
        **戻り値:**
        ```json
        {"labels": ["最も適切なカテゴリ"]}
        ```
        
        **活用例:**
        - カスタマーサポートの問い合わせ分類
        - SNS投稿のトピック分類
        - 製品レビューのカテゴリ分け
        """)
    
    st.markdown("---")
    
    col_single, col_batch = st.columns(2)
    
    with col_single:
        st.markdown("#### 🏷️ 単一テキストの分類")
        
        # 投稿を選択または入力
        classify_mode = st.radio("入力方法", ["投稿から選択", "テキストを入力"], key="classify_mode", horizontal=True)
        
        if classify_mode == "投稿から選択" and 'CONTENT' in df.columns:
            post_options = df['CONTENT'].dropna().head(20).tolist()
            classify_text = st.selectbox("投稿を選択", post_options, key="classify_select")
        else:
            classify_text = st.text_area("分類するテキスト", "この新しいデスクライトは目に優しくて、在宅ワークにぴったりです！", key="classify_input", height=100)
        
        classify_categories = st.text_input(
            "カテゴリ（カンマ区切り）",
            "製品レビュー, 質問, 購入報告, クレーム, その他",
            key="classify_categories"
        )
        
        @st.fragment
        def single_classify():
            if st.button("🏷️ 分類実行", type="primary", key="run_classify_single"):
                categories = [c.strip() for c in classify_categories.split(",")]
                if classify_text and categories:
                    with st.spinner("分類中..."):
                        result = call_ai_classify(classify_text[:500], categories)
                        st.success(f"**分類結果**: {result}")
                else:
                    st.warning("テキストとカテゴリを入力してください。")
        
        single_classify()
    
    with col_batch:
        st.markdown("#### 📊 一括分類分析")
        st.markdown("複数の投稿を一括でカテゴリ分類します。")
        
        batch_size = st.slider("分析する投稿数", 5, 15, 10, key="batch_size")
        batch_categories = st.text_input(
            "分類カテゴリ",
            "製品レビュー, 質問, 購入報告, クレーム, その他",
            key="batch_categories"
        )
        
        @st.fragment
        def batch_classify():
            if st.button("📊 一括分類実行", type="primary", key="run_batch_classify"):
                if 'CONTENT' in df.columns:
                    categories = [c.strip() for c in batch_categories.split(",")]
                    batch_texts = df['CONTENT'].dropna().head(batch_size).tolist()
                    
                    results = []
                    progress_bar = st.progress(0)
                    
                    for i, text in enumerate(batch_texts):
                        result = call_ai_classify(str(text)[:500], categories)
                        results.append({
                            "投稿": str(text)[:80] + "..." if len(str(text)) > 80 else str(text),
                            "分類結果": result
                        })
                        progress_bar.progress((i + 1) / len(batch_texts))
                    
                    results_df = pd.DataFrame(results)
                    st.dataframe(results_df, use_container_width=True, hide_index=True)
                    
                    # 分類結果の集計
                    st.markdown("##### 分類結果サマリー")
                    category_counts = results_df['分類結果'].value_counts()
                    
                    fig_batch = go.Figure(data=[go.Pie(
                        labels=category_counts.index.tolist(),
                        values=category_counts.values.tolist(),
                        hole=0.4
                    )])
                    
                    fig_batch.update_layout(height=300, margin=dict(l=20, r=20, t=20, b=20))
                    st.plotly_chart(fig_batch, use_container_width=True)
                else:
                    st.warning("投稿データがありません。")
        
        batch_classify()
    
    st.markdown("---")
    
    # 類似投稿検索（AI_SIMILARITY / ベクトル検索）
    st.markdown("#### 🔍 類似投稿検索（ベクトル類似度）")
    st.markdown("入力したテキストと類似する投稿を検索します（EMBED_TEXT + VECTOR_COSINE_SIMILARITY使用）。")
    
    search_text = st.text_input("検索テキスト", "デスクライトの品質について知りたい", key="similarity_search")
    
    @st.fragment
    def similarity_search():
        if st.button("🔍 類似投稿を検索", type="secondary", key="run_similarity"):
            if search_text and 'CONTENT' in df.columns:
                with st.spinner("類似投稿を検索中..."):
                    sample_posts = df['CONTENT'].dropna().head(20).tolist()
                    
                    similarities = []
                    for post in sample_posts:
                        sim = call_ai_similarity(search_text, str(post)[:300])
                        similarities.append({
                            "投稿": str(post)[:100] + "..." if len(str(post)) > 100 else str(post),
                            "類似度": f"{sim:.3f}"
                        })
                    
                    sim_df = pd.DataFrame(similarities)
                    sim_df = sim_df.sort_values('類似度', ascending=False)
                    
                    st.session_state.similarity_results = sim_df
            else:
                st.warning("検索テキストを入力してください。")
        
        if st.session_state.similarity_results is not None:
            st.markdown("##### 🔍 類似度の高い投稿")
            st.dataframe(st.session_state.similarity_results, use_container_width=True, hide_index=True)
    
    similarity_search()

st.markdown("---")
st.caption("**GLACIER CREATIVE STUDIO** | SNS分析")
