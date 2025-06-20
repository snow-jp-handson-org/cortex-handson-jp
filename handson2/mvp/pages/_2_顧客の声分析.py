# =========================================================
# Snowflake Cortex Handson シナリオ#2
# AIを用いた顧客の声分析アプリケーション
# Step2: 顧客の声分析ページ
# =========================================================
# 概要: レビューデータの高度な分析とカテゴリ分類
# 使用する機能: AI_CLASSIFY, AI_FILTER, AI_AGG, AI_SUMMARIZE_AGG, AI_SIMILARITY
# =========================================================
# Created by Tsubasa Kanno @Snowflake
# 最終更新: 2025/06/16
# =========================================================

import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit
from datetime import datetime
import time

# ページ設定
st.set_page_config(layout="wide")

# Snowflakeセッション取得
@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# 定数設定
# =========================================================

# 分析カテゴリ
ANALYSIS_CATEGORIES = [
    "商品品質",
    "配送サービス", 
    "価格",
    "カスタマーサービス",
    "店舗環境",
    "その他"
]

# =========================================================
# ユーティリティ関数
# =========================================================
def check_table_exists(table_name: str) -> bool:
    """テーブルの存在確認"""
    try:
        result = session.sql(f"SHOW TABLES LIKE '{table_name}'").collect()
        if len(result) > 0:
            return True
    except:
        pass
    
    try:
        session.sql(f"SELECT 1 FROM {table_name} LIMIT 1").collect()
        return True
    except:
        pass
    
    return False

def get_table_count(table_name: str) -> int:
    """テーブルのレコード数を取得"""
    try:
        result = session.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()
        return result[0]['COUNT']
    except:
        return 0

# =========================================================
# メインページタイトル
# =========================================================
st.title("🗣️ Step2: 顧客の声分析")
st.header("AISQL機能を使った高度なレビューデータ分析")
st.markdown("---")

# =========================================================
# データ状況確認
# =========================================================
st.subheader("📊 データ状況確認")

# 必要テーブルの確認
required_tables = {
    "CUSTOMER_REVIEWS": "顧客レビューデータ",
    "CUSTOMER_ANALYSIS": "前処理済み分析データ"
}

col1, col2 = st.columns(2)

table_status = {}
for table_name, description in required_tables.items():
    exists = check_table_exists(table_name)
    count = get_table_count(table_name) if exists else 0
    table_status[table_name] = {"exists": exists, "count": count}
    
    status_icon = "✅" if exists else "❌"
    
    if table_name == "CUSTOMER_REVIEWS":
        with col1:
            st.metric(
                f"{status_icon} {description}", 
                f"{count:,}件",
                help="元の顧客レビューデータ"
            )
    else:
        with col2:
            st.metric(
                f"{status_icon} {description}", 
                f"{count:,}件",
                help="前処理済みのチャンクデータ"
            )

# 全テーブルが存在するかチェック
all_tables_exist = all(status["exists"] for status in table_status.values())

if not all_tables_exist:
    st.error("⚠️ 必要なテーブルが見つかりません。Step1のデータ準備を完了してください。")
    st.stop()

st.markdown("---")

# =========================================================
# セクション1: AISQL機能紹介
# =========================================================
st.subheader("🧠 セクション1: AISQL機能")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **AISQLの主要機能:**
    
    - `AI_CLASSIFY`: マルチラベル分類
    - `AI_FILTER`: 条件フィルタリング
    - `AI_AGG`: 集約分析
    - `AI_SIMILARITY`: 類似レビュー検出
    """)

with col2:
    st.markdown("""
    **分析の流れ:**
    
    1. レビューをカテゴリ分類
    2. 条件でフィルタリング
    3. 購入チャネル別に集約分析
    4. 類似レビューの検出
    """)

# =========================================================
# セクション2: AI_CLASSIFY分析
# =========================================================
st.markdown("---")

@st.fragment
def section_2_classify():
    st.subheader("🏷️ セクション2: AI_CLASSIFY - マルチラベル分類")
    
    if st.button("🏷️ AI_CLASSIFY実行（全件）", type="primary"):
        with st.spinner("レビューの自動分類中..."):
            try:
                # AI_CLASSIFY関数でカテゴリ分類（:labelsでJSON抽出）
                category_query = """
                SELECT 
                    review_id,
                    review_text,
                    rating,
                    purchase_channel,
                    AI_CLASSIFY(
                        review_text, 
                        ARRAY_CONSTRUCT('商品品質', '配送サービス', '価格', 'カスタマーサービス', '店舗環境', 'その他')
                    ):labels[0]::string as category
                FROM CUSTOMER_REVIEWS 
                WHERE review_text IS NOT NULL
                """
                
                results = session.sql(category_query).collect()
                
                if results:
                    st.success(f"✅ {len(results)}件のレビューを分類完了")
                    
                    df_results = pd.DataFrame([row.as_dict() for row in results])
                    st.session_state['classify_results'] = df_results
                    
                    # カテゴリ分布の可視化
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        category_counts = df_results['CATEGORY'].value_counts()
                        fig = px.pie(
                            values=category_counts.values,
                            names=category_counts.index,
                            title="カテゴリ分布"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        fig = px.bar(
                            x=category_counts.index,
                            y=category_counts.values,
                            title="カテゴリ別件数",
                            labels={"x": "カテゴリ", "y": "件数"}
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
            except Exception as e:
                st.error(f"❌ 分類エラー: {str(e)}")
    
    # 分類結果の詳細分析機能
    if 'classify_results' in st.session_state:
        df_results = st.session_state['classify_results']
        
        st.markdown("---")
        st.markdown("#### 📊 カテゴリ別詳細分析")
        
        # カテゴリ選択
        selected_category = st.selectbox(
            "分析したいカテゴリを選択:",
            ["全カテゴリ"] + sorted(df_results['CATEGORY'].unique().tolist()),
            key="category_select"
        )
        
        # フィルタされたデータ
        if selected_category == "全カテゴリ":
            filtered_df = df_results
        else:
            filtered_df = df_results[df_results['CATEGORY'] == selected_category]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("対象レビュー数", f"{len(filtered_df)}件")
        with col2:
            avg_rating = filtered_df['RATING'].mean()
            st.metric("平均評価", f"{avg_rating:.2f}")
        with col3:
            if len(filtered_df) > 0:
                top_channel = filtered_df['PURCHASE_CHANNEL'].mode()[0]
                st.metric("主要チャネル", top_channel)
        
        # ページネーション機能
        items_per_page = st.slider("1ページあたりの表示件数:", 5, 50, 10, key="items_per_page")
        total_pages = max(1, (len(filtered_df) - 1) // items_per_page + 1)
        
        if total_pages > 1:
            current_page = st.selectbox(
                f"ページ選択 (全{total_pages}ページ):",
                range(1, total_pages + 1),
                key="current_page"
            )
        else:
            current_page = 1
        
        # 現在のページのデータ表示
        start_idx = (current_page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        page_data = filtered_df.iloc[start_idx:end_idx]
        
        for _, row in page_data.iterrows():
            with st.expander(f"🏷️ {row['CATEGORY']} | 評価: {row['RATING']} | {row['PURCHASE_CHANNEL']}"):
                st.write(f"**レビューID**: {row['REVIEW_ID']}")
                st.write(f"**レビュー内容**: {row['REVIEW_TEXT']}")

section_2_classify()

# =========================================================
# セクション3: AI_FILTER分析
# =========================================================
st.markdown("---")

@st.fragment
def section_3_filter():
    st.subheader("🔍 セクション3: AI_FILTER - スマートフィルタリング")
    
    # フィルタ条件の入力方法選択
    filter_input_type = st.radio(
        "フィルタ条件の指定方法:",
        ["サンプルから選択", "自由入力"],
        horizontal=True,
        key="filter_input_type"
    )
    
    if filter_input_type == "サンプルから選択":
        filter_options = [
            "配送や梱包に関する問題が言及されているか？",
            "商品の品質に満足している表現が含まれるか？",
            "価格に関する言及が含まれているか？",
            "カスタマーサービスについて言及しているか？"
        ]
        selected_filter = st.selectbox("フィルタ条件を選択:", filter_options)
    else:
        selected_filter = st.text_input(
            "フィルタ条件を入力:",
            placeholder="例：新商品について言及しているか？",
            help="レビューから抽出したい条件を自然言語で入力してください"
        )
    
    if st.button("🔍 AI_FILTER実行（全件）", type="primary"):
        if not selected_filter or selected_filter.strip() == "":
            st.error("フィルタ条件を入力してください。")
        else:
            with st.spinner("スマートフィルタリング実行中..."):
                try:
                    # AI_FILTER関数で条件マッチング（全件対象）
                    filter_query = f"""
                    SELECT 
                        review_id,
                        review_text,
                        rating,
                        purchase_channel,
                        AI_FILTER(CONCAT('{selected_filter}: ', review_text)) as filter_result
                    FROM CUSTOMER_REVIEWS 
                    WHERE review_text IS NOT NULL
                    """
                    
                    results = session.sql(filter_query).collect()
                    
                    if results:
                        matched_results = [r for r in results if r['FILTER_RESULT']]
                        
                        st.success(f"✅ {len(matched_results)}件が条件にマッチしました（全{len(results)}件中）")
                        
                        if matched_results:
                            # マッチ率の可視化
                            match_rate = len(matched_results) / len(results) * 100
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                fig = px.pie(
                                    values=[len(matched_results), len(results) - len(matched_results)],
                                    names=['マッチ', '非マッチ'],
                                    title=f"フィルタ結果 (マッチ率: {match_rate:.1f}%)"
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            
                            with col2:
                                # チャネル別マッチ分析
                                df_matched = pd.DataFrame([r.as_dict() for r in matched_results])
                                channel_counts = df_matched['PURCHASE_CHANNEL'].value_counts()
                                fig = px.bar(
                                    x=channel_counts.index,
                                    y=channel_counts.values,
                                    title="チャネル別マッチ件数",
                                    labels={"x": "購入チャネル", "y": "件数"}
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            
                            # マッチしたレビューの詳細表示
                            st.markdown("#### 📝 マッチしたレビュー詳細")
                            for result in matched_results[:20]:  # 最初の20件のみ表示
                                data = result.as_dict()
                                
                                with st.expander(f"📋 レビューID: {data['REVIEW_ID']} | 評価: {data['RATING']} | {data['PURCHASE_CHANNEL']}"):
                                    st.write(f"**レビュー内容**: {data['REVIEW_TEXT']}")
                                    st.success(f"**フィルタ結果**: 条件にマッチ")
                            
                            if len(matched_results) > 20:
                                st.info(f"さらに{len(matched_results) - 20}件のマッチした結果があります。")
                        else:
                            st.info("条件にマッチするレビューが見つかりませんでした。")
                    
                except Exception as e:
                    st.error(f"❌ フィルタエラー: {str(e)}")

section_3_filter()

# =========================================================
# セクション4: AI_AGG分析
# =========================================================
st.markdown("---")

@st.fragment
def section_4_agg():
    st.subheader("📊 セクション4: AI_AGG - 購入チャネル別集約分析")
    
    # 集約プロンプトの入力方法選択
    agg_input_type = st.radio(
        "分析観点の指定方法:",
        ["サンプルから選択", "自由入力"],
        horizontal=True,
        key="agg_input_type"
    )
    
    if agg_input_type == "サンプルから選択":
        agg_prompts = [
            "各チャネルで特に言及されている特徴を一言で表現してください。",
            "顧客の主な不満点は何ですか？",
            "顧客の主な満足点は何ですか？",
            "改善すべき点を一つ挙げてください。"
        ]
        selected_agg_prompt = st.selectbox("集約分析の観点を選択:", agg_prompts)
    else:
        selected_agg_prompt = st.text_input(
            "分析観点を入力:",
            placeholder="例：このチャネルの特徴的な口コミの傾向は？",
            help="各購入チャネルのレビューから分析したい観点を自然言語で入力してください"
        )
    
    if st.button("📊 AI_AGG実行", type="primary"):
        if not selected_agg_prompt or selected_agg_prompt.strip() == "":
            st.error("分析観点を入力してください。")
        else:
            with st.spinner("購入チャネル別集約分析実行中..."):
                try:
                    # AI_AGG関数でチャネル別集約分析（TRANSLATE関数で日本語化）
                    agg_query = f"""
                    SELECT 
                        purchase_channel,
                        COUNT(*) as review_count,
                        AVG(rating) as avg_rating,
                        SNOWFLAKE.CORTEX.TRANSLATE(
                            AI_AGG(
                                review_text, 
                                '{selected_agg_prompt}'
                            ),
                            '',
                            'ja'
                        ) as channel_insights
                    FROM CUSTOMER_REVIEWS
                    WHERE review_text IS NOT NULL
                    GROUP BY purchase_channel
                    """
                    
                    results = session.sql(agg_query).collect()
                    
                    if results:
                        st.success(f"✅ {len(results)}つの購入チャネルの分析完了")
                        
                        for result in results:
                            data = result.as_dict()
                            
                            with st.expander(f"📈 {data['PURCHASE_CHANNEL']} チャネル"):
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.metric("レビュー数", f"{data['REVIEW_COUNT']}件")
                                    st.metric("平均評価", f"{data['AVG_RATING']:.2f}")
                                
                                with col2:
                                    st.markdown("**AI集約分析結果:**")
                                    st.write(data['CHANNEL_INSIGHTS'])
                    
                except Exception as e:
                    st.error(f"❌ AI_AGG分析エラー: {str(e)}")

section_4_agg()

# =========================================================
# セクション5: AI_SIMILARITY分析
# =========================================================
st.markdown("---")

@st.fragment
def section_5_similarity():
    st.subheader("🔗 セクション5: AI_SIMILARITY - 類似レビュー検出")
    
    # 基準となるレビューテキストの入力
    base_text = st.text_area(
        "基準となるレビューテキストを入力:",
        value="商品の品質は素晴らしいが、配送が遅かった。",
        height=80
    )
    
    similarity_threshold = st.slider("類似度閾値:", 0.0, 1.0, 0.7, step=0.1)
    
    if st.button("🔗 AI_SIMILARITY実行（全件）", type="primary"):
        with st.spinner("類似レビューを検索中..."):
            try:
                # AI_SIMILARITY関数で類似度計算（全件対象）
                similarity_query = f"""
                SELECT 
                    review_id,
                    review_text,
                    rating,
                    purchase_channel,
                    AI_SIMILARITY('{base_text}', review_text) as similarity_score
                FROM CUSTOMER_REVIEWS 
                WHERE review_text IS NOT NULL
                ORDER BY similarity_score DESC
                """
                
                results = session.sql(similarity_query).collect()
                
                if results:
                    # 閾値以上の類似度のレビューをフィルタ
                    similar_reviews = [r for r in results if r['SIMILARITY_SCORE'] >= similarity_threshold]
                    
                    st.success(f"✅ 類似度{similarity_threshold}以上のレビューを{len(similar_reviews)}件発見（全{len(results)}件中）")
                    
                    if similar_reviews:
                        # 類似度分布の可視化
                        df_similarity = pd.DataFrame([r.as_dict() for r in results])
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # 類似度ヒストグラム
                            fig = px.histogram(
                                df_similarity,
                                x='SIMILARITY_SCORE',
                                nbins=20,
                                title="類似度分布",
                                labels={"x": "類似度スコア", "y": "件数"}
                            )
                            fig.add_vline(x=similarity_threshold, line_dash="dash", line_color="red", 
                                        annotation_text=f"閾値: {similarity_threshold}")
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with col2:
                            # 閾値以上のレビューのチャネル分布
                            df_filtered = pd.DataFrame([r.as_dict() for r in similar_reviews])
                            channel_counts = df_filtered['PURCHASE_CHANNEL'].value_counts()
                            fig = px.pie(
                                values=channel_counts.values,
                                names=channel_counts.index,
                                title=f"類似レビューのチャネル分布"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        # 類似レビューの詳細表示
                        st.markdown("#### 🔗 類似レビュー詳細（上位15件）")
                        for result in similar_reviews[:15]:
                            data = result.as_dict()
                            similarity = data['SIMILARITY_SCORE']
                            
                            # 類似度に応じた色分け
                            if similarity >= 0.8:
                                similarity_color = "🟢"
                            elif similarity >= 0.6:
                                similarity_color = "🟡"
                            else:
                                similarity_color = "🟠"
                            
                            with st.expander(f"{similarity_color} レビューID: {data['REVIEW_ID']} | 類似度: {similarity:.3f} | {data['PURCHASE_CHANNEL']}"):
                                st.write(f"**レビュー内容**: {data['REVIEW_TEXT']}")
                                st.write(f"**評価**: {data['RATING']}")
                                st.write(f"**類似度スコア**: {similarity:.3f}")
                        
                        if len(similar_reviews) > 15:
                            st.info(f"さらに{len(similar_reviews) - 15}件の類似レビューがあります。")
                    else:
                        st.info(f"類似度{similarity_threshold}以上のレビューが見つかりませんでした。")
                
            except Exception as e:
                st.error(f"❌ 類似度分析エラー: {str(e)}")

section_5_similarity()

# =========================================================
# セクション6: 統合分析レポート
# =========================================================
st.markdown("---")

@st.fragment
def section_6_integrated():
    st.subheader("🚀 セクション6: 統合分析レポート")
    
    if st.button("🚀 統合分析実行（全件）", type="primary"):
        with st.spinner("統合分析実行中..."):
            try:
                # 複数のAISQLを組み合わせた統合分析（全件対象）
                # まず基本データを取得
                base_query = """
                SELECT 
                    review_id,
                    review_text,
                    rating,
                    purchase_channel,
                    SNOWFLAKE.CORTEX.SENTIMENT(review_text) as sentiment_score,
                    AI_CLASSIFY(
                        review_text, 
                        ARRAY_CONSTRUCT('商品品質', '配送サービス', '価格', 'カスタマーサービス', '店舗環境', 'その他')
                    ):labels[0]::string as category
                FROM CUSTOMER_REVIEWS 
                WHERE review_text IS NOT NULL
                """
                
                # AI_SUMMARIZE_AGGを使用してカテゴリ別要約を取得
                summary_query = """
                SELECT 
                    category,
                    purchase_channel,
                    SNOWFLAKE.CORTEX.TRANSLATE(
                        AI_SUMMARIZE_AGG(review_text),
                        '',
                        'ja'
                    ) as category_summary
                FROM (
                    SELECT 
                        review_text,
                        purchase_channel,
                        AI_CLASSIFY(
                            review_text, 
                            ARRAY_CONSTRUCT('商品品質', '配送サービス', '価格', 'カスタマーサービス', '店舗環境', 'その他')
                        ):labels[0]::string as category
                    FROM CUSTOMER_REVIEWS 
                    WHERE review_text IS NOT NULL
                )
                GROUP BY category, purchase_channel
                """
                
                # 基本データを取得
                base_results = session.sql(base_query).collect()
                # カテゴリ別要約を取得
                summary_results = session.sql(summary_query).collect()
                
                if base_results and summary_results:
                    df_base = pd.DataFrame([row.as_dict() for row in base_results])
                    df_summary = pd.DataFrame([row.as_dict() for row in summary_results])
                    
                    # 基本データとサマリーデータを結合
                    df_results = df_base.merge(
                        df_summary, 
                        on=['CATEGORY', 'PURCHASE_CHANNEL'], 
                        how='left'
                    )
                    
                    # 統合分析結果をsession_stateに保存
                    st.session_state['integrated_results'] = df_results
                    st.session_state['category_summaries'] = df_summary
                    
                    st.success(f"✅ 統合分析完了（{len(base_results)}件のレビュー、{len(summary_results)}のカテゴリ別要約）")
                
            except Exception as e:
                st.error(f"❌ 統合分析エラー: {str(e)}")
    
    # 統合分析結果の表示
    if 'integrated_results' in st.session_state:
        df_results = st.session_state['integrated_results']
        
        # 感情スコアの定義説明
        st.info("""
        **📊 感情スコア定義:**
        - **ポジティブ**: 0.1以上 😊
        - **ニュートラル**: -0.1～0.1 😐  
        - **ネガティブ**: -0.1未満 😞
        """)
        
        # 全体統計の可視化
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_sentiment = df_results['SENTIMENT_SCORE'].mean()
            st.metric("平均感情スコア", f"{avg_sentiment:.3f}")
        
        with col2:
            positive_ratio = (df_results['SENTIMENT_SCORE'] > 0.1).mean() * 100
            st.metric("ポジティブ率", f"{positive_ratio:.1f}%")
        
        with col3:
            negative_ratio = (df_results['SENTIMENT_SCORE'] < -0.1).mean() * 100
            st.metric("ネガティブ率", f"{negative_ratio:.1f}%")
        
        with col4:
            most_common_category = df_results['CATEGORY'].mode()[0]
            st.metric("最多カテゴリ", most_common_category)
        
        # 感情とカテゴリの分析グラフ
        st.markdown("#### 📈 詳細分析チャート")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 感情分布
            df_results['sentiment_label'] = df_results['SENTIMENT_SCORE'].apply(
                lambda x: 'ポジティブ' if x > 0.1 else ('ネガティブ' if x < -0.1 else 'ニュートラル')
            )
            sentiment_counts = df_results['sentiment_label'].value_counts()
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title="感情分布",
                color_discrete_map={
                    'ポジティブ': '#2E8B57',
                    'ニュートラル': '#FFD700', 
                    'ネガティブ': '#DC143C'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # カテゴリ別感情スコア
            category_sentiment = df_results.groupby('CATEGORY')['SENTIMENT_SCORE'].mean().reset_index()
            fig = px.bar(
                category_sentiment,
                x='CATEGORY',
                y='SENTIMENT_SCORE',
                title="カテゴリ別平均感情スコア",
                labels={"CATEGORY": "カテゴリ", "SENTIMENT_SCORE": "平均感情スコア"},
                color='SENTIMENT_SCORE',
                color_continuous_scale='RdYlGn'
            )
            fig.add_hline(y=0, line_dash="dash", line_color="black", annotation_text="ニュートラル")
            st.plotly_chart(fig, use_container_width=True)
        
        # チャネル別分析
        col1, col2 = st.columns(2)
        
        with col1:
            # チャネル別件数
            channel_counts = df_results['PURCHASE_CHANNEL'].value_counts()
            fig = px.bar(
                x=channel_counts.index,
                y=channel_counts.values,
                title="購入チャネル別レビュー件数",
                labels={"x": "購入チャネル", "y": "件数"}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # チャネル別平均評価と感情スコア
            channel_analysis = df_results.groupby('PURCHASE_CHANNEL').agg({
                'RATING': 'mean',
                'SENTIMENT_SCORE': 'mean'
            }).reset_index()
            
            fig = px.scatter(
                channel_analysis,
                x='RATING',
                y='SENTIMENT_SCORE',
                size=[len(df_results[df_results['PURCHASE_CHANNEL'] == ch]) for ch in channel_analysis['PURCHASE_CHANNEL']],
                hover_name='PURCHASE_CHANNEL',
                title="チャネル別：評価 vs 感情スコア",
                labels={"RATING": "平均評価", "SENTIMENT_SCORE": "平均感情スコア"}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # カテゴリ別詳細分析
        st.markdown("#### 📋 カテゴリ別詳細分析")
        
        # カテゴリ選択
        analysis_category = st.selectbox(
            "詳細分析するカテゴリを選択:",
            ["全体概要"] + sorted(df_results['CATEGORY'].unique().tolist()),
            key="analysis_category"
        )
        
        if analysis_category == "全体概要":
            # 全体サマリー
            st.markdown("##### 🔍 全体分析サマリー")
            
            # 感情別上位レビュー
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**😊 最もポジティブなレビュー**")
                most_positive = df_results.loc[df_results['SENTIMENT_SCORE'].idxmax()]
                st.write(f"感情スコア: {most_positive['SENTIMENT_SCORE']:.3f}")
                st.write(f"カテゴリ: {most_positive['CATEGORY']}")
                st.write(f"レビュー: {most_positive['REVIEW_TEXT'][:100]}...")
            
            with col2:
                st.markdown("**😐 最もニュートラルなレビュー**")
                df_neutral = df_results[abs(df_results['SENTIMENT_SCORE']) < 0.1]
                if not df_neutral.empty:
                    most_neutral = df_neutral.loc[df_neutral['SENTIMENT_SCORE'].abs().idxmin()]
                    st.write(f"感情スコア: {most_neutral['SENTIMENT_SCORE']:.3f}")
                    st.write(f"カテゴリ: {most_neutral['CATEGORY']}")
                    st.write(f"レビュー: {most_neutral['REVIEW_TEXT'][:100]}...")
                else:
                    st.write("ニュートラルなレビューがありません")
            
            with col3:
                st.markdown("**😞 最もネガティブなレビュー**")
                most_negative = df_results.loc[df_results['SENTIMENT_SCORE'].idxmin()]
                st.write(f"感情スコア: {most_negative['SENTIMENT_SCORE']:.3f}")
                st.write(f"カテゴリ: {most_negative['CATEGORY']}")
                st.write(f"レビュー: {most_negative['REVIEW_TEXT'][:100]}...")
        
        else:
            # 特定カテゴリの詳細分析
            category_data = df_results[df_results['CATEGORY'] == analysis_category]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("レビュー数", f"{len(category_data)}件")
            with col2:
                avg_rating = category_data['RATING'].mean()
                st.metric("平均評価", f"{avg_rating:.2f}")
            with col3:
                avg_sentiment = category_data['SENTIMENT_SCORE'].mean()
                st.metric("平均感情スコア", f"{avg_sentiment:.3f}")
            
            # ページネーション機能（セクション2と同様の実装）
            items_per_page_6 = st.slider("1ページあたりの表示件数:", 5, 50, 10, key="items_per_page_6")
            total_pages_6 = max(1, (len(category_data) - 1) // items_per_page_6 + 1)
            
            if total_pages_6 > 1:
                current_page_6 = st.selectbox(
                    f"ページ選択 (全{total_pages_6}ページ):",
                    range(1, total_pages_6 + 1),
                    key="current_page_6"
                )
            else:
                current_page_6 = 1
            
            # 現在のページのデータ表示
            start_idx_6 = (current_page_6 - 1) * items_per_page_6
            end_idx_6 = start_idx_6 + items_per_page_6
            page_data_6 = category_data.iloc[start_idx_6:end_idx_6]
            
            # カテゴリ別AI要約の表示
            st.markdown(f"##### 🤖 {analysis_category} カテゴリのAI_SUMMARIZE_AGG要約")
            if 'category_summaries' in st.session_state:
                df_summaries = st.session_state['category_summaries']
                category_summaries = df_summaries[df_summaries['CATEGORY'] == analysis_category]
                
                for _, summary_row in category_summaries.iterrows():
                    with st.info(f"**{summary_row['PURCHASE_CHANNEL']}チャネル**: {summary_row['CATEGORY_SUMMARY']}"):
                        pass
            
            # カテゴリ内の全レビュー表示（ページネーション付き）
            st.markdown(f"##### 📝 {analysis_category} カテゴリのレビュー詳細")
            for _, row in page_data_6.iterrows():
                sentiment = row['SENTIMENT_SCORE']
                if sentiment > 0.1:
                    sentiment_emoji = "😊"
                    sentiment_label = "ポジティブ"
                elif sentiment < -0.1:
                    sentiment_emoji = "😞"
                    sentiment_label = "ネガティブ"
                else:
                    sentiment_emoji = "😐"
                    sentiment_label = "ニュートラル"
                
                with st.expander(f"{sentiment_emoji} {sentiment_label} ({sentiment:.2f}) | 評価: {row['RATING']} | {row['PURCHASE_CHANNEL']}"):
                    st.write(f"**レビューID**: {row['REVIEW_ID']}")
                    st.write(f"**レビュー内容**: {row['REVIEW_TEXT']}")
                    
                    # 該当するカテゴリ・チャネルの集約要約を表示
                    if 'category_summaries' in st.session_state:
                        df_summaries = st.session_state['category_summaries']
                        matching_summary = df_summaries[
                            (df_summaries['CATEGORY'] == row['CATEGORY']) & 
                            (df_summaries['PURCHASE_CHANNEL'] == row['PURCHASE_CHANNEL'])
                        ]
                        if not matching_summary.empty:
                            st.write(f"**このカテゴリ・チャネルのAI集約要約**: {matching_summary.iloc[0]['CATEGORY_SUMMARY']}")

section_6_integrated()

st.markdown("---")
st.subheader("🎯 Step2 完了！")
st.success("""
✅ **AISQL機能を使った顧客の声分析が完了しました！**

**使用したAISQL機能:**
- `AI_CLASSIFY`: マルチラベル分類（全件対象・カテゴリ別詳細分析）
- `AI_FILTER`: スマートフィルタリング（全件対象・マッチ率可視化）
- `AI_AGG`: 購入チャネル別集約分析（日本語翻訳付き）
- `AI_SIMILARITY`: 類似レビュー検出（全件対象・分布可視化）
- `AI_SUMMARIZE_AGG`: カテゴリ・チャネル別集約要約（複数レビューを効率的に要約）
- 従来機能: `SENTIMENT`（感情分析）

**分析の価値:**
- 全件データに基づく正確な分析
- カテゴリ別・チャネル別の深掘り分析
- 感情分析による顧客満足度の可視化
- 類似レビューによるパターン発見
- AI_SUMMARIZE_AGGによる複数レビューの効率的な集約要約
""")

st.info("💡 **次のステップ**: Step3では、シンプルなチャットボットの実装を学習します。")

st.markdown("---")
st.markdown(f"**Snowflake Cortex Handson シナリオ#2 | Step2: 顧客の声分析**") 
