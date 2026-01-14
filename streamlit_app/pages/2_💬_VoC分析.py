"""
================================================================================
VoC（顧客の声）分析
================================================================================
SNS投稿と音声ログの感情分析結果を可視化

機能:
- SNS投稿の感情分析ダッシュボード
- 音声ログの問い合わせ分析
- プラットフォーム別・カテゴリ別の傾向
================================================================================
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import altair as alt
import pandas as pd

# ============================================================================
# ページ設定
# ============================================================================
st.set_page_config(
    page_title="VoC分析 | GlacierStyle",
    page_icon="💬",
    layout="wide"
)

# ============================================================================
# セッション取得
# ============================================================================
@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()

# ============================================================================
# ヘッダー
# ============================================================================
st.title("💬 VoC（顧客の声）分析")
st.markdown("SNS投稿と音声ログから顧客の声を分析し、インサイトを抽出します。")
st.divider()

# ============================================================================
# タブ切り替え
# ============================================================================
tab1, tab2 = st.tabs(["📱 SNS投稿分析", "📞 音声ログ分析"])

# ============================================================================
# SNS投稿分析
# ============================================================================
with tab1:
    st.markdown("### 📱 SNS投稿の感情分析")
    
    # データ取得
    @st.cache_data(ttl=300)
    def get_sns_data(_session):
        return _session.sql("""
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
                POST_CATEGORY,
                OVERALL_SENTIMENT,
                EXTRACTED_CATEGORY,
                EXTRACTED_PRODUCT_NAME
            FROM GOLD_SNS_MENTIONS_ANALYZED
        """).to_pandas()
    
    try:
        sns_df = get_sns_data(session)
        
        # サイドバーフィルター
        with st.sidebar:
            st.header("🔧 SNSフィルター")
            
            # プラットフォーム
            platforms = sns_df['PLATFORM'].dropna().unique().tolist()
            selected_platforms = st.multiselect(
                "プラットフォーム",
                options=platforms,
                default=platforms,
                key="sns_platforms"
            )
            
            # 感情
            sentiments = sns_df['OVERALL_SENTIMENT'].dropna().unique().tolist()
            selected_sentiments = st.multiselect(
                "感情",
                options=sentiments,
                default=sentiments,
                key="sns_sentiments"
            )
        
        # フィルター適用
        filtered_sns = sns_df[
            (sns_df['PLATFORM'].isin(selected_platforms)) &
            (sns_df['OVERALL_SENTIMENT'].isin(selected_sentiments))
        ]
        
        # サマリー
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("投稿数", f"{len(filtered_sns):,}")
        
        with col2:
            positive_pct = (filtered_sns['OVERALL_SENTIMENT'] == 'positive').sum() / len(filtered_sns) * 100
            st.metric("ポジティブ率", f"{positive_pct:.1f}%")
        
        with col3:
            total_likes = filtered_sns['LIKES'].sum()
            st.metric("総いいね数", f"{total_likes:,}")
        
        with col4:
            total_engagement = filtered_sns['LIKES'].sum() + filtered_sns['RETWEETS'].sum() + filtered_sns['REPLIES'].sum()
            st.metric("総エンゲージメント", f"{total_engagement:,}")
        
        st.divider()
        
        # チャート
        col1, col2 = st.columns(2)
        
        with col1:
            # 感情別分布
            sentiment_counts = filtered_sns['OVERALL_SENTIMENT'].value_counts().reset_index()
            sentiment_counts.columns = ['Sentiment', 'Count']
            
            color_scale = alt.Scale(
                domain=['positive', 'neutral', 'negative'],
                range=['#27ae60', '#95a5a6', '#e74c3c']
            )
            
            chart = alt.Chart(sentiment_counts).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('Count:Q'),
                color=alt.Color('Sentiment:N', scale=color_scale, title='感情'),
                tooltip=['Sentiment', 'Count']
            ).properties(title='感情分布', height=300)
            
            st.altair_chart(chart, use_container_width=True)
        
        with col2:
            # プラットフォーム別投稿数
            platform_counts = filtered_sns['PLATFORM'].value_counts().reset_index()
            platform_counts.columns = ['Platform', 'Count']
            
            chart = alt.Chart(platform_counts).mark_bar(color='#4a90a4').encode(
                x=alt.X('Platform:N', title='プラットフォーム'),
                y=alt.Y('Count:Q', title='投稿数'),
                tooltip=['Platform', 'Count']
            ).properties(title='プラットフォーム別投稿数', height=300)
            
            st.altair_chart(chart, use_container_width=True)
        
        st.divider()
        
        # カテゴリ別感情分析
        st.markdown("### 📊 カテゴリ別感情分析")
        
        category_sentiment = filtered_sns.groupby(['EXTRACTED_CATEGORY', 'OVERALL_SENTIMENT']).size().reset_index(name='Count')
        
        chart = alt.Chart(category_sentiment).mark_bar().encode(
            x=alt.X('EXTRACTED_CATEGORY:N', title='カテゴリ'),
            y=alt.Y('Count:Q', title='投稿数'),
            color=alt.Color('OVERALL_SENTIMENT:N', scale=color_scale, title='感情'),
            tooltip=['EXTRACTED_CATEGORY', 'OVERALL_SENTIMENT', 'Count']
        ).properties(height=350)
        
        st.altair_chart(chart, use_container_width=True)
        
        st.divider()
        
        # 投稿一覧
        st.markdown("### 📝 投稿一覧")
        
        # 感情でフィルター
        sentiment_filter = st.selectbox(
            "感情でフィルター",
            options=["すべて", "positive", "neutral", "negative"],
            key="sns_list_filter"
        )
        
        display_df = filtered_sns.copy()
        if sentiment_filter != "すべて":
            display_df = display_df[display_df['OVERALL_SENTIMENT'] == sentiment_filter]
        
        # 投稿表示
        for _, row in display_df.head(10).iterrows():
            sentiment = row['OVERALL_SENTIMENT']
            emoji = "🟢" if sentiment == 'positive' else "🟡" if sentiment == 'neutral' else "🔴"
            
            with st.container(border=True):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.markdown(f"**{row['DISPLAY_NAME']}** (@{row['USERNAME']})")
                    st.write(row['CONTENT'])
                    st.caption(f"📅 {row['POSTED_AT']} | {row['PLATFORM']}")
                
                with col2:
                    st.markdown(f"{emoji} **{sentiment}**")
                    st.write(f"❤️ {row['LIKES']} | 🔄 {row['RETWEETS']} | 💬 {row['REPLIES']}")
                    if row['EXTRACTED_PRODUCT_NAME']:
                        st.caption(f"🏷️ {row['EXTRACTED_PRODUCT_NAME']}")
        
        if len(display_df) > 10:
            st.info(f"他 {len(display_df) - 10} 件の投稿があります。")
    
    except Exception as e:
        st.error(f"データの取得に失敗しました: {e}")

# ============================================================================
# 音声ログ分析
# ============================================================================
with tab2:
    st.markdown("### 📞 音声ログの問い合わせ分析")
    
    # データ取得
    @st.cache_data(ttl=300)
    def get_voice_data(_session):
        return _session.sql("""
            SELECT 
                CALL_ID,
                SCENARIO_ID,
                CATEGORY,
                INQUIRY_CATEGORY,
                OVERALL_SENTIMENT,
                AGENT_ID,
                CALL_DURATION_SEC,
                CALL_START_TIME,
                TRANSCRIBED_TEXT_SUMMARY
            FROM GOLD_VOICE_LOGS
        """).to_pandas()
    
    try:
        voice_df = get_voice_data(session)
        
        # サマリー
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("通話件数", f"{len(voice_df):,}")
        
        with col2:
            avg_duration = voice_df['CALL_DURATION_SEC'].mean()
            st.metric("平均通話時間", f"{avg_duration:.0f}秒")
        
        with col3:
            negative_pct = (voice_df['OVERALL_SENTIMENT'] == 'negative').sum() / len(voice_df) * 100
            st.metric("ネガティブ率", f"{negative_pct:.1f}%")
        
        with col4:
            categories = voice_df['CATEGORY'].nunique()
            st.metric("カテゴリ数", f"{categories}")
        
        st.divider()
        
        # チャート
        col1, col2 = st.columns(2)
        
        with col1:
            # カテゴリ別件数
            category_counts = voice_df['CATEGORY'].value_counts().reset_index()
            category_counts.columns = ['Category', 'Count']
            
            chart = alt.Chart(category_counts).mark_bar(color='#9b59b6').encode(
                y=alt.Y('Category:N', title='カテゴリ', sort='-x'),
                x=alt.X('Count:Q', title='件数'),
                tooltip=['Category', 'Count']
            ).properties(title='問い合わせカテゴリ別件数', height=300)
            
            st.altair_chart(chart, use_container_width=True)
        
        with col2:
            # 感情別分布
            sentiment_counts = voice_df['OVERALL_SENTIMENT'].value_counts().reset_index()
            sentiment_counts.columns = ['Sentiment', 'Count']
            
            color_scale = alt.Scale(
                domain=['positive', 'neutral', 'negative'],
                range=['#27ae60', '#95a5a6', '#e74c3c']
            )
            
            chart = alt.Chart(sentiment_counts).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('Count:Q'),
                color=alt.Color('Sentiment:N', scale=color_scale, title='感情'),
                tooltip=['Sentiment', 'Count']
            ).properties(title='通話の感情分布', height=300)
            
            st.altair_chart(chart, use_container_width=True)
        
        st.divider()
        
        # 問い合わせカテゴリ別感情
        st.markdown("### 📊 問い合わせカテゴリ別感情分析")
        
        category_sentiment = voice_df.groupby(['CATEGORY', 'OVERALL_SENTIMENT']).size().reset_index(name='Count')
        
        chart = alt.Chart(category_sentiment).mark_bar().encode(
            x=alt.X('CATEGORY:N', title='カテゴリ'),
            y=alt.Y('Count:Q', title='件数'),
            color=alt.Color('OVERALL_SENTIMENT:N', scale=color_scale, title='感情'),
            tooltip=['CATEGORY', 'OVERALL_SENTIMENT', 'Count']
        ).properties(height=350)
        
        st.altair_chart(chart, use_container_width=True)
        
        st.divider()
        
        # 通話ログ一覧
        st.markdown("### 📋 通話ログ一覧")
        
        # フィルター
        col1, col2 = st.columns(2)
        
        with col1:
            category_filter = st.selectbox(
                "カテゴリでフィルター",
                options=["すべて"] + voice_df['CATEGORY'].unique().tolist(),
                key="voice_category_filter"
            )
        
        with col2:
            sentiment_filter = st.selectbox(
                "感情でフィルター",
                options=["すべて", "positive", "neutral", "negative"],
                key="voice_sentiment_filter"
            )
        
        display_df = voice_df.copy()
        if category_filter != "すべて":
            display_df = display_df[display_df['CATEGORY'] == category_filter]
        if sentiment_filter != "すべて":
            display_df = display_df[display_df['OVERALL_SENTIMENT'] == sentiment_filter]
        
        # 通話表示
        for _, row in display_df.head(10).iterrows():
            sentiment = row['OVERALL_SENTIMENT']
            emoji = "🟢" if sentiment == 'positive' else "🟡" if sentiment == 'neutral' else "🔴"
            
            with st.container(border=True):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.markdown(f"**{row['CALL_ID']}**")
                    st.write(row['TRANSCRIBED_TEXT_SUMMARY'] if row['TRANSCRIBED_TEXT_SUMMARY'] else "(要約なし)")
                    st.caption(f"📅 {row['CALL_START_TIME']} | 🕐 {row['CALL_DURATION_SEC']}秒")
                
                with col2:
                    st.markdown(f"{emoji} **{sentiment}**")
                    st.write(f"📁 {row['CATEGORY']}")
                    st.caption(f"🎯 {row['INQUIRY_CATEGORY']}")
        
        if len(display_df) > 10:
            st.info(f"他 {len(display_df) - 10} 件の通話があります。")
    
    except Exception as e:
        st.error(f"データの取得に失敗しました: {e}")
