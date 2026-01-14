"""
================================================================================
広告クリエイティブ分析
================================================================================
広告クリエイティブのKPIを分析

機能:
- KPI（CTR, CVR, CPA）のダッシュボード
- プラットフォーム別・セグメント別分析
- クリエイティブ詳細表示
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
    page_title="広告クリエイティブ分析 | GlacierStyle",
    page_icon="📢",
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
st.title("📢 広告クリエイティブ分析")
st.markdown("広告クリエイティブのKPIを分析し、パフォーマンスを可視化します。")
st.divider()

# ============================================================================
# データ取得
# ============================================================================
@st.cache_data(ttl=300)
def get_ad_creatives(_session):
    """広告クリエイティブデータを取得"""
    query = """
        SELECT 
            CREATIVE_ID,
            CREATIVE_NAME,
            CREATIVE_TYPE,
            CAMPAIGN_ID,
            PLATFORM,
            TARGET_SEGMENT,
            COPY_TEXT,
            HEADLINE,
            IMPRESSIONS,
            CLICKS,
            CONVERSIONS,
            SPEND,
            CTR,
            CVR,
            CPA,
            IMAGE_STYLE,
            APPEAL_TYPE
        FROM GOLD_AD_CREATIVE_ANALYSIS
    """
    return _session.sql(query).to_pandas()

try:
    df = get_ad_creatives(session)
    
    # ============================================================================
    # フィルター
    # ============================================================================
    with st.sidebar:
        st.header("🔧 フィルター")
        
        # プラットフォーム選択
        platforms = df['PLATFORM'].dropna().unique().tolist()
        selected_platforms = st.multiselect(
            "プラットフォーム",
            options=platforms,
            default=platforms
        )
        
        # ターゲットセグメント選択
        segments = df['TARGET_SEGMENT'].dropna().unique().tolist()
        selected_segments = st.multiselect(
            "ターゲットセグメント",
            options=segments,
            default=segments
        )
        
        # ソート
        sort_by = st.selectbox(
            "並び替え",
            options=["CTR（高い順）", "CTR（低い順）", "CPA（低い順）", "CVR（高い順）"],
            index=0
        )
    
    # フィルター適用
    filtered_df = df[
        (df['PLATFORM'].isin(selected_platforms)) &
        (df['TARGET_SEGMENT'].isin(selected_segments))
    ].copy()
    
    # ソート適用
    if "CTR（高い順）" in sort_by:
        filtered_df = filtered_df.sort_values('CTR', ascending=False)
    elif "CTR（低い順）" in sort_by:
        filtered_df = filtered_df.sort_values('CTR', ascending=True)
    elif "CPA（低い順）" in sort_by:
        filtered_df = filtered_df.sort_values('CPA', ascending=True)
    elif "CVR（高い順）" in sort_by:
        filtered_df = filtered_df.sort_values('CVR', ascending=False)
    
    # ============================================================================
    # サマリーKPI
    # ============================================================================
    st.markdown("### 📊 全体サマリー")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("クリエイティブ数", f"{len(filtered_df):,}")
    
    with col2:
        avg_ctr = filtered_df['CTR'].mean()
        st.metric("平均CTR", f"{avg_ctr:.2f}%")
    
    with col3:
        avg_cvr = filtered_df['CVR'].mean()
        st.metric("平均CVR", f"{avg_cvr:.2f}%")
    
    with col4:
        avg_cpa = filtered_df['CPA'].mean()
        st.metric("平均CPA", f"¥{avg_cpa:,.0f}")
    
    with col5:
        total_spend = filtered_df['SPEND'].sum()
        st.metric("総広告費", f"¥{total_spend:,.0f}")
    
    st.divider()
    
    # ============================================================================
    # プラットフォーム別分析
    # ============================================================================
    st.markdown("### 📈 プラットフォーム別パフォーマンス")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # プラットフォーム別CTR
        platform_stats = filtered_df.groupby('PLATFORM').agg({
            'CTR': 'mean',
            'CVR': 'mean',
            'CPA': 'mean',
            'SPEND': 'sum'
        }).reset_index()
        
        chart_ctr = alt.Chart(platform_stats).mark_bar(color='#4a90a4').encode(
            x=alt.X('PLATFORM:N', title='プラットフォーム'),
            y=alt.Y('CTR:Q', title='平均CTR (%)'),
            tooltip=['PLATFORM', alt.Tooltip('CTR:Q', format='.2f')]
        ).properties(title='プラットフォーム別 平均CTR', height=300)
        
        st.altair_chart(chart_ctr, use_container_width=True)
    
    with col2:
        # プラットフォーム別CPA
        chart_cpa = alt.Chart(platform_stats).mark_bar(color='#e67e22').encode(
            x=alt.X('PLATFORM:N', title='プラットフォーム'),
            y=alt.Y('CPA:Q', title='平均CPA (円)'),
            tooltip=['PLATFORM', alt.Tooltip('CPA:Q', format=',.0f')]
        ).properties(title='プラットフォーム別 平均CPA', height=300)
        
        st.altair_chart(chart_cpa, use_container_width=True)
    
    st.divider()
    
    # ============================================================================
    # セグメント別分析
    # ============================================================================
    st.markdown("### 👥 ターゲットセグメント別パフォーマンス")
    
    segment_stats = filtered_df.groupby('TARGET_SEGMENT').agg({
        'CTR': 'mean',
        'CVR': 'mean',
        'CPA': 'mean',
        'SPEND': 'sum',
        'CREATIVE_ID': 'count'
    }).reset_index()
    segment_stats.columns = ['TARGET_SEGMENT', 'CTR', 'CVR', 'CPA', 'SPEND', 'COUNT']
    
    chart_segment = alt.Chart(segment_stats).mark_bar(color='#9b59b6').encode(
        y=alt.Y('TARGET_SEGMENT:N', title='ターゲットセグメント', sort='-x'),
        x=alt.X('CTR:Q', title='平均CTR (%)'),
        tooltip=['TARGET_SEGMENT', alt.Tooltip('CTR:Q', format='.2f'), alt.Tooltip('COUNT:Q', title='件数')]
    ).properties(title='セグメント別 平均CTR', height=300)
    
    st.altair_chart(chart_segment, use_container_width=True)
    
    st.divider()
    
    # ============================================================================
    # クリエイティブ一覧
    # ============================================================================
    st.markdown("### 🖼️ クリエイティブ詳細")
    st.markdown("各広告クリエイティブのKPIを表示します。")
    
    # 3列表示
    for i in range(0, len(filtered_df), 3):
        cols = st.columns(3)
        
        for j, col in enumerate(cols):
            if i + j < len(filtered_df):
                row = filtered_df.iloc[i + j]
                
                with col:
                    with st.container():
                        # クリエイティブ名
                        st.markdown(f"**{row['CREATIVE_NAME']}**")
                        st.caption(f"{row['PLATFORM']} | {row['CREATIVE_TYPE']} | {row['TARGET_SEGMENT']}")
                        
                        # 詳細情報
                        if row['HEADLINE']:
                            st.markdown(f"📝 _{row['HEADLINE']}_")
                        
                        st.divider()
                        
                        # KPI表示
                        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
                        
                        with kpi_col1:
                            st.metric("CTR", f"{row['CTR']:.2f}%")
                        
                        with kpi_col2:
                            st.metric("CVR", f"{row['CVR']:.2f}%")
                        
                        with kpi_col3:
                            st.metric("CPA", f"¥{row['CPA']:,.0f}")
                        
                        # 詳細
                        with st.expander("詳細を見る"):
                            st.write(f"**キャンペーンID:** {row['CAMPAIGN_ID']}")
                            st.write(f"**インプレッション:** {row['IMPRESSIONS']:,}")
                            st.write(f"**クリック数:** {row['CLICKS']:,}")
                            st.write(f"**コンバージョン:** {row['CONVERSIONS']:,}")
                            st.write(f"**広告費:** ¥{row['SPEND']:,}")
                            st.write(f"**画像スタイル:** {row['IMAGE_STYLE']}")
                            if row['COPY_TEXT']:
                                st.write(f"**コピー:** {row['COPY_TEXT'][:100]}...")
    
    st.divider()
    
    # ============================================================================
    # データテーブル
    # ============================================================================
    st.markdown("### 📋 データ一覧")
    
    with st.expander("全データを表示", expanded=False):
        display_df = filtered_df[['CREATIVE_ID', 'CREATIVE_NAME', 'PLATFORM', 'CREATIVE_TYPE', 
                                   'TARGET_SEGMENT', 'IMPRESSIONS', 'CLICKS', 'CONVERSIONS', 
                                   'SPEND', 'CTR', 'CVR', 'CPA']].copy()
        
        # カラム名を日本語に変更
        display_df.columns = ['ID', 'クリエイティブ名', 'プラットフォーム', 'タイプ', 
                              'セグメント', 'インプレッション', 'クリック', 'CV', 
                              '広告費', 'CTR(%)', 'CVR(%)', 'CPA']
        
        st.dataframe(display_df, use_container_width=True)
        
        # CSVダウンロード
        csv = display_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 CSVダウンロード",
            data=csv,
            file_name="ad_creatives_analysis.csv",
            mime="text/csv"
        )

except Exception as e:
    st.error(f"データの取得に失敗しました。")
    st.caption(f"エラー詳細: {e}")
    st.info("Part 1〜3のNotebookを実行してデータを準備してください。")
