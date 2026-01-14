"""
================================================================================
広告クリエイティブ分析
================================================================================
広告画像とKPIを並列表示し、パフォーマンスを分析

機能:
- 広告画像のプレビュー表示
- KPI（CTR, CVR, CPA）のダッシュボード
- プラットフォーム別・セグメント別分析
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
st.markdown("広告画像とKPIを並列表示し、クリエイティブのパフォーマンスを分析します。")
st.divider()

# ============================================================================
# フィルター
# ============================================================================
with st.sidebar:
    st.header("🔧 フィルター")
    
    # プラットフォーム選択
    platforms = session.sql("""
        SELECT DISTINCT PLATFORM FROM AD_CREATIVES ORDER BY PLATFORM
    """).to_pandas()['PLATFORM'].tolist()
    
    selected_platforms = st.multiselect(
        "プラットフォーム",
        options=platforms,
        default=platforms
    )
    
    # ターゲットセグメント選択
    segments = session.sql("""
        SELECT DISTINCT TARGET_SEGMENT FROM AD_CREATIVES ORDER BY TARGET_SEGMENT
    """).to_pandas()['TARGET_SEGMENT'].tolist()
    
    selected_segments = st.multiselect(
        "ターゲットセグメント",
        options=segments,
        default=segments
    )
    
    # CTRソート
    sort_by = st.selectbox(
        "並び替え",
        options=["CTR_PERCENT（高い順）", "CTR_PERCENT（低い順）", "CPA（低い順）", "CVR_PERCENT（高い順）"],
        index=0
    )

# ============================================================================
# データ取得
# ============================================================================
@st.cache_data(ttl=300)
def get_ad_creatives(_session, platforms, segments):
    platforms_str = "', '".join(platforms)
    segments_str = "', '".join(segments)
    
    query = f"""
        SELECT 
            CREATIVE_ID,
            CREATIVE_NAME,
            CAMPAIGN_NAME,
            PLATFORM,
            AD_FORMAT,
            TARGET_SEGMENT,
            IMAGE_FILE_PATH,
            IMPRESSIONS,
            CLICKS,
            CONVERSIONS,
            AD_SPEND,
            CTR_PERCENT,
            CVR_PERCENT,
            CPA
        FROM AD_CREATIVES
        WHERE PLATFORM IN ('{platforms_str}')
          AND TARGET_SEGMENT IN ('{segments_str}')
    """
    return _session.sql(query).to_pandas()

if selected_platforms and selected_segments:
    df = get_ad_creatives(session, selected_platforms, selected_segments)
    
    # ソート適用
    if "CTR_PERCENT（高い順）" in sort_by:
        df = df.sort_values('CTR_PERCENT', ascending=False)
    elif "CTR_PERCENT（低い順）" in sort_by:
        df = df.sort_values('CTR_PERCENT', ascending=True)
    elif "CPA（低い順）" in sort_by:
        df = df.sort_values('CPA', ascending=True)
    elif "CVR_PERCENT（高い順）" in sort_by:
        df = df.sort_values('CVR_PERCENT', ascending=False)
    
    # ============================================================================
    # サマリーKPI
    # ============================================================================
    st.markdown("### 📊 全体サマリー")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("クリエイティブ数", f"{len(df):,}")
    
    with col2:
        avg_ctr = df['CTR_PERCENT'].mean()
        st.metric("平均CTR", f"{avg_ctr:.2f}%")
    
    with col3:
        avg_cvr = df['CVR_PERCENT'].mean()
        st.metric("平均CVR", f"{avg_cvr:.2f}%")
    
    with col4:
        avg_cpa = df['CPA'].mean()
        st.metric("平均CPA", f"¥{avg_cpa:,.0f}")
    
    with col5:
        total_spend = df['AD_SPEND'].sum()
        st.metric("総広告費", f"¥{total_spend:,.0f}")
    
    st.divider()
    
    # ============================================================================
    # プラットフォーム別分析
    # ============================================================================
    st.markdown("### 📈 プラットフォーム別パフォーマンス")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # プラットフォーム別CTR
        platform_stats = df.groupby('PLATFORM').agg({
            'CTR_PERCENT': 'mean',
            'CVR_PERCENT': 'mean',
            'CPA': 'mean',
            'AD_SPEND': 'sum'
        }).reset_index()
        
        chart_ctr = alt.Chart(platform_stats).mark_bar(color='#4a90a4').encode(
            x=alt.X('PLATFORM:N', title='プラットフォーム'),
            y=alt.Y('CTR_PERCENT:Q', title='平均CTR (%)'),
            tooltip=['PLATFORM', alt.Tooltip('CTR_PERCENT:Q', format='.2f')]
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
    # クリエイティブ一覧（画像付き）
    # ============================================================================
    st.markdown("### 🖼️ クリエイティブ詳細")
    st.markdown("各広告クリエイティブの画像とKPIを表示します。")
    
    # ステージURLのベースパス
    STAGE_BASE_URL = "@GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA.DATA_STAGE/ad_images/"
    
    # 3列表示
    for i in range(0, len(df), 3):
        cols = st.columns(3)
        
        for j, col in enumerate(cols):
            if i + j < len(df):
                row = df.iloc[i + j]
                
                with col:
                    with st.container(border=True):
                        # クリエイティブ名
                        st.markdown(f"**{row['CREATIVE_NAME']}**")
                        st.caption(f"{row['PLATFORM']} | {row['AD_FORMAT']} | {row['TARGET_SEGMENT']}")
                        
                        # 画像表示（ステージから取得を試みる）
                        image_file = row['IMAGE_FILE_PATH']
                        if image_file:
                            try:
                                # ステージからの画像表示
                                image_url = f"{STAGE_BASE_URL}{image_file}"
                                st.markdown(f"📷 `{image_file}`")
                                # Note: SiSでは直接ステージ画像を表示するのは制限がある場合があります
                                # 実際の運用ではPresigned URLやBase64エンコードを使用
                            except:
                                st.markdown(f"📷 `{image_file}`")
                        
                        # KPI表示
                        st.divider()
                        
                        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
                        
                        with kpi_col1:
                            st.metric("CTR", f"{row['CTR_PERCENT']:.2f}%")
                        
                        with kpi_col2:
                            st.metric("CVR", f"{row['CVR_PERCENT']:.2f}%")
                        
                        with kpi_col3:
                            st.metric("CPA", f"¥{row['CPA']:,.0f}")
                        
                        # 詳細
                        with st.expander("詳細を見る"):
                            st.write(f"**キャンペーン:** {row['CAMPAIGN_NAME']}")
                            st.write(f"**インプレッション:** {row['IMPRESSIONS']:,}")
                            st.write(f"**クリック数:** {row['CLICKS']:,}")
                            st.write(f"**コンバージョン:** {row['CONVERSIONS']:,}")
                            st.write(f"**広告費:** ¥{row['AD_SPEND']:,}")
    
    st.divider()
    
    # ============================================================================
    # データテーブル
    # ============================================================================
    st.markdown("### 📋 データ一覧")
    
    with st.expander("全データを表示", expanded=False):
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "CREATIVE_ID": "ID",
                "CREATIVE_NAME": "クリエイティブ名",
                "CAMPAIGN_NAME": "キャンペーン",
                "PLATFORM": "プラットフォーム",
                "AD_FORMAT": "フォーマット",
                "TARGET_SEGMENT": "セグメント",
                "IMAGE_FILE_PATH": "画像ファイル",
                "IMPRESSIONS": st.column_config.NumberColumn("インプレッション", format="%d"),
                "CLICKS": st.column_config.NumberColumn("クリック", format="%d"),
                "CONVERSIONS": st.column_config.NumberColumn("CV", format="%d"),
                "AD_SPEND": st.column_config.NumberColumn("広告費", format="¥%d"),
                "CTR_PERCENT": st.column_config.NumberColumn("CTR (%)", format="%.2f"),
                "CVR_PERCENT": st.column_config.NumberColumn("CVR (%)", format="%.2f"),
                "CPA": st.column_config.NumberColumn("CPA", format="¥%.0f"),
            }
        )
        
        # CSVダウンロード
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 CSVダウンロード",
            data=csv,
            file_name="ad_creatives_analysis.csv",
            mime="text/csv"
        )

else:
    st.warning("フィルターでプラットフォームとセグメントを選択してください。")
