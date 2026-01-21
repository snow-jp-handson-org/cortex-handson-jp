# =========================================================
# GLACIER CREATIVE STUDIO
# ダッシュボードページ
# =========================================================
# 概要: 広告パフォーマンスの全体像を視覚的に把握
# =========================================================

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

# ページ設定
st.set_page_config(layout="wide")

# =========================================================
# ユーティリティ関数
# =========================================================
@st.cache_resource
def get_session():
    """Snowflakeセッションを取得"""
    return get_active_session()


def format_number(value: float, prefix: str = "", suffix: str = "") -> str:
    """数値をフォーマット"""
    if pd.isna(value):
        return "-"
    if value >= 1_000_000:
        formatted = f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        formatted = f"{value/1_000:.1f}K"
    else:
        formatted = f"{value:,.0f}"
    return f"{prefix}{formatted}{suffix}"


def format_currency(value: float) -> str:
    """通貨フォーマット"""
    return format_number(value, prefix="¥")


@st.cache_data(ttl=600)
def get_creative_data() -> pd.DataFrame:
    """広告クリエイティブデータを取得"""
    session = get_session()
    
    query = """
    SELECT 
        g.CREATIVE_ID,
        g.CAMPAIGN_ID,
        g.CREATIVE_NAME,
        g.CREATIVE_TYPE,
        g.TARGET_SEGMENT,
        g.PLATFORM,
        g.IMPRESSIONS,
        g.CLICKS,
        g.CONVERSIONS,
        g.SPEND,
        g.CTR,
        g.CVR,
        g.CPA
    FROM GOLD_AD_CREATIVE_ANALYSIS g
    ORDER BY g.CREATIVE_ID
    """
    
    return session.sql(query).to_pandas()


@st.cache_data(ttl=600)
def get_creative_summary() -> dict:
    """クリエイティブのサマリー統計を取得"""
    session = get_session()
    
    query = """
    SELECT 
        SUM(IMPRESSIONS) AS TOTAL_IMPRESSIONS,
        SUM(CLICKS) AS TOTAL_CLICKS,
        SUM(CONVERSIONS) AS TOTAL_CONVERSIONS,
        SUM(SPEND) AS TOTAL_SPEND,
        ROUND(SUM(CLICKS) / NULLIF(SUM(IMPRESSIONS), 0) * 100, 2) AS AVG_CTR,
        ROUND(SUM(CONVERSIONS) / NULLIF(SUM(CLICKS), 0) * 100, 2) AS AVG_CVR,
        ROUND(SUM(SPEND) / NULLIF(SUM(CONVERSIONS), 0), 0) AS AVG_CPA,
        COUNT(*) AS CREATIVE_COUNT
    FROM GOLD_AD_CREATIVE_ANALYSIS
    """
    
    df = session.sql(query).to_pandas()
    return df.iloc[0].to_dict()


@st.cache_data(ttl=600)
def get_platform_stats() -> pd.DataFrame:
    """プラットフォーム別統計を取得"""
    session = get_session()
    
    query = """
    SELECT 
        PLATFORM,
        COUNT(*) AS CREATIVE_COUNT,
        SUM(IMPRESSIONS) AS IMPRESSIONS,
        SUM(CLICKS) AS CLICKS,
        SUM(CONVERSIONS) AS CONVERSIONS,
        SUM(SPEND) AS SPEND,
        ROUND(SUM(CLICKS) / NULLIF(SUM(IMPRESSIONS), 0) * 100, 2) AS CTR,
        ROUND(SUM(CONVERSIONS) / NULLIF(SUM(CLICKS), 0) * 100, 2) AS CVR,
        ROUND(SUM(SPEND) / NULLIF(SUM(CONVERSIONS), 0), 0) AS CPA
    FROM GOLD_AD_CREATIVE_ANALYSIS
    GROUP BY PLATFORM
    ORDER BY IMPRESSIONS DESC
    """
    
    return session.sql(query).to_pandas()


# =========================================================
# メインコンテンツ
# =========================================================

st.title("📊 広告パフォーマンスダッシュボード")
st.markdown("広告クリエイティブの全体的なパフォーマンスを確認できます。")
st.markdown("---")

# データ取得
df = get_creative_data()
summary = get_creative_summary()
platform_stats = get_platform_stats()

if df.empty:
    st.error("データが見つかりません。データベースの設定を確認してください。")
    st.stop()

# =========================================================
# KPIサマリー
# =========================================================
st.subheader("📈 KPIサマリー")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label="総インプレッション",
        value=format_number(summary['TOTAL_IMPRESSIONS'])
    )

with col2:
    st.metric(
        label="総クリック",
        value=format_number(summary['TOTAL_CLICKS'])
    )

with col3:
    st.metric(
        label="総コンバージョン",
        value=format_number(summary['TOTAL_CONVERSIONS'])
    )

with col4:
    st.metric(
        label="総費用",
        value=format_currency(summary['TOTAL_SPEND'])
    )

with col5:
    st.metric(
        label="平均CPA",
        value=format_currency(summary['AVG_CPA'])
    )

st.markdown("---")

# =========================================================
# グラフエリア（鮮やかな色に変更）
# =========================================================
col_left, col_right = st.columns(2)

# 鮮やかな色パレット
COLORS = {
    'primary': '#3B82F6',    # 鮮やかな青
    'secondary': '#10B981',  # エメラルドグリーン
    'accent': '#F59E0B',     # 明るいオレンジ
    'google': '#4285F4',     # Google Blue
    'meta': '#0668E1',       # Meta Blue
    'line': '#06C755',       # LINE Green
}

with col_left:
    st.subheader("📊 プラットフォーム別パフォーマンス")
    
    if not platform_stats.empty and platform_stats['CTR'].notna().any():
        fig_platform = go.Figure()
        
        fig_platform.add_trace(go.Bar(
            name='CTR (%)',
            x=platform_stats['PLATFORM'].tolist(),
            y=platform_stats['CTR'].tolist(),
            marker_color=COLORS['primary'],
            text=platform_stats['CTR'].apply(lambda x: f"{x:.2f}%"),
            textposition='outside'
        ))
        
        fig_platform.add_trace(go.Bar(
            name='CVR (%)',
            x=platform_stats['PLATFORM'].tolist(),
            y=platform_stats['CVR'].tolist(),
            marker_color=COLORS['secondary'],
            text=platform_stats['CVR'].apply(lambda x: f"{x:.2f}%"),
            textposition='outside'
        ))
        
        fig_platform.update_layout(
            barmode='group',
            height=350,
            margin=dict(l=20, r=20, t=40, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis_title="プラットフォーム",
            yaxis_title="割合 (%)"
        )
        
        st.plotly_chart(fig_platform, use_container_width=True)
    else:
        st.info("グラフを表示するためのデータがありません。")

with col_right:
    st.subheader("🎯 CPA比較（低いほど効率的）")
    
    if not platform_stats.empty and platform_stats['CPA'].notna().any():
        cpa_data = platform_stats[['PLATFORM', 'CPA']].dropna()
        
        if not cpa_data.empty:
            # プラットフォームごとの色
            platform_colors = []
            for p in cpa_data['PLATFORM'].tolist():
                if p.lower() == 'google':
                    platform_colors.append(COLORS['google'])
                elif p.lower() == 'meta':
                    platform_colors.append(COLORS['meta'])
                elif p.lower() == 'line':
                    platform_colors.append(COLORS['line'])
                else:
                    platform_colors.append(COLORS['accent'])
            
            fig_cpa = go.Figure(data=[
                go.Bar(
                    x=cpa_data['PLATFORM'].tolist(),
                    y=cpa_data['CPA'].tolist(),
                    marker_color=platform_colors,
                    text=[f"¥{int(x):,}" for x in cpa_data['CPA'].tolist()],
                    textposition='outside'
                )
            ])
            
            fig_cpa.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=40, b=40),
                xaxis_title="プラットフォーム",
                yaxis_title="CPA (円)"
            )
            
            st.plotly_chart(fig_cpa, use_container_width=True)
    else:
        st.info("グラフを表示するためのデータがありません。")

st.markdown("---")

# =========================================================
# 費用配分
# =========================================================
st.subheader("💰 費用配分")

col1, col2 = st.columns([1, 2])

with col1:
    if not platform_stats.empty and platform_stats['SPEND'].notna().any():
        spend_data = platform_stats[['PLATFORM', 'SPEND']].dropna()
        
        if not spend_data.empty:
            # プラットフォームごとの色
            pie_colors = []
            for p in spend_data['PLATFORM'].tolist():
                if p.lower() == 'google':
                    pie_colors.append(COLORS['google'])
                elif p.lower() == 'meta':
                    pie_colors.append(COLORS['meta'])
                elif p.lower() == 'line':
                    pie_colors.append(COLORS['line'])
                else:
                    pie_colors.append(COLORS['accent'])
            
            fig_pie = go.Figure(data=[go.Pie(
                labels=spend_data['PLATFORM'].tolist(),
                values=spend_data['SPEND'].tolist(),
                hole=0.4,
                marker_colors=pie_colors
            )])
            
            fig_pie.update_layout(
                height=300,
                margin=dict(l=20, r=20, t=20, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=-0.2)
            )
            
            st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    if not platform_stats.empty:
        display_df = platform_stats[['PLATFORM', 'IMPRESSIONS', 'CLICKS', 'CONVERSIONS', 'SPEND', 'CTR', 'CVR', 'CPA']].copy()
        display_df['IMPRESSIONS'] = display_df['IMPRESSIONS'].apply(lambda x: format_number(x))
        display_df['CLICKS'] = display_df['CLICKS'].apply(lambda x: format_number(x))
        display_df['CONVERSIONS'] = display_df['CONVERSIONS'].apply(lambda x: format_number(x))
        display_df['SPEND'] = display_df['SPEND'].apply(lambda x: format_currency(x))
        display_df['CTR'] = display_df['CTR'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
        display_df['CVR'] = display_df['CVR'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
        display_df['CPA'] = display_df['CPA'].apply(lambda x: f"¥{x:,.0f}" if pd.notna(x) else "-")
        
        display_df.columns = ['プラットフォーム', 'IMP', 'Click', 'CV', '費用', 'CTR', 'CVR', 'CPA']
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)

st.markdown("---")

# =========================================================
# TOP/WORSTランキング（改善されたデザイン）
# =========================================================
st.subheader("🏆 クリエイティブランキング")

col_top, col_worst = st.columns(2)

with col_top:
    st.markdown("#### ✨ TOP 5（CVR順）")
    
    top_df = df.nlargest(5, 'CVR')[['CREATIVE_NAME', 'PLATFORM', 'CVR', 'CPA', 'CONVERSIONS']].reset_index(drop=True)
    
    for rank, (idx, row) in enumerate(top_df.iterrows(), 1):
        with st.container(border=True):
            # ランキングと名前を同じ行に
            col_rank, col_name = st.columns([1, 5])
            with col_rank:
                if rank == 1:
                    st.markdown(f"### 🥇")
                elif rank == 2:
                    st.markdown(f"### 🥈")
                elif rank == 3:
                    st.markdown(f"### 🥉")
                else:
                    st.markdown(f"### {rank}")
            with col_name:
                st.markdown(f"**{row['CREATIVE_NAME']}**")
                st.caption(f"{row['PLATFORM'].upper()}")
            
            # メトリクス
            m1, m2, m3 = st.columns(3)
            m1.metric("CVR", f"{row['CVR']:.2f}%")
            m2.metric("CPA", f"¥{row['CPA']:,.0f}")
            m3.metric("CV", f"{row['CONVERSIONS']:,.0f}")

with col_worst:
    st.markdown("#### ⚠️ WORST 5（CVR順）")
    
    worst_df = df.nsmallest(5, 'CVR')[['CREATIVE_NAME', 'PLATFORM', 'CVR', 'CPA', 'CONVERSIONS']].reset_index(drop=True)
    
    for rank, (idx, row) in enumerate(worst_df.iterrows(), 1):
        with st.container(border=True):
            # ランキングと名前を同じ行に
            col_rank, col_name = st.columns([1, 5])
            with col_rank:
                st.markdown(f"### {rank}")
            with col_name:
                st.markdown(f"**{row['CREATIVE_NAME']}**")
                st.caption(f"{row['PLATFORM'].upper()}")
            
            # メトリクス
            m1, m2, m3 = st.columns(3)
            m1.metric("CVR", f"{row['CVR']:.2f}%")
            m2.metric("CPA", f"¥{row['CPA']:,.0f}")
            m3.metric("CV", f"{row['CONVERSIONS']:,.0f}")

st.markdown("---")

# =========================================================
# キャンペーン × プラットフォーム マトリクス
# =========================================================
st.subheader("🗺️ キャンペーン × プラットフォーム マトリクス")

if 'CAMPAIGN_ID' in df.columns and not df.empty:
    try:
        pivot_df = df.pivot_table(
            values='CVR',
            index='CAMPAIGN_ID',
            columns='PLATFORM',
            aggfunc='mean'
        ).fillna(0)
        
        if not pivot_df.empty and pivot_df.values.sum() > 0:
            fig_heatmap = go.Figure(data=go.Heatmap(
                z=pivot_df.values,
                x=pivot_df.columns.tolist(),
                y=pivot_df.index.tolist(),
                colorscale='Blues',
                text=[[f"{val:.2f}%" for val in row] for row in pivot_df.values],
                texttemplate="%{text}",
                textfont={"size": 12},
                hoverongaps=False
            ))
            
            fig_heatmap.update_layout(
                height=400,
                margin=dict(l=20, r=20, t=40, b=40),
                xaxis_title="プラットフォーム",
                yaxis_title="キャンペーン"
            )
            
            st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.info("ヒートマップを表示するためのデータがありません。")
    except Exception as e:
        st.info(f"ヒートマップの表示に必要なデータが不足しています。")
else:
    st.info("キャンペーンデータがありません。")

st.markdown("---")
st.caption("💡 詳細な分析は「クリエイティブ分析」ページで行えます。")
