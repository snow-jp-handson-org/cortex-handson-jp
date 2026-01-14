"""
================================================================================
GlacierStyle Analytics App
================================================================================
GlacierStyle ECサイトの分析ダッシュボード

機能:
- 広告クリエイティブ分析（KPIダッシュボード）
- VoC分析（顧客の声の可視化）
- マルチモーダル検索（Cortex Search連携）

※ Streamlit in Snowflake (SiS) で動作
================================================================================
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

# ============================================================================
# ページ設定
# ============================================================================
st.set_page_config(
    page_title="GlacierStyle Analytics",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# カスタムCSS
# ============================================================================
st.markdown("""
<style>
    /* メインタイトル */
    .main-title {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #1e3a5f 0%, #4a90a4 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    
    /* サブタイトル */
    .sub-title {
        color: #6b7280;
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }
    
    /* カード */
    .metric-card {
        background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
        border-radius: 12px;
        padding: 1.5rem;
        border-left: 4px solid #4a90a4;
        margin-bottom: 1rem;
    }
    
    /* ナビゲーションカード */
    .nav-card {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        transition: transform 0.2s, box-shadow 0.2s;
        border: 1px solid #e5e7eb;
    }
    
    .nav-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.12);
    }
    
    /* フッター */
    .footer {
        text-align: center;
        color: #9ca3af;
        padding: 2rem 0;
        font-size: 0.875rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# セッション取得
# ============================================================================
@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()

# ============================================================================
# メインコンテンツ
# ============================================================================
# ヘッダー
st.markdown('<p class="main-title">🏔️ GlacierStyle Analytics</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-title">ECサイト分析ダッシュボード - 売上・VoC・広告効果を一元管理</p>', unsafe_allow_html=True)

# サマリーメトリクス
st.markdown("### 📊 クイックサマリー（2024年12月）")

col1, col2, col3, col4 = st.columns(4)

try:
    # 注文数
    with col1:
        orders_count = session.sql("""
            SELECT COUNT(*) as cnt FROM FACT_ORDERS 
            WHERE ORDER_DATETIME >= '2024-12-01' AND ORDER_DATETIME < '2025-01-01'
        """).collect()[0]['CNT']
        st.metric("注文件数", f"{orders_count:,}")

    # 売上合計
    with col2:
        total_sales = session.sql("""
            SELECT SUM(TOTAL_AMOUNT) as total FROM FACT_ORDERS 
            WHERE ORDER_DATETIME >= '2024-12-01' AND ORDER_DATETIME < '2025-01-01'
        """).collect()[0]['TOTAL']
        if total_sales:
            st.metric("売上合計", f"¥{total_sales:,.0f}")
        else:
            st.metric("売上合計", "N/A")

    # SNS投稿数
    with col3:
        sns_count = session.sql("""
            SELECT COUNT(*) as cnt FROM GOLD_SNS_MENTIONS_ANALYZED
        """).collect()[0]['CNT']
        st.metric("SNS投稿数", f"{sns_count:,}")

    # 音声ログ数
    with col4:
        voice_count = session.sql("""
            SELECT COUNT(*) as cnt FROM GOLD_VOICE_LOGS
        """).collect()[0]['CNT']
        st.metric("音声ログ数", f"{voice_count:,}")

except Exception as e:
    st.warning(f"データ取得中にエラーが発生しました。データのロードが完了しているか確認してください。")
    st.caption(f"エラー詳細: {e}")

st.divider()

# ナビゲーション
st.markdown("### 🧭 分析メニュー")
st.markdown("左のサイドバーから各分析機能にアクセスできます。")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    <div class="nav-card">
        <h4>📢 広告クリエイティブ分析</h4>
        <p style="color: #6b7280; font-size: 0.9rem;">
        広告クリエイティブのKPI（CTR、CVR、CPA）を分析。
        プラットフォーム別・セグメント別のパフォーマンスを可視化。
        </p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="nav-card">
        <h4>💬 VoC分析</h4>
        <p style="color: #6b7280; font-size: 0.9rem;">
        SNS投稿と音声ログの感情分析結果を可視化。
        カテゴリ別・プラットフォーム別の傾向を把握。
        </p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div class="nav-card">
        <h4>🔍 マルチモーダル検索</h4>
        <p style="color: #6b7280; font-size: 0.9rem;">
        Cortex Searchを使用した統合検索UI。
        FAQ、マニュアル、音声ログ、SNSを横断検索。
        </p>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Cortex AI機能の紹介
st.markdown("### 🤖 使用しているCortex AI機能")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **データ処理・分析**
    - `AI_SENTIMENT` - 感情分析
    - `AI_CLASSIFY` - カテゴリ分類
    - `AI_EXTRACT` - 情報抽出
    - `AI_COMPLETE` - テキスト生成
    """)

with col2:
    st.markdown("""
    **検索・インテリジェンス**
    - `Cortex Search` - ハイブリッド検索
    - `Semantic View` - Text2SQL
    - `Cortex Agent` - 自然言語分析
    """)

# フッター
st.markdown("---")
st.markdown("""
<div class="footer">
    GlacierStyle Analytics | Powered by Snowflake Cortex AI & Streamlit in Snowflake
</div>
""", unsafe_allow_html=True)
