# =========================================================
# GLACIER CREATIVE STUDIO
# 広告クリエイティブ分析・企画支援プラットフォーム
# メインページ
# =========================================================
# 概要: アプリケーションの概要とナビゲーション
# =========================================================

import streamlit as st
from snowflake.snowpark.context import get_active_session

# ページ設定
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflakeセッション取得
@st.cache_resource
def get_snowflake_session():
    """Snowflakeセッションを取得"""
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# セッションステートの初期化（モデル選択のキャッシュ）
# =========================================================
if "selected_model" not in st.session_state:
    st.session_state.selected_model = "claude-sonnet-4-5"

# =========================================================
# メインページタイトル
# =========================================================
st.title("🎨 GLACIER CREATIVE STUDIO")
st.header("広告クリエイティブ分析・企画支援プラットフォーム")

st.markdown("""
Snowflake Intelligenceでは難しい**ビジュアル中心の分析**と**インタラクティブな広告企画支援**を実現します。
さらに、**AI_AGG、AI_CLASSIFY、AI_SENTIMENT**などの高度なSnowflake AI関数を活用した分析機能を提供します。
""")

st.markdown("---")

# =========================================================
# セクション1: 機能一覧（カードデザイン）
# =========================================================
st.subheader("📌 機能一覧")

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown("### 📊 ダッシュボード")
        st.markdown("広告パフォーマンスの全体像を視覚的に把握。")
        st.markdown("""
        - KPIサマリー（インプレッション、クリック、CV）
        - プラットフォーム別パフォーマンス比較
        - TOP/WORSTクリエイティブランキング
        """)
    
    with st.container(border=True):
        st.markdown("### 💡 広告企画支援")
        st.markdown("次の広告制作のためのインサイト提供。")
        st.markdown("""
        - 勝ちパターン分析
        - AIによるキャッチコピー生成
        - クリエイティブブリーフ作成
        """)
    
    with st.container(border=True):
        st.markdown("### 📱 SNS分析 🆕")
        st.markdown("SNS投稿からトレンドを分析。")
        st.markdown("""
        - ワードクラウド・キーワード抽出
        - AI_AGGによる集約分析
        - AI_CLASSIFY / AI_SENTIMENTによる分類
        """)

with col2:
    with st.container(border=True):
        st.markdown("### 🖼️ クリエイティブ分析")
        st.markdown("個別クリエイティブの詳細分析。")
        st.markdown("""
        - 画像・コピー・KPIの統合表示
        - AIによる効果要因分析
        - **マルチモーダル画像分析** 🆕
        """)
    
    with st.container(border=True):
        st.markdown("### 📁 クリエイティブ管理")
        st.markdown("広告クリエイティブのアセット管理。")
        st.markdown("""
        - 一覧表示・フィルタリング
        - 新規クリエイティブ登録
        - CSVエクスポート
        """)

st.markdown("---")

# =========================================================
# セクション2: Snowflake AI関数の紹介
# =========================================================
st.subheader("🚀 活用しているSnowflake AI関数")

with st.container(border=True):
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 🤖 AI_COMPLETE")
        st.markdown("""
        自由形式のプロンプトでAI分析を実行。
        マルチモーダル対応で画像分析も可能。
        """)
    
    with col2:
        st.markdown("#### 🔬 AI_AGG")
        st.markdown("""
        複数のテキストを集約し、統合的な
        インサイトを抽出。大量データの分析に最適。
        """)
    
    with col3:
        st.markdown("#### 🏷️ AI_CLASSIFY")
        st.markdown("""
        テキストを指定したカテゴリに
        自動分類。感情分析にも活用可能。
        """)

st.markdown("---")

# =========================================================
# セクション3: クイックスタート
# =========================================================
st.subheader("🎯 クイックスタート")

with st.container(border=True):
    st.markdown("""
    **はじめての方へ**
    
    1. 📊 **ダッシュボード** で全体のパフォーマンスを確認
    2. 🖼️ **クリエイティブ分析** で個別の広告を深掘り（AI画像分析も！）
    3. 📱 **SNS分析** でトレンドとユーザーの声を把握
    4. 💡 **広告企画支援** で次のキャンペーンを企画
    5. 📁 **クリエイティブ管理** でアセットを整理
    
    👈 左側のサイドバーから各ページにアクセスしてください。
    """)

st.markdown("---")

# =========================================================
# セクション4: データソース
# =========================================================
with st.expander("📚 データソースについて"):
    st.markdown("""
    このアプリケーションは以下のデータを使用しています：
    
    | データソース | 説明 |
    |-------------|------|
    | `GOLD_AD_CREATIVE_ANALYSIS` | 広告クリエイティブの分析済みKPI |
    | `RAW_AD_CREATIVES` | 広告クリエイティブの生データ（画像パス含む） |
    | `DATA_STAGE/ad_images/` | 広告画像ファイル |
    | `GOLD_SNS_MENTIONS_ANALYZED` | SNS投稿分析データ（VoC連携用） |
    
    データは2024年12月のキャンペーンデータを使用しています。
    """)

st.markdown("---")
st.caption("**GLACIER CREATIVE STUDIO** | Powered by Snowflake Cortex AI")
