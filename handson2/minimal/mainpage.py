# =========================================================
# Snowflake Cortex Handson シナリオ#2 - Minimal版
# AIを用いた顧客の声分析アプリケーション（データ準備・分析のみ）
# =========================================================

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# =========================================================
# ページ設定とセッション初期化
# =========================================================
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# メインページコンテンツ
# =========================================================
st.title("❄️ Snowflake Cortex Handson - Minimal版")
st.header("AIを活用した顧客の声分析")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### 🚀 このワークショップについて
    
    **Snowflake Cortex AI**のAI機能を使用して、
    顧客の声分析システムを体験します。
    
    #### 📋 使用するAI機能
    - **TRANSLATE**: 多言語翻訳
    - **SENTIMENT**: 感情分析
    - **EMBED_TEXT_1024**: ベクトル埋め込み
    - **SPLIT_TEXT_RECURSIVE_CHARACTER**: テキスト分割
    - **AI_CLASSIFY**: テキスト分類
    - **AI_FILTER**: 条件フィルタリング
    - **AI_AGG**: データ集約分析
    - **AI_SIMILARITY**: 類似度計算
    """)
    
with col2:
    st.markdown("""
    ### 🏢 想定企業
    
    **株式会社スノーリテール**
    - 食品スーパーマーケットチェーン
    - 首都圏150店舗 + EC事業
    
    #### 🎯 解決する課題
    - 顧客レビューの自動分析
    - 感情分析による満足度把握
    - カテゴリ分類の自動化
    """)

st.markdown("---")
st.info("💡 **使い方**: サイドバーから各ステップに進んでください。")

st.markdown("---")
st.markdown("**Snowflake Cortex Handson - Minimal版 | メインページ**")
