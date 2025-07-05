# =========================================================
# Snowflake Discover
# Snowflake Cortex AI で実現する次世代の VoC (顧客の声) アプリケーション
# メインページ
# =========================================================
# 概要: ワークショップの概要とナビゲーションページ
# 使用する機能: ワークショップ全体のガイダンス
# =========================================================
# Created by Tsubasa Kanno @Snowflake
# 最終更新: 2025/07/06
# =========================================================

import streamlit as st
import pandas as pd
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
# ユーティリティ関数
# =========================================================
def check_table_exists(table_name: str) -> bool:
    """テーブルが存在するかチェック"""
    try:
        session.sql(f"DESC {table_name}").collect()
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

# =========================================================
# メインページタイトル
# =========================================================
st.title("❄️ Snowflake Cortex Handson シナリオ#2")
st.header("AIを活用した顧客の声分析")

# =========================================================
# セクション1: ワークショップについて
# =========================================================
def render_workshop_overview():
    """ワークショップ概要セクションを表示"""
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🚀 ワークショップについて
        
        このワークショップでは、**Snowflake Cortex AI**の各種AI機能を使用して、
        小売業界における顧客の声分析システムを構築します。
        
        #### 📋 学習する技術
        
        ##### 🤖 Cortex AISQL関数
        - **AI_COMPLETE**: 高度なテキスト生成・対話
        - **AI_CLASSIFY**: テキスト分類・カテゴリ分け
        - **AI_FILTER**: 条件ベースの絞り込み
        - **AI_AGG**: データの集約分析
        - **AI_SUMMARIZE_AGG**: 集約データの要約
        - **AI_SIMILARITY**: テキスト類似度計算
        - **TRANSLATE**: 多言語翻訳
        - **SENTIMENT**: 感情分析
        - **EMBED_TEXT_1024**: ベクトル埋め込み
        - **SPLIT_TEXT_RECURSIVE_CHARACTER**: テキスト分割
        
        ##### 🔍 Cortex AI機能
        - **Cortex Search**: 高度な検索システム
        - **Cortex Analyst**: 自然言語からのSQLクエリ生成
        """)
        
    with col2:
        st.markdown("""
        ### 🏢 想定企業
        
        **株式会社スノーリテール**
        - 食品スーパーマーケットチェーン
        - 首都圏150店舗 + EC事業
        - 従業員数: 約15,000名
        - 年間売上: 約2,500億円
        
        #### 🎯 解決する課題
        - 顧客データの統合分析
        - レビュー分析の自動化
        - CS部門の業務効率化
        - データドリブンな意思決定
        """)

# =========================================================
# セクション2: ワークショップ手順
# =========================================================
def render_workshop_steps():
    """ワークショップ手順セクションを表示"""
    st.markdown("### 📚 ワークショップ手順")
    
    # 各ステップの定義
    steps = [
        {
            "step": "Step 1",
            "title": "データ準備", 
            "description": "テーブル作成、テキスト処理、ベクトル化",
            "functions": ["SPLIT_TEXT_RECURSIVE_CHARACTER", "EMBED_TEXT_1024", "TRANSLATE", "SENTIMENT"],
            "time": "10分"
        },
        {
            "step": "Step 2", 
            "title": "顧客の声分析",
            "description": "AI_CLASSIFY, AI_FILTER, AI_AGG, AI_SUMMARIZE_AGG, AI_SIMILARITYを使った分析",
            "functions": ["AI_CLASSIFY", "AI_FILTER", "AI_AGG", "AI_SUMMARIZE_AGG", "AI_SIMILARITY"],
            "time": "20分"
        },
        {
            "step": "Step 3",
            "title": "シンプルチャットボット",
            "description": "AI_COMPLETEを使った基本的なチャットボット",
            "functions": ["AI_COMPLETE"],
            "time": "10分"
        },
        {
            "step": "Step 4",
            "title": "RAGチャットボット", 
            "description": "Cortex Searchとの組み合わせによる高度なQ&A",
            "functions": ["Cortex Search", "AI_COMPLETE"],
            "time": "10分"
        },
        {
            "step": "Step 5",
            "title": "Cortex Analyst分析",
            "description": "自然言語によるデータ分析とダッシュボード",
            "functions": ["Cortex Analyst", "AI_COMPLETE"],
            "time": "10分"
        }
    ]
    
    # 各ステップの詳細表示
    for i, step_info in enumerate(steps):
        with st.expander(f"📍 {step_info['step']}: {step_info['title']} ({step_info['time']})"):
            st.markdown(f"**概要**: {step_info['description']}")
            st.markdown("**使用する関数**:")
            for func in step_info['functions']:
                st.write(f"- `{func}`")

# =========================================================
# メインアプリケーション
# =========================================================
def main():
    """メインアプリケーション"""
    
    # ワークショップ概要を表示
    render_workshop_overview()
    
    st.markdown("---")
    
    # ワークショップ手順を表示
    render_workshop_steps()
    
    st.markdown("---")
    st.info("💡 **使い方**: サイドバーから各ステップに進んでハンズオンを開始してください。")

if __name__ == "__main__":
    main()

st.markdown("---")
st.markdown("**Snowflake Cortex AI で実現する次世代の VoC (顧客の声) アプリケーション | メインページ**") 
