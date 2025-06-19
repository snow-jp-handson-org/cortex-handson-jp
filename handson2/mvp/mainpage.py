# =========================================================
# Snowflake Cortex Handson シナリオ#2
# AIを用いた顧客の声分析アプリケーション
# =========================================================
# Created by Tsubasa Kanno @Snowflake
# 最終更新: 2025/06/16
# =========================================================

# =========================================================
# 必要なライブラリのインポート
# =========================================================
import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, call_function, when_matched, when_not_matched

# =========================================================
# ページ設定とセッション初期化
# =========================================================
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflakeセッションの取得
@st.cache_resource
def get_snowflake_session():
    """Snowflakeセッションを取得"""
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# 共通関数
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

def display_info_card(title: str, value: str, description: str = ""):
    """情報カードを表示"""
    st.metric(
        label=title,
        value=value,
        help=description
    )

def display_success_message(message: str):
    """成功メッセージを表示"""
    st.success(f"✅ {message}")

def display_error_message(message: str):
    """エラーメッセージを表示"""
    st.error(f"❌ {message}")

# =========================================================
# メインページコンテンツ
# =========================================================
def render_home_page():
    """ホームページを表示"""
    st.title("❄️ Snowflake Cortex Handson シナリオ#2")
    st.header("AIを活用した顧客の声分析")
    
    # 基本情報を2列で表示
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
    
    # ワークショップ手順
    st.markdown("---")
    st.markdown("### 📚 ワークショップ手順")
    
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
            "description": "AI_CLASSIFY, AI_FILTER, AI_AGGを使った分析",
            "functions": ["AI_CLASSIFY", "AI_FILTER", "AI_AGG"],
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
            "functions": ["Cortex Analyst", "AI_AGG"],
            "time": "10分"
        }
    ]
    
    for i, step_info in enumerate(steps):
        with st.expander(f"📍 {step_info['step']}: {step_info['title']} ({step_info['time']})"):
            st.markdown(f"**概要**: {step_info['description']}")
            st.markdown("**使用する関数**:")
            for func in step_info['functions']:
                st.write(f"- `{func}`")

    st.markdown("---")
    st.info("💡 **使い方**: サイドバーから各ステップに進んでハンズオンを開始してください。")

# =========================================================
# メインアプリケーション
# =========================================================
def main():
    """メインアプリケーション"""
    
    # メインページを表示
    render_home_page()
    
    # フッター
    st.markdown("---")
    st.markdown(
        "**Snowflake Cortex Handson シナリオ#2 | メインページ**"
    )

if __name__ == "__main__":
    main() 