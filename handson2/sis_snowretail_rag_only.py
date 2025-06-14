# =========================================================
# ↑上記で、Plotly, snowflake,core, snowflake-ml-pythonの追加が必要
# =========================================================

# =========================================================
# 必要なライブラリのインポート
# =========================================================
# 基本ライブラリ
import streamlit as st
import pandas as pd
import json

# Streamlitの設定
st.set_page_config(layout="wide")

# 可視化ライブラリ
import plotly.express as px

# Snowflake関連ライブラリ
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete as CompleteText
from snowflake.core import Root

# =========================================================
# 定数定義
# =========================================================
# Cortex Analyst APIの設定
ANALYST_API_ENDPOINT = "/api/v2/cortex/analyst/message"
ANALYST_API_TIMEOUT = 50  # 秒

# モデル設定
# 埋め込みモデル選択肢
EMBEDDING_MODELS = [
    "multilingual-e5-large",
    "voyage-multilingual-2",
    "snowflake-arctic-embed-l-v2.0",
    "nv-embed-qa-4"
]

# COMPLETE関数用のLLMモデル選択肢
COMPLETE_MODELS = [
    "claude-3-5-sonnet",
    "deepseek-r1",
    "mistral-large2",
    "llama3.3-70b",
    "snowflake-llama-3.3-70b"
]

# Cortex Search Service用のベクトル埋め込みモデル選択肢
SEARCH_MODELS = [
    "voyage-multilingual-2",
    "snowflake-arctic-embed-m-v1.5",
    "snowflake-arctic-embed-l-v2.0"
]

# デフォルトのレビューカテゴリ
DEFAULT_CATEGORIES = [
    "商品の品質",
    "価格",
    "接客サービス",
    "店舗環境",
    "配送・梱包",
    "品揃え",
    "使いやすさ",
    "鮮度",
    "その他"
]

# サンプル質問
SAMPLE_QUESTIONS = [
    "品切れを防ぐための在庫管理の方法は？",
    "返品ポリシーについて教えてください。",
    "売場の清掃や整理整頓の基準を教えてください。"
]


# =========================================================
# Snowflake接続と共通ユーティリティ関数
# =========================================================

# Snowflakeセッションの取得
snowflake_session = get_active_session()

def get_available_warehouses() -> list:
    """利用可能なSnowflakeウェアハウスの一覧を取得します。
    
    Returns:
        list: ウェアハウス名のリスト（取得失敗時は空リスト）
    """
    try:
        result = snowflake_session.sql("SHOW WAREHOUSES").collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"ウェアハウスの取得に失敗しました: {str(e)}")
        return []

# =========================================================
# メイン処理
# =========================================================

# モデル設定
st.sidebar.title("モデル設定")

complete_model = st.sidebar.selectbox(
    "Completeモデルを選択してください",
    COMPLETE_MODELS,
    index=0
)

# メインコンテンツ
st.title("🏪 スノーリテール 社内アプリ")
st.markdown("---")

"""RAGチャットボットページを表示します。"""
st.header("RAGチャットボット")

# ワークショップ向けの説明
st.info("""
## 📚 RAGチャットボットについて

このページでは、Cortex Searchを用いたRetrieval-Augmented Generation (RAG) フレームワークの高度なチャットボットを体験できます。

### 主な機能
* **検索対象の自動リフレッシュ機能**: Cortex Searchを使用して検索対象ドキュメントを定期的に最新化
* **ハイブリッド検索**: キーワード検索と曖昧検索の両方からドキュメントを検索することが可能

### 使い方のヒント
* 社内文書に関する質問や、製品・サービスに関する具体的な質問をしてみてください
* 質問が具体的であるほど、より関連性の高いドキュメントが検索されます
* 参考ドキュメントを展開すると、応答の生成に使用されたドキュメントを確認できます

### 注意事項
Cortex Search Serviceはドキュメントの更新に伴うコンピューティングコスト以外にも、インデックス化されたデータサイズに対しての料金も発生します。長期間使用しない場合はCortex Search Serviceを削除するなどをご検討ください。
""")

# サンプル質問の表示
st.markdown("#### サンプル質問")
for q in SAMPLE_QUESTIONS:
    st.markdown(f"- {q}")


# Snowflake Root オブジェクトの初期化
root = Root(snowflake_session)

# 現在のデータベースとスキーマを取得
current_db_schema = snowflake_session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
current_database = current_db_schema['CURRENT_DATABASE()']
current_schema = current_db_schema['CURRENT_SCHEMA()']

st.markdown("---")

# セッション状態の初期化
if "rag_messages" not in st.session_state:
    st.session_state.rag_messages = []
    st.session_state.rag_chat_history = ""

# チャット履歴のクリアボタン
if st.button("チャット履歴をクリア"):
    st.session_state.rag_messages = []
    st.session_state.rag_chat_history = ""
    st.rerun()

# チャット履歴の表示
for message in st.session_state.rag_messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "relevant_docs" in message:
            with st.expander("参考ドキュメント"):
                for doc in message["relevant_docs"]:
                    st.markdown(f"""
                    **タイトル**: {doc['title']}  
                    **種類**: {doc['document_type']}  
                    **部署**: {doc['department']}  
                    **内容**: {doc['content'][:200]}...
                    """)


# ユーザー入力の処理
if prompt := st.chat_input("質問を入力してください"):
    # ユーザーメッセージの表示と履歴の更新
    st.session_state.rag_messages.append({"role": "user", "content": prompt})
    st.session_state.rag_chat_history += f"User: {prompt}\n"
    with st.chat_message("user"):
        st.markdown(prompt)
    
    try:
        # 日本語のクエリを英語に変換
        translate_prompt = f"""
        次のユーザーからの質問を、Cortex Searchで使用する英語のキーワードに変換してください。
        キーワードのみを出力してください。

        質問: {prompt}
        """
        keywords = CompleteText(complete_model, translate_prompt)
        
        # Cortex Search Serviceの取得
        search_service = (
            root.databases[current_database]
            .schemas[current_schema]
            .cortex_search_services["snow_retail_search_service"]
        )
        
        # 検索の実行
        search_results = search_service.search(
            query=keywords,
            columns=["title", "content", "document_type", "department"],
            limit=3
        )
        
        # 検索結果の取得
        relevant_docs = [
            {
                "title": result["title"],
                "content": result["content"],
                "document_type": result["document_type"],
                "department": result["department"]
            }
            for result in search_results.results
        ]
        
        # 検索結果をコンテキストとして使用
        context = "参考文書:\n"
        for doc in relevant_docs:
            context += f"""
            タイトル: {doc['title']}
            種類: {doc['document_type']}
            部署: {doc['department']}
            内容: {doc['content']}
            ---
            """
        
        # COMPLETEを使用して応答を生成
        prompt_template = f"""
        あなたはスノーリテールの社内アシスタントです。
        以下の文脈を参考に、ユーザーからの質問に日本語で回答してください。
        わからない場合は、その旨を正直に伝えてください。

        文脈:
        {context}

        質問: {prompt}
        """
        
        response = CompleteText(complete_model, prompt_template)
        
        # アシスタントの応答を表示
        with st.chat_message("assistant"):
            st.markdown(response)
            with st.expander("参考ドキュメント"):
                for doc in relevant_docs:
                    st.markdown(f"""
                    **タイトル**: {doc['title']}  
                    **種類**: {doc['document_type']}  
                    **部署**: {doc['department']}  
                    **内容**: {doc['content'][:200]}...
                    """)
        
        # チャット履歴に追加
        st.session_state.rag_messages.append({
            "role": "assistant",
            "content": response,
            "relevant_docs": relevant_docs
        })
        st.session_state.rag_chat_history += f"AI: {response}\n"
        
    except Exception as e:
        st.error(f"応答の生成中にエラーが発生しました: {str(e)}")
        st.code(str(e))
