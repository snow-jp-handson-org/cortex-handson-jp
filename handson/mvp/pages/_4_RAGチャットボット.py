# =========================================================
# Snowflake Discover
# Snowflake Cortex AI で実現する次世代の VoC (顧客の声) アプリケーション
# Step4: RAGチャットボット
# =========================================================
# 概要: Cortex SearchとCOMPLETEを使ったRAGチャットボットの実装
# 使用する機能: Cortex Search, AI_COMPLETE関数
# =========================================================
# Created by Tsubasa Kanno @Snowflake
# 最終更新: 2025/07/06
# =========================================================

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root

# ページ設定
st.set_page_config(layout="wide")

# =========================================================
# Snowflakeセッション接続
# =========================================================
@st.cache_resource
def get_snowflake_session():
    """Snowflakeセッションを取得"""
    return get_active_session()

session = get_snowflake_session()

# Snowflake Root オブジェクトの初期化（Cortex Search Python API用）
root = Root(session)

# =========================================================
# 設定値（定数）
# =========================================================
# 利用可能なLLMモデル
LLM_MODELS = [
    "llama4-maverick",
    "claude-4-sonnet", 
    "mistral-large2"
]

# Cortex Searchサービス名
SEARCH_SERVICE_NAME = "SNOW_RETAIL_SEARCH_SERVICE"

# セッション状態の初期化
if 'selected_llm_model' not in st.session_state:
    st.session_state.selected_llm_model = LLM_MODELS[0]

# =========================================================
# データ・サービス確認関数
# =========================================================
def check_table_exists(table_name: str) -> bool:
    """テーブルの存在確認"""
    try:
        session.sql(f"SELECT 1 FROM {table_name} LIMIT 1").collect()
        return True
    except:
        return False

def check_cortex_search_service(service_name: str) -> bool:
    """Cortex Searchサービスが利用可能かを確認"""
    try:
        result = session.sql(f"SHOW CORTEX SEARCH SERVICES LIKE '{service_name}'").collect()
        return len(result) > 0
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
# Cortex Search関数（Python API使用）
# =========================================================
def search_documents_with_cortex(query: str, service_name: str = SEARCH_SERVICE_NAME, limit: int = 3, 
                                department_filter: str = "すべて", doc_type_filter: str = "すべて") -> list:
    """
    Cortex SearchのPython APIを使用してドキュメントを検索（フィルタ機能付き）
    企業の知識ベースから関連情報を高精度で検索
    """
    try:
        # 現在のデータベースとスキーマを取得
        current_db_schema = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
        current_database = current_db_schema['CURRENT_DATABASE()']
        current_schema = current_db_schema['CURRENT_SCHEMA()']
        
        # Cortex Search Serviceの取得
        search_service = (
            root.databases[current_database]
            .schemas[current_schema]
            .cortex_search_services[service_name.lower()]
        )
        
        # 検索の実行
        search_args = {
            "query": query,
            "columns": ["title", "content", "document_type", "department"],
            "limit": limit
        }
        
        # フィルタ条件の構築（Cortex Search APIの正しい辞書形式）
        filter_conditions = []
        if department_filter != "すべて":
            filter_conditions.append({"@eq": {"department": department_filter}})
        if doc_type_filter != "すべて":
            filter_conditions.append({"@eq": {"document_type": doc_type_filter}})
        
        # フィルタが指定されている場合は追加
        if filter_conditions:
            if len(filter_conditions) == 1:
                search_args["filter"] = filter_conditions[0]
            else:
                # 複数の条件がある場合は@andで結合
                search_args["filter"] = {"@and": filter_conditions}
        
        search_results = search_service.search(**search_args)
        
        # 検索結果を辞書のリストに変換
        results = []
        for result in search_results.results:
            results.append({
                "title": result.get("title", "タイトルなし"),
                "content": result.get("content", ""),
                "document_type": result.get("document_type", "N/A"),
                "department": result.get("department", "N/A")
            })
        
        return results
        
    except Exception as e:
        st.error(f"検索エラー: {str(e)}")
        return []

# =========================================================
# RAG応答生成関数
# =========================================================
def generate_rag_response(question: str, context: str, model: str) -> str:
    """
    検索結果を基にRAG応答を生成
    企業ドメインの知識を活用した正確な回答を生成
    """
    try:
        # プロンプトのエスケープ処理
        escaped_question = question.replace("'", "''")
        escaped_context = context.replace("'", "''")
        
        # 企業ドメインに特化したプロンプト
        prompt = f"""あなたは企業のカスタマーサポート担当者です。
以下の企業の公式ドキュメントから得られた情報を基に、お客様の質問に正確にお答えください。

企業ドキュメントの情報:
{escaped_context}

お客様の質問: {escaped_question}

回答の際は以下を心がけてください：
- 企業ドキュメントの情報を最優先に使用
- 情報が不足している場合は、「詳細については○○部門にお問い合わせください」などの案内を含める
- 親切で分かりやすい言葉で回答
- 必要に応じて手順を番号付きで説明"""
        
        # プロンプトのエスケープ処理
        escaped_prompt = prompt.replace("'", "''")
        
        # Cortex COMPLETEで回答生成
        complete_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{escaped_prompt}'
        ) as response
        """
        
        result = session.sql(complete_query).collect()
        
        if result and result[0]['RESPONSE']:
            response = result[0]['RESPONSE']
            
            # 応答の後処理
            # 1. 先頭と末尾のダブルクォーテーションを除去
            if response.startswith('"') and response.endswith('"'):
                response = response[1:-1]
            
            # 2. エスケープ文字を適切な文字に変換
            response = response.replace('\\n', '\n')  # 改行文字
            response = response.replace('\\t', '\t')  # タブ文字
            response = response.replace('\\"', '"')   # ダブルクォーテーション
            response = response.replace("\\'", "'")   # シングルクォーテーション
            response = response.replace('\\\\', '\\') # バックスラッシュ
            
            return response
        else:
            return "申し訳ございませんが、回答を生成できませんでした。"
            
    except Exception as e:
        return f"エラーが発生しました: {str(e)}"

# =========================================================
# メインページ
# =========================================================
st.title("📖 Step4: RAGチャットボット")
st.header("企業ドメインを理解するAIアシスタント")

# =========================================================
# サイドバー設定
# =========================================================
st.sidebar.header("⚙️ RAG設定")

# LLMモデル選択
selected_llm_model = st.sidebar.selectbox(
    "LLMモデル:",
    LLM_MODELS,
    index=LLM_MODELS.index(st.session_state.selected_llm_model),
    help="回答生成に使用するLLMモデルを選択"
)

# モデル選択の更新
if selected_llm_model != st.session_state.selected_llm_model:
    st.session_state.selected_llm_model = selected_llm_model

# 検索設定
search_limit = st.sidebar.slider(
    "関連ドキュメント数:",
    min_value=1,
    max_value=5,
    value=3,
    help="参考にするドキュメントの数"
)

# ドキュメントフィルタ機能
st.sidebar.markdown("---")
st.sidebar.subheader("🔍 検索フィルタ")

filter_department = st.sidebar.selectbox(
    "部署でフィルタ:",
    ["すべて", "営業部", "マーケティング部", "カスタマーサービス部", "商品開発部", "物流部", "経営企画部"],
    index=0,
    help="特定の部署のドキュメントのみを検索対象にできます"
)

filter_doc_type = st.sidebar.selectbox(
    "ドキュメントタイプでフィルタ:",
    ["すべて", "社内FAQ", "商品カタログ", "業務マニュアル", "企業ガイドライン", "レポート", "顧客対応"],
    index=0,
    help="特定の種類のドキュメントのみを検索対象にできます"
)

# 設定情報の表示
st.sidebar.info(f"""
**現在の設定:**
- LLMモデル: `{st.session_state.selected_llm_model}`
- 参考ドキュメント: {search_limit}件
- 部署フィルタ: {filter_department}
- 文書タイプ: {filter_doc_type}

**RAGの仕組み:**
1. 🔍 企業ドキュメントから関連情報を検索
2. 📚 検索結果をAIに提供
3. 🤖 企業の知識に基づいて回答生成
""")

st.markdown("---")

# =========================================================
# データ・サービス状況確認
# =========================================================
st.subheader("📊 システム状況確認")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 📄 企業ドキュメント")
    docs_available = check_table_exists("SNOW_RETAIL_DOCUMENTS")
    if docs_available:
        doc_count = get_table_count("SNOW_RETAIL_DOCUMENTS")
        st.metric("✅ 知識ベース", f"{doc_count:,}件", help="企業の公式ドキュメント")
    else:
        st.metric("❌ 知識ベース", "未設定")

with col2:
    st.markdown("#### 🔍 Cortex Search")
    search_available = check_cortex_search_service(SEARCH_SERVICE_NAME)
    if search_available:
        st.metric("✅ 検索サービス", "利用可能", help="高精度な意味検索エンジン")
    else:
        st.metric("❌ 検索サービス", "未設定")

# 必要なサービスの確認
if not docs_available or not search_available:
    st.error("""
    ⚠️ **必要なデータ・サービスが不足しています**
    
    - 企業ドキュメント: Step1のデータ準備で作成
    - Cortex Search: 事前に設定済みの検索サービス
    
    データ準備を完了してから再度お試しください。
    """)
    st.stop()

st.markdown("---")

# =========================================================
# RAGの説明セクション
# =========================================================
st.subheader("🧠 RAG（検索拡張生成）について")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **従来のチャットボット:**
    - 一般的な知識のみで回答
    - 企業固有の情報は理解できない
    - ハルシネーション（嘘の情報）のリスク
    """)

with col2:
    st.markdown("""
    **RAGチャットボット:**
    - ✅ 企業の公式ドキュメントを参照
    - ✅ 最新の企業情報に基づく回答
    - ✅ 回答の根拠となる資料を明示
    """)

# =========================================================
# ドキュメント検索デモ
# =========================================================
st.markdown("---")
st.subheader("🔍 Step1: ドキュメント検索")
st.markdown("企業の知識ベースから関連情報を検索します")

# 検索クエリ入力
search_query = st.text_input(
    "🔍 検索したい内容:",
    value="商品の返品について",
    placeholder="例: 配送料金、返品方法、商品保証など"
)

if st.button("🔎 検索実行", type="primary"):
    if search_query:
        with st.spinner("📚 企業ドキュメントを検索中..."):
            # Cortex Searchで検索実行（フィルタ適用）
            search_results = search_documents_with_cortex(
                search_query, SEARCH_SERVICE_NAME, search_limit, 
                filter_department, filter_doc_type
            )
            
            if search_results:
                st.success(f"✅ {len(search_results)}件の関連ドキュメントを発見")
                
                # 検索結果の表示
                for i, result in enumerate(search_results, 1):
                    title = result.get('title', 'タイトルなし')
                    content = result.get('content', '')
                    doc_type = result.get('document_type', 'N/A')
                    department = result.get('department', 'N/A')
                    
                    with st.expander(f"📄 関連ドキュメント {i}: {title}"):
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            st.markdown(f"**内容:** {content[:300]}...")
                        
                        with col2:
                            st.markdown(f"**種類:** {doc_type}")
                            if department != 'N/A':
                                st.markdown(f"**部門:** {department}")
            else:
                st.warning("関連するドキュメントが見つかりませんでした。別のキーワードでお試しください。")

# =========================================================
# RAGチャットボット
# =========================================================
st.markdown("---")
st.subheader("🤖 Step2: RAGチャットボット")
st.markdown("企業ドメインを理解したAIアシスタントとの対話")

# チャット履歴の初期化
if "rag_chat_history" not in st.session_state:
    st.session_state.rag_chat_history = []

# チャット履歴の表示
if st.session_state.rag_chat_history:
    st.markdown("#### 💭 対話履歴")
    for message in st.session_state.rag_chat_history:
        if message["role"] == "user":
            with st.chat_message("user", avatar="👤"):
                st.write(message["content"])
        elif message["role"] == "assistant":
            with st.chat_message("assistant", avatar="📖"):
                st.write(message["content"])
                # 参考ドキュメントの表示
                if "sources" in message and message["sources"]:
                    with st.expander("📚 参考にした企業ドキュメント"):
                        for i, source in enumerate(message["sources"], 1):
                            st.markdown(f"**{i}.** {source}")

# 質問入力エリア
col1, col2 = st.columns([4, 1])

with col1:
    user_question = st.text_input(
        "💬 企業について質問してください:",
        key="rag_input",
        placeholder="例: 商品が破損していた場合の対応について教えて"
    )

with col2:
    st.write("")  # 高さ調整用
    clear_chat = st.button("🗑️ クリア", help="チャット履歴をクリア")

# 回答生成処理
if st.button("🚀 RAG回答生成", type="primary", use_container_width=True):
    if user_question:
        # ユーザー質問を履歴に追加
        st.session_state.rag_chat_history.append({"role": "user", "content": user_question})
        
        with st.spinner("🔍 関連情報を検索中..."):
            # 企業ドキュメントから関連情報を検索（フィルタ適用）
            search_results = search_documents_with_cortex(
                user_question, SEARCH_SERVICE_NAME, search_limit,
                filter_department, filter_doc_type
            )
            
            # 検索結果をコンテキストに変換
            context_documents = []
            source_titles = []
            
            for result in search_results:
                title = result.get('title', 'タイトルなし')
                content = result.get('content', '')
                context_documents.append(f"ドキュメント: {title}\n内容: {content}")
                source_titles.append(title)
            
            context = "\n\n".join(context_documents) if context_documents else "関連する企業ドキュメントが見つかりませんでした。"
        
        with st.spinner("🤖 回答を生成中..."):
            # RAG応答の生成
            rag_response = generate_rag_response(user_question, context, st.session_state.selected_llm_model)
            
            # AI応答を履歴に追加
            st.session_state.rag_chat_history.append({
                "role": "assistant", 
                "content": rag_response,
                "sources": source_titles
            })
        
        st.rerun()

# チャットクリア処理
if clear_chat:
    st.session_state.rag_chat_history = []
    st.rerun()

# =========================================================
# よくある質問テンプレート
# =========================================================
st.markdown("---")
st.subheader("💡 よくある質問テンプレート")
st.markdown("ワンクリックで企業に関する質問ができます")

# 質問カテゴリ（社内ドキュメントに基づく回答可能な質問）
question_categories = {
    "商品・サービス": [
        "プライベートブランド商品の特徴について教えてください",
        "スノーフレッシュ オーガニック野菜シリーズについて詳しく教えてください",
        "商品の返品・交換の条件を教えてください",
        "PB商品の品質保証について教えてください"
    ],
    "店舗・サービス": [
        "ポイントカードの有効期限について教えてください",
        "ネットスーパーの配送料金と時間帯について教えてください",
        "店舗での商品取り置きサービスについて教えてください",
        "店舗の接客方針について教えてください"
    ],
    "企業・戦略": [
        "スノーリテールの基本理念について教えてください",
        "顧客満足度向上のための取り組みについて教えてください",
        "物流・在庫管理の改善について教えてください",
        "オムニチャネル戦略について教えてください"
    ]
}

# タブで質問カテゴリを表示
tab1, tab2, tab3 = st.tabs(list(question_categories.keys()))

for tab, (category, questions) in zip([tab1, tab2, tab3], question_categories.items()):
    with tab:
        st.markdown(f"#### {category}に関する質問")
        
        # 2列レイアウトで質問ボタン配置
        cols = st.columns(2)
        
        for i, question in enumerate(questions):
            with cols[i % 2]:
                if st.button(question, key=f"template_{category}_{i}", use_container_width=True):
                    # テンプレート質問を実行
                    st.session_state.rag_chat_history.append({"role": "user", "content": question})
                    
                    with st.spinner("🔍 企業ドキュメントを検索中..."):
                        search_results = search_documents_with_cortex(
                            question, SEARCH_SERVICE_NAME, search_limit,
                            filter_department, filter_doc_type
                        )
                        
                        context_documents = []
                        source_titles = []
                        
                        for result in search_results:
                            title = result.get('title', 'タイトルなし')
                            content = result.get('content', '')
                            context_documents.append(f"ドキュメント: {title}\n内容: {content}")
                            source_titles.append(title)
                        
                        context = "\n\n".join(context_documents) if context_documents else "関連する企業ドキュメントが見つかりませんでした。"
                    
                    with st.spinner("🤖 回答を生成中..."):
                        rag_response = generate_rag_response(question, context, st.session_state.selected_llm_model)
                        
                        st.session_state.rag_chat_history.append({
                            "role": "assistant", 
                            "content": rag_response,
                            "sources": source_titles
                        })
                    
                    st.rerun()

# =========================================================
# RAG統計情報
# =========================================================
st.markdown("---")
st.subheader("📊 RAGチャットボット統計")

col1, col2, col3, col4 = st.columns(4)

# 統計計算
total_messages = len(st.session_state.rag_chat_history)
user_questions = len([msg for msg in st.session_state.rag_chat_history if msg["role"] == "user"])
ai_responses = len([msg for msg in st.session_state.rag_chat_history if msg["role"] == "assistant"])
responses_with_sources = len([msg for msg in st.session_state.rag_chat_history 
                             if msg["role"] == "assistant" and msg.get("sources")])

with col1:
    st.metric("💬 総メッセージ", f"{total_messages}件")

with col2:
    st.metric("❓ ユーザー質問", f"{user_questions}件")

with col3:
    st.metric("📖 RAG回答", f"{ai_responses}件")

with col4:
    st.metric("📚 根拠資料付き", f"{responses_with_sources}件")

# RAG統計の追加情報
if ai_responses > 0:
    # 平均的な利用状況の表示
    response_rate = (ai_responses / user_questions) * 100 if user_questions > 0 else 0
    st.info(f"📈 **利用状況**: 応答率 {response_rate:.1f}% - RAGシステムが正常に動作している指標")

# =========================================================
# Step4 完了メッセージ
# =========================================================
st.markdown("---")
st.subheader("🎯 Step4 完了！")
st.success("""
✅ **RAGチャットボットの実装が完了しました！**

**実装した機能:**
- 企業ドキュメントの意味検索（Cortex Search）
- 検索結果に基づくRAG回答生成（COMPLETE）
- 回答の根拠となる資料の明示
- 企業ドメインに特化した対話

**Step3との違い:**
- 一般的な知識 → 企業固有の知識を活用
- 推測による回答 → ドキュメントに基づく正確な回答
- 根拠不明 → 参考資料を明示
""")

st.info("💡 **次のステップ**: Step5では、Cortex Analystを使った高度なデータ分析機能を学習します。")

# フッター
st.markdown("---")
st.markdown("**Snowflake Cortex AI で実現する次世代の VoC (顧客の声) アプリケーション | Step4: RAGチャットボット**") 