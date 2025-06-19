# =========================================================
# Snowflake Cortex Handson シナリオ#2
# AIを用いた顧客の声分析アプリケーション
# Step3: シンプルチャットボットページ
# =========================================================
# 概要: AI_COMPLETEを使った基本的なチャットボットの実装
# 使用する機能: AI_COMPLETE関数によるAI対話
# ペルソナ設定、よくある質問、チャット統計を含む
# =========================================================

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# ページ設定
st.set_page_config(layout="wide")

# Snowflakeセッション取得
@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# 定数設定
# =========================================================
# LLMモデル選択肢
LLM_MODELS = [
    "llama4-maverick",
    "claude-4-sonnet", 
    "mistral-large2"
]

# session_stateで選択されたLLMモデルを初期化
if 'selected_llm_model' not in st.session_state:
    st.session_state.selected_llm_model = LLM_MODELS[0]

# =========================================================
# ユーティリティ関数
# =========================================================
def get_ai_response(model: str, prompt: str):
    """AI応答を取得"""
    try:
        # プロンプトのエスケープ処理
        escaped_prompt = prompt.replace("'", "''")
        
        # 通常のテキスト応答
        query = f"""
        SELECT 『★★★修正対象★★★』(
            '{model}',
            '{escaped_prompt}'
        ) as response
        """
        result = session.sql(query).collect()
        return result[0]['RESPONSE'] if result else "応答を取得できませんでした。"
    except Exception as e:
        return f"エラーが発生しました: {str(e)}"

# =========================================================
# メインページタイトル
# =========================================================
st.title("🤖 Step3: シンプルチャットボット")
st.header("AI_COMPLETEを使った基本的なチャットボットの実装")
st.markdown("""
このページでは、AI_COMPLETE関数を使ったシンプルで実用的なチャットボットを体験できます。
ペルソナ設定、よくある質問、チャット統計など、基本的な機能に集中した分かりやすい実装です。
""")

# =========================================================
# サイドバー設定
# =========================================================
st.sidebar.header("⚙️ 設定")

# LLMモデルの選択
selected_llm_model = st.sidebar.selectbox(
    "LLMモデルを選択:",
    LLM_MODELS,
    index=LLM_MODELS.index(st.session_state.selected_llm_model),
    key="chatbot_llm_model_selectbox",
    help="チャットボットに使用するLLMモデルを選択してください"
)

# 選択が変更された場合、session_stateを更新
if selected_llm_model != st.session_state.selected_llm_model:
    st.session_state.selected_llm_model = selected_llm_model

# チャットボットのペルソナ設定
persona_options = {
    "親切なアシスタント": "あなたは親切で丁寧なAIアシスタントです。ユーザーの質問に分かりやすく回答してください。",
    "技術サポート": "あなたは技術サポートの専門家です。技術的な問題を分かりやすく説明し、解決策を提示してください。",
    "創作アシスタント": "あなたは創造的で発想豊かなアシスタントです。アイデア出しや創作活動をサポートしてください。",
    "学習サポート": "あなたは優秀な教師です。複雑な概念を分かりやすく説明し、学習をサポートしてください。"
}

selected_persona = st.sidebar.selectbox(
    "チャットボットのペルソナ:",
    list(persona_options.keys()),
    help="チャットボットの対応スタイルを選択してください"
)

st.sidebar.info(f"""
**選択中の設定:**
- LLMモデル: `{st.session_state.selected_llm_model}`
- ペルソナ: {selected_persona}

設定に応じてチャットボットの応答が変わります。
""")

st.markdown("---")

# =========================================================
# セクション1: 基本チャットボット
# =========================================================
@st.fragment
def section_1_basic_chat():
    st.subheader("💬 セクション1: 基本チャットボット")
    
    # チャット履歴の初期化
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # チャット履歴の表示
    if st.session_state.messages:
        st.markdown("#### 💭 チャット履歴")
        for message in st.session_state.messages:
            if message["role"] == "user":
                with st.chat_message("user", avatar="👤"):
                    st.write(message["content"])
            else:
                with st.chat_message("assistant", avatar="🤖"):
                    st.write(message["content"])
    
    # ユーザー入力エリア
    col1, col2 = st.columns([4, 1])
    
    with col1:
        user_input = st.text_input(
            "💬 メッセージを入力してください:", 
            key="user_input", 
            placeholder="何でもお聞きください..."
        )
    
    with col2:
        st.write("")  # 空白for alignment
        clear_chat = st.button("🗑️ クリア", help="チャット履歴をクリアします")
    
    # 送信処理
    if st.button("📤 送信", type="primary", use_container_width=True):
        if user_input:
            # ユーザーメッセージを履歴に追加
            st.session_state.messages.append({"role": "user", "content": user_input})
            
            # システムプロンプトの設定
            system_prompt = persona_options[selected_persona]
            
            # 会話履歴を含むプロンプトの作成
            conversation_history = ""
            for msg in st.session_state.messages[-10:]:  # 最新10件のみ使用
                if msg["role"] == "user":
                    conversation_history += f"ユーザー: {msg['content']}\n"
                else:
                    conversation_history += f"アシスタント: {msg['content']}\n"
            
            full_prompt = f"""{system_prompt}

以下は今までの会話履歴です：
{conversation_history}

上記の会話を踏まえて、最新のユーザーの質問に適切に回答してください。回答は簡潔で分かりやすくしてください。"""
            
            # AI応答を取得
            with st.spinner("🤔 考え中..."):
                ai_response = get_ai_response(st.session_state.selected_llm_model, full_prompt)
            
            # AI応答を履歴に追加
            st.session_state.messages.append({"role": "assistant", "content": ai_response})
            
            st.rerun()
    
    # チャットクリア処理
    if clear_chat:
        st.session_state.messages = []
        st.rerun()

section_1_basic_chat()

# =========================================================
# セクション2: よくある質問
# =========================================================
st.markdown("---")

@st.fragment
def section_2_faq():
    st.subheader("💡 セクション2: よくある質問")
    st.markdown("ワンクリックで質問を送信できます。")
    
    # よくある質問のカテゴリ別定義
    faq_categories = {
        "一般的な質問": [
            "こんにちは！何ができますか？",
            "効率的な時間管理のコツを教えて",
            "おすすめの読書リストを作成して",
            "健康的な生活習慣について教えて"
        ],
        "技術・データ関連": [
            "SQLの基本的な使い方を教えて",
            "AIと機械学習の違いを説明して",
            "データ分析の手順を教えて",
            "クラウドサービスの利点は？"
        ],
        "創作・アイデア": [
            "ブログ記事のタイトル案を5つ考えて",
            "創造的な問題解決のコツは？",
            "新しいプロジェクトのアイデアをください",
            "効果的なプレゼンの構成を教えて"
        ]
    }
    
    # タブでカテゴリ分け
    tab1, tab2, tab3 = st.tabs(["一般的な質問", "技術・データ関連", "創作・アイデア"])
    
    for i, (tab, category) in enumerate(zip([tab1, tab2, tab3], faq_categories.keys())):
        with tab:
            st.markdown(f"#### {category}")
            
            # 2列レイアウトで質問ボタンを配置
            cols = st.columns(2)
            
            for j, question in enumerate(faq_categories[category]):
                with cols[j % 2]:
                    if st.button(question, key=f"faq_{i}_{j}", use_container_width=True):
                        # 質問をチャットに追加
                        if "messages" not in st.session_state:
                            st.session_state.messages = []
                        
                        st.session_state.messages.append({"role": "user", "content": question})
                        
                        # システムプロンプトと組み合わせ
                        system_prompt = persona_options[selected_persona]
                        full_prompt = f"""{system_prompt}

ユーザーからの質問: {question}

上記の質問に対して、分かりやすく実用的な回答をしてください。"""
                        
                        # AI応答を取得
                        with st.spinner("🤔 考え中..."):
                            ai_response = get_ai_response(st.session_state.selected_llm_model, full_prompt)
                        
                        # AI応答を履歴に追加
                        st.session_state.messages.append({"role": "assistant", "content": ai_response})
                        
                        st.rerun()

section_2_faq()

# =========================================================
# セクション3: チャット統計
# =========================================================
st.markdown("---")

@st.fragment
def section_3_statistics():
    st.subheader("📊 セクション3: チャット統計")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # 基本チャットの統計
    total_messages = len(st.session_state.get('messages', []))
    user_messages = len([msg for msg in st.session_state.get('messages', []) if msg["role"] == "user"])
    ai_messages = len([msg for msg in st.session_state.get('messages', []) if msg["role"] == "assistant"])
    
    with col1:
        st.metric("💬 総チャット数", f"{total_messages}件")
    
    with col2:
        st.metric("👤 ユーザー発言", f"{user_messages}件")
    
    with col3:
        st.metric("🤖 AI応答", f"{ai_messages}件")
    
    with col4:
        current_model = st.session_state.selected_llm_model
        st.metric("🧠 使用モデル", current_model)
    
    # 簡単な分析表示
    if total_messages > 0:
        st.markdown("#### 📈 チャット分析")
        col1, col2 = st.columns(2)
        
        with col1:
            # 日本語文字数の平均を計算（より適切な指標）
            avg_chars_per_message = sum(len(msg["content"]) for msg in st.session_state.get('messages', []) if msg["role"] == "user") / max(user_messages, 1)
            st.info(f"平均文字数/質問: {avg_chars_per_message:.1f}文字")
        
        with col2:
            if user_messages > 0:
                response_rate = (ai_messages / user_messages) * 100
                st.info(f"AI応答率: {response_rate:.1f}%")

section_3_statistics()

st.markdown("---")
st.subheader("🎯 Step3 完了！")
st.success("""
✅ **AI_COMPLETEを使ったシンプルチャットボットの実装が完了しました！**

**実装した機能:**
- 基本的なAIチャットボット（AI_COMPLETE使用）
- モデル選択とペルソナ設定（4種類）
- よくある質問のショートカット機能（3カテゴリ）
- 会話履歴の管理と表示
- チャット統計の可視化と分析

**使用したAI機能:**
- `AI_COMPLETE`: テキスト生成・対話
- プロンプトエンジニアリング
- 会話コンテキストの管理
- ペルソナベースの応答制御

**学習のポイント:**
- AI_COMPLETE関数の基本的な使い方
- ペルソナ設定による応答スタイルの変更
- 会話履歴を活用した文脈理解
- Streamlitでの対話型UIの実装
""")

st.info("💡 **次のステップ**: Step4では、社内データを活用したRAGチャットボットの実装を学習します。")

st.markdown("---")
st.markdown(f"**Snowflake Cortex Handson シナリオ#2 | Step3: シンプルチャットボット**") 