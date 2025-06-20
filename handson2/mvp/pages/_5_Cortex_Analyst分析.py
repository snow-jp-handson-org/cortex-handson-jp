# =========================================================
# Snowflake Cortex Handson シナリオ#2
# AIを用いた顧客の声分析アプリケーション
# Step5: Cortex Analyst分析
# =========================================================
# 概要: Cortex Analystを使った自然言語データ分析
# 特徴: セマンティックビューとYMLファイルの両方に対応した高精度SQL自動生成
# 使用する機能: Cortex Analyst API、セマンティックビュー、セマンティックモデルファイル
# =========================================================
# Created by Tsubasa Kanno @Snowflake
# 最終更新: 2025/06/16
# =========================================================

import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
from datetime import datetime

# ページ設定
st.set_page_config(layout="wide")

# =========================================================
# Snowflakeセッション接続
# =========================================================
@st.cache_resource
def get_snowflake_session():
    """Snowflakeセッションを取得（キャッシュ付き）"""
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# 設定値（定数）
# =========================================================
# 利用可能なLLMモデル
LLM_MODELS = [
    "llama4-maverick",
    "claude-4-sonnet", 
    "mistral-large2"
]

# Cortex Analyst APIの設定
ANALYST_API_ENDPOINT = "/api/v2/cortex/analyst/message"
ANALYST_API_TIMEOUT = 50  # 秒

# セマンティックモデルの設定
SEMANTIC_MODEL_STAGE = "SEMANTIC_MODEL_STAGE"

# セッション状態の初期化
if 'selected_llm_model' not in st.session_state:
    st.session_state.selected_llm_model = LLM_MODELS[0]

# =========================================================
# ユーティリティ関数
# =========================================================
def check_table_exists(table_name: str) -> bool:
    """テーブルが存在するかチェック"""
    try:
        session.sql(f"SELECT 1 FROM {table_name} LIMIT 1").collect()
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

def get_all_semantic_models() -> list:
    """利用可能なセマンティックモデルを取得"""
    models = []
    
    # セマンティックビューの取得
    try:
        semantic_views = session.sql("SHOW SEMANTIC VIEWS").collect()
        for view in semantic_views:
            view_name = view['name']
            models.append({
                "display_name": f"[ビュー] {view_name}",
                "actual_name": view_name,
                "type": "semantic_view"
            })
    except Exception:
        pass
    
    # YMLファイルの取得
    try:
        yml_files = session.sql(f"LIST @{SEMANTIC_MODEL_STAGE}").collect()
        for file_info in yml_files:
            file_name = file_info['name']
            if file_name.lower().endswith('.yml') or file_name.lower().endswith('.yaml'):
                file_name_only = file_name.split('/')[-1]
                actual_path = f"@{SEMANTIC_MODEL_STAGE}/{file_name_only}"
                models.append({
                    "display_name": f"[YML] {file_name_only}",
                    "actual_name": actual_path,
                    "type": "semantic_model_file"
                })
    except Exception:
        pass
    
    return models

def get_model_info_from_display_name(display_name: str, models_list: list) -> dict:
    """表示名から実際のモデル情報を取得"""
    for model in models_list:
        if model["display_name"] == display_name:
            return {
                "actual_name": model["actual_name"],
                "type": model["type"]
            }
    return None

def execute_cortex_analyst_query(question: str, model_info: dict) -> dict:
    """Cortex Analyst APIを使用して自然言語質問を分析"""
    try:
        messages = [
            {
                "role": "user",
                "content": [{"type": "text", "text": question}]
            }
        ]
        
        request_body = {"messages": messages}
        
        # セマンティックモデルのタイプに応じてパラメータを設定
        if model_info["type"] == "semantic_view":
            request_body["semantic_view"] = model_info["actual_name"]
        else:
            request_body["semantic_model_file"] = model_info["actual_name"]
        
        # Cortex Analyst API呼び出し
        try:
            import _snowflake
            resp = _snowflake.send_snow_api_request(
                "POST",
                ANALYST_API_ENDPOINT,
                {},
                {},
                request_body,
                None,
                ANALYST_API_TIMEOUT * 1000,
            )
            
            if resp["status"] < 400:
                response_data = json.loads(resp["content"])
                if "message" in response_data and "content" in response_data["message"]:
                    content_list = response_data["message"]["content"]
                    
                    response_text = ""
                    sql_query = ""
                    result_data = None
                    
                    for item in content_list:
                        if item["type"] == "text":
                            response_text += item["text"] + "\n\n"
                        elif item["type"] == "sql":
                            sql_query = item["statement"]
                    
                    # 英語レスポンスを日本語に翻訳
                    if response_text:
                        try:
                            translated_response = session.sql("""
                                SELECT SNOWFLAKE.CORTEX.TRANSLATE(?, 'en', 'ja') as translated
                            """, params=[response_text.strip()]).collect()[0]['TRANSLATED']
                            response_text = translated_response
                        except Exception:
                            pass
                    
                    # SQLを実行してデータフレームを取得
                    try:
                        if sql_query and sql_query.strip():
                            result_data = session.sql(sql_query).to_pandas()
                        else:
                            result_data = pd.DataFrame()
                    except Exception as sql_error:
                        return {
                            "success": False,
                            "sql": sql_query,
                            "data": None,
                            "response_text": response_text,
                            "message": f"SQL実行エラー: {str(sql_error)}"
                        }
                    
                    return {
                        "success": True,
                        "sql": sql_query,
                        "data": result_data,
                        "response_text": response_text.strip(),
                        "message": "分析が正常に完了しました"
                    }
                else:
                    raise Exception("APIレスポンスの形式が不正です")
            else:
                error_content = json.loads(resp["content"])
                error_msg = f"APIエラー (ステータス: {resp['status']}): {error_content.get('message', '不明なエラー')}"
                raise Exception(error_msg)
        
        except ImportError:
            return {
                "success": False,
                "sql": "",
                "data": None,
                "response_text": "",
                "message": "Cortex Analyst APIにアクセスできません。Streamlit in Snowflake環境で実行してください。"
            }
        
    except Exception as e:
        return {
            "success": False,
            "sql": "",
            "data": None,
            "response_text": "",
            "message": f"Cortex Analystエラー: {str(e)}"
        }

# =========================================================
# シンプルなカスタマイズグラフ機能
# =========================================================
@st.fragment
def create_customizable_graph(df: pd.DataFrame, unique_key: str):
    """ユーザーがカスタマイズ可能なグラフを表示する（シンプル版）"""
    if df.empty:
        st.warning("表示するデータがありません")
        return
    
    st.subheader("📊 カスタマイズグラフ")
    
    # データ情報の表示
    numeric_cols = []
    text_cols = []
    
    for col in df.columns:
        # 数値として扱えるかテスト
        try:
            pd.to_numeric(df[col].dropna().iloc[:3], errors='raise')
            numeric_cols.append(col)
        except (ValueError, TypeError):
            text_cols.append(col)
    
    st.info(f"📈 データ: {len(df)}行 x {len(df.columns)}列 | 数値列: {len(numeric_cols)}個 | テキスト列: {len(text_cols)}個")
    
    # グラフタイプの選択
    graph_type = st.selectbox(
        "グラフタイプを選択",
        ["棒グラフ", "折れ線グラフ", "散布図", "円グラフ", "ヒストグラム"],
        key=f"{unique_key}_graph_type"
    )
    
    # グラフタイプに応じた設定
    if graph_type in ["棒グラフ", "折れ線グラフ", "散布図"]:
        col1, col2 = st.columns(2)
        
        with col1:
            x_axis = st.selectbox("X軸を選択", df.columns, key=f"{unique_key}_x_axis")
        
        with col2:
            y_axis = st.selectbox("Y軸を選択", df.columns, key=f"{unique_key}_y_axis")
        
        # 色分けオプション
        color_options = ["なし"] + [col for col in df.columns if col not in [x_axis, y_axis]]
        color_option = st.selectbox("色分け項目（オプション）", color_options, key=f"{unique_key}_color")
        color_col = None if color_option == "なし" else color_option
        
        # データの数値変換（売上データなど）
        display_df = df.copy()
        if y_axis in numeric_cols or 'sales' in y_axis.lower() or '売上' in y_axis.lower():
            try:
                # カンマ除去と数値変換
                if display_df[y_axis].dtype == 'object':
                    display_df[y_axis] = display_df[y_axis].astype(str).str.replace(',', '').str.replace('¥', '')
                    display_df[y_axis] = pd.to_numeric(display_df[y_axis], errors='coerce')
            except:
                pass
        
        # グラフ生成
        try:
            if graph_type == "棒グラフ":
                fig = px.bar(display_df, x=x_axis, y=y_axis, color=color_col, 
                           title=f"{x_axis}ごとの{y_axis}")
            elif graph_type == "折れ線グラフ":
                fig = px.line(display_df, x=x_axis, y=y_axis, color=color_col, 
                            title=f"{y_axis}の推移", markers=True)
            else:  # 散布図
                fig = px.scatter(display_df, x=x_axis, y=y_axis, color=color_col, 
                               title=f"{x_axis} vs {y_axis}")
            
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True, key=f"{unique_key}_main_chart")
            
        except Exception as e:
            st.error(f"グラフ作成エラー: {str(e)}")
            
    elif graph_type == "円グラフ":
        col1, col2 = st.columns(2)
        
        with col1:
            name_col = st.selectbox("カテゴリ列を選択", text_cols if text_cols else df.columns,
                                  key=f"{unique_key}_name_col")
        
        with col2:
            value_col = st.selectbox("値の列を選択", numeric_cols if numeric_cols else df.columns,
                                   key=f"{unique_key}_value_col")
        
        try:
            # 数値変換
            display_df = df.copy()
            if display_df[value_col].dtype == 'object':
                display_df[value_col] = display_df[value_col].astype(str).str.replace(',', '').str.replace('¥', '')
                display_df[value_col] = pd.to_numeric(display_df[value_col], errors='coerce')
            
            # 円グラフ生成
            pie_df = display_df.groupby(name_col)[value_col].sum().reset_index()
            fig = px.pie(pie_df, names=name_col, values=value_col, 
                        title=f"{name_col}ごとの{value_col}の割合")
            st.plotly_chart(fig, use_container_width=True, key=f"{unique_key}_pie_chart")
            
        except Exception as e:
            st.error(f"円グラフ作成エラー: {str(e)}")
            
    elif graph_type == "ヒストグラム":
        hist_col = st.selectbox("分布を表示する列を選択", numeric_cols if numeric_cols else df.columns,
                              key=f"{unique_key}_hist_col")
        
        try:
            # 数値変換
            display_df = df.copy()
            if display_df[hist_col].dtype == 'object':
                display_df[hist_col] = display_df[hist_col].astype(str).str.replace(',', '').str.replace('¥', '')
                display_df[hist_col] = pd.to_numeric(display_df[hist_col], errors='coerce')
            
            fig = px.histogram(display_df, x=hist_col, title=f"{hist_col}の分布")
            st.plotly_chart(fig, use_container_width=True, key=f"{unique_key}_hist_chart")
            
        except Exception as e:
            st.error(f"ヒストグラム作成エラー: {str(e)}")

# =========================================================
# メインページ
# =========================================================
st.title("📈 Step5: Cortex Analyst分析")
st.header("企業データを自然言語で分析する高度なAIアシスタント")

st.markdown("""
このページでは、Snowflake Cortex Analystを使用した高度なデータ分析機能を体験できます。
Step3・Step4との違いは、**セマンティックビューまたはYMLファイル**を活用することで、より正確で信頼性の高いSQL生成が可能な点です。

🚀 **自動検出機能**: 利用可能なセマンティックビューとYMLファイルを自動検出し、統一されたインターフェースで選択できます。
📊 **カスタムグラフ**: データに応じてユーザーが自由にグラフを設定でき、売上データを正確に可視化できます。
""")

# =========================================================
# サイドバー設定
# =========================================================
st.sidebar.header("⚙️ Analyst設定")

# LLMモデルの選択
selected_llm_model = st.sidebar.selectbox(
    "LLMモデル:",
    LLM_MODELS,
    index=LLM_MODELS.index(st.session_state.selected_llm_model),
    help="Cortex Analystで使用するLLMモデルを選択"
)

if selected_llm_model != st.session_state.selected_llm_model:
    st.session_state.selected_llm_model = selected_llm_model

# 可視化設定
enable_charts = st.sidebar.checkbox(
    "カスタマイズグラフ表示",
    value=True,
    help="分析結果に対してカスタマイズ可能なグラフ設定を表示"
)

# セマンティックモデルの選択
st.sidebar.markdown("---")
st.sidebar.subheader("📊 セマンティックモデル設定")

all_semantic_models = get_all_semantic_models()

if all_semantic_models:
    selected_semantic_model = st.sidebar.selectbox(
        "使用するセマンティックモデル:",
        [model["display_name"] for model in all_semantic_models],
        index=0,
        help="分析に使用するセマンティックモデルを選択"
    )
    
    st.sidebar.success("✅ セマンティックモデル選択済み")
    
    model_info = get_model_info_from_display_name(selected_semantic_model, all_semantic_models)
    if model_info:
        if model_info["type"] == "semantic_view":
            st.sidebar.code(model_info["actual_name"], language="sql")
            st.sidebar.caption("🏗️ セマンティックビュー形式")
        else:
            st.sidebar.code(model_info["actual_name"], language="yaml")
            st.sidebar.caption("📄 YMLファイル形式")
else:
    st.sidebar.error("❌ セマンティックモデルが見つかりません")
    selected_semantic_model = None
    model_info = None

st.sidebar.info("""
**Cortex Analystの仕組み:**
1. 🧠 自然言語の質問を理解
2. 📋 セマンティックモデルを参照
3. 🔧 最適なSQLクエリを生成
4. 📊 データベースで実行
5. 📈 カスタマイズ可能なグラフで可視化
""")

st.markdown("---")

# =========================================================
# データ状況確認
# =========================================================
st.subheader("📊 システム状況確認")

col1, col2, col3 = st.columns(3)

required_tables = {
    "RETAIL_DATA_WITH_PRODUCT_MASTER": "店舗売上データ",
    "EC_DATA_WITH_PRODUCT_MASTER": "EC売上データ"
}

with col1:
    st.markdown("#### 📄 データソース")
    total_records = 0
    for table_name, description in required_tables.items():
        exists = check_table_exists(table_name)
        count = get_table_count(table_name) if exists else 0
        total_records += count
        status_icon = "✅" if exists else "❌"
        st.write(f"{status_icon} {description}: **{count:,}件**")
    
    if total_records > 0:
        st.success(f"合計 {total_records:,} 件のデータが利用可能")
    else:
        st.error("データが見つかりません")

with col2:
    st.markdown("#### 🧠 Cortex Analyst")
    if selected_semantic_model and model_info:
        st.success("✅ セマンティックモデル: 利用可能")
        st.success("✅ Cortex Analyst API: 利用可能")
        
        if model_info["type"] == "semantic_view":
            st.info("🏗️ セマンティックビュー形式を使用中")
        else:
            st.info("📄 YMLファイル形式を使用中")
    else:
        st.error("❌ セマンティックモデル: 未設定")
        st.warning("❌ Cortex Analyst API: 利用不可")

with col3:
    st.markdown("#### ⚙️ 分析設定")
    st.write(f"🤖 **LLMモデル**: {st.session_state.selected_llm_model}")
    st.write(f"📊 **カスタムグラフ**: {'有効' if enable_charts else '無効'}")
    if all_semantic_models:
        st.write(f"📋 **選択モデル**: {selected_semantic_model}")

# 前提条件のチェック
if not selected_semantic_model or not model_info:
    st.error(f"""
    ⚠️ **セマンティックモデルが設定されていません**
    
    Cortex Analystを使用するには、セマンティックビューまたはYMLファイルが必要です。
    """)
    st.stop()

st.markdown("---")

# =========================================================
# 自然言語分析チャット
# =========================================================
st.subheader("🔍 自然言語データ分析")

# チャット履歴の初期化
if "analyst_chat_history" not in st.session_state:
    st.session_state.analyst_chat_history = []

# チャット履歴の表示
if st.session_state.analyst_chat_history:
    st.markdown("#### 💭 分析履歴")
    for i, message in enumerate(st.session_state.analyst_chat_history):
        if message["role"] == "user":
            with st.chat_message("user", avatar="👤"):
                st.write(message["content"])
        elif message["role"] == "analyst":
            with st.chat_message("assistant", avatar="📊"):
                st.write(message["content"])
                
                # 分析結果の表示
                if "result" in message and message["result"]["success"]:
                    if message["result"]["data"] is not None and not message["result"]["data"].empty:
                        st.dataframe(message["result"]["data"], use_container_width=True)
                        
                        # グラフ設定の表示
                        if enable_charts:
                            st.info("💡 データが取得できました。下記でグラフをカスタマイズできます。")
                            create_customizable_graph(message["result"]["data"], f"msg_{i}")
                    
                    # 生成されたSQLの表示
                    if message["result"]["sql"]:
                        with st.expander("📝 生成されたSQL"):
                            st.code(message["result"]["sql"], language="sql")

# 質問入力エリア
col1, col2 = st.columns([4, 1])

with col1:
    user_question = st.text_input(
        "💬 データについて質問してください:",
        key="analyst_input",
        placeholder="例: 売上TOP10の商品とその売上金額を教えて"
    )

with col2:
    st.write("")
    clear_chat = st.button("🗑️ クリア", help="チャット履歴をクリア")

# 分析実行処理
if st.button("🚀 Cortex Analyst分析", type="primary", use_container_width=True):
    if user_question:
        # ユーザー質問を履歴に追加
        st.session_state.analyst_chat_history.append({"role": "user", "content": user_question})
        
        with st.spinner("🧠 Cortex Analystが分析中..."):
            # Cortex Analyst分析を実行
            current_model_info = get_model_info_from_display_name(selected_semantic_model, all_semantic_models)
            result = execute_cortex_analyst_query(user_question, current_model_info)
            
            if result["success"]:
                response_text = result.get("response_text", "分析が完了しました。")
                
                # アシスタントの応答を履歴に追加
                st.session_state.analyst_chat_history.append({
                    "role": "analyst", 
                    "content": response_text,
                    "result": result
                })
            else:
                error_message = f"申し訳ありません。分析中にエラーが発生しました。\n\n**エラー内容**: {result['message']}"
                st.session_state.analyst_chat_history.append({
                    "role": "analyst", 
                    "content": error_message,
                    "result": result
                })
        
        st.rerun()

# チャットクリア処理
if clear_chat:
    st.session_state.analyst_chat_history = []
    st.rerun()

# =========================================================
# よくある分析テンプレート
# =========================================================
st.markdown("---")
st.subheader("💡 よくある分析テンプレート")

analysis_templates = [
    "売上TOP10の商品とその売上金額を教えて",
    "月別の売上推移を時系列で見せて",
    "店舗とECの売上を比較して",
    "商品別の売上ランキングを作って"
]

col1, col2 = st.columns(2)

for i, question in enumerate(analysis_templates):
    with col1 if i % 2 == 0 else col2:
        if st.button(question, key=f"template_{i}", use_container_width=True):
            # テンプレート質問を実行
            st.session_state.analyst_chat_history.append({"role": "user", "content": question})
            
            with st.spinner("🧠 Cortex Analystが分析中..."):
                template_model_info = get_model_info_from_display_name(selected_semantic_model, all_semantic_models)
                result = execute_cortex_analyst_query(question, template_model_info)
                
                if result["success"]:
                    response_text = result.get("response_text", "分析が完了しました。")
                    st.session_state.analyst_chat_history.append({
                        "role": "analyst", 
                        "content": response_text,
                        "result": result
                    })
                else:
                    error_message = f"分析中にエラーが発生しました: {result['message']}"
                    st.session_state.analyst_chat_history.append({
                        "role": "analyst", 
                        "content": error_message,
                        "result": result
                    })
            
            st.rerun()

# =========================================================
# Step5 完了メッセージ
# =========================================================
st.markdown("---")
st.subheader("🎯 Step5 完了！")
st.success("""
✅ **Cortex Analyst分析の実装が完了しました！**

**実装した機能:**
- セマンティックビューとYMLファイルの自動検出・統合選択
- セマンティックモデルを活用した高精度SQL生成
- 自然言語による企業データ分析
- シンプルなカスタマイズグラフ機能
- よくある分析テンプレート
- 分析履歴と統計情報
""")

st.info("🎉 **ワークショップ完了**: 全5ステップのSnowflake Cortex Handsonが完了しました！")

# フッター
st.markdown("---")
st.markdown("**Snowflake Cortex Handson シナリオ#2 | Step5: Cortex Analyst分析**")
