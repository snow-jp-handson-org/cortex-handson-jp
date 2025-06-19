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
# ハンズオンでは事前に作成されたセマンティックビューまたはYAMLファイルを使用
SEMANTIC_MODEL_STAGE = "SEMANTIC_MODEL_STAGE"

# セッション状態の初期化
if 'selected_llm_model' not in st.session_state:
    st.session_state.selected_llm_model = LLM_MODELS[0]

# =========================================================
# ユーティリティ関数
# =========================================================
def check_table_exists(table_name: str) -> bool:
    """
    指定されたテーブルが存在するかチェック
    
    Args:
        table_name: 確認するテーブル名
    Returns:
        bool: テーブルが存在すればTrue
    """
    try:
        session.sql(f"SELECT 1 FROM {table_name} LIMIT 1").collect()
        return True
    except:
        return False

def get_table_count(table_name: str) -> int:
    """
    テーブルのレコード数を取得
    
    Args:
        table_name: カウントするテーブル名
    Returns:
        int: レコード数
    """
    try:
        result = session.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()
        return result[0]['COUNT']
    except:
        return 0

def get_all_semantic_models() -> list:
    """
    利用可能なすべてのセマンティックモデル（ビューとYMLファイル）を取得
    
    Returns:
        list: セマンティックモデルの統合リスト
              各要素は {"display_name": "表示名", "actual_name": "実際の名前", "type": "タイプ"}
    """
    models = []
    
    # セマンティックビューの取得
    try:
        semantic_views = session.sql("""
            SHOW SEMANTIC VIEWS
        """).collect()
        
        for view in semantic_views:
            view_name = view['name']
            models.append({
                "display_name": f"[ビュー] {view_name}",
                "actual_name": view_name,
                "type": "semantic_view"
            })
    except Exception:
        # セマンティックビューの取得に失敗した場合は警告なしで継続
        pass
    
    # YMLファイルの取得
    try:
        # ステージ内のYMLファイルを検索
        yml_files = session.sql(f"""
            LIST @{SEMANTIC_MODEL_STAGE}
        """).collect()
        
        for file_info in yml_files:
            file_name = file_info['name']
            if file_name.lower().endswith('.yml') or file_name.lower().endswith('.yaml'):
                # ステージパスからファイル名のみを抽出
                file_name_only = file_name.split('/')[-1]
                actual_path = f"@{SEMANTIC_MODEL_STAGE}/{file_name_only}"
                models.append({
                    "display_name": f"[YML] {file_name_only}",
                    "actual_name": actual_path,
                    "type": "semantic_model_file"
                })
    except Exception:
        # ステージアクセスに失敗した場合、カレントディレクトリでYMLファイルを検索
        try:
            yml_files = session.sql("""
                SHOW FILES LIKE '%.yml'
            """).collect()
            
            for file_info in yml_files:
                file_name = file_info['name']
                models.append({
                    "display_name": f"[YML] {file_name}",
                    "actual_name": file_name,
                    "type": "semantic_model_file"
                })
        except Exception:
            # YMLファイルの検索にも失敗した場合は警告なしで継続
            pass
    
    return models

def get_model_info_from_display_name(display_name: str, models_list: list) -> dict:
    """
    表示名から実際のモデル情報を取得
    
    Args:
        display_name: 選択された表示名
        models_list: モデル一覧
    Returns:
        dict: モデル情報（actual_name, type）
    """
    for model in models_list:
        if model["display_name"] == display_name:
            return {
                "actual_name": model["actual_name"],
                "type": model["type"]
            }
    return None

def execute_cortex_analyst_query(question: str, model_info: dict) -> dict:
    """
    Cortex Analyst APIを使用して自然言語質問を分析
    
    Args:
        question: 自然言語での質問
        model_info: モデル情報（actual_name, type）
    Returns:
        dict: 分析結果（成功/失敗、データ、SQL、メッセージ）
    """
    try:
        # メッセージの準備（Cortex Analyst API形式）
        messages = [
            {
                "role": "user",
                "content": [{"type": "text", "text": question}]
            }
        ]
        
        # リクエストボディの準備（モデルタイプに応じて使い分け）
        request_body = {
            "messages": messages,
        }
        
        # セマンティックモデルのタイプに応じてパラメータを設定
        if model_info["type"] == "semantic_view":
            request_body["semantic_view"] = model_info["actual_name"]
        else:  # semantic_model_file
            request_body["semantic_model_file"] = model_info["actual_name"]
        
        # Cortex Analyst API呼び出し
        try:
            import _snowflake
            # Snowflake内部APIを使用してCortex Analystを呼び出し
            resp = _snowflake.send_snow_api_request(
                "POST",
                ANALYST_API_ENDPOINT,
                {},  # headers
                {},  # params
                request_body,
                None,  # request_guid
                ANALYST_API_TIMEOUT * 1000,  # ミリ秒に変換
            )
            
            # レスポンスの処理
            if resp["status"] < 400:
                response_data = json.loads(resp["content"])
                if "message" in response_data and "content" in response_data["message"]:
                    content_list = response_data["message"]["content"]
                    
                    # テキストとSQLを抽出
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
                            # 翻訳に失敗した場合は元のテキストを使用
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
            # Snowflake内部APIが使用できない場合のフォールバック
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

def create_smart_visualization(df: pd.DataFrame, title: str):
    """
    データフレームから適切な可視化を自動作成
    
    Args:
        df: 可視化するデータフレーム
        title: グラフのタイトル
    Returns:
        plotly.graph_objects.Figure or None: 作成されたグラフ
    """
    try:
        if df.empty or len(df.columns) < 2:
            return None
            
        # 数値列を検出
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if len(numeric_cols) >= 1:
            x_col = df.columns[0]
            y_col = numeric_cols[0]
            
            # データが多すぎる場合は上位15件のみ表示
            display_df = df.head(15)
            
            # データタイプに基づいて適切なグラフを選択
            if display_df[x_col].dtype == 'object':  # カテゴリカルデータ
                fig = px.bar(
                    display_df, 
                    x=x_col, 
                    y=y_col,
                    title=title,
                    color=y_col,
                    color_continuous_scale="viridis"
                )
                fig.update_layout(showlegend=False)
            else:  # 数値データ
                fig = px.line(
                    display_df, 
                    x=x_col, 
                    y=y_col,
                    title=title,
                    markers=True
                )
                fig.update_traces(line=dict(width=3))
            
            # レイアウトの調整
            fig.update_layout(
                height=400,
                xaxis_title=x_col,
                yaxis_title=y_col,
                font=dict(size=12)
            )
            
            return fig
    except Exception:
        pass
    
    return None

# =========================================================
# メインページ
# =========================================================
st.title("📈 Step5: Cortex Analyst分析")
st.header("企業データを自然言語で分析する高度なAIアシスタント")

st.markdown("""
このページでは、Snowflake Cortex Analystを使用した高度なデータ分析機能を体験できます。
Step3・Step4との違いは、**セマンティックビューまたはYMLファイル**を活用することで、より正確で信頼性の高いSQL生成が可能な点です。

🚀 **自動検出機能**: 利用可能なセマンティックビューとYMLファイルを自動検出し、統一されたインターフェースで選択できます。
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

# モデル選択の更新
if selected_llm_model != st.session_state.selected_llm_model:
    st.session_state.selected_llm_model = selected_llm_model

# 可視化設定
enable_auto_chart = st.sidebar.checkbox(
    "自動グラフ作成",
    value=True,
    help="分析結果を自動的にグラフ化"
)

# セマンティックモデルの選択
st.sidebar.markdown("---")
st.sidebar.subheader("📊 セマンティックモデル設定")

# 利用可能なセマンティックモデルを取得
all_semantic_models = get_all_semantic_models()

if all_semantic_models:
    selected_semantic_model = st.sidebar.selectbox(
        "使用するセマンティックモデル:",
        [model["display_name"] for model in all_semantic_models],
        index=0,
        help="分析に使用するセマンティックモデルを選択（セマンティックビューとYMLファイルを自動検出）"
    )
    
    st.sidebar.success("✅ セマンティックモデル選択済み")
    
    # 選択されたモデルの詳細情報を表示
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

# モデル検出結果の表示
if all_semantic_models:
    semantic_views_count = len([m for m in all_semantic_models if m["type"] == "semantic_view"])
    yml_files_count = len([m for m in all_semantic_models if m["type"] == "semantic_model_file"])
    
    st.sidebar.info(f"""
    **検出されたモデル:**
    🏗️ セマンティックビュー: {semantic_views_count}個
    📄 YMLファイル: {yml_files_count}個
    """)

st.sidebar.info(f"""
**Cortex Analystの仕組み:**
1. 🧠 自然言語の質問を理解
2. 📋 セマンティックモデルを参照
3. 🔧 最適なSQLクエリを生成
4. 📊 データベースで実行
5. 📈 結果を分かりやすく表示
""")

st.markdown("---")

# =========================================================
# データ状況確認
# =========================================================
st.subheader("📊 システム状況確認")

col1, col2, col3 = st.columns(3)

# 利用可能データの確認
required_tables = {
    "RETAIL_DATA_WITH_PRODUCT_MASTER": "店舗売上データ",
    "EC_DATA_WITH_PRODUCT_MASTER": "EC売上データ", 
    "CUSTOMER_REVIEWS": "顧客レビューデータ"
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
        
        # 選択されたモデルタイプを表示
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
    st.write(f"📈 **自動グラフ**: {'有効' if enable_auto_chart else '無効'}")
    if all_semantic_models:
        st.write(f"📋 **選択モデル**: {selected_semantic_model}")
        # モデル統計情報
        semantic_views_count = len([m for m in all_semantic_models if m["type"] == "semantic_view"])
        yml_files_count = len([m for m in all_semantic_models if m["type"] == "semantic_model_file"])
        st.caption(f"検出: ビュー{semantic_views_count}個・YML{yml_files_count}個")

# 必要な前提条件のチェック
if not selected_semantic_model or not model_info:
    st.error(f"""
    ⚠️ **セマンティックモデルが設定されていません**
    
    Cortex Analystを使用するには、セマンティックビューまたはYMLファイルが必要です。
    
    **確認事項:**
    - セマンティックビューが作成されているか
    - YMLファイルがステージ `{SEMANTIC_MODEL_STAGE}` にアップロードされているか
    - 適切なアクセス権限があるか
    """)
    st.stop()

st.markdown("---")

# =========================================================
# Cortex Analystとは
# =========================================================
st.subheader("🧠 Cortex Analyst vs 従来の方法")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **従来のSQL生成方法（Step3）:**
    
    - 🤖 一般的なLLMでSQL生成
    - ❓ データベース構造の理解が曖昧
    - ⚠️ 不正確なクエリが生成される可能性
    - 🔧 手動でのSQL修正が必要
    """)

with col2:
    st.markdown("""
    **Cortex Analyst（Step5）:**
    
    - ✅ セマンティックモデルでデータ理解
    - ✅ ビジネスルールを考慮したSQL生成
    - ✅ 高精度で信頼性の高いクエリ
    - ✅ 自動的な結果検証と最適化
    - 📊 セマンティックビューとYMLファイルに対応
    """)

# =========================================================
# 自然言語分析チャット
# =========================================================
st.markdown("---")
st.subheader("🔍 自然言語データ分析")

# チャット履歴の初期化
if "analyst_chat_history" not in st.session_state:
    st.session_state.analyst_chat_history = []

# チャット履歴の表示
if st.session_state.analyst_chat_history:
    st.markdown("#### 💭 分析履歴")
    for message in st.session_state.analyst_chat_history:
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
                    
                    # 生成されたSQLの表示
                    if message["result"]["sql"]:
                        with st.expander("生成されたSQL"):
                            st.code(message["result"]["sql"], language="sql")
                    
                    # グラフの表示
                    if "chart" in message and message["chart"]:
                        st.plotly_chart(message["chart"], use_container_width=True, key=f"analyst_chart_{st.session_state.analyst_chat_history.index(message)}")

# 質問入力エリア
col1, col2 = st.columns([4, 1])

with col1:
    user_question = st.text_input(
        "💬 データについて質問してください:",
        key="analyst_input",
        placeholder="例: 売上TOP10の商品とその売上金額を教えて"
    )

with col2:
    st.write("")  # 高さ調整用
    clear_chat = st.button("🗑️ クリア", help="チャット履歴をクリア")

# 分析実行処理
if st.button("🚀 Cortex Analyst分析", type="primary", use_container_width=True):
    if user_question:
        # ユーザー質問を履歴に追加
        st.session_state.analyst_chat_history.append({"role": "user", "content": user_question})
        
        with st.spinner("🧠 Cortex Analystが分析中..."):
            # Cortex Analyst分析を実行（モデル情報を取得）
            current_model_info = get_model_info_from_display_name(selected_semantic_model, all_semantic_models)
            result = execute_cortex_analyst_query(user_question, current_model_info)
            
            if result["success"]:
                # 成功した場合の処理
                response_text = result.get("response_text", "分析が完了しました。")
                
                # グラフの作成
                chart = None
                if enable_auto_chart and result["data"] is not None and not result["data"].empty:
                    chart = create_smart_visualization(result["data"], user_question[:50] + "...")
                
                # アシスタントの応答を履歴に追加
                st.session_state.analyst_chat_history.append({
                    "role": "analyst", 
                    "content": response_text,
                    "result": result,
                    "chart": chart
                })
            else:
                # 失敗した場合の処理
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
st.markdown("ワンクリックで分析を実行できます")

# 分析テンプレートの定義
analysis_templates = {
    "売上分析": [
        "売上TOP10の商品とその売上金額を教えて",
        "月別の売上推移を時系列で見せて",
        "店舗とECの売上を比較して",
        "商品カテゴリ別の売上ランキングを作って"
    ],
    "顧客分析": [
        "顧客満足度の高い商品TOP5とその評価を教えて",
        "レビュー評価の低い商品とその理由を分析して",
        "評価4以上の商品の売上合計を計算して",
        "商品別の平均評価と売上の相関を見せて"
    ],
    "トレンド分析": [
        "最近3ヶ月で売上が伸びている商品を特定して",
        "季節ごとの売上変動パターンを分析して",
        "前年同月比で売上成長率を計算して",
        "売上が減少傾向にある商品を警告リストで表示して"
    ]
}

tab1, tab2, tab3 = st.tabs(list(analysis_templates.keys()))

for i, (tab, category) in enumerate(zip([tab1, tab2, tab3], analysis_templates.keys())):
    with tab:
        st.markdown(f"#### {category}に関する分析")
        
        # 2列レイアウトで質問ボタン配置
        cols = st.columns(2)
        
        for j, question in enumerate(analysis_templates[category]):
            with cols[j % 2]:
                if st.button(question, key=f"template_analyst_{category}_{j}", use_container_width=True):
                    # テンプレート質問を実行
                    st.session_state.analyst_chat_history.append({"role": "user", "content": question})
                    
                    with st.spinner("🧠 Cortex Analystが分析中..."):
                        # モデル情報を取得してテンプレート分析を実行
                        template_model_info = get_model_info_from_display_name(selected_semantic_model, all_semantic_models)
                        result = execute_cortex_analyst_query(question, template_model_info)
                        
                        if result["success"]:
                            response_text = result.get("response_text", "分析が完了しました。")
                            
                            # グラフの作成
                            chart = None
                            if enable_auto_chart and result["data"] is not None and not result["data"].empty:
                                chart = create_smart_visualization(result["data"], question[:50] + "...")
                            
                            st.session_state.analyst_chat_history.append({
                                "role": "analyst", 
                                "content": response_text,
                                "result": result,
                                "chart": chart
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
# 分析統計情報
# =========================================================
st.markdown("---")
st.subheader("📊 Cortex Analyst統計")

col1, col2, col3, col4 = st.columns(4)

# 統計計算
total_questions = len([msg for msg in st.session_state.analyst_chat_history if msg["role"] == "user"])
total_analyses = len([msg for msg in st.session_state.analyst_chat_history if msg["role"] == "analyst"])
successful_analyses = len([msg for msg in st.session_state.analyst_chat_history 
                          if msg["role"] == "analyst" and msg.get("result", {}).get("success", False)])
analyses_with_data = len([msg for msg in st.session_state.analyst_chat_history 
                         if msg["role"] == "analyst" and msg.get("result", {}).get("success", False) 
                         and msg.get("result", {}).get("data") is not None 
                         and not msg.get("result", {}).get("data", pd.DataFrame()).empty])

with col1:
    st.metric("💬 質問数", f"{total_questions}件")

with col2:
    st.metric("🧠 分析実行", f"{total_analyses}件")

with col3:
    st.metric("✅ 成功した分析", f"{successful_analyses}件")

with col4:
    st.metric("📊 データ取得成功", f"{analyses_with_data}件")

# 成功率の表示
if total_analyses > 0:
    success_rate = (successful_analyses / total_analyses) * 100
    data_success_rate = (analyses_with_data / total_analyses) * 100
    st.info(f"📈 **分析成功率**: {success_rate:.1f}% | **データ取得成功率**: {data_success_rate:.1f}%")

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
- 分析結果の自動可視化
- よくある分析テンプレート
- 分析履歴と統計情報

**特徴:**
- 手動でのモデルタイプ選択が不要
- 利用可能なモデルを自動検出
- セマンティックビューとYMLファイルの柔軟な選択
- 統一されたシンプルなUI

**Step3・Step4との違い:**
- セマンティックモデルによる正確なデータ理解
- ビジネスルールを考慮したSQL生成
- より信頼性の高い分析結果
- 複数のセマンティックモデル形式に対応
""")

st.info("🎉 **ワークショップ完了**: 全5ステップのSnowflake Cortex Handsonが完了しました！")

# フッター
st.markdown("---")
st.markdown("**Snowflake Cortex Handson シナリオ#2 | Step5: Cortex Analyst分析**") 