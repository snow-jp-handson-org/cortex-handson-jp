# =========================================================
# Snowflake Cortex AI ワークショップ
# AIチャットボットアプリケーション - MVP
# =========================================================
# 概要: 
# このアプリケーションは、Snowflake Cortex AIとStreamlitを使用して、
# チャットボットによる社内文書の検索と売上分析を行うデモアプリケーションです。
#
# 機能:
# - シンプルなチャットボット
# - Cortex Searchを用いたRAGチャットボットによる社内文書Q&A
# - Cortex Analystによる自然言語分析
# 
# Created by Takuya Shoji @Snowflake
# 最終更新: 2025/05/07
# =========================================================

# =========================================================
# 必要なライブラリのインポート
# =========================================================
# 基本ライブラリ
import streamlit as st
import pandas as pd
import json
import time
import requests

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

# =========================================================
# Snowflake接続と共通ユーティリティ関数
# =========================================================

# Snowflakeセッションの取得
snowflake_session = get_active_session()

def check_table_exists(table_name: str) -> bool:
    """指定されたテーブルが存在するかチェックします。
    
    Args:
        table_name (str): チェックするテーブル名
    
    Returns:
        bool: テーブルが存在する場合はTrue、存在しない場合はFalse
    """
    try:
        snowflake_session.sql(f"DESC {table_name}").collect()
        return True
    except:
        return False

def get_table_count(table_name: str) -> int:
    """指定されたテーブルのレコード数を取得します。
    
    Args:
        table_name (str): レコード数を取得するテーブル名
    
    Returns:
        int: テーブル内のレコード数（テーブルが存在しない場合は0）
    """
    try:
        result = snowflake_session.sql(f"""
            SELECT COUNT(*) as count FROM {table_name}
        """).collect()
        return result[0]['COUNT']
    except:
        return 0

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
# Cortex Search Service 操作
# =========================================================

def check_search_service_exists() -> bool:
    """Cortex Search Serviceが存在するかチェックします。
    
    Returns:
        bool: 検索サービスが存在する場合はTrue、存在しない場合はFalse
    """
    try:
        # 現在のデータベースとスキーマを取得
        current_db_schema = snowflake_session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
        
        # サービスの存在確認
        result = snowflake_session.sql(f"""
            SHOW CORTEX SEARCH SERVICES LIKE 'snow_retail_search_service'
        """).collect()
        
        return len(result) > 0
    
    except Exception:
        return False  # エラーが発生した場合は存在しないと判断

def create_snow_retail_search_service(warehouse, model) -> bool:
    """Cortex Search Serviceを作成します。
    
    Args:
        warehouse (str): 使用するSnowflakeウェアハウス名
        model (str): 使用する埋め込みモデル名
    
    Returns:
        bool: サービス作成に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        # 現在のデータベースとスキーマを取得
        current_db_schema = snowflake_session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
        
        # サービスの作成
        try:
            snowflake_session.sql(f"""
            CREATE OR REPLACE CORTEX SEARCH SERVICE snow_retail_search_service
            ON content
            ATTRIBUTES title, document_type, department
            WAREHOUSE = '{warehouse}'
            TARGET_LAG = '1 day'
            EMBEDDING_MODEL = '{model}'
            AS
                SELECT 
                document_id,
                title,
                content,
                document_type,
                department,
                created_at,
                updated_at,
                version
            FROM SNOW_RETAIL_DOCUMENTS
            """).collect()
        except Exception as sql_error:
            # SQL実行中にエラーが発生した場合でも、サービスが作成されている可能性を確認
            if check_search_service_exists():
                st.success("Cortex Search Serviceは正常に作成されました。")
                
                # アクセス権限の付与
                snowflake_session.sql(f"""
                    GRANT USAGE ON CORTEX SEARCH SERVICE snow_retail_search_service TO ROLE CURRENT_ROLE()
                """).collect()
                
                return True
            else:
                # 本当にエラーがあった場合
                raise sql_error
        
        # アクセス権限の付与
        snowflake_session.sql(f"""
            GRANT USAGE ON CORTEX SEARCH SERVICE snow_retail_search_service TO ROLE CURRENT_ROLE()
        """).collect()
        
        st.success("Cortex Search Serviceを作成しました。")
        return True
    
    except Exception as e:
        # サービス作成中に例外が発生してもう一度存在確認
        if check_search_service_exists():
            st.success("Cortex Search Serviceは正常に作成されました。")
            return True
        
        st.error(f"Cortex Search Serviceの作成に失敗しました: {str(e)}")
        return False

def delete_snow_retail_search_service() -> bool:
    """スノーリテールのCortex Search Serviceを削除します。
    
    Returns:
        bool: 削除に成功した場合はTrue、失敗した場合はFalse
    """
    try:
        # 現在のデータベースとスキーマを取得
        current_db_schema = snowflake_session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
        current_database = current_db_schema['CURRENT_DATABASE()']
        current_schema = current_db_schema['CURRENT_SCHEMA()']
        
        snowflake_session.sql(f"""
            DROP CORTEX SEARCH SERVICE {current_database}.{current_schema}.snow_retail_search_service
        """).collect()
        st.success("Cortex Search Serviceを削除しました。")
        return True
    except Exception as e:
        st.error(f"Cortex Search Serviceの削除に失敗しました: {str(e)}")
        return False

# =========================================================
# UI関数
# =========================================================

def render_analyst_chatbot_page():
    """Cortex Analystを使用した分析チャットボットページを表示します。"""
    st.header("分析チャットボット")
    
    # ワークショップ向けの説明
    st.info("""
    ## 📊 分析チャットボットについて
    
    このページでは、Snowflake Cortex Analystを使用したデータ分析チャットボットを体験できます。
    
    ### 主な機能
    * **自然言語でのデータ分析**: 質問をSQLに自動変換し、データベースに対して実行
    * **視覚化**: 分析結果を自動的にグラフ化して表示
    * **日本語対応**: 英語で返される分析結果を自動的に日本語に翻訳
    
    ### 使い方のヒント
    * 今回は店舗とECの取引データを元にセマンティックモデルを作成しているため、販売数量や売上金額などの分析に適しています。
    * データに関する質問を具体的に記述してください（例：「2023年の四半期ごとの売上推移を教えて」）
    * 質問はデータに関連するものに限定されます（一般的な会話ではなく、データ分析のクエリとして解釈されます）
    * 分析結果はグラフと表形式で表示され、生成されたSQLも確認できます
    
    ### セマンティックモデル
    このチャットボットは、選択したセマンティックモデルを使用してデータベースのスキーマを理解しています。
    """)
    
    # セマンティックモデルの選択
    semantic_model_files = get_semantic_model_files()
    
    if not semantic_model_files:
        st.error(f"""
        セマンティックモデルファイルが見つかりません。
        ステージ 'SEMANTIC_MODEL_STAGE' にセマンティックモデルファイル（.yamlまたは.yml）をアップロードしてください。
        """)
        return
    
    SEMANTIC_MODEL_STAGE = "SEMANTIC_MODEL_STAGE"
    selected_model_file = st.selectbox(
        "使用するセマンティックモデルを選択してください",
        semantic_model_files,
        index=0
    )
    
    # 選択されたセマンティックモデルのパス
    full_stage_path = f"@{SEMANTIC_MODEL_STAGE}/{selected_model_file}"
    
    # セッション状態の初期化
    if "analyst_messages" not in st.session_state:
        st.session_state.analyst_messages = []
    
    # チャット履歴のクリアボタン
    if st.button("チャット履歴をクリア"):
        st.session_state.analyst_messages = []
        st.rerun()
    
    # チャット履歴の表示
    for message in st.session_state.analyst_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if "result" in message and message["result"] is not None:
                # 結果が DataFrame であれば表示
                if isinstance(message["result"], pd.DataFrame) and not message["result"].empty:
                    st.dataframe(message["result"])
                
                # SQLクエリが含まれていれば表示
                if "sql" in message and message["sql"]:
                    with st.expander("生成されたSQL"):
                        st.code(message["sql"], language="sql")
                
                # グラフが含まれていれば表示
                if "chart" in message and message["chart"]:
                    st.plotly_chart(message["chart"], use_container_width=True, key=f"analyst_chart_{st.session_state.analyst_messages.index(message)}")
    
    # ユーザー入力の処理
    if prompt := st.chat_input("データについて質問してください"):
        # ユーザーメッセージの表示
        st.session_state.analyst_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # 回答生成の処理
        with st.spinner("回答を生成中..."):
            try:
                # メッセージの準備
                messages = [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": prompt}]
                    }
                ]
                
                # リクエストボディの準備
                request_body = {
                    "messages": messages,
                    "semantic_model_file": full_stage_path,
                }
                
                # Cortex Analyst API呼び出し
                try:
                    import _snowflake
                    # Snowflake内部APIを使用
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
                            chart = None
                            
                            for item in content_list:
                                if item["type"] == "text":
                                    response_text += item["text"] + "\n\n"
                                elif item["type"] == "sql":
                                    sql_query = item["statement"]
                            
                            # 英語のレスポンスを日本語に翻訳
                            if response_text:
                                try:
                                    translated_response = snowflake_session.sql("""
                                        SELECT SNOWFLAKE.CORTEX.TRANSLATE(?, 'en', 'ja') as translated
                                    """, params=[response_text.strip()]).collect()[0]['TRANSLATED']
                                    response_text = translated_response
                                except Exception as translate_error:
                                    st.warning(f"翻訳中にエラーが発生しました。元の英語レスポンスを表示します: {str(translate_error)}")
                            
                            # SQLを実行してデータフレームを取得
                            try:
                                # SQLクエリが存在し、空でない場合のみ実行
                                if sql_query and sql_query.strip():
                                    result_data = snowflake_session.sql(sql_query).to_pandas()
                                else:
                                    # SQLが生成されなかった場合
                                    result_data = None
                                    chart = None
                                
                                # シンプルなグラフを作成（データに基づいて）
                                if result_data is not None and not result_data.empty and len(result_data.columns) >= 2:
                                    x_col = result_data.columns[0]
                                    y_col = result_data.columns[1]
                                    
                                    # データタイプに基づいて適切なグラフを選択
                                    if result_data[x_col].dtype == 'object':  # カテゴリデータ
                                        chart = px.bar(
                                            result_data,
                                            x=x_col,
                                            y=y_col,
                                            title="分析結果"
                                        )
                                    else:  # 数値データ
                                        chart = px.line(
                                            result_data,
                                            x=x_col,
                                            y=y_col,
                                            title="分析結果"
                                        )
                            except Exception as sql_error:
                                st.error(f"SQL実行エラー: {str(sql_error)}")
                                result_data = None
                                chart = None
                            
                            # 応答をチャット履歴に追加
                            st.session_state.analyst_messages.append({
                                "role": "assistant", 
                                "content": response_text.strip(),
                                "result": result_data,
                                "sql": sql_query,
                                "chart": chart
                            })
                            
                            # 応答を表示
                            with st.chat_message("assistant"):
                                st.markdown(response_text.strip())
                                
                                if result_data is not None and not result_data.empty:
                                    st.dataframe(result_data)
                                
                                if chart:
                                    st.plotly_chart(chart, use_container_width=True)
                                
                                if sql_query:
                                    with st.expander("生成されたSQL"):
                                        st.code(sql_query, language="sql")
                        else:
                            raise Exception("APIレスポンスの形式が不正です")
                    else:
                        error_content = json.loads(resp["content"])
                        error_msg = f"""
                        🚨 APIエラーが発生しました 🚨
                        
                        * ステータスコード: `{resp['status']}`
                        * リクエストID: `{error_content.get('request_id', 'N/A')}`
                        * エラーコード: `{error_content.get('error_code', 'N/A')}`
                        
                        メッセージ:
                        ```
                        {error_content.get('message', '不明なエラー')}
                        ```
                        """
                        raise Exception(error_msg)
                
                except ImportError:
                    # Snowflake内部APIが使用できない場合
                    st.error("Snowflakeの内部APIにアクセスできません。Streamlit in Snowflake環境で実行してください。")
                    
                    # 代替のモック応答を生成（デモ用）
                    response_text = f"質問: {prompt}\n\n申し訳ありませんが、Cortex Analystへのアクセスが現在利用できません。本来であれば、ここにデータ分析の結果が日本語で表示されます。"
                    result_data = pd.DataFrame({
                        "カテゴリ": ["商品の品質", "価格", "接客サービス", "店舗環境", "その他"],
                        "レビュー数": [45, 32, 28, 15, 10]
                    })
                    
                    # モックチャート
                    chart = px.bar(
                        result_data,
                        x="カテゴリ",
                        y="レビュー数",
                        title="カテゴリ別レビュー数（モックデータ）"
                    )
                    
                    # 応答をチャット履歴に追加
                    st.session_state.analyst_messages.append({
                        "role": "assistant", 
                        "content": response_text,
                        "result": result_data,
                        "sql": "-- モックSQL\nSELECT category_name, COUNT(*) as count\nFROM REVIEW_TAGS\nGROUP BY category_name\nORDER BY count DESC",
                        "chart": chart
                    })
                    
                    # 応答を表示
                    with st.chat_message("assistant"):
                        st.markdown(response_text)
                        st.dataframe(result_data)
                        st.plotly_chart(chart, use_container_width=True)
                        
                        with st.expander("モックSQL"):
                            st.code("-- モックSQL\nSELECT category_name, COUNT(*) as count\nFROM REVIEW_TAGS\nGROUP BY category_name\nORDER BY count DESC", language="sql")
                
            except Exception as e:
                error_msg = f"""
                Cortex Analystにアクセスできません。
                エラー: {str(e)}
                
                **確認事項:**
                1. セマンティックモデルファイル '{selected_model_file}' がステージ '{SEMANTIC_MODEL_STAGE}' に存在するか確認してください。
                2. Cortex Analystサービスが有効になっているか確認してください。
                3. 必要な権限が付与されているか確認してください。
                
                **セマンティックモデルのパス:** {full_stage_path}
                """
                
                st.error(error_msg)
                st.code(str(e))
                
                # エラーメッセージをチャット履歴に追加
                st.session_state.analyst_messages.append({
                    "role": "assistant", 
                    "content": f"エラーが発生しました: {str(e)}",
                    "result": None
                })

def render_simple_chatbot_page():
    """シンプルチャットボットページを表示します。"""
    st.header("シンプルチャットボット")
    
    # ワークショップ向けの説明
    st.info("""
    ## 🤖 シンプルチャットボットについて
    
    このページでは、Snowflake Cortexの生成AIモデルを使用した基本的なチャットボットを体験できます。
    
    ### 主な機能
    * **テキスト生成**: COMPLETE関数を使用して、入力プロンプトに基づいた応答を生成
    * **チャット履歴の保持**: 会話の文脈を保持し、より自然な対話を実現
    
    ### 使い方のヒント
    * 質問や指示を自然な文章で入力してください
    * 複雑な質問の場合は、具体的に詳細を記述するとより良い応答が得られます
    * このシンプルなチャットボットは外部データを参照せず、モデルの知識だけで応答を生成します
    """)
    
    # セッション状態の初期化
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.chat_history = ""
    
    # チャット履歴のクリアボタン
    if st.button("チャット履歴をクリア"):
        st.session_state.messages = []
        st.session_state.chat_history = ""
        st.rerun()
    
    # チャット履歴の表示
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # ユーザー入力の処理
    if prompt := st.chat_input("メッセージを入力してください"):
        # ユーザーメッセージの表示と履歴の更新
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.session_state.chat_history += f"User: {prompt}\n"
        with st.chat_message("user"):
            st.markdown(prompt)
        
        try:
            # Cortex Completeを使用して応答を生成
            full_prompt = st.session_state.chat_history + "AI: "
            response = CompleteText(complete_model, full_prompt)
            
            # 応答の表示と履歴の更新
            st.session_state.messages.append({"role": "assistant", "content": response})
            st.session_state.chat_history += f"AI: {response}\n"
            with st.chat_message("assistant"):
                st.markdown(response)
            
        except Exception as e:
            st.error(f"応答の生成中にエラーが発生しました: {str(e)}")

def render_rag_chatbot_page():
    """RAGチャットボットページを表示します。"""
    st.header("RAGチャットボット")
    
    # ワークショップ向けの説明
    st.info("""
    ## 📚 RAGチャットボットについて
    
    このページでは、Cortex Searchを用いたRetrieval-Augmented Generation (RAG) フレームワークの高度なチャットボットを体験できます。
    
    ### 主な機能
    * **多言語対応**: Cortex Searchは日本語を含む複数の言語に対応しているため、自然な日本語での質問が可能
    * **検索対象の自動リフレッシュ機能**: Cortex Searchを使用して検索対象ドキュメントを定期的に最新化
    * **ハイブリッド検索**: キーワード検索と曖昧検索の両方からドキュメントを検索することが可能
    
    ### 使い方のヒント
    * 社内文書に関する質問や、製品・サービスに関する具体的な質問を日本語で尋ねてみてください
    * 質問が具体的であるほど、より関連性の高いドキュメントが検索されます
    * 参考ドキュメントを展開すると、応答の生成に使用されたドキュメントを確認できます
    
    ### 注意事項
    Cortex Search Serviceはドキュメントの更新に伴うコンピューティングコスト以外にも、インデックス化されたデータサイズに対しての料金も発生します。長期間使用しない場合はCortex Search Serviceを削除するなどをご検討ください。
    """)
    
    # Snowflake Root オブジェクトの初期化
    root = Root(snowflake_session)
    
    # 現在のデータベースとスキーマを取得
    current_db_schema = snowflake_session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
    current_database = current_db_schema['CURRENT_DATABASE()']
    current_schema = current_db_schema['CURRENT_SCHEMA()']
    
    # 部署とドキュメントタイプの取得
    try:
        departments = snowflake_session.sql("""
            SELECT DISTINCT department FROM snow_retail_documents
            ORDER BY department
        """).collect()
        department_list = [row['DEPARTMENT'] for row in departments]
        
        document_types = snowflake_session.sql("""
            SELECT DISTINCT document_type FROM snow_retail_documents
            ORDER BY document_type
        """).collect()
        document_type_list = [row['DOCUMENT_TYPE'] for row in document_types]
    except Exception as e:
        st.warning("部署とドキュメントタイプの取得に失敗しました。フィルター機能は使用できません。")
        department_list = []
        document_type_list = []
    
    # Cortex Search Serviceの管理
    st.subheader("Cortex Search Serviceの管理")
    
    # サービスの存在確認
    service_exists = check_search_service_exists()
    
    if service_exists:
        st.success("Cortex Search Serviceが利用可能です。")
        if st.button("Cortex Search Serviceを削除"):
            if delete_snow_retail_search_service():
                st.rerun()
    else:
        st.error("Cortex Search Serviceが見つかりません。ワークショップの準備ステップでCortex Search Serviceが正しく作成されているか確認してください。")
        st.info("Cortex Search Serviceはワークショップの前段階で作成される必要があります。")
        return
    
    st.markdown("---")
    
    # 検索フィルターの設定
    with st.expander("検索フィルター設定", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            selected_departments = st.multiselect(
                "部署で絞り込み",
                options=department_list,
                default=[]
            )
        
        with col2:
            selected_document_types = st.multiselect(
                "ドキュメントタイプで絞り込み",
                options=document_type_list,
                default=[]
            )
    
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
            # Cortex Search Serviceの取得
            search_service = (
                root.databases[current_database]
                .schemas[current_schema]
                .cortex_search_services["snow_retail_search_service"]
            )
            
            # フィルターの構築
            filter_conditions = []
            
            # 部署フィルターの追加
            if selected_departments:
                dept_conditions = []
                for dept in selected_departments:
                    dept_conditions.append({"@eq": {"department": dept}})
                
                if len(dept_conditions) == 1:
                    filter_conditions.append(dept_conditions[0])
                else:
                    filter_conditions.append({"@or": dept_conditions})
            
            # ドキュメントタイプフィルターの追加
            if selected_document_types:
                type_conditions = []
                for doc_type in selected_document_types:
                    type_conditions.append({"@eq": {"document_type": doc_type}})
                
                if len(type_conditions) == 1:
                    filter_conditions.append(type_conditions[0])
                else:
                    filter_conditions.append({"@or": type_conditions})
            
            # 最終的なフィルターの組み立て
            search_filter = None
            if filter_conditions:
                if len(filter_conditions) == 1:
                    search_filter = filter_conditions[0]
                else:
                    search_filter = {"@and": filter_conditions}
            
            # フィルター情報の表示
            if selected_departments or selected_document_types:
                filter_info = []
                if selected_departments:
                    filter_info.append(f"部署: {', '.join(selected_departments)}")
                if selected_document_types:
                    filter_info.append(f"ドキュメントタイプ: {', '.join(selected_document_types)}")
                st.info(f"以下の条件で検索します: {' / '.join(filter_info)}")
            
            # 検索の実行
            search_args = {
                "query": prompt,
                "columns": ["title", "content", "document_type", "department"],
                "limit": 3
            }
            
            # フィルターがある場合は追加
            if search_filter:
                search_args["filter"] = search_filter
            
            search_results = search_service.search(**search_args)
            
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

def get_semantic_model_files() -> list:
    """ステージ内のセマンティックモデルファイル一覧を取得します。
    
    Returns:
        list: ファイル名のリスト（取得失敗時は空リスト）
    """
    try:
        SEMANTIC_MODEL_STAGE = "SEMANTIC_MODEL_STAGE"
        stage_files = snowflake_session.sql(f"""
            LIST @{SEMANTIC_MODEL_STAGE}
        """).collect()
        
        # YAMLファイルだけをフィルタリング
        yaml_files = []
        for file in stage_files:
            filename = file['name']
            # ステージ名が含まれている場合は削除
            if '/' in filename:
                filename = filename.split('/')[-1]
            
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                yaml_files.append(filename)
        
        return yaml_files
    except Exception as e:
        st.error(f"セマンティックモデルファイルの取得に失敗しました: {str(e)}")
        return []

# =========================================================
# メイン処理
# =========================================================

# サイドバーでの機能選択
st.sidebar.title("機能選択")
selected_function = st.sidebar.radio(
    "機能を選択してください",
    ["シンプルチャットボット", "RAGチャットボット", "分析チャットボット"]
)

# モデル設定
st.sidebar.title("モデル設定")

# モデル選択UI
complete_model = st.sidebar.selectbox(
    "Completeモデルを選択してください",
    COMPLETE_MODELS,
    index=0
)

# メインコンテンツ
st.title("🏪 スノーリテール AIチャットボットアプリ")
st.markdown("---")

# 選択された機能に応じた処理
if selected_function == "シンプルチャットボット":
    render_simple_chatbot_page()
elif selected_function == "RAGチャットボット":
    render_rag_chatbot_page()
elif selected_function == "分析チャットボット":
    render_analyst_chatbot_page() 