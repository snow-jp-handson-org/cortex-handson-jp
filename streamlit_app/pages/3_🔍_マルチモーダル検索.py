"""
================================================================================
マルチモーダル検索
================================================================================
Cortex Searchを使用した統合検索UI

機能:
- FAQ検索
- 運営マニュアル検索
- 音声ログ検索
- SNS投稿検索
- 横断検索
================================================================================
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import json

# ============================================================================
# ページ設定
# ============================================================================
st.set_page_config(
    page_title="マルチモーダル検索 | GlacierStyle",
    page_icon="🔍",
    layout="wide"
)

# ============================================================================
# セッション取得
# ============================================================================
@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()

# ============================================================================
# ヘッダー
# ============================================================================
st.title("🔍 マルチモーダル検索")
st.markdown("Cortex Searchを使用して、FAQ、マニュアル、音声ログ、SNS投稿を横断的に検索します。")
st.divider()

# ============================================================================
# 検索サービス設定
# ============================================================================
SEARCH_SERVICES = {
    "FAQ": {
        "name": "SEARCH_FAQ",
        "icon": "❓",
        "description": "よくある質問と回答",
        "columns": ["CHAPTER", "SECTION", "CONTENT_CHUNK"],
        "display_columns": ["CHAPTER", "SECTION"]
    },
    "運営マニュアル": {
        "name": "SEARCH_OPERATION_MANUALS",
        "icon": "📖",
        "description": "業務手順書・運営マニュアル",
        "columns": ["DEPARTMENT", "CHAPTER", "SECTION", "CONTENT_CHUNK"],
        "display_columns": ["DEPARTMENT", "CHAPTER", "SECTION"]
    },
    "音声ログ": {
        "name": "SEARCH_VOICE_LOGS",
        "icon": "📞",
        "description": "コールセンター通話の要約",
        "columns": ["CALL_ID", "CATEGORY", "OVERALL_SENTIMENT", "TRANSCRIBED_TEXT_SUMMARY"],
        "display_columns": ["CALL_ID", "CATEGORY", "OVERALL_SENTIMENT"]
    },
    "SNS投稿": {
        "name": "SEARCH_SNS_MENTIONS",
        "icon": "📱",
        "description": "SNS上の顧客の声",
        "columns": ["POST_ID", "PLATFORM", "OVERALL_SENTIMENT", "CONTENT"],
        "display_columns": ["POST_ID", "PLATFORM", "OVERALL_SENTIMENT"]
    }
}

# ============================================================================
# 検索関数
# ============================================================================
def search_cortex(service_name: str, query: str, columns: list, limit: int = 5, filter_json: str = None):
    """Cortex Searchを実行"""
    try:
        columns_str = '", "'.join(columns)
        
        if filter_json:
            search_params = f'''{{
                "query": "{query}",
                "columns": ["{columns_str}"],
                "filter": {filter_json},
                "limit": {limit}
            }}'''
        else:
            search_params = f'''{{
                "query": "{query}",
                "columns": ["{columns_str}"],
                "limit": {limit}
            }}'''
        
        sql = f"""
            SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                '{service_name}',
                '{search_params}'
            ) AS result_json
        """
        
        result = session.sql(sql).collect()
        
        if result and result[0]['RESULT_JSON']:
            return json.loads(result[0]['RESULT_JSON'])
        return None
    
    except Exception as e:
        st.error(f"検索エラー: {e}")
        return None

# ============================================================================
# サイドバー設定
# ============================================================================
with st.sidebar:
    st.header("🔧 検索設定")
    
    # 検索対象選択
    search_target = st.radio(
        "検索対象",
        options=["個別検索", "横断検索"],
        key="search_target"
    )
    
    if search_target == "個別検索":
        selected_service = st.selectbox(
            "検索サービス",
            options=list(SEARCH_SERVICES.keys()),
            format_func=lambda x: f"{SEARCH_SERVICES[x]['icon']} {x}"
        )
    
    # 検索件数
    limit = st.slider("検索件数", min_value=1, max_value=20, value=5)
    
    st.divider()
    
    # サービス情報
    st.markdown("### ℹ️ 検索サービス一覧")
    for name, config in SEARCH_SERVICES.items():
        st.caption(f"{config['icon']} **{name}**: {config['description']}")

# ============================================================================
# メイン検索UI
# ============================================================================
# 検索入力
st.markdown("### 🔎 検索クエリ入力")

col1, col2 = st.columns([4, 1])

with col1:
    query = st.text_input(
        "検索キーワード",
        placeholder="例: 返品の手続き方法、配送遅延、商品の品質",
        label_visibility="collapsed"
    )

with col2:
    search_button = st.button("🔍 検索", type="primary", use_container_width=True)

# クイック検索ボタン
st.markdown("**クイック検索:**")
quick_col1, quick_col2, quick_col3, quick_col4 = st.columns(4)

with quick_col1:
    if st.button("返品手続き", use_container_width=True):
        query = "返品の手続き方法"
        search_button = True

with quick_col2:
    if st.button("配送遅延", use_container_width=True):
        query = "配送が遅れている"
        search_button = True

with quick_col3:
    if st.button("商品の品質", use_container_width=True):
        query = "商品の品質 不良"
        search_button = True

with quick_col4:
    if st.button("おすすめ商品", use_container_width=True):
        query = "おすすめ 人気商品"
        search_button = True

st.divider()

# ============================================================================
# 検索実行・結果表示
# ============================================================================
if search_button and query:
    
    if search_target == "個別検索":
        # 個別検索
        config = SEARCH_SERVICES[selected_service]
        
        st.markdown(f"### {config['icon']} {selected_service}の検索結果")
        
        with st.spinner("検索中..."):
            results = search_cortex(
                config['name'],
                query,
                config['columns'],
                limit
            )
        
        if results and 'results' in results:
            result_list = results['results']
            
            if result_list:
                st.success(f"{len(result_list)} 件の結果が見つかりました")
                
                for i, item in enumerate(result_list):
                    # スコア取得
                    scores = item.get('@scores', {})
                    similarity = scores.get('cosine_similarity', 0)
                    
                    with st.container(border=True):
                        # ヘッダー
                        header_parts = []
                        for col in config['display_columns']:
                            if col in item and item[col]:
                                header_parts.append(str(item[col]))
                        
                        st.markdown(f"**#{i+1}** | " + " > ".join(header_parts))
                        
                        # スコア表示
                        st.caption(f"類似度: {similarity:.3f}")
                        
                        # コンテンツ
                        content_key = config['columns'][-1]  # 最後のカラムがコンテンツ
                        if content_key in item:
                            content = item[content_key]
                            if len(content) > 500:
                                content = content[:500] + "..."
                            st.write(content)
            else:
                st.info("該当する結果が見つかりませんでした")
        else:
            st.warning("検索結果を取得できませんでした")
    
    else:
        # 横断検索
        st.markdown("### 🌐 横断検索結果")
        
        # 全サービスで検索
        all_results = {}
        
        with st.spinner("全サービスで検索中..."):
            for name, config in SEARCH_SERVICES.items():
                results = search_cortex(
                    config['name'],
                    query,
                    config['columns'],
                    3  # 横断検索は各サービス3件
                )
                if results and 'results' in results:
                    all_results[name] = results['results']
        
        # 結果表示
        if all_results:
            total_count = sum(len(v) for v in all_results.values())
            st.success(f"合計 {total_count} 件の結果が見つかりました")
            
            # タブで表示
            tabs = st.tabs([f"{SEARCH_SERVICES[name]['icon']} {name} ({len(results)})" 
                           for name, results in all_results.items() if results])
            
            for tab, (name, results) in zip(tabs, [(n, r) for n, r in all_results.items() if r]):
                config = SEARCH_SERVICES[name]
                
                with tab:
                    for i, item in enumerate(results):
                        scores = item.get('@scores', {})
                        similarity = scores.get('cosine_similarity', 0)
                        
                        with st.container(border=True):
                            # ヘッダー
                            header_parts = []
                            for col in config['display_columns']:
                                if col in item and item[col]:
                                    header_parts.append(str(item[col]))
                            
                            st.markdown(f"**#{i+1}** | " + " > ".join(header_parts))
                            st.caption(f"類似度: {similarity:.3f}")
                            
                            # コンテンツ
                            content_key = config['columns'][-1]
                            if content_key in item:
                                content = str(item[content_key])
                                if len(content) > 300:
                                    content = content[:300] + "..."
                                st.write(content)
        else:
            st.warning("検索結果を取得できませんでした")

elif search_button:
    st.warning("検索キーワードを入力してください")

# ============================================================================
# フィルター付き検索（詳細検索）
# ============================================================================
st.divider()

with st.expander("🎛️ 詳細検索（フィルター付き）", expanded=False):
    st.markdown("属性でフィルタリングして検索できます。")
    
    # サービス選択
    adv_service = st.selectbox(
        "検索サービス",
        options=list(SEARCH_SERVICES.keys()),
        format_func=lambda x: f"{SEARCH_SERVICES[x]['icon']} {x}",
        key="adv_service"
    )
    
    # フィルター設定
    st.markdown("**フィルター条件:**")
    
    filter_col, filter_value = None, None
    
    if adv_service == "音声ログ":
        col1, col2 = st.columns(2)
        with col1:
            filter_col = st.selectbox("フィルター項目", ["なし", "CATEGORY", "OVERALL_SENTIMENT"], key="voice_filter_col")
        with col2:
            if filter_col == "CATEGORY":
                filter_value = st.selectbox("値", ["問い合わせ", "クレーム", "注文"], key="voice_filter_val")
            elif filter_col == "OVERALL_SENTIMENT":
                filter_value = st.selectbox("値", ["positive", "neutral", "negative"], key="voice_filter_val2")
    
    elif adv_service == "SNS投稿":
        col1, col2 = st.columns(2)
        with col1:
            filter_col = st.selectbox("フィルター項目", ["なし", "PLATFORM", "OVERALL_SENTIMENT"], key="sns_filter_col")
        with col2:
            if filter_col == "PLATFORM":
                filter_value = st.selectbox("値", ["twitter", "instagram", "facebook"], key="sns_filter_val")
            elif filter_col == "OVERALL_SENTIMENT":
                filter_value = st.selectbox("値", ["positive", "neutral", "negative"], key="sns_filter_val2")
    
    # 検索実行
    adv_query = st.text_input("検索キーワード", key="adv_query")
    adv_limit = st.slider("検索件数", 1, 10, 5, key="adv_limit")
    
    if st.button("🔍 フィルター検索実行", key="adv_search"):
        if adv_query:
            config = SEARCH_SERVICES[adv_service]
            
            # フィルター構築
            filter_json = None
            if filter_col and filter_col != "なし" and filter_value:
                filter_json = f'{{"@eq": {{"{filter_col}": "{filter_value}"}}}}'
            
            with st.spinner("検索中..."):
                results = search_cortex(
                    config['name'],
                    adv_query,
                    config['columns'],
                    adv_limit,
                    filter_json
                )
            
            if results and 'results' in results:
                result_list = results['results']
                
                if result_list:
                    st.success(f"{len(result_list)} 件の結果が見つかりました")
                    
                    for i, item in enumerate(result_list):
                        scores = item.get('@scores', {})
                        similarity = scores.get('cosine_similarity', 0)
                        
                        with st.container(border=True):
                            header_parts = []
                            for col in config['display_columns']:
                                if col in item and item[col]:
                                    header_parts.append(str(item[col]))
                            
                            st.markdown(f"**#{i+1}** | " + " > ".join(header_parts))
                            st.caption(f"類似度: {similarity:.3f}")
                            
                            content_key = config['columns'][-1]
                            if content_key in item:
                                content = str(item[content_key])
                                if len(content) > 500:
                                    content = content[:500] + "..."
                                st.write(content)
                else:
                    st.info("該当する結果が見つかりませんでした")
            else:
                st.warning("検索結果を取得できませんでした")
        else:
            st.warning("検索キーワードを入力してください")

# ============================================================================
# フッター
# ============================================================================
st.divider()
st.caption("💡 **Cortex Search** はハイブリッド検索（キーワード + セマンティック）により、高精度な検索結果を提供します。")
