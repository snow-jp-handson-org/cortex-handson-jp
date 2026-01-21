# =========================================================
# GLACIER CREATIVE STUDIO
# クリエイティブ分析ページ
# =========================================================
# 概要: 個別クリエイティブの詳細分析とAIインサイト
# =========================================================

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# ページ設定
st.set_page_config(layout="wide")

# =========================================================
# ユーティリティ関数
# =========================================================
@st.cache_resource
def get_session():
    """Snowflakeセッションを取得"""
    return get_active_session()


def format_number(value: float) -> str:
    """数値をフォーマット"""
    if pd.isna(value):
        return "-"
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K"
    return f"{value:,.0f}"


def process_ai_response(response: str) -> str:
    """AI応答の後処理（エスケープ文字の変換）"""
    if not response:
        return ""
    
    if response.startswith('"') and response.endswith('"'):
        response = response[1:-1]
    
    response = response.replace('\\n', '\n')
    response = response.replace('\\t', '\t')
    response = response.replace('\\"', '"')
    response = response.replace("\\'", "'")
    response = response.replace('\\\\', '\\')
    
    return response


@st.cache_data(ttl=600)
def get_creative_data() -> pd.DataFrame:
    """広告クリエイティブデータを取得（画像パス含む）"""
    session = get_session()
    
    query = """
    SELECT 
        g.CREATIVE_ID,
        g.CAMPAIGN_ID,
        g.CREATIVE_NAME,
        g.CREATIVE_TYPE,
        g.COPY_TEXT,
        g.HEADLINE,
        g.CTA_TEXT,
        g.TARGET_SEGMENT,
        g.PLATFORM,
        g.IMPRESSIONS,
        g.CLICKS,
        g.CONVERSIONS,
        g.SPEND,
        g.CTR,
        g.CVR,
        g.CPA,
        r.IMAGE_FILE_PATH,
        GET_PRESIGNED_URL(@DATA_STAGE, 'ad_images/' || r.IMAGE_FILE_PATH, 3600) AS IMAGE_URL
    FROM GOLD_AD_CREATIVE_ANALYSIS g
    LEFT JOIN RAW_AD_CREATIVES r ON g.CREATIVE_ID = r.CREATIVE_ID
    ORDER BY g.CREATIVE_ID
    """
    
    return session.sql(query).to_pandas()


def call_cortex_complete(prompt: str, model: str = "claude-sonnet-4-5") -> str:
    """Cortex AIのAI_COMPLETE関数を呼び出し"""
    session = get_session()
    
    escaped_prompt = prompt.replace("'", "''").replace("\\", "\\\\")
    
    query = f"""
    SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
        '{model}',
        '{escaped_prompt}'
    ) AS RESPONSE
    """
    
    try:
        result = session.sql(query).collect()
        if result and result[0]['RESPONSE']:
            return process_ai_response(result[0]['RESPONSE'])
        return "応答を取得できませんでした。"
    except Exception as e:
        return f"エラーが発生しました: {str(e)}"


def resize_image_for_analysis(image_file_path: str) -> str:
    """画像を分析用にリサイズ（小さいJPG形式に変換）"""
    session = get_session()
    
    # 画像をリサイズしてJPGに変換（Snowflakeの機能を使用）
    # 注: Snowflakeでは直接リサイズできないため、小さい画像を優先的に選ぶ
    # または、ファイル名からサムネイル版を探す
    
    # サムネイル版があれば使用
    if '_thumb' not in image_file_path:
        base_name = image_file_path.rsplit('.', 1)[0]
        ext = image_file_path.rsplit('.', 1)[1] if '.' in image_file_path else 'png'
        thumbnail_path = f"{base_name}_thumb.{ext}"
        
        # サムネイルが存在するかチェック
        try:
            check_query = f"""
            SELECT COUNT(*) AS CNT FROM DIRECTORY(@DATA_STAGE) 
            WHERE RELATIVE_PATH = 'ad_images/{thumbnail_path}'
            """
            result = session.sql(check_query).collect()
            if result and result[0]['CNT'] > 0:
                return thumbnail_path
        except:
            pass
    
    return image_file_path


def call_cortex_complete_with_image(prompt: str, image_file_path: str) -> str:
    """Cortex AIのAI_COMPLETE関数を画像付きで呼び出し（マルチモーダル - Llama4 Maverick固定）"""
    session = get_session()
    
    escaped_prompt = prompt.replace("'", "''").replace("\\", "\\\\")
    
    # 画像分析はLlama4 Maverickのみ対応（ファイルサイズ制限も緩い）
    model = "llama4-maverick"
    
    # TO_FILE関数を使用してステージから画像を読み込む
    query = f"""
    SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
        '{model}',
        '{escaped_prompt}',
        TO_FILE('@DATA_STAGE', 'ad_images/{image_file_path}')
    ) AS RESPONSE
    """
    
    try:
        result = session.sql(query).collect()
        if result and result[0]['RESPONSE']:
            return process_ai_response(str(result[0]['RESPONSE']))
        return "応答を取得できませんでした。"
    except Exception as e:
        error_msg = str(e)
        if "File data exceeds the limit" in error_msg:
            return "エラー: 画像ファイルサイズが大きすぎます（3.75MB制限）。より小さい画像でお試しください。"
        return f"エラーが発生しました: {error_msg}"


# =========================================================
# セッションステートの初期化
# =========================================================
if "selected_model" not in st.session_state:
    st.session_state.selected_model = "claude-sonnet-4-5"

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "ai_analysis_result" not in st.session_state:
    st.session_state.ai_analysis_result = None

if "image_analysis_result" not in st.session_state:
    st.session_state.image_analysis_result = None

# =========================================================
# メインコンテンツ
# =========================================================

st.title("🖼️ クリエイティブ分析")
st.markdown("個別の広告クリエイティブを詳細に分析し、AIがインサイトを提供します。")
st.markdown("---")

# =========================================================
# サイドバー：AI設定（セッションステートで管理）
# =========================================================
st.sidebar.subheader("🤖 AI設定")

model_options = {
    "Claude Sonnet 4.5": "claude-sonnet-4-5",
    "OpenAI GPT-5": "openai-gpt-5",
    "Llama 4 Maverick": "llama4-maverick",
    "OpenAI GPT OSS 120B": "openai-gpt-oss-120b",
}

# モデル選択（on_changeで確実に更新）
model_names = list(model_options.keys())

def update_model():
    st.session_state.selected_model = model_options[st.session_state.model_selector]

# 現在のモデル名を取得
current_model_name = [k for k, v in model_options.items() if v == st.session_state.selected_model]
default_index = model_names.index(current_model_name[0]) if current_model_name else 0

st.sidebar.selectbox(
    "モデル選択", 
    model_names, 
    index=default_index,
    key="model_selector",
    on_change=update_model
)

st.sidebar.info(f"""
**選択中の設定:**
- モデル: `{st.session_state.selected_model}`

💡 **画像分析について:**
- 画像分析は自動的に **Llama4 Maverick** を使用します
- ファイルサイズ上限: 3.75MB
""")

# データ取得
df = get_creative_data()

if df.empty:
    st.error("データが見つかりません。データベースの設定を確認してください。")
    st.stop()

# クリエイティブ選択
creative_options = df['CREATIVE_NAME'].tolist()
selected_creative = st.selectbox(
    "🎯 分析するクリエイティブを選択",
    options=creative_options,
    index=0
)

selected_row = df[df['CREATIVE_NAME'] == selected_creative].iloc[0]

st.markdown("---")

# =========================================================
# クリエイティブ画像（大きく表示）
# =========================================================
st.subheader("📷 クリエイティブ画像")

image_url = None
image_file_path = None
if 'IMAGE_URL' in selected_row and selected_row['IMAGE_URL'] and pd.notna(selected_row['IMAGE_URL']):
    image_url = selected_row['IMAGE_URL']
    image_file_path = selected_row.get('IMAGE_FILE_PATH', None)
    col_img_left, col_img_center, col_img_right = st.columns([1, 2, 1])
    with col_img_center:
        try:
            st.image(image_url, use_container_width=True)
        except Exception as e:
            st.info(f"📷 画像の読み込みに失敗しました")
else:
    st.info("📷 画像がありません")

st.markdown("---")

# =========================================================
# パフォーマンスとコピー要素（2カラム）
# =========================================================
col_perf, col_copy = st.columns(2)

with col_perf:
    st.subheader("📊 パフォーマンス")
    
    row1_cols = st.columns(3)
    with row1_cols[0]:
        st.metric("インプレッション", format_number(selected_row['IMPRESSIONS']))
    with row1_cols[1]:
        st.metric("クリック", format_number(selected_row['CLICKS']))
    with row1_cols[2]:
        st.metric("コンバージョン", format_number(selected_row['CONVERSIONS']))
    
    row2_cols = st.columns(3)
    with row2_cols[0]:
        st.metric("CTR", f"{selected_row['CTR']:.2f}%")
    with row2_cols[1]:
        st.metric("CVR", f"{selected_row['CVR']:.2f}%")
    with row2_cols[2]:
        st.metric("CPA", f"¥{selected_row['CPA']:,.0f}")
    
    st.markdown(f"**費用**: ¥{format_number(selected_row['SPEND'])}")

with col_copy:
    st.subheader("📝 コピー要素")
    
    st.markdown("**ヘッドライン**")
    st.info(selected_row.get('HEADLINE', '-'))
    
    st.markdown("**CTA**")
    st.success(selected_row.get('CTA_TEXT', '-'))
    
    st.markdown("**ターゲット**")
    st.write(selected_row.get('TARGET_SEGMENT', '-'))

# ボディコピー（全幅）
st.markdown("---")
st.subheader("📄 ボディコピー")
st.write(selected_row.get('COPY_TEXT', '-'))

st.markdown("---")

# =========================================================
# AI分析セクション（Fragment使用）
# =========================================================
st.subheader("🤖 AI分析")

@st.fragment
def ai_analysis_section():
    """AI分析セクション（Fragmentで部分更新）"""
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔍 コピー・KPI分析", type="primary", use_container_width=True):
            with st.spinner("🤔 AIが分析中..."):
                analysis_prompt = f"""あなたは広告クリエイティブの分析専門家です。以下の広告クリエイティブについて分析してください。

【クリエイティブ情報】
- 名前: {selected_row['CREATIVE_NAME']}
- タイプ: {selected_row.get('CREATIVE_TYPE', 'image')}
- ヘッドライン: {selected_row.get('HEADLINE', '-')}
- ボディコピー: {selected_row.get('COPY_TEXT', '-')}
- CTA: {selected_row.get('CTA_TEXT', '-')}
- ターゲット: {selected_row.get('TARGET_SEGMENT', '-')}
- プラットフォーム: {selected_row.get('PLATFORM', '-')}

【パフォーマンス結果】
- インプレッション: {selected_row['IMPRESSIONS']:,}
- クリック: {selected_row['CLICKS']:,}
- CTR: {selected_row['CTR']:.2f}%
- コンバージョン: {selected_row['CONVERSIONS']:,}
- CVR: {selected_row['CVR']:.2f}%
- CPA: ¥{selected_row['CPA']:,.0f}

以下の観点で分析し、箇条書きで回答してください：

1. 【効果要因】このクリエイティブのパフォーマンスが良い/悪い理由（3点）
2. 【ターゲット適合性】ターゲットセグメントとコピーの適合度
3. 【改善ポイント】さらに効果を高めるための具体的な改善案（3点）
4. 【総合評価】5段階評価（★で表示）とコメント

日本語で回答してください。"""
                
                st.session_state.ai_analysis_result = call_cortex_complete(analysis_prompt, st.session_state.selected_model)
    
    with col2:
        if image_file_path and pd.notna(image_file_path):
            if st.button("🖼️ 画像をAI分析（Llama4）", type="secondary", use_container_width=True):
                with st.spinner("🤔 AIが画像を分析中（Llama4 Maverick使用）..."):
                    image_prompt = """この広告クリエイティブ画像を分析してください。

以下の観点で詳細に分析し、日本語で回答してください：

1. 【ビジュアル要素】
   - 色彩設計（メインカラー、アクセントカラー）
   - 構図・レイアウト
   - 商品の見せ方

2. 【訴求力の評価】
   - ターゲットへの訴求度
   - 視認性・可読性
   - 感情的なアピール

3. 【改善提案】
   - より効果的にするための具体的な修正案（3点）

4. 【総合評価】
   - 5段階評価（★で表示）とコメント"""
                    
                    st.session_state.image_analysis_result = call_cortex_complete_with_image(
                        image_prompt, image_file_path
                    )
        else:
            st.button("🖼️ 画像をAI分析", disabled=True, help="画像がありません", use_container_width=True)
    
    # 分析結果の表示
    if st.session_state.ai_analysis_result:
        with st.expander("📊 コピー・KPI分析結果", expanded=True):
            st.markdown(st.session_state.ai_analysis_result)
    
    if st.session_state.image_analysis_result:
        with st.expander("🖼️ 画像分析結果（Llama4 Maverick使用）", expanded=True):
            st.markdown(st.session_state.image_analysis_result)

ai_analysis_section()

st.markdown("---")

# =========================================================
# AIチャット（通常のセクション - rerunで更新）
# =========================================================
st.subheader("💬 AIに質問")

# チャット履歴の表示
chat_container = st.container(height=350)
with chat_container:
    if st.session_state.chat_history:
        for message in st.session_state.chat_history:
            with st.chat_message(message["role"], avatar="👤" if message["role"] == "user" else "🤖"):
                st.write(message["content"])
    else:
        st.info("このクリエイティブについて何でも質問してください。")

# チャット入力とクリアボタン
col_clear, _ = st.columns([1, 5])
with col_clear:
    if st.button("🗑️ 履歴クリア"):
        st.session_state.chat_history = []
        st.rerun()

if prompt := st.chat_input("クリエイティブについて質問してください"):
    st.session_state.chat_history.append({"role": "user", "content": prompt})
    
    context_prompt = f"""あなたは広告クリエイティブの専門家です。以下のクリエイティブについての質問に答えてください。

【クリエイティブ情報】
- 名前: {selected_row['CREATIVE_NAME']}
- ヘッドライン: {selected_row.get('HEADLINE', '-')}
- ボディコピー: {selected_row.get('COPY_TEXT', '-')}
- CTA: {selected_row.get('CTA_TEXT', '-')}
- ターゲット: {selected_row.get('TARGET_SEGMENT', '-')}
- CTR: {selected_row['CTR']:.2f}%, CVR: {selected_row['CVR']:.2f}%, CPA: ¥{selected_row['CPA']:,.0f}

【質問】
{prompt}

具体的かつ実践的なアドバイスを日本語で回答してください。"""
    
    response = call_cortex_complete(context_prompt, st.session_state.selected_model)
    st.session_state.chat_history.append({"role": "assistant", "content": response})
    st.rerun()

st.markdown("---")

# =========================================================
# 類似クリエイティブ比較
# =========================================================
st.subheader("🔄 類似クリエイティブとの比較")

same_platform = df[
    (df['PLATFORM'] == selected_row['PLATFORM']) & 
    (df['CREATIVE_NAME'] != selected_creative)
].head(3)

if not same_platform.empty:
    cols = st.columns(len(same_platform) + 1)
    
    with cols[0]:
        with st.container(border=True):
            st.markdown("**📌 選択中**")
            name = selected_row['CREATIVE_NAME']
            st.markdown(f"**{name[:20]}...**" if len(name) > 20 else f"**{name}**")
            st.metric("CTR", f"{selected_row['CTR']:.2f}%")
            st.metric("CVR", f"{selected_row['CVR']:.2f}%")
            st.metric("CPA", f"¥{selected_row['CPA']:,.0f}")
    
    for i, (_, row) in enumerate(same_platform.iterrows()):
        with cols[i + 1]:
            with st.container(border=True):
                name = row['CREATIVE_NAME']
                st.markdown(f"**{name[:20]}...**" if len(name) > 20 else f"**{name}**")
                
                ctr_delta = row['CTR'] - selected_row['CTR']
                cvr_delta = row['CVR'] - selected_row['CVR']
                cpa_delta = row['CPA'] - selected_row['CPA']
                
                st.metric("CTR", f"{row['CTR']:.2f}%", f"{ctr_delta:+.2f}%")
                st.metric("CVR", f"{row['CVR']:.2f}%", f"{cvr_delta:+.2f}%")
                st.metric("CPA", f"¥{row['CPA']:,.0f}", f"¥{cpa_delta:+,.0f}", delta_color="inverse")
else:
    st.info("比較可能なクリエイティブがありません。")

st.markdown("---")
st.caption("**GLACIER CREATIVE STUDIO** | クリエイティブ分析")
