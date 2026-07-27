# =========================================================
# GLACIER CREATIVE STUDIO
# 広告企画支援ページ
# =========================================================
# 概要: 次の広告制作のためのインサイト提供とAI企画支援
# =========================================================

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
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
    """広告クリエイティブデータを取得"""
    session = get_session()
    
    query = """
    SELECT 
        CREATIVE_ID,
        CAMPAIGN_ID,
        CREATIVE_NAME,
        CREATIVE_TYPE,
        COPY_TEXT,
        HEADLINE,
        CTA_TEXT,
        TARGET_SEGMENT,
        PLATFORM,
        IMPRESSIONS,
        CLICKS,
        CONVERSIONS,
        SPEND,
        CTR,
        CVR,
        CPA
    FROM GOLD_AD_CREATIVE_ANALYSIS
    ORDER BY CVR DESC
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


# =========================================================
# セッションステートの初期化
# =========================================================
if "selected_model" not in st.session_state:
    st.session_state.selected_model = "llama4-maverick"

if "generated_copy" not in st.session_state:
    st.session_state.generated_copy = None

if "winning_pattern_result" not in st.session_state:
    st.session_state.winning_pattern_result = None

if "copy_generation_result" not in st.session_state:
    st.session_state.copy_generation_result = None

if "brief_result" not in st.session_state:
    st.session_state.brief_result = None

# =========================================================
# メインコンテンツ
# =========================================================

st.title("💡 広告企画支援")
st.markdown("過去の実績分析とAIによる企画立案支援で、次のキャンペーンを成功に導きます。")
st.markdown("---")

# =========================================================
# サイドバー設定
# =========================================================
st.sidebar.subheader("🤖 AI設定")

model_options = {
    "Llama 4 Maverick": "llama4-maverick",
    "Claude Sonnet 4.5": "claude-sonnet-4-5",
    "Claude Haiku 4.5": "claude-haiku-4-5",
    "OpenAI GPT-5": "openai-gpt-5",
    "OpenAI GPT-5 Mini": "openai-gpt-5-mini",
}

model_names = list(model_options.keys())
current_model_name = [k for k, v in model_options.items() if v == st.session_state.selected_model]
default_index = model_names.index(current_model_name[0]) if current_model_name else 0

selected_model_name = st.sidebar.selectbox("モデル選択", model_names, index=default_index)
st.session_state.selected_model = model_options[selected_model_name]

st.sidebar.info(f"""
**選択中の設定:**
- モデル: `{st.session_state.selected_model}`
""")

# データ取得
df = get_creative_data()

if df.empty:
    st.error("データが見つかりません。データベースの設定を確認してください。")
    st.stop()

# タブ構成
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 効果分析",
    "🏆 勝ちパターン",
    "✍️ コピー生成",
    "📋 ブリーフ作成"
])

# =========================================================
# タブ1: 効果分析
# =========================================================
with tab1:
    st.subheader("📈 訴求軸別・ターゲット別 効果分析")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🎯 ターゲット別パフォーマンス")
        
        target_stats = df.groupby('TARGET_SEGMENT').agg({
            'IMPRESSIONS': 'sum',
            'CLICKS': 'sum',
            'CONVERSIONS': 'sum',
            'SPEND': 'sum',
            'CVR': 'mean'
        }).reset_index()
        target_stats = target_stats.sort_values('CVR', ascending=True)
        
        if not target_stats.empty:
            fig_target = go.Figure(data=[
                go.Bar(
                    x=target_stats['CVR'].tolist(),
                    y=target_stats['TARGET_SEGMENT'].tolist(),
                    orientation='h',
                    marker_color='#3B82F6',
                    text=[f"{x:.2f}%" for x in target_stats['CVR'].tolist()],
                    textposition='outside'
                )
            ])
            
            fig_target.update_layout(
                height=400,
                margin=dict(l=20, r=80, t=20, b=20),
                xaxis_title="CVR (%)",
                yaxis_title=""
            )
            st.plotly_chart(fig_target, use_container_width=True)
    
    with col2:
        st.markdown("#### 📊 プラットフォーム × 指標マトリクス")
        
        platform_detail = df.groupby('PLATFORM').agg({
            'CTR': 'mean',
            'CVR': 'mean',
            'CPA': 'mean'
        }).reset_index()
        
        if not platform_detail.empty:
            fig_radar = go.Figure()
            
            colors = ['#3B82F6', '#10B981', '#F59E0B']
            for i, (_, row) in enumerate(platform_detail.iterrows()):
                cpa_score = max(0, 5 - (row['CPA'] / 500))
                fig_radar.add_trace(go.Scatterpolar(
                    r=[row['CTR'], row['CVR'], cpa_score],
                    theta=['CTR', 'CVR', 'CPA効率'],
                    fill='toself',
                    name=row['PLATFORM'].upper(),
                    marker_color=colors[i % len(colors)]
                ))
            
            fig_radar.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, 5])),
                showlegend=True,
                height=400,
                margin=dict(l=80, r=80, t=40, b=40)
            )
            st.plotly_chart(fig_radar, use_container_width=True)
    
    st.markdown("---")
    
    st.markdown("#### 🔘 CTA別パフォーマンス")
    
    cta_stats = df.groupby('CTA_TEXT').agg({
        'CLICKS': 'sum',
        'CONVERSIONS': 'sum',
        'CVR': 'mean'
    }).reset_index().sort_values('CVR', ascending=False).head(10)
    
    if not cta_stats.empty:
        fig_cta = go.Figure(data=[
            go.Bar(
                x=cta_stats['CTA_TEXT'].tolist(),
                y=cta_stats['CVR'].tolist(),
                marker_color='#10B981',
                text=[f"{x:.2f}%" for x in cta_stats['CVR'].tolist()],
                textposition='outside'
            )
        ])
        
        fig_cta.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=80),
            xaxis_title="",
            yaxis_title="CVR (%)",
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig_cta, use_container_width=True)

# =========================================================
# タブ2: 勝ちパターン（Fragment使用）
# =========================================================
with tab2:
    st.subheader("🏆 勝ちパターン分析")
    
    top_creatives = df.nlargest(10, 'CVR')
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### 📊 TOP 10 クリエイティブ")
        
        display_df = top_creatives[['CREATIVE_NAME', 'TARGET_SEGMENT', 'PLATFORM', 'CTR', 'CVR', 'CPA']].copy()
        display_df['CTR'] = display_df['CTR'].apply(lambda x: f"{x:.2f}%")
        display_df['CVR'] = display_df['CVR'].apply(lambda x: f"{x:.2f}%")
        display_df['CPA'] = display_df['CPA'].apply(lambda x: f"¥{x:,.0f}")
        display_df.columns = ['クリエイティブ名', 'ターゲット', 'PF', 'CTR', 'CVR', 'CPA']
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("#### 📈 平均値との比較")
        
        avg_ctr = df['CTR'].mean()
        avg_cvr = df['CVR'].mean()
        avg_cpa = df['CPA'].mean()
        
        top_avg_ctr = top_creatives['CTR'].mean()
        top_avg_cvr = top_creatives['CVR'].mean()
        top_avg_cpa = top_creatives['CPA'].mean()
        
        st.metric("CTR", f"{top_avg_ctr:.2f}%", f"{(top_avg_ctr - avg_ctr):+.2f}%")
        st.metric("CVR", f"{top_avg_cvr:.2f}%", f"{(top_avg_cvr - avg_cvr):+.2f}%")
        st.metric("CPA", f"¥{top_avg_cpa:,.0f}", f"¥{(top_avg_cpa - avg_cpa):+,.0f}", delta_color="inverse")
    
    st.markdown("---")
    
    @st.fragment
    def winning_pattern_analysis():
        """勝ちパターン分析（Fragmentで部分更新）"""
        if st.button("🔍 勝ちパターンをAI分析", type="primary"):
            with st.spinner("🤔 AIが分析中..."):
                top_info = []
                for _, row in top_creatives.iterrows():
                    top_info.append(f"- {row['CREATIVE_NAME']}: HL「{row.get('HEADLINE', '-')}」CTA「{row.get('CTA_TEXT', '-')}」ターゲット「{row['TARGET_SEGMENT']}」CVR {row['CVR']:.2f}%")
                
                analysis_prompt = f"""あなたは広告クリエイティブの分析専門家です。以下の高パフォーマンスクリエイティブ（TOP10）の共通要素を分析してください。

【高パフォーマンスクリエイティブ】
{chr(10).join(top_info)}

以下の観点で分析し、箇条書きで回答してください：

1. 【コピーの共通パターン】（3点）
2. 【効果的なターゲティング】（3点）
3. 【訴求軸の傾向】（3点）
4. 【次回キャンペーンへの提言】（3点）

日本語で回答してください。"""
                
                st.session_state.winning_pattern_result = call_cortex_complete(analysis_prompt, st.session_state.selected_model)
        
        if st.session_state.winning_pattern_result:
            with st.expander("📊 勝ちパターン分析結果", expanded=True):
                st.markdown(st.session_state.winning_pattern_result)
    
    winning_pattern_analysis()

# =========================================================
# タブ3: コピー生成（Fragment使用）
# =========================================================
with tab3:
    st.subheader("✍️ キャッチコピー生成")
    
    col1, col2 = st.columns(2)
    
    with col1:
        product_name = st.text_input("商品名/商品カテゴリ", placeholder="例: LEDデスクライト", key="copy_product")
        target_segment = st.text_input("ターゲットセグメント", placeholder="例: 在宅ワーカー30-40代", key="copy_target")
    
    with col2:
        appeal_point = st.text_input("訴求したいポイント", placeholder="例: 目の疲れ軽減", key="copy_appeal")
        tone = st.selectbox("トーン", ["機能的・信頼感", "感情的・共感", "カジュアル・親しみやすい", "高級感・プレミアム", "緊急性・限定感"], key="copy_tone")
    
    @st.fragment
    def copy_generation():
        """コピー生成（Fragmentで部分更新）"""
        if st.button("💡 コピーを生成", type="primary", key="generate_copy"):
            if product_name and target_segment:
                with st.spinner("🤔 AIがコピーを生成中..."):
                    copy_prompt = f"""あなたは広告コピーライターです。以下の条件で広告コピーを生成してください。

【条件】
- 商品: {product_name}
- ターゲット: {target_segment}
- 訴求ポイント: {appeal_point if appeal_point else '特になし'}
- トーン: {tone}

以下を生成してください：

【ヘッドライン案】（5案）
【ボディコピー案】（3案）
【CTA案】（3案）

日本語で回答してください。"""
                    
                    response = call_cortex_complete(copy_prompt, st.session_state.selected_model)
                    st.session_state.copy_generation_result = response
                    # ブリーフ作成タブで使えるようにセッションステートに保存
                    st.session_state.generated_copy = {
                        "product": product_name,
                        "target": target_segment,
                        "appeal": appeal_point,
                        "tone": tone,
                        "copy_result": response
                    }
            else:
                st.warning("商品名とターゲットセグメントを入力してください。")
        
        if st.session_state.copy_generation_result:
            st.success("コピーが生成されました！")
            with st.expander("✍️ 生成されたコピー案", expanded=True):
                st.markdown(st.session_state.copy_generation_result)
            st.download_button("📋 コピー案をダウンロード", st.session_state.copy_generation_result, "copy_ideas.txt", "text/plain")
            st.info("💡 生成したコピーは「ブリーフ作成」タブで活用できます。タブを切り替えて確認してください。")
    
    copy_generation()

# =========================================================
# タブ4: ブリーフ作成（Fragment使用）
# =========================================================
with tab4:
    st.subheader("📋 クリエイティブブリーフ作成")
    
    # コピー生成結果の連携表示（セッションステートを直接参照）
    if st.session_state.generated_copy:
        st.success("✅ コピー生成の結果を引き継いでブリーフを作成できます！")
        use_copy_result = st.checkbox("コピー生成の結果を活用する", value=True, key="use_copy_checkbox")
        
        if use_copy_result:
            st.info(f"📌 商品: {st.session_state.generated_copy['product']} | ターゲット: {st.session_state.generated_copy['target']}")
    else:
        st.info("💡 「コピー生成」タブでコピーを生成すると、ここで活用できます。")
        use_copy_result = False
    
    col1, col2 = st.columns(2)
    
    with col1:
        campaign_objective = st.selectbox("キャンペーン目的", ["売上向上", "認知拡大", "新規顧客獲得", "リピート促進", "アプリインストール"], key="brief_objective")
        
        # コピー生成結果があれば自動入力
        default_product = st.session_state.generated_copy['product'] if st.session_state.generated_copy and use_copy_result else ""
        default_target = st.session_state.generated_copy['target'] if st.session_state.generated_copy and use_copy_result else ""
        
        target_product = st.text_input("対象商品/カテゴリ", value=default_product, placeholder="例: 北欧インテリア全般", key="brief_product")
        target_audience = st.text_input("ターゲットオーディエンス", value=default_target, placeholder="例: 30-50代女性", key="brief_audience")
    
    with col2:
        budget_range = st.selectbox("予算規模", ["〜50万円", "50万〜100万円", "100万〜300万円", "300万〜500万円", "500万円〜"], key="brief_budget")
        platforms = st.multiselect("配信プラットフォーム", ["Google広告", "Meta広告", "LINE広告", "TikTok広告", "YouTube広告"], default=["Google広告", "Meta広告"], key="brief_platforms")
        campaign_period = st.text_input("キャンペーン期間", placeholder="例: 2025年1月〜2月", key="brief_period")
    
    additional_notes = st.text_area("その他の要望・注意事項", placeholder="例: 新春セールに合わせた訴求", height=100, key="brief_notes")
    
    @st.fragment
    def brief_generation():
        """ブリーフ生成（Fragmentで部分更新）"""
        if st.button("📝 ブリーフを生成", type="primary", key="generate_brief"):
            if target_product and target_audience:
                with st.spinner("🤔 AIがブリーフを作成中..."):
                    
                    copy_context = ""
                    if use_copy_result and st.session_state.generated_copy:
                        copy_context = f"""
【参考：生成済みコピー案】
{st.session_state.generated_copy['copy_result']}

上記のコピー案を参考に、ブリーフ内のコピー方向性を具体的に記載してください。
"""
                    
                    brief_prompt = f"""あなたは広告代理店のストラテジックプランナーです。以下の情報を元に、クリエイティブブリーフを作成してください。

【キャンペーン概要】
- 目的: {campaign_objective}
- 対象商品: {target_product}
- ターゲット: {target_audience}
- 予算: {budget_range}
- プラットフォーム: {', '.join(platforms)}
- 期間: {campaign_period if campaign_period else '未定'}
- 追加要望: {additional_notes if additional_notes else 'なし'}

{copy_context}

以下の形式でクリエイティブブリーフを作成してください：

# クリエイティブブリーフ

## 1. エグゼクティブサマリー
## 2. ターゲットインサイト
## 3. 推奨訴求軸
## 4. クリエイティブ方向性
## 5. コピー案（具体的なヘッドライン、ボディコピー、CTA）
## 6. KPI目標設定
## 7. 次のステップ

日本語で詳細に作成してください。"""
                    
                    st.session_state.brief_result = call_cortex_complete(brief_prompt, st.session_state.selected_model)
            else:
                st.warning("対象商品とターゲットオーディエンスを入力してください。")
        
        if st.session_state.brief_result:
            st.success("ブリーフが作成されました！")
            with st.expander("📋 クリエイティブブリーフ", expanded=True):
                st.markdown(st.session_state.brief_result)
            
            col_dl1, col_dl2 = st.columns(2)
            with col_dl1:
                st.download_button("📄 TXTダウンロード", st.session_state.brief_result, "creative_brief.txt", "text/plain")
            with col_dl2:
                st.download_button("📋 MDダウンロード", st.session_state.brief_result, "creative_brief.md", "text/markdown")
    
    brief_generation()

st.markdown("---")
st.caption("**GLACIER CREATIVE STUDIO** | 広告企画支援")
