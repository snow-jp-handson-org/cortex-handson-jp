# =========================================================
# GLACIER CREATIVE STUDIO
# クリエイティブ管理ページ
# =========================================================
# 概要: 広告クリエイティブのアセット管理と一覧表示
# =========================================================

import streamlit as st
import pandas as pd
import io
from datetime import datetime
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


def format_currency(value: float) -> str:
    """通貨フォーマット"""
    return f"¥{format_number(value)}"


def get_all_creative_data() -> pd.DataFrame:
    """すべての広告クリエイティブデータを取得（RAW + 分析済みフラグ付き）"""
    session = get_session()
    
    # RAWデータと分析済みデータを結合
    query = """
    SELECT 
        r.CREATIVE_ID,
        r.CAMPAIGN_ID,
        r.CREATIVE_NAME,
        r.CREATIVE_TYPE,
        r.COPY_TEXT,
        r.HEADLINE,
        r.CTA_TEXT,
        r.TARGET_SEGMENT,
        r.PLATFORM,
        COALESCE(g.IMPRESSIONS, r.IMPRESSIONS) AS IMPRESSIONS,
        COALESCE(g.CLICKS, r.CLICKS) AS CLICKS,
        COALESCE(g.CONVERSIONS, r.CONVERSIONS) AS CONVERSIONS,
        COALESCE(g.SPEND, r.SPEND) AS SPEND,
        COALESCE(g.CTR, ROUND(r.CLICKS / NULLIF(r.IMPRESSIONS, 0) * 100, 2)) AS CTR,
        COALESCE(g.CVR, ROUND(r.CONVERSIONS / NULLIF(r.CLICKS, 0) * 100, 2)) AS CVR,
        COALESCE(g.CPA, ROUND(r.SPEND / NULLIF(r.CONVERSIONS, 0), 0)) AS CPA,
        r.IMAGE_FILE_PATH,
        r.START_DATE,
        r.END_DATE,
        GET_PRESIGNED_URL(@DATA_STAGE, 'ad_images/' || r.IMAGE_FILE_PATH, 3600) AS IMAGE_URL,
        CASE WHEN g.CREATIVE_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_ANALYZED
    FROM RAW_AD_CREATIVES r
    LEFT JOIN GOLD_AD_CREATIVE_ANALYSIS g ON r.CREATIVE_ID = g.CREATIVE_ID
    ORDER BY r.CREATIVE_ID DESC
    """
    
    return session.sql(query).to_pandas()


def upload_image_to_stage(uploaded_file) -> str:
    """画像ファイルをステージにアップロード"""
    session = get_session()
    
    try:
        file_stream = io.BytesIO(uploaded_file.getvalue())
        file_name = uploaded_file.name
        
        session.file.put_stream(
            file_stream,
            f"@DATA_STAGE/ad_images/{file_name}",
            auto_compress=False,
            overwrite=True
        )
        
        return file_name
    except Exception as e:
        raise Exception(f"画像アップロードエラー: {str(e)}")


def insert_creative(creative_data: dict, image_file_path: str = None) -> tuple:
    """クリエイティブをRAW_AD_CREATIVESに登録"""
    session = get_session()
    
    try:
        # 新しいCREATIVE_IDを生成
        new_id = f"CRE-{datetime.now().strftime('%Y-%m%d%H%M%S')}"
        
        # エスケープ処理
        escaped_name = creative_data['creative_name'].replace("'", "''")
        escaped_headline = creative_data['headline'].replace("'", "''") if creative_data['headline'] else ''
        escaped_cta = creative_data['cta'].replace("'", "''") if creative_data['cta'] else ''
        escaped_copy = creative_data['copy_text'].replace("'", "''") if creative_data['copy_text'] else ''
        escaped_target = creative_data['target_segment'].replace("'", "''") if creative_data['target_segment'] else ''
        
        # INSERT文を実行
        insert_query = f"""
        INSERT INTO RAW_AD_CREATIVES (
            CREATIVE_ID,
            CAMPAIGN_ID,
            CREATIVE_NAME,
            CREATIVE_TYPE,
            PLATFORM,
            TARGET_SEGMENT,
            HEADLINE,
            CTA_TEXT,
            COPY_TEXT,
            IMAGE_FILE_PATH,
            IMPRESSIONS,
            CLICKS,
            CONVERSIONS,
            SPEND,
            START_DATE,
            END_DATE
        ) VALUES (
            '{new_id}',
            '{creative_data['campaign_id']}',
            '{escaped_name}',
            '{creative_data['creative_type']}',
            '{creative_data['platform']}',
            '{escaped_target}',
            '{escaped_headline}',
            '{escaped_cta}',
            '{escaped_copy}',
            {f"'{image_file_path}'" if image_file_path else 'NULL'},
            {creative_data['impressions']},
            {creative_data['clicks']},
            {creative_data['conversions']},
            {creative_data['spend']},
            CURRENT_DATE(),
            DATEADD(month, 1, CURRENT_DATE())
        )
        """
        
        session.sql(insert_query).collect()
        return True, new_id
        
    except Exception as e:
        return False, str(e)


# =========================================================
# メインコンテンツ
# =========================================================

st.title("📁 クリエイティブ管理")
st.markdown("広告クリエイティブのアセットを一覧管理します。")
st.markdown("---")

# タブ構成
tab_list, tab_create = st.tabs(["📋 一覧・管理", "➕ 新規作成"])

# =========================================================
# タブ1: 一覧・管理
# =========================================================
with tab_list:
    # データ取得
    df = get_all_creative_data()
    
    if df.empty:
        st.warning("データが見つかりません。新規作成タブからクリエイティブを登録してください。")
    else:
        st.subheader("🔍 フィルタリング")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            campaigns = ['すべて'] + sorted(df['CAMPAIGN_ID'].dropna().unique().tolist())
            selected_campaign = st.selectbox("キャンペーン", campaigns)
        
        with col2:
            platforms = ['すべて'] + sorted(df['PLATFORM'].dropna().unique().tolist())
            selected_platform = st.selectbox("プラットフォーム", platforms)
        
        with col3:
            types = ['すべて'] + sorted(df['CREATIVE_TYPE'].dropna().unique().tolist())
            selected_type = st.selectbox("タイプ", types)
        
        with col4:
            # 分析済み/未分析フィルタ
            analysis_filter = st.selectbox(
                "分析ステータス",
                ["すべて表示", "分析済みのみ", "未分析のみ"],
                help="GOLDテーブルに存在するかどうかでフィルタリング"
            )
        
        # ソート選択
        sort_options = {
            'CVR（高→低）': ('CVR', False),
            'CVR（低→高）': ('CVR', True),
            'CTR（高→低）': ('CTR', False),
            'CTR（低→高）': ('CTR', True),
            'CPA（低→高）': ('CPA', True),
            'CPA（高→低）': ('CPA', False),
            '費用（高→低）': ('SPEND', False),
            '費用（低→高）': ('SPEND', True),
            '登録日（新→古）': ('START_DATE', False),
        }
        selected_sort = st.selectbox("ソート", list(sort_options.keys()))
        
        # フィルタ適用
        filtered_df = df.copy()
        
        if selected_campaign != 'すべて':
            filtered_df = filtered_df[filtered_df['CAMPAIGN_ID'] == selected_campaign]
        
        if selected_platform != 'すべて':
            filtered_df = filtered_df[filtered_df['PLATFORM'] == selected_platform]
        
        if selected_type != 'すべて':
            filtered_df = filtered_df[filtered_df['CREATIVE_TYPE'] == selected_type]
        
        # 分析ステータスフィルタ
        if analysis_filter == "分析済みのみ":
            filtered_df = filtered_df[filtered_df['IS_ANALYZED'] == True]
        elif analysis_filter == "未分析のみ":
            filtered_df = filtered_df[filtered_df['IS_ANALYZED'] == False]
        
        sort_col, sort_asc = sort_options[selected_sort]
        if sort_col in filtered_df.columns:
            filtered_df = filtered_df.sort_values(sort_col, ascending=sort_asc, na_position='last')
        
        st.markdown("---")
        
        # アクションボタンと件数表示
        col_action1, col_action2, col_action3 = st.columns([1, 1, 2])
        
        with col_action1:
            csv = filtered_df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 CSVエクスポート",
                data=csv,
                file_name=f"creatives_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col_action2:
            if st.button("🔄 データ更新"):
                st.rerun()
        
        with col_action3:
            analyzed_count = len(filtered_df[filtered_df['IS_ANALYZED'] == True])
            st.markdown(f"**表示: {len(filtered_df)}件** (分析済み: {analyzed_count}件 / 未分析: {len(filtered_df) - analyzed_count}件)")
        
        st.markdown("---")
        
        # 表示形式選択
        view_mode = st.radio("表示形式", ["リスト表示", "グリッド表示（画像付き）"], horizontal=True)
        
        if view_mode == "グリッド表示（画像付き）":
            cols_per_row = 3
            rows = [filtered_df.iloc[i:i+cols_per_row] for i in range(0, len(filtered_df), cols_per_row)]
            
            for row_data in rows:
                cols = st.columns(cols_per_row)
                for i, (idx, row) in enumerate(row_data.iterrows()):
                    with cols[i]:
                        with st.container(border=True):
                            # 分析済みバッジ
                            if row['IS_ANALYZED']:
                                st.caption("✅ 分析済み")
                            else:
                                st.caption("⏳ 未分析")
                            
                            if 'IMAGE_URL' in row and row['IMAGE_URL'] and pd.notna(row['IMAGE_URL']):
                                try:
                                    st.image(row['IMAGE_URL'], use_container_width=True)
                                except Exception as e:
                                    st.info("📷 画像読込エラー")
                            else:
                                st.info("📷 画像なし")
                            
                            name = row['CREATIVE_NAME']
                            st.markdown(f"**{name[:25]}...**" if len(name) > 25 else f"**{name}**")
                            
                            platform = row['PLATFORM'].upper() if pd.notna(row['PLATFORM']) else '-'
                            creative_type = row['CREATIVE_TYPE'] if pd.notna(row['CREATIVE_TYPE']) else '-'
                            st.caption(f"📍 {platform} | {creative_type}")
                            
                            ctr = row['CTR'] if pd.notna(row['CTR']) else 0
                            cvr = row['CVR'] if pd.notna(row['CVR']) else 0
                            cpa = row['CPA'] if pd.notna(row['CPA']) else 0
                            st.caption(f"CTR: {ctr:.2f}% | CVR: {cvr:.2f}%")
                            st.caption(f"CPA: ¥{cpa:,.0f}")
        
        else:
            # リスト表示
            display_cols = ['CREATIVE_ID', 'CREATIVE_NAME', 'IS_ANALYZED', 'CREATIVE_TYPE', 'PLATFORM',
                           'TARGET_SEGMENT', 'IMPRESSIONS', 'CLICKS', 'CONVERSIONS',
                           'SPEND', 'CTR', 'CVR', 'CPA']
            
            available_cols = [c for c in display_cols if c in filtered_df.columns]
            display_df = filtered_df[available_cols].copy()
            
            if 'IS_ANALYZED' in display_df.columns:
                display_df['IS_ANALYZED'] = display_df['IS_ANALYZED'].apply(lambda x: "✅" if x else "⏳")
            if 'IMPRESSIONS' in display_df.columns:
                display_df['IMPRESSIONS'] = display_df['IMPRESSIONS'].apply(lambda x: format_number(x) if pd.notna(x) else "-")
            if 'CLICKS' in display_df.columns:
                display_df['CLICKS'] = display_df['CLICKS'].apply(lambda x: format_number(x) if pd.notna(x) else "-")
            if 'CONVERSIONS' in display_df.columns:
                display_df['CONVERSIONS'] = display_df['CONVERSIONS'].apply(lambda x: format_number(x) if pd.notna(x) else "-")
            if 'SPEND' in display_df.columns:
                display_df['SPEND'] = display_df['SPEND'].apply(lambda x: format_currency(x) if pd.notna(x) else "-")
            if 'CTR' in display_df.columns:
                display_df['CTR'] = display_df['CTR'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
            if 'CVR' in display_df.columns:
                display_df['CVR'] = display_df['CVR'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
            if 'CPA' in display_df.columns:
                display_df['CPA'] = display_df['CPA'].apply(lambda x: f"¥{x:,.0f}" if pd.notna(x) else "-")
            
            display_df.columns = [
                'ID', 'クリエイティブ名', '分析', 'タイプ', 'PF',
                'ターゲット', 'IMP', 'Click', 'CV',
                '費用', 'CTR', 'CVR', 'CPA'
            ][:len(available_cols)]
            
            st.dataframe(display_df, use_container_width=True, height=500, hide_index=True)
        
        st.markdown("---")
        
        # 詳細表示
        st.subheader("📋 クリエイティブ詳細")
        
        selected_for_detail = st.selectbox(
            "詳細を表示するクリエイティブを選択",
            options=filtered_df['CREATIVE_NAME'].tolist(),
            key="detail_select"
        )
        
        if selected_for_detail:
            detail_row = filtered_df[filtered_df['CREATIVE_NAME'] == selected_for_detail].iloc[0]
            
            with st.expander(f"📋 {selected_for_detail} の詳細", expanded=True):
                # 分析ステータス
                if detail_row['IS_ANALYZED']:
                    st.success("✅ このクリエイティブは分析済みです（GOLDテーブルに存在）")
                else:
                    st.warning("⏳ このクリエイティブは未分析です（RAWデータのみ）")
                
                col1, col2, col3 = st.columns([1, 1, 1])
                
                with col1:
                    st.markdown("**基本情報**")
                    st.write(f"ID: {detail_row['CREATIVE_ID']}")
                    st.write(f"キャンペーン: {detail_row['CAMPAIGN_ID']}")
                    st.write(f"タイプ: {detail_row['CREATIVE_TYPE']}")
                    platform = detail_row['PLATFORM'].upper() if pd.notna(detail_row['PLATFORM']) else '-'
                    st.write(f"プラットフォーム: {platform}")
                    st.write(f"ターゲット: {detail_row['TARGET_SEGMENT']}")
                
                with col2:
                    st.markdown("**パフォーマンス**")
                    impressions = detail_row['IMPRESSIONS'] if pd.notna(detail_row['IMPRESSIONS']) else 0
                    clicks = detail_row['CLICKS'] if pd.notna(detail_row['CLICKS']) else 0
                    conversions = detail_row['CONVERSIONS'] if pd.notna(detail_row['CONVERSIONS']) else 0
                    st.metric("インプレッション", format_number(impressions))
                    st.metric("クリック", format_number(clicks))
                    st.metric("コンバージョン", format_number(conversions))
                
                with col3:
                    st.markdown("**効率指標**")
                    ctr = detail_row['CTR'] if pd.notna(detail_row['CTR']) else 0
                    cvr = detail_row['CVR'] if pd.notna(detail_row['CVR']) else 0
                    cpa = detail_row['CPA'] if pd.notna(detail_row['CPA']) else 0
                    st.metric("CTR", f"{ctr:.2f}%")
                    st.metric("CVR", f"{cvr:.2f}%")
                    st.metric("CPA", f"¥{cpa:,.0f}")
        
        st.markdown("---")
        
        # 統計サマリー
        st.subheader("📊 フィルタ結果サマリー")
        
        summary_cols = st.columns(5)
        
        with summary_cols[0]:
            st.metric("クリエイティブ数", f"{len(filtered_df)}件")
        
        with summary_cols[1]:
            avg_ctr = filtered_df['CTR'].mean() if 'CTR' in filtered_df.columns else 0
            st.metric("平均CTR", f"{avg_ctr:.2f}%" if pd.notna(avg_ctr) else "-")
        
        with summary_cols[2]:
            avg_cvr = filtered_df['CVR'].mean() if 'CVR' in filtered_df.columns else 0
            st.metric("平均CVR", f"{avg_cvr:.2f}%" if pd.notna(avg_cvr) else "-")
        
        with summary_cols[3]:
            avg_cpa = filtered_df['CPA'].mean() if 'CPA' in filtered_df.columns else 0
            st.metric("平均CPA", f"¥{avg_cpa:,.0f}" if pd.notna(avg_cpa) else "-")
        
        with summary_cols[4]:
            total_spend = filtered_df['SPEND'].sum() if 'SPEND' in filtered_df.columns else 0
            st.metric("総費用", format_currency(total_spend) if pd.notna(total_spend) else "-")

# =========================================================
# タブ2: 新規作成
# =========================================================
with tab_create:
    st.subheader("➕ 新規クリエイティブ登録")
    st.info("💡 新しい広告クリエイティブをRAW_AD_CREATIVESに登録します。登録後は「一覧・管理」タブで確認できます（「🔄 データ更新」をクリック）。")
    
    # 画像アップロード
    st.markdown("#### 📷 クリエイティブ画像")
    uploaded_file = st.file_uploader(
        "画像ファイルをアップロード",
        type=['png', 'jpg', 'jpeg', 'gif', 'webp'],
        help="PNG, JPG, JPEG, GIF, WEBP形式に対応"
    )
    
    if uploaded_file:
        col_preview, col_info = st.columns([1, 2])
        with col_preview:
            st.image(uploaded_file, caption=uploaded_file.name, use_container_width=True)
        with col_info:
            st.write(f"**ファイル名:** {uploaded_file.name}")
            st.write(f"**ファイルサイズ:** {uploaded_file.size / 1024:.1f} KB")
    
    st.markdown("---")
    
    st.markdown("#### 📝 基本情報")
    
    # キャンペーン一覧を取得
    try:
        df_campaigns = get_all_creative_data()
        existing_campaigns = sorted(df_campaigns['CAMPAIGN_ID'].dropna().unique().tolist()) if not df_campaigns.empty else []
    except:
        existing_campaigns = []
    
    col1, col2 = st.columns(2)
    
    with col1:
        new_creative_name = st.text_input("クリエイティブ名 *", placeholder="例: 春の新生活_メイン")
        
        if existing_campaigns:
            new_campaign_id = st.selectbox("キャンペーン *", existing_campaigns)
        else:
            new_campaign_id = st.text_input("キャンペーンID *", placeholder="例: CMP-2025-SPRING01")
        
        new_creative_type = st.selectbox("クリエイティブタイプ *", ["image", "video", "carousel"])
        new_platform = st.selectbox("プラットフォーム *", ["google", "meta", "line"])
    
    with col2:
        new_target_segment = st.text_input("ターゲットセグメント *", placeholder="例: 新生活準備層20-35歳")
        new_headline = st.text_input("ヘッドライン", placeholder="例: 新生活を彩るインテリア")
        new_cta = st.text_input("CTA", placeholder="例: 今すぐチェック")
    
    new_copy_text = st.text_area(
        "ボディコピー",
        placeholder="例: この春、新しい生活を始めるあなたへ。グレイシャースタイルが厳選した北欧デザインのインテリアで、心地よい空間づくりを。",
        height=100
    )
    
    st.markdown("---")
    st.markdown("#### 📊 パフォーマンス初期値")
    
    perf_cols = st.columns(4)
    with perf_cols[0]:
        new_impressions = st.number_input("インプレッション", min_value=0, value=0)
    with perf_cols[1]:
        new_clicks = st.number_input("クリック", min_value=0, value=0)
    with perf_cols[2]:
        new_conversions = st.number_input("コンバージョン", min_value=0, value=0)
    with perf_cols[3]:
        new_spend = st.number_input("費用（円）", min_value=0, value=0)
    
    st.markdown("---")
    
    if st.button("✅ クリエイティブを登録", type="primary"):
        if new_creative_name and new_target_segment and new_campaign_id:
            
            # 画像をステージにアップロード
            image_file_path = None
            if uploaded_file:
                with st.spinner("📷 画像をアップロード中..."):
                    try:
                        image_file_path = upload_image_to_stage(uploaded_file)
                        st.success(f"画像「{image_file_path}」をアップロードしました。")
                    except Exception as e:
                        st.warning(f"画像アップロードに失敗しました: {str(e)}")
            
            # クリエイティブデータを登録
            creative_data = {
                'creative_name': new_creative_name,
                'campaign_id': new_campaign_id,
                'creative_type': new_creative_type,
                'platform': new_platform,
                'target_segment': new_target_segment,
                'headline': new_headline,
                'cta': new_cta,
                'copy_text': new_copy_text,
                'impressions': new_impressions,
                'clicks': new_clicks,
                'conversions': new_conversions,
                'spend': new_spend
            }
            
            success, result = insert_creative(creative_data, image_file_path)
            
            if success:
                st.success(f"✅ クリエイティブ「{new_creative_name}」を登録しました！（ID: {result}）")
                st.balloons()
                
                # 登録内容の確認
                calc_ctr = (new_clicks / new_impressions * 100) if new_impressions > 0 else 0
                calc_cvr = (new_conversions / new_clicks * 100) if new_clicks > 0 else 0
                calc_cpa = (new_spend / new_conversions) if new_conversions > 0 else 0
                
                with st.expander("📋 登録内容の確認", expanded=True):
                    st.markdown(f"""
                    **基本情報**
                    - クリエイティブID: {result}
                    - クリエイティブ名: {new_creative_name}
                    - キャンペーン: {new_campaign_id}
                    - タイプ: {new_creative_type}
                    - プラットフォーム: {new_platform}
                    - ターゲット: {new_target_segment}
                    
                    **コピー要素**
                    - ヘッドライン: {new_headline or '-'}
                    - CTA: {new_cta or '-'}
                    - ボディコピー: {new_copy_text[:50] if new_copy_text else '-'}...
                    
                    **画像**
                    - ファイル名: {image_file_path or 'なし'}
                    
                    **パフォーマンス予測**
                    - CTR: {calc_ctr:.2f}%
                    - CVR: {calc_cvr:.2f}%
                    - CPA: ¥{calc_cpa:,.0f}
                    """)
                
                st.info("💡 「一覧・管理」タブで「🔄 データ更新」ボタンをクリックすると、登録したクリエイティブが表示されます。")
            else:
                st.error(f"❌ 登録に失敗しました: {result}")
        else:
            st.warning("⚠️ クリエイティブ名、キャンペーンID、ターゲットセグメントは必須です。")

st.markdown("---")
st.caption("**GLACIER CREATIVE STUDIO** | クリエイティブ管理")
