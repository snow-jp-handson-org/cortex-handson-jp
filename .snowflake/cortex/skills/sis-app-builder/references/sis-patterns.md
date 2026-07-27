# SiS コードパターン集

Streamlit in Snowflake で使う定番パターン。

## セッション取得

```python
from snowflake.snowpark.context import get_active_session
import streamlit as st

@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()
```

SiS 内では `get_active_session()` で Snowpark セッションを取得する。ローカル実行は不要。

## データ取得 + キャッシュ

```python
@st.cache_data(ttl=600)
def get_data():
    session = get_session()
    return session.sql("SELECT * FROM MY_TABLE").to_pandas()
```

- `ttl=600` で 10 分間キャッシュ
- 重い集計は SQL 側でやる

## Cortex AI 関数呼び出し（SQL 経由）

```python
def call_ai_complete(prompt, model="llama4-maverick"):
    session = get_session()
    escaped = prompt.replace("'", "''")
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.AI_COMPLETE('{model}', '{escaped}') AS RESPONSE
    """).collect()
    return result[0]['RESPONSE']
```

AI_AGG の例:
```python
def call_ai_agg(column_expr, instruction, table, where="", limit=100):
    session = get_session()
    escaped = instruction.replace("'", "''")
    query = f"""
        SELECT AI_AGG({column_expr}, '{escaped}') AS RESULT
        FROM (SELECT {column_expr} FROM {table} {where} LIMIT {limit})
    """
    return session.sql(query).collect()[0]['RESULT']
```

## Plotly チャート

```python
import plotly.graph_objects as go

# 棒グラフ
fig = go.Figure(data=[go.Bar(
    x=df['CATEGORY'].tolist(),
    y=df['COUNT'].tolist(),
    marker_color='#3B82F6'
)])
fig.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=40))
st.plotly_chart(fig, use_container_width=True)

# 円グラフ
fig = go.Figure(data=[go.Pie(
    labels=df['LABEL'].tolist(),
    values=df['VALUE'].tolist(),
    hole=0.4
)])
st.plotly_chart(fig, use_container_width=True)
```

## KPI メトリクス

```python
col1, col2, col3 = st.columns(3)
col1.metric("総件数", f"{total:,}")
col2.metric("平均値", f"{avg:.1f}")
col3.metric("率", f"{rate:.1f}%")
```

## ページ設定

```python
st.set_page_config(layout="wide")
st.title("📊 ページタイトル")
st.markdown("---")
```

## マルチページ構成

`pages/` ディレクトリにファイルを配置:

```
main.py
pages/
  1_📊_ダッシュボード.py
  2_🖼️_分析.py
  3_📞_音声ログ.py
```

番号と絵文字でサイドバーの表示順と見た目を制御。

## environment.yml（依存管理）

```yaml
name: sf_env
channels:
  - snowflake
dependencies:
  - streamlit=1.51.0
  - plotly
  - pandas
  - snowflake-snowpark-python
```

## セッションステート

```python
if "selected_model" not in st.session_state:
    st.session_state.selected_model = "llama4-maverick"
```

ページ遷移しても値が維持される。

## AI 応答のエスケープ処理

```python
def clean_response(text):
    if not text:
        return ""
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    text = text.replace('\\n', '\n').replace('\\"', '"')
    return text
```
