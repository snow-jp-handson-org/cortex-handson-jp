---
name: sis-app-builder
description: "Streamlit in Snowflake (SiS) アプリの構築・更新。Workspace上のソースコードを探索し、新規ページ追加やCortex AI関数の組み込みを支援。Triggers: SiS, Streamlit, アプリ, ページ追加, ダッシュボード, 可視化, streamlit app"
---

# SiS アプリビルダー

Streamlit in Snowflake アプリを Workspace 上で構築・更新するスキル。

## ワークフロー

```
Step 1: ソースコード探索
    ↓
Step 2: 新規 or 更新を確認
    ↓
Step 3: コード生成・編集
    ↓
Step 4: 動作確認
```

### Step 1: ソースコード探索

**目的**: 対象の SiS アプリのソースコードを Workspace 上で見つける。

1. 既存アプリがあるか確認:
   ```sql
   SHOW STREAMLIT IN SCHEMA;
   ```
2. 対象アプリの詳細を確認:
   ```sql
   DESCRIBE STREAMLIT <アプリ名>;
   ```
3. ソースファイルの一覧を確認:
   ```sql
   -- embedded stage の場合
   LIST @<DB>.<SCHEMA>.%<アプリ名>;
   ```
4. Workspace（Projects > Streamlit）でソースコードを開いて読む

**見つからない場合**: ユーザに以下を質問する:
- アプリ名は何か？
- どのデータベース・スキーマに作りたいか？
- 新規作成か、既存アプリの修正か？

### Step 2: 新規 or 更新を確認

⚠️ **必ずユーザに確認してから進む**

| パターン | 進め方 |
|---------|--------|
| **既存アプリに新ページ追加** | `pages/` ディレクトリに新ファイルを作成 |
| **既存ページの修正** | 対象ファイルを直接編集 |
| **新規アプリ作成** | エントリポイント + 設定ファイルを一式作成 |

### Step 3: コード生成・編集

コードを書く前に `references/sis-patterns.md` を読み込み、SiS 固有のパターンに従う。

**新規ページ追加の場合**:
- ファイル名: `pages/<番号>_<絵文字>_<名前>.py` (例: `pages/6_📞_音声ログ分析.py`)
- 既存ページのスタイル（色パレット、レイアウト）に合わせる
- `get_active_session()` でセッション取得
- Plotly でチャート描画

**新規アプリ作成の場合**:

Workspace 上で直接作成を試みる:
1. Snowsight → Projects → Streamlit → + Streamlit App
2. エントリポイント（`main.py`）を作成
3. `environment.yml` で依存パッケージを定義
4. `.streamlit/config.toml` でテーマ設定

Workspace 直接作成がうまくいかない場合は `references/stage-deploy.md` を読み込んでステージ経由で作成する。

**Cortex AI 関数を使う場合**:

以下のスキルと連携する:

| やりたいこと | スキル |
|---|---|
| AI関数 (AI_COMPLETE, AI_AGG 等) | `$cortex-ai-functions` |
| データ品質チェック | `$data-quality` |
| リネージ確認 | `$lineage` |
| コスト分析 | `$cost-intelligence` |
| ガバナンス | `$data-governance` |
| ML モデル連携 | `$machine-learning` |

### Step 4: 動作確認

1. Streamlit エディタで「Run」をクリック
2. エラーがあれば修正
3. 全ページが正常に動作することを確認

## 停止ポイント

- ⚠️ Step 2: 新規 or 更新をユーザに確認する前に進まない
- ⚠️ Step 3: コードを編集する前にユーザに方針を提示して承認を得る
