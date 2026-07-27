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

### Step 1: ソースコード探索 & 状況把握

**目的**: 対象アプリのソースコードの場所と、デプロイ方式を特定する。

1. 既存アプリがあるか確認:
   ```sql
   SHOW STREAMLIT IN SCHEMA;
   ```
2. 既存アプリがある場合、詳細を確認:
   ```sql
   DESCRIBE STREAMLIT <アプリ名>;
   ```
   - `root_location` を見る — ソースの場所がわかる
3. ソースの場所に応じてファイル一覧を確認:
   ```sql
   LIST @<root_location で得たパス>;
   ```
4. Workspace（Projects > Streamlit）でソースコードを開いて読む

**見つからない場合**: ユーザに以下を質問する:
- アプリ名は何か？
- どのデータベース・スキーマに作りたいか？
- 新規作成か、既存アプリの修正か？
- ソースコードはどこにあるか？（Git リポジトリ / ステージ / Workspace 上で直接作成）

### Step 2: 新規 or 更新を確認

⚠️ **必ずユーザに確認してから進む**

| パターン | 進め方 |
|---------|--------|
| **既存アプリに新ページ追加** | `pages/` ディレクトリに新ファイルを作成 |
| **既存ページの修正** | 対象ファイルを直接編集 |
| **新規アプリ作成** | ソースの場所を確認してから作成 |

### Step 3: コード生成・編集

コードを書く前に `references/sis-patterns.md` を読み込み、SiS 固有のパターンに従う。

**新規ページ追加の場合**:
- ファイル名: `pages/<番号>_<絵文字>_<名前>.py` (例: `pages/6_📞_音声ログ分析.py`)
- 既存ページのスタイル（色パレット、レイアウト）に合わせる
- `get_active_session()` でセッション取得
- Plotly でチャート描画

**新規アプリ作成 / 既存アプリの再デプロイの場合**:

`references/deploy.md` を読み込む。Step 1 の結果に応じてデプロイ方法を判断:

| Step 1 でわかったこと | デプロイ方法 |
|---|---|
| Workspace 上にソースがある、かつ編集した可能性がある | Workspace → ステージ → CREATE STREAMLIT |
| `DESCRIBE STREAMLIT` の `root_location` が `@GIT_REPO/...` | Git 経由で再デプロイ |
| `SHOW GIT REPOSITORIES` で Git Integration が見つかった & コードがコミット済み | Git 経由で新規デプロイ |
| 不明 | **ユーザに聞く** |

> Workspace → ステージ経由が CoCo Snowsight での標準フロー。全て SQL で完結し手動操作は不要。

### Step 4: 動作確認

1. Streamlit エディタで「Run」をクリック
2. エラーがあれば修正
3. 全ページが正常に動作することを確認

## 停止ポイント

- ⚠️ Step 2: 新規 or 更新をユーザに確認する前に進まない
- ⚠️ Step 3: コードを編集する前にユーザに方針を提示して承認を得る
