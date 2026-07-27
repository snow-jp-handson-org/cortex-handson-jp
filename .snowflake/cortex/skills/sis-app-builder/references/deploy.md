# SiS アプリのデプロイ

## 方法 1: Workspace → ステージ経由（CoCo Snowsight 標準）

CoCo でコードを編集した後、SQL だけで完結するデプロイフロー。

### 1. デプロイ用ステージ作成

```sql
CREATE STAGE IF NOT EXISTS <DB>.<SCHEMA>.SIS_DEPLOY_STAGE
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
```

### 2. Workspace からステージへコピー

```sql
COPY FILES INTO @<DB>.<SCHEMA>.SIS_DEPLOY_STAGE/
FROM 'snow://workspace/<WORKSPACE_NAME>/versions/live/<ソースディレクトリ>/'
PATTERN = '.*';
```

- `<WORKSPACE_NAME>` は Workspace 名（例: `USER$.PUBLIC."cortex-handson-jp"`）
- `<ソースディレクトリ>` はアプリのルートディレクトリ（例: `streamlit_app`）
- `PATTERN = '.*'` で全ファイル（`pages/`, `environment.yml` 等）を一括コピー

### 3. STREAMLIT オブジェクト作成

```sql
CREATE OR REPLACE STREAMLIT <アプリ名>
  FROM @<DB>.<SCHEMA>.SIS_DEPLOY_STAGE
  MAIN_FILE = 'main.py'
  QUERY_WAREHOUSE = <ウェアハウス>
  COMMENT = 'アプリの説明';
```

### 4. コード更新時の再デプロイ

ステージをクリーンにしてから再コピー:

```sql
REMOVE @<DB>.<SCHEMA>.SIS_DEPLOY_STAGE;

COPY FILES INTO @<DB>.<SCHEMA>.SIS_DEPLOY_STAGE/
FROM 'snow://workspace/<WORKSPACE_NAME>/versions/live/<ソースディレクトリ>/'
PATTERN = '.*';

CREATE OR REPLACE STREAMLIT <アプリ名>
  FROM @<DB>.<SCHEMA>.SIS_DEPLOY_STAGE
  MAIN_FILE = 'main.py'
  QUERY_WAREHOUSE = <ウェアハウス>;
```

## 方法 2: Git リポジトリから直接デプロイ

Git Integration が設定済みで、コードがコミット済みの場合。

```sql
CREATE OR REPLACE STREAMLIT <アプリ名>
  FROM @<GIT_REPO>/branches/<ブランチ>/<ソースディレクトリ>
  MAIN_FILE = 'main.py'
  QUERY_WAREHOUSE = <ウェアハウス>
  COMMENT = 'アプリの説明';
```

### Git Integration の確認

```sql
SHOW GIT REPOSITORIES IN SCHEMA;
LIST @<GIT_REPO>/branches/main/;
```

### コード更新を反映

```sql
ALTER GIT REPOSITORY <GIT_REPO> FETCH;
CREATE OR REPLACE STREAMLIT <アプリ名>
  FROM @<GIT_REPO>/branches/<ブランチ>/<ソースディレクトリ>
  MAIN_FILE = 'main.py'
  QUERY_WAREHOUSE = <ウェアハウス>;
```

## 注意点

- Workspace → ステージ方式では `COPY FILES INTO` が `pages/` 配下も含めて一括コピーする
- Git 経由の場合、`ALTER GIT REPOSITORY FETCH` でリモートの最新を取得してから再デプロイ
- `CREATE OR REPLACE STREAMLIT` は既存アプリを置き換える（アトミック操作）
