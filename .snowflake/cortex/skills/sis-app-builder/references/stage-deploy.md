# ステージ経由デプロイ（フォールバック）

Workspace 上で直接作成できない場合、ステージ経由で SiS アプリを作成する。

## 手順

### 1. ステージ作成

```sql
CREATE STAGE IF NOT EXISTS MY_STAGE
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
```

### 2. ファイルをステージにアップロード

Snowsight の Data > Databases > ステージを開き、ファイルをアップロード。
または SQL:

```sql
-- ローカルから PUT する場合（SnowSQL / snow CLI が必要）
PUT file:///path/to/main.py @MY_STAGE/app/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/environment.yml @MY_STAGE/app/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

### 3. STREAMLIT オブジェクト作成

```sql
CREATE OR REPLACE STREAMLIT MY_APP
  FROM '@MY_DB.MY_SCHEMA.MY_STAGE/app/'
  MAIN_FILE = 'main.py'
  QUERY_WAREHOUSE = MY_WH;
```

### 4. ライブバージョンに反映

```sql
ALTER STREAMLIT MY_APP ADD LIVE VERSION FROM LAST;
```

これを実行しないと USAGE 権限のユーザからは見えない。

### 5. ファイル更新時

ファイルを再度 PUT して上書きした後:

```sql
ALTER STREAMLIT MY_APP ADD LIVE VERSION FROM LAST;
```

## 注意点

- `AUTO_COMPRESS=FALSE` を忘れると Python ファイルが圧縮されてエラーになる
- `pages/` ディレクトリ内のファイルも個別に PUT が必要
- `environment.yml` はエントリポイントと同じディレクトリに配置する
