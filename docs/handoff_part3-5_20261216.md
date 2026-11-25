# 12/16 ハンズオン Part 3〜5 引き継ぎ資料

作成: 2026-11-24（Shimada）　ブランチ: `pri_20261216`

Part 1・2（データの取り込みと AI 関数による加工）は完成しています。Part 3〜5（Cortex Search・セマンティックビュー・Cortex Agent）と、Dashboards・CoWork の手順は、まだ参加者向けの教材がありません。この資料は、その作成に必要な情報をまとめたものです。

- ユースケース・データ・検証結果の全体像は [`docs/usecase_and_data_20261216.html`](usecase_and_data_20261216.html) が正本です（以下「設計資料」）。この資料は、その中から Part 3〜5 に必要な部分と、まだ決まっていないことを抜き出しています。
- 設計資料のシナリオは、検証環境で Agent と CoWork まで動かして確認済みです（設計資料12章）。Part 3〜5 は、検証用の SQL（`tools/verify/`）を教材の形にする作業が中心になります。

## 1. 現在の状態

| ファイル | 状態 | 内容 |
| --- | --- | --- |
| `setup_20261216.sql` | 完成 | DB・スキーマ・ウェアハウス・ステージ・Git 連携・データのコピー・商品/顧客/注文の CSV ロードと列コメント・クロスリージョン推論・Snowflake Intelligence オブジェクトまで。再実行してもデータは消えない |
| `part1_20261216.ipynb` | 完成 | CoCo への質問、通話音声 98件の AI_TRANSCRIBE、AI_PARSE_DOCUMENT（PDF 11本・99ページ） |
| `part2_20261216.ipynb` | 完成 | AI_COMPLETE（構造化出力）で通話の6項目、AI_EXTRACT で PDF の属性を取り出し、AI_REDACT でマスクして検索用チャンク `KNOWLEDGE_CHUNKS` を作成。末尾に補足パート（時間が余った人向け） |
| `checkpoint_20261216.sql` | Part 1・2 のみ | 各パートの完了時点のテーブルを、検証済みの CSV（`data/checkpoint/`）から復元する |
| Part 3〜5 のノートブック | **未作成** | 旧教材の `part3_add_metadata.ipynb`・`part4_cortex_search.ipynb`・`part5_cortex_agent.ipynb` は旧データ向けで、そのままは使えない |
| Dashboards・CoWork の手順 | **未作成** | 作成プロンプトと共有手順は設計資料5・6章にある |

設計資料2章の「7つの価値」のうち、Part 3〜5 で見せるのは 3（属性で絞り込める検索）、4（Autopilot のセマンティックビュー）、5（Agent）、6（Dashboards と CoWork）です。

## 2. Part 3 が受け取るテーブル

テーブルはメダリオンアーキテクチャの名前で説明しています（Part 1・2 のノートブックの記述と合わせてください）。

| 層 | テーブル | 作成 | 件数 |
| --- | --- | --- | --- |
| Bronze | `RAW_CALL_TRANSCRIPTS` / `RAW_DOCUMENTS` | Part 1 | 98 / 99ページ |
| Silver | `DIM_PRODUCTS` / `DIM_CUSTOMERS` / `FACT_ORDERS` | setup | 55 / 300 / 2,913 |
| Silver | `CALL_ANALYSIS` / `DOC_ATTRIBUTES` | Part 2 | 98 / 11 |
| Gold | `KNOWLEDGE_CHUNKS` | Part 2 | 200 |
| Gold | セマンティックビュー | Part 4（予定） | — |

- 場所: `GLACIERSTYLE_DB.EC_ANALYTICS_SCHEMA`、ウェアハウス `GLACIERSTYLE_WH`
- `KNOWLEDGE_CHUNKS` の列: `CHUNK_ID, CHUNK_TEXT, TITLE, DOC_TYPE, DEPARTMENT, AUDIENCE, DOC_DATE, PRODUCT_CATEGORY, PRODUCT_IDS (ARRAY), SENTIMENT`。列の意味と取り得る値は設計資料10章を参照してください
- `CALL_ANALYSIS` の列: `CALL_ID, CALL_DATETIME, CUSTOMER_ID, ORDER_ID, PRODUCT_ID, INQUIRY_TYPE, PRODUCT_NAME, PRODUCT_CATEGORY, SENTIMENT, CHURN_RISK, SUMMARY, TRANSCRIPT_MASKED`（98件中10件は注文・商品とつながらない通話で、`ORDER_ID`・`PRODUCT_ID` が NULL）
- `RAW_CALL_TRANSCRIPTS` の列: `CALL_ID, CALL_DATETIME, CUSTOMER_ID, ORDER_ID, PRODUCT_ID, AGENT_ID, CALL_DURATION_SEC, TRANSCRIPT_TEXT`（本文は AI_TRANSCRIBE の結果そのままで、話者の区別なし）
- 返品は `FACT_ORDERS.order_status = 'returned'`（キャンセルは `'cancelled'`）、ロットは `FACT_ORDERS.lot_no`、商品カテゴリ（照明など）は `DIM_PRODUCTS.category_l2` です
- Part 2 の補足パートで作る `SUP_*`・`RAW_SNS_MENTIONS`・`RAW_AD_CREATIVES` は、Part 3 以降で使わないでください（補足は飛ばしてもよい設計のため）

## 3. 作るもの（設計資料からの抜き出し）

### Part 3: Cortex Search
- サービス `KNOWLEDGE_SEARCH` を1つ作る。定義は設計資料10章（`ATTRIBUTES` に属性列7つ）
- UC3: 同じ質問「返品できますか」を、フィルタだけ変えて3回 `SEARCH_PREVIEW` する（なし／社外公開／CS部×社内限定×通話以外）。③には `@not`（通話以外）が必要（設計資料4章）
- ダッシュボードのフィルタとの違い（設計資料4章の表）を説明に使う

### Part 4: セマンティックビュー
- 本番は Autopilot（画面操作）で作る想定
- **未検証**: 検証で使ったのは手書きのセマンティックビュー（`tools/verify/07_agent.sql` の `CS_QUALITY_SV`）です。Autopilot で作ったものがシーン4（ロット×解約リスク×会員ランク）に答えられるかは確認していません。手書き版で使った関係・指標・同義語（返品率、会員ランク、解約リスクなど）は、Autopilot の結果を見直すときの参考になります

### Part 5: Cortex Agent
- Agent「CS・品質アシスタント」に3つのツールを登録する（定義は `tools/verify/07_agent.sql`）
  - `CS_Quality_Analyst`: セマンティックビュー
  - `Knowledge_Search`: `columns_and_descriptions` で属性列を `filterable: true` にし、説明に取り得る値を書く。これで Agent が質問からフィルタを組み立てた
  - `Customer_FAQ_Search`: 同じサービスに `filter: {"@eq": {"AUDIENCE": "社外公開"}}` を固定
- UC4 の権限デモ（お客様窓口ロールでは FAQ ツールしか使えない）は**未検証**。ロールの作成と権限付与の SQL はまだない

### Dashboards と CoWork（UC1・UC2）
- ダッシュボードは Snowsight の Workspaces で CoCo に作らせる。作成プロンプトは設計資料6章（DB 名は参加者環境の `GLACIERSTYLE_DB` に読み替える）
- CoWork でのシーン1〜4の質問と、期待する回答・トレースは設計資料5章と12章
- artifact の共有には `MANAGE ACCESS ON DASHBOARD ARTIFACTS` 権限が必要。CoCo Desktop からはダッシュボードの artifact を作れない

### checkpoint の追加
`checkpoint_20261216.sql` は Part 2 まで。Part 3〜5 で作るオブジェクト（Search サービス・セマンティックビュー・Agent）の復元手順を足す場合は、同じファイルに「Part 3 完了時点」などの節を追加してください。テーブルの復元は、Git の `data/checkpoint/*.csv` からロードする方式です（AI 関数を再実行しない）。

## 4. 再利用できる検証用の資産

`tools/verify/` に、設計資料の検証で使ったスクリプトがあります。

| ファイル | 内容 | 再利用時の注意 |
| --- | --- | --- |
| `05_search.sql` | `KNOWLEDGE_CHUNKS` と `KNOWLEDGE_SEARCH` の作成 | チャンクの作成は Part 2 に移したので、Part 3 で使うのはサービスの作成部分だけ |
| `06_search_tests.py` | UC3 の3通りと、シーン1〜3の固定フィルタでの検索 | フィルタの JSON をそのまま教材に使える |
| `07_agent.sql` | セマンティックビュー（手書き）と Agent の定義 | 上記のとおり |
| `08_agent_tests.py` | シーン1〜4を `DATA_AGENT_RUN` で実行し、ツールとフィルタを記録 | 結果は `agent_results.json` |

**注意**: これらは検証用 DB `GLACIERSTYLE_DEC_DB` を指しています。テーブル名は11/24に新しい名前に書き換えましたが、書き換え後の再実行はしていません。教材にするときは `GLACIERSTYLE_DB` に読み替え、動作を確認してください。

## 5. 教材の書き方（Part 1・2 に合わせる場合）

- ノートブックの構成: はじめに（前提条件・処理内容・作るもの・料金の注意）→ 環境設定セル → 節ごとに「説明 → 図解 → SQL」→ データ確認 → まとめ（作ったもの・使った機能・次のステップ）
- 会話・説明・コメント・ログはすべて日本語（リポジトリ直下の `AGENTS.md`）
- SQL セルは、メタデータ（`language: sql`、`name` と `resultVariableName` に同じ値）で表します。セルの本文に `%%sql` は書きません
- Part 1・2 は、生成スクリプト `/tmp/build_notebooks_20261216.py` で ipynb を書き出しています。このスクリプトはまだリポジトリに入っていません（Shimada の手元のみ）。必要なら共有します
- ノートブックをエディタで開いたまま別の手段で書き換えると、エディタの保存で古い版に戻ることがあります。生成や一括編集の前に、ノートブックを閉じてください

## 6. 検証でわかった注意点

- **クロスリージョン推論**: `setup_20261216.sql` で `CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'` を設定しています。検証環境もこの設定で動かしました（設定しない場合の Agent の動作は未確認）
- **データの日付**: データの終わり（2026年11月30日）が検証日より先だったため、Agent が「第2報の発行日が今日より後」と注意書きを付けました。12/16 の本番では問題になりません。ドライランで気になる場合は、Agent の指示に「本日は2026年12月16日として扱う」を足します（設計資料12章）
- **トレースの時刻**: AI Observability のトレースは UTC で記録されます。日本時間で絞り込むと0件になります
- **画像を扱うモデル**: 東京リージョンでは、Claude 系のモデルに画像を渡すと空文字が返りました（`openai-gpt-5.4-mini` は動作）。Part 3〜5 で画像を使う場合は注意してください
- **AI クレジット**: Part 1・2 の全件処理は、検証の再実行を含めて 1.63 クレジット、Part 2 の補足は約 0.62 クレジットでした。全体の予算は $40 以内です（設計資料1章）

## 7. 未確定事項（設計資料13章のうち Part 3〜5 に関係するもの）

- Autopilot で作ったセマンティックビューが、シーン4に答えられるか
- ロールを切り替えた権限デモ（UC4）の手順と、ロールの作成 SQL
- 当日のトライアルアカウントで Snowflake Dashboards と CoWork の artifact が使えるか
- Dashboards のチャートアクションと、参加者向けの短いプロンプトで同等のダッシュボードができるか
- 「商品カテゴリ別 返品率」の折れ線で、照明の上昇が目立つようにする工夫
- CoWork の「壁打ちの入り口」の質問（設計資料5章）が想定どおりに答えるか
- 後片付け用の SQL（旧教材の `cleanup.sql` に相当するもの）がない

## 8. 問い合わせ先

- Part 1・2、データ、設計資料: Shimada
- 認識合わせの相手: Jia さん
