# AGENTS.md - GlacierStyle EC Analytics ハンズオン

## プロジェクト概要

GlacierStyle（架空のECサイト）のデータを使った Snowflake Cortex AI ハンズオンコンテンツです。
データの取り込みからAI分析、Cortex Search / Cortex Agent の構築までを5つのノートブックで段階的に体験します。

## 言語に関する注意事項

このプロジェクトは**日本語話者向けのハンズオン**です。以下の点を厳守してください：

- **会話・応答はすべて日本語**で行うこと
- **コード内のコメントは日本語**で記述すること
- **処理ログ・ステータスメッセージは日本語**で出力すること（例: `'【Step 1】環境設定が完了しました'`）
- **エラーメッセージの説明やトラブルシューティングも日本語**で行うこと
- **マークダウンセルの説明文は日本語**で記述すること
- SQL文やPythonコードのキーワード・関数名はそのままで構いませんが、**説明やコメントは日本語**にすること
- **ユーザーへの追加の質問案（提案・選択肢）も日本語**で提示すること

## プロジェクト構成

```
/
├── setup.sql                    # 環境セットアップ（DB/スキーマ/ステージ/Git連携/SiS/Intelligence）
├── part1_data_ingest.ipynb      # データ取り込み（CSV/JSON/PDF/音声）
├── part2_data_process.ipynb     # AI関数によるデータ加工・変換
├── part3_add_metadata.ipynb     # メタデータ自動付与・セマンティックビュー作成
├── part4_cortex_search.ipynb    # Cortex Search サービスの構築
├── part5_cortex_agent.ipynb     # Cortex Agent の構築
├── streamlit_app/               # Streamlit in Snowflake アプリ
├── data/                        # データファイル
├── images/                      # ハンズオン用画像
└── .snowflake/cortex/skills/    # Cortex Code スキル
```

## Snowflake環境

- **データベース**: `GLACIERSTYLE_DB`
- **スキーマ**: `EC_ANALYTICS_SCHEMA`
- **ウェアハウス**: `GLACIERSTYLE_WH`
- **ステージ**: `DATA_STAGE`（データファイル）, `EXTRACTED_IMAGES_STAGE`（抽出画像）
