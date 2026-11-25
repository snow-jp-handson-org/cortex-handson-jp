# データ変更ログ（12/16 ハンズオン向け）

ブランチ: `develop_20261216_kshimada` → `pri_20261216`
比較対象: `main`（前回 10/7 ハンズオンの版）

## 削除したデータ

| 日付 | ファイル | 前回の用途 | 削除した理由 | 復元方法 |
|---|---|---|---|---|
| 2026-11-20 | `data/web_logs.json`（約7.6MB） | Web アクセスログ（`fact_web_logs`）。セマンティックビューと Streamlit の分析で使用 | 構造化データで、Cortex AI 関数の補足パートにならない。2024年のデータで、商品ID（0/371）・顧客ID（0/60）が新しいデータと一致しない | `git checkout main -- data/web_logs.json` |
| 2026-11-20 | `data/payments.csv`（360件） | クレジット決済（`fact_payments`）。セマンティックビューで使用 | 構造化データで、Cortex AI 関数と関係がない。注文ID（0/360）が新しい `orders.csv` と一致しない | `git checkout main -- data/payments.csv` |

判断の経緯: 再構成案でデータを「EC構造化・通話・PDF」の3種に絞る方針。SNS・広告・Web ログ・決済のうち、AI 関数の補足として価値がある SNS と広告だけを Part2 の補足パートとして残し、Web ログと決済は削除した（2026-11-20、Shimada 判断）。

影響: 前回版の `setup.sql`（COPY INTO）、`part1_data_ingest.ipynb`、`part3_add_metadata.ipynb`（セマンティックビュー）、`streamlit_app/` が、この2ファイルを参照している。これらは 12/16 版への改修で参照を外す。

## 補足パートとして残し、新しいデータに合わせたもの

| 日付 | ファイル | 変更内容 | スクリプト |
|---|---|---|---|
| 2026-11-20 | `data/sns_logs.json`（265件） | 日付を 2024-12 → 2026-10-15〜11-30 にずらした。言及商品（20種）を新しい商品マスタの近い商品名に置き換えた。LEDデスクライト 調光式 への言及のうち8件を「ちらつき」の声に差し替えた | `tools/convert_sns_logs.py` |
| 2026-11-20 | `data/ad_creatives.csv`（15件）、`data/ad_images/` | 配信期間とIDを 2026年（10〜11月）にずらした。商品名と画像は変更していない（画像と一体のため。新しい商品マスタにない商品も含む） | 手作業（日付を23か月後にずらす） |

## 通話を音声に一本化（2026-11-25）

| ファイル | 変更内容 | スクリプト |
|---|---|---|
| `data/voice_logs/CALL-yyyymmdd-nnnn.mp3`（88件、約14MB） | 通話の台本88件を macOS `say` で読み上げて追加。オペレーターは Kyoko、顧客は性別ごとに3声。英数字は読み方を補正（LED→エルイーディー など） | `tools/generate_voice.py` |
| `data/voice_logs/voice_logs_metadata.json`（10件 → 98件） | 88件分を追加し、全件に `order_id`・`product_id` を持たせた（既存10件は null）。問い合わせ種別の答えになる `category` と、使わない `scenario_id` を削除 | `tools/build_voice_metadata.py` |
| `data/call_transcripts.csv` → `tools/call_scripts.csv` | ハンズオンでは読まなくなったため、音声の台本として `tools/` へ移動（setup のコピー対象から外れる） | `tools/generate_data.py` の出力先も変更 |
| `data/checkpoint/raw_voice_logs.csv` | 削除し、`raw_call_transcripts.csv`（98件）を追加。`call_analysis.csv` から `SOURCE` 列を削除 | 東京の検証環境から書き出し |

判断の経緯: setup で CSV からロードしていた `RAW_CALL_TRANSCRIPTS` と、Part 1 で音声から作っていた `RAW_VOICE_LOGS` は、同じ「通話」を別々に持っていた（Jia さん指摘）。Part 1 で「音声 98件＋メタデータ JSON → AI_TRANSCRIBE → `RAW_CALL_TRANSCRIPTS`」に一本化し、`RAW_VOICE_LOGS` は廃止した。AI_COMPLETE で話者（オペレーター／顧客）を付ける整形も試したが、精度が変わらなかったため入れていない。Part 2 の `CALL_ANALYSIS` は AI_EXTRACT から AI_COMPLETE（構造化出力、`openai-gpt-5.4-mini`）に変更（2026-11-25、Shimada 判断）。

検証結果（東京）: 文字起こし 98件 約50秒、CALL_ANALYSIS 約30秒。問い合わせ種別 88/88、感情 63/88 が正解ラベルと一致。「商品不具合」28件のうち26件が LEDデスクライト 調光式 × LOT-2609-B（残り2件は注文とつながらない既存の音声サンプル）。音声にする前の「27件中26件」と同じ傾向。

## 以前の変更（2026-11-13〜16）

- `data/products.csv` 576件 → 55件、`customers.csv` 100件 → 300件、`orders.csv` 500件 → 2,913件（2026年6〜11月）に再生成（`tools/generate_data.py`）
- `data/call_transcripts.csv`（88件）、`data/documents/*.pdf`（品質報告・通達・店舗報告 9本）を追加（`tools/generate_pdfs.py`）
- `data/faq_document.pdf`、`data/operation_manual_w_images.pdf` の日付を 2026年に更新（`tools/update_pdf_dates.py`）
- `data/voice_logs/voice_logs_metadata.json` の日付を 2026年11月に更新
