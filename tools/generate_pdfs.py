"""
12/16 ハンズオン用 PDF と属性定義の生成スクリプト
- 品質報告書（品質管理部・社内限定）: quality_report_202610.pdf / quality_report_202611.pdf
- 特例対応通達（カスタマーサポート部・社内限定）: cs_notice_202610.pdf
- 店舗月次報告（店舗運営部・社内限定）: store_report_202606.pdf 〜 store_report_202611.pdf
- 属性の正解: tools/expected_document_attributes.csv（AI_EXTRACT の結果の確認用。参加者には配布しない）

日本語は reportlab の CID フォント（HeiseiKakuGo-W5）＋ wordWrap='CJK' で描画する。
実行: uv run --with reportlab python tools/generate_pdfs.py
"""
import csv
import random
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

random.seed(20261216)
ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "data" / "documents"
DOCS.mkdir(parents=True, exist_ok=True)

FONT = "HeiseiKakuGo-W5"
pdfmetrics.registerFont(UnicodeCIDFont(FONT))
H1 = ParagraphStyle("h1", fontName=FONT, fontSize=16, leading=22, spaceAfter=8, wordWrap="CJK")
H2 = ParagraphStyle("h2", fontName=FONT, fontSize=12, leading=18, spaceBefore=10, spaceAfter=4, wordWrap="CJK")
BODY = ParagraphStyle("body", fontName=FONT, fontSize=10, leading=16, wordWrap="CJK")
META = ParagraphStyle("meta", fontName=FONT, fontSize=9, leading=13, textColor=colors.grey, wordWrap="CJK")

DEFECT_PRODUCT_ID = "PRD-LGT-0001"
DEFECT_PRODUCT = "LEDデスクライト 調光式"
DEFECT_LOT = "LOT-2609-B"


def table(rows, widths=None):
    t = Table(rows, colWidths=widths)
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), FONT),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e0f2fe")),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def build(path, title, meta_lines, blocks):
    doc = SimpleDocTemplate(str(path), pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm,
                            topMargin=16 * mm, bottomMargin=16 * mm, title=title)
    story = [Paragraph(title, H1)] + [Paragraph(m, META) for m in meta_lines] + [Spacer(1, 6)]
    for kind, val in blocks:
        if kind == "h2":
            story.append(Paragraph(val, H2))
        elif kind == "p":
            story.append(Paragraph(val, BODY))
        elif kind == "table":
            story += [Spacer(1, 4), table(*val), Spacer(1, 4)]
    doc.build(story)


# ---------------------------------------------------------------------------
# 品質報告書
# ---------------------------------------------------------------------------
def quality_reports():
    build(DOCS / "quality_report_202610.pdf", "品質不具合 調査報告書（第1報）",
          ["文書番号: QA-2026-1015　発行日: 2026年10月15日　作成: 商品本部 品質管理部", "機密区分: 社内限定"],
          [("h2", "1. 概要"),
           ("p", f"2026年10月上旬より、{DEFECT_PRODUCT}（商品ID: {DEFECT_PRODUCT_ID}）について「点灯中に明かりがちらつく」「明るさが不安定になる」"
                 "という問い合わせと返品が急増している。カスタマーサポート部からのエスカレーションを受け、品質管理部で調査を行った。"),
           ("h2", "2. 調査結果"),
           ("p", f"返品回収品12台を分解調査した結果、全数でLEDドライバ基板のコンデンサに容量抜けを確認した。"
                 f"不具合品はすべて2026年9月入荷のロット {DEFECT_LOT} に属しており、それ以前のロット（LOT-2606-A）では同様の事象は確認されていない。"),
           ("table", ([["項目", "内容"],
                       ["対象商品", f"{DEFECT_PRODUCT}（{DEFECT_PRODUCT_ID}）"],
                       ["対象ロット", f"{DEFECT_LOT}（2026年9月入荷分）"],
                       ["推定原因", "仕入先工場でのLEDドライバ用コンデンサの部品変更（代替部品の耐熱不足）"],
                       ["症状", "点灯から10分程度でちらつき・明るさの変動。最大輝度で発生しやすい"],
                       ["安全性", "発煙・発火のおそれはない（耐熱試験で確認済み）"]], [40 * mm, 130 * mm])),
           ("h2", "3. 暫定対応"),
           ("p", f"(1) ロット {DEFECT_LOT} の出荷を2026年10月14日に停止した。(2) 仕入先 SUP-00022 に是正処置報告を要求した。"
                 "(3) カスタマーサポート部に特例対応を依頼した（通達 CS-2026-1016 参照）。"),
           ("h2", "4. 今後の予定"),
           ("p", "仕入先の是正処置完了後、対策品ロット（LOT-2611-C）の受入検査を実施し、11月中旬を目処に第2報で報告する。")])

    build(DOCS / "quality_report_202611.pdf", "品質不具合 調査報告書（第2報）",
          ["文書番号: QA-2026-1120　発行日: 2026年11月20日　作成: 商品本部 品質管理部", "機密区分: 社内限定"],
          [("h2", "1. 第1報からの更新"),
           ("p", f"{DEFECT_PRODUCT}（{DEFECT_PRODUCT_ID}）のロット {DEFECT_LOT} について、仕入先から是正処置報告を受領した。"
                 "原因は第1報の推定どおり、コンデンサの代替部品の耐熱不足であった。"),
           ("h2", "2. 対策品の受入検査"),
           ("table", ([["検査項目", "基準", "結果"],
                       ["連続点灯試験（72時間）", "ちらつき0件", "合格（0件/50台）"],
                       ["高温環境試験（40℃）", "輝度変動5%以内", "合格（最大2.1%）"],
                       ["調光動作試験", "全段階で正常", "合格"]], [60 * mm, 50 * mm, 60 * mm])),
           ("p", "対策品ロット LOT-2611-C は2026年11月25日より出荷を再開する。"),
           ("h2", "3. 影響範囲"),
           ("p", f"ロット {DEFECT_LOT} の販売数は約150台。10月〜11月の返品は累計で約40件、関連する問い合わせは25件を超えている。"
                 "同じお客様から繰り返し問い合わせを受けている事例があり、カスタマーサポート部と連携して個別にフォローする。"),
           ("h2", "4. 再発防止"),
           ("p", "仕入先での部品変更時に、事前の変更申請と当社での評価を必須とする。購買部と品質管理部で取引基本契約の改定を進める。")])


# ---------------------------------------------------------------------------
# 特例対応通達（UC3 で FAQ との違いを見せる文書）
# ---------------------------------------------------------------------------
def cs_notice():
    build(DOCS / "cs_notice_202610.pdf", f"【通達】{DEFECT_PRODUCT} 不具合に関する特例対応について",
          ["文書番号: CS-2026-1016　発行日: 2026年10月16日　作成: EC事業本部 カスタマーサポート部", "機密区分: 社内限定（お客様への開示不可）"],
          [("h2", "1. 背景"),
           ("p", f"品質管理部の調査報告（QA-2026-1015）により、{DEFECT_PRODUCT}（{DEFECT_PRODUCT_ID}）のロット {DEFECT_LOT} に"
                 "ちらつきの不具合があることが判明した。お客様の不安を最小限にするため、以下の特例対応を行う。"),
           ("h2", "2. 特例対応の内容"),
           ("table", ([["項目", "通常の対応（FAQ記載）", "特例対応（対象ロットのみ）"],
                       ["受付期限", "商品到着後14日以内", "期限なし（2027年3月31日まで）"],
                       ["対応方法", "返品・返金", "対策品への交換を優先。希望があれば返金"],
                       ["送料", "当社負担（着払い）", "当社負担。集荷手配も当社で行う"],
                       ["お詫び", "なし", "500円分のクーポンを付与"]], [32 * mm, 62 * mm, 76 * mm])),
           ("h2", "3. 対応手順"),
           ("p", "(1) お客様から注文番号を伺い、受注システムでロット番号を確認する。"
                 f"(2) ロットが {DEFECT_LOT} の場合は本通達の特例対応を案内する。それ以外のロットは通常の不良品対応とする。"
                 "(3) 同じお客様からの2回目以降の問い合わせは、リーダーにエスカレーションし、折り返し連絡とする。"),
           ("h2", "4. お客様への説明時の注意"),
           ("p", "原因の詳細（部品名・仕入先名）はお客様に開示しない。「製造工程の一部に問題があったため、改善した製品と交換させていただきます」と説明する。")])


# ---------------------------------------------------------------------------
# 店舗月次報告（UC5: PDF にしかない売上）
# ---------------------------------------------------------------------------
STORES = ["表参道店", "二子玉川店", "横浜店", "梅田店", "名古屋店", "福岡天神店"]
CATS = ["インテリア", "キッチン", "ライフスタイル雑貨", "アパレル"]
COMMENTS = {
    6: "梅雨入りで来店客数は前年並み。収納用品の動きが良い。",
    7: "夏のギフト需要でキッチン食器が好調。",
    8: "お盆期間の帰省客で横浜店・梅田店の来店が増加。",
    9: "秋の新商品の照明を店頭で展開し、デスクライトの指名買いが増えた。",
    10: "デスクライトについて「ちらつく」という店頭での相談が複数あった。品質管理部へ報告済み。",
    11: "ブラックフライデーセールで全店売上が前月比約1.4倍。アパレル小物（マフラー）が好調。",
}


def store_reports():
    for m in range(6, 12):
        season = 1.4 if m == 11 else 1.0
        rows = [["店舗"] + CATS + ["合計"]]
        for s in STORES:
            vals = [int(random.uniform(1.8, 4.5) * 1_000_000 * season) for _ in CATS]
            rows.append([s] + [f"{v:,}" for v in vals] + [f"{sum(vals):,}"])
        build(DOCS / f"store_report_2026{m:02d}.pdf", f"実店舗 月次営業報告（2026年{m}月）",
              [f"発行日: 2026年{m + 1 if m < 12 else 1}月5日　作成: 店舗運営部", "機密区分: 社内限定"],
              [("h2", "1. 店舗別・カテゴリ別売上（税込、円）"),
               ("table", (rows, [26 * mm] + [27 * mm] * 4 + [32 * mm])),
               ("h2", "2. 今月の所感"),
               ("p", COMMENTS[m])])


# ---------------------------------------------------------------------------
# 属性定義
# ---------------------------------------------------------------------------
def attributes():
    rows = [
        # file_name, doc_type, title, department, audience, doc_date, product_category, product_ids
        ("faq_document.pdf", "FAQ", "よくあるご質問", "カスタマーサポート部", "社外公開", "2026-06-01", "", ""),
        ("operation_manual_w_images.pdf", "マニュアル", "カスタマーサポート運営マニュアル", "カスタマーサポート部", "社内限定", "2026-06-01", "", ""),
        ("quality_report_202610.pdf", "品質報告", "品質不具合 調査報告書（第1報）", "品質管理部", "社内限定", "2026-10-15", "照明", DEFECT_PRODUCT_ID),
        ("quality_report_202611.pdf", "品質報告", "品質不具合 調査報告書（第2報）", "品質管理部", "社内限定", "2026-11-20", "照明", DEFECT_PRODUCT_ID),
        ("cs_notice_202610.pdf", "通達", f"{DEFECT_PRODUCT} 特例対応通達", "カスタマーサポート部", "社内限定", "2026-10-16", "照明", DEFECT_PRODUCT_ID),
    ]
    for m in range(6, 12):
        rows.append((f"store_report_2026{m:02d}.pdf", "店舗報告", f"実店舗 月次営業報告（2026年{m}月）", "店舗運営部", "社内限定",
                     f"2026-{m + 1:02d}-05", "", ""))
    with open(ROOT / "tools" / "expected_document_attributes.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["file_name", "doc_type", "title", "department", "audience", "doc_date", "product_category", "product_ids"])
        w.writerows(rows)


if __name__ == "__main__":
    quality_reports()
    cs_notice()
    store_reports()
    attributes()
    print("【完了】PDF と属性の正解を生成しました:", sorted(p.name for p in DOCS.glob("*.pdf")))
