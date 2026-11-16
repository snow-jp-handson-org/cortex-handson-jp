"""
既存の FAQ / 運営マニュアル PDF の日付を 2026年基準に置き換える（レイアウトはそのまま、該当文字列だけ差し替え）
- AI_EXTRACT で PDF 冒頭から発行日を取り出す際に、2026年のストーリーと矛盾しないようにするため
- 元ファイルは data/*.pdf を上書きする（元の版は git 履歴に残る）

実行: uv run --with pymupdf python tools/update_pdf_dates.py
"""
from pathlib import Path

import fitz  # PyMuPDF

DATA = Path(__file__).resolve().parent.parent / "data"

# (ページ番号(1始まり), 置換前, 置換後)
REPLACEMENTS = {
    "faq_document.pdf": [
        (1, "2024年12月1日", "2026年6月1日"),
        # 例示の行は日付が3つ並ぶため、行ごと差し替える（個別に置き換えると文字が重なる）
        (27, "例：2024年12月1日のランク判定 → 2023年12月1日〜2024年11月30日の購入金額",
             "例：2026年6月1日のランク判定 → 2025年6月1日〜2026年5月31日の購入金額"),
    ],
    "operation_manual_w_images.pdf": [
        (1, "CS-OPM-2024-001", "CS-OPM-2026-001"),
        (1, "2024年12月1日", "2026年6月1日"),
        (1, "2024-12-01", "2026-06-01"),
        (1, "2024-10-15", "2026-04-15"),
        (1, "2024-08-01", "2026-02-01"),
        (1, "2024-06-01", "2025-12-01"),
        (1, "2024-04-01", "2025-10-01"),
        (1, "2024-01-15", "2025-07-15"),
        (1, "2023-10-01", "2025-04-01"),
        (20, "ORD-2024-", "ORD-2026-"),
    ],
}


def main():
    for name, reps in REPLACEMENTS.items():
        path = DATA / name
        doc = fitz.open(path)
        inserts = []
        for page_no, old, new in reps:
            page = doc[page_no - 1]
            rects = page.search_for(old)
            assert rects, f"{name} p{page_no}: 「{old}」が見つからない"
            for r in rects:
                page.add_redact_annot(r, fill=(1, 1, 1))
                inserts.append((page, r, new))
        # 先にすべて塗りつぶしてから、元の位置に新しい文字を書き込む
        for page in doc:
            page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)
        for page, r, new in inserts:
            # 英数字だけなら半角フォント、和文を含むなら日本語フォント。大きさは周りの本文にそろえる
            font = "helv" if new.isascii() else "japan"
            page.insert_text((r.x0, r.y1 - 2), new, fontname=font, fontsize=10.0)
        tmp = path.with_suffix(".tmp.pdf")
        doc.save(tmp, garbage=3, deflate=True)
        doc.close()
        tmp.replace(path)
        print(f"【完了】{name} の日付を更新しました（{len(reps)} 箇所）")


if __name__ == "__main__":
    main()
